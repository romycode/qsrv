#!/usr/bin/env python3
"""Runs every query of the qsrv dashboards through a live Grafana.

check_dashboards.py GRAFANA_URL DASHBOARD.json...

Fails on: a dashboard not provisioned, a query Grafana or Prometheus
rejects, a template variable without values. Lists panels without data
(expected for events the CI load doesn't produce, e.g. HTTP 500s).
Writes a Markdown summary to $GITHUB_STEP_SUMMARY when set.
"""
import base64
import json
import os
import re
import sys
import urllib.request

grafana, *paths = sys.argv[1:]
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()
DS = {"type": "prometheus", "uid": "prometheus"}
VARS = {"job": "qsrv", "instance": ".+", "tenant": ".+", "queue": ".+"}
ONE_QUEUE = "ci/orders"  # the qsrv-queue dashboard looks at one queue


def call(method, path, body=None):
    req = urllib.request.Request(grafana + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    try:
        return status, json.loads(raw or b"{}")
    except ValueError:
        return status, {"message": raw[:300].decode(errors="replace")}


def crash(kind, value, tb):
    import traceback
    msg = "".join(traceback.format_exception(kind, value, tb)).replace("\n", " | ")
    print("::error title=check_dashboards crashed::" + msg)
    sys.exit(1)


sys.excepthook = crash


def substitute(expr, uid):
    expr = expr.replace('queue="$queue"', f'queue="{ONE_QUEUE}"') if uid == "qsrv-queue" else expr
    for k, v in VARS.items():
        expr = re.sub(r"\$\{" + k + r"(:[a-z]+)?\}|\$" + k + r"\b", v, expr)
    return expr


failures, empty, ok = [], [], 0
status, found = call("GET", "/api/search?tag=qsrv")
if status != 200 or not isinstance(found, list):
    print(f"::error title=Grafana search failed::{status} {found}")
    sys.exit(1)
provisioned = {d["uid"] for d in found}
print("provisioned:", sorted(provisioned))

for path in paths:
    local = json.load(open(path))
    uid = local["uid"]
    if uid not in provisioned:
        failures.append(f"{uid}: not provisioned in Grafana")
        continue
    status, remote = call("GET", f"/api/dashboards/uid/{uid}")
    if status != 200:
        failures.append(f"{uid}: GET dashboard {status}")
        continue
    d = remote["dashboard"]

    for v in d["templating"]["list"]:
        if v["type"] != "query":
            continue
        q = substitute(v["query"]["query"] if isinstance(v["query"], dict) else v["query"], uid)
        m = re.fullmatch(r"label_values\((.*),\s*(\w+)\)", q)
        status, res = call("GET", f"/api/datasources/proxy/uid/prometheus/api/v1/label/{m.group(2)}/values?match[]=" + urllib.request.quote(m.group(1)))
        values = res.get("data", []) if status == 200 else []
        if v.get("regex"):
            rx = re.compile(v["regex"].strip("/"))
            values = sorted({mm.group(1) for x in values if (mm := rx.search(x))})
        if not values:
            failures.append(f"{uid}: variable ${v['name']} has no values ({status}: {q})")

    for p in d["panels"]:
        for t in p.get("targets", []):
            expr = substitute(t["expr"], uid)
            body = {"from": "now-15m", "to": "now", "queries": [{
                "refId": t["refId"], "datasource": DS, "expr": expr, "instant": t.get("instant", False),
                "range": t.get("range", True), "format": t.get("format", "time_series"), "intervalMs": 15000, "maxDataPoints": 300,
                **({"interval": t["interval"]} if t.get("interval") else {})}]}
            status, res = call("POST", "/api/ds/query", body)
            r = res.get("results", {}).get(t["refId"], {})
            if status != 200 or r.get("error"):
                failures.append(f"{uid} / {p['title']} [{t['refId']}]: {status} {r.get('error') or res.get('message')}\n      {expr}")
                continue
            frames = r.get("frames", [])
            has_data = any(len(f.get("data", {}).get("values", [[]])[-1] or []) > 0 for f in frames)
            if has_data:
                ok += 1
            else:
                empty.append(f"{uid} / {p['title']} [{t['refId']}]")

lines = [f"**Grafana dashboards**: {ok} queries with data, {len(empty)} without data, {len(failures)} failed."]
if empty:
    lines += ["", "<details><summary>Without data (expected for events the CI load doesn't produce)</summary>", ""] + [f"- {e}" for e in empty] + ["</details>"]
if failures:
    lines += ["", "Failed:"] + [f"- {f}" for f in failures]
report = "\n".join(lines)
print(report)
# Annotations: visible on the PR and through the API, unlike the log.
for f in failures:
    print("::error title=Dashboard query failed::" + f.replace("\n", " ").replace("%", "%25"))
for e in empty[:20]:
    print("::warning title=Panel without data::" + e)
if os.environ.get("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
        f.write(report + "\n")
sys.exit(1 if failures else 0)
