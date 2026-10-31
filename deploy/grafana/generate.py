#!/usr/bin/env python3
"""Generates qsrv's Grafana dashboards (Grafana 11+ JSON model).

    python3 deploy/grafana/generate.py deploy/grafana/dashboards

Edit the panels here and regenerate, rather than editing the JSON: queries,
units, thresholds and colours stay consistent across dashboards. CI
(.github/workflows/monitoring.yml) runs every query against a live stack.
Standard library only.
"""
import json
import sys

DS = {"type": "prometheus", "uid": "${datasource}"}
SEL = 'job=~"$job", instance=~"$instance"'
QSEL = SEL + ', queue=~"$queue"'


class Grid:
    """Lays panels out left to right, wrapping at 24 columns."""

    def __init__(self):
        self.x = self.y = self.rowh = 0
        self.next_id = 1

    def place(self, w, h):
        if self.x + w > 24:
            self.x, self.y, self.rowh = 0, self.y + self.rowh, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.rowh = max(self.rowh, h)
        return pos

    def newline(self):
        if self.x:
            self.x, self.y, self.rowh = 0, self.y + self.rowh, 0

    def id(self):
        i = self.next_id
        self.next_id += 1
        return i


def target(expr, legend="", ref="A", instant=False, fmt="time_series", interval=""):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend or "__auto", "refId": ref, "range": not instant, "instant": instant, "format": fmt}
    if interval:
        t["interval"] = interval
    return t


def thresholds(*steps):
    """steps: (value or None, color)"""
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}


def row(g, title, collapsed=False):
    g.newline()
    p = {"type": "row", "title": title, "id": g.id(), "collapsed": collapsed, "panels": [], "gridPos": {"x": 0, "y": g.y, "w": 24, "h": 1}}
    g.y += 1
    return p


def stat(g, title, targets, unit="short", w=4, h=4, th=None, desc="", mappings=None, text_mode="auto", color_mode="value", reduce="lastNotNull", decimals=None, graph=True, links=None):
    fd = {"unit": unit, "thresholds": th or thresholds((None, "text")), "color": {"mode": "thresholds"}, "mappings": mappings or []}
    if decimals is not None:
        fd["decimals"] = decimals
    if links:
        fd["links"] = links
    return {
        "type": "stat", "title": title, "description": desc, "id": g.id(), "gridPos": g.place(w, h), "datasource": DS,
        "targets": targets,
        "fieldConfig": {"defaults": fd, "overrides": []},
        "options": {"reduceOptions": {"calcs": [reduce], "fields": "", "values": False}, "orientation": "auto",
                    "textMode": text_mode, "colorMode": color_mode, "graphMode": "area" if graph else "none", "justifyMode": "auto",
                    "wideLayout": True, "showPercentChange": False},
    }


def ts(g, title, targets, unit="short", w=12, h=8, desc="", stack=False, th=None, show_th=False, bars=False, min0=True, overrides=None, legend_calcs=None, decimals=None):
    custom = {"drawStyle": "bars" if bars else "line", "lineWidth": 1 if bars else 2, "fillOpacity": 70 if bars else (35 if stack else 8),
              "showPoints": "never", "spanNulls": False, "gradientMode": "none",
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "thresholdsStyle": {"mode": "line+area" if show_th else "off"}, "axisSoftMin": 0 if min0 else None}
    fd = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}, "thresholds": th or thresholds((None, "green"))}
    if decimals is not None:
        fd["decimals"] = decimals
    return {
        "type": "timeseries", "title": title, "description": desc, "id": g.id(), "gridPos": g.place(w, h), "datasource": DS,
        "targets": targets,
        "fieldConfig": {"defaults": fd, "overrides": overrides or []},
        "options": {"legend": {"displayMode": "table" if legend_calcs else "list", "placement": "bottom", "showLegend": True, "calcs": legend_calcs or []},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def color_override(name, color):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}


def text(g, title, content, w=24, h=8):
    return {"type": "text", "title": title, "id": g.id(), "gridPos": g.place(w, h), "options": {"mode": "markdown", "content": content}}


STATE_COLORS = [color_override("ready", "#0b7a75"), color_override("delayed", "#e0a020"), color_override("in_flight", "#6f62f5"), color_override("waiting", "#8b96a5")]
AGE_TH = thresholds((None, "green"), (300, "#e0a020"), (3600, "red"))

# --- variables -----------------------------------------------------------------


def var_query(name, label, query, regex="", multi=True, include_all=True, all_value=None, current_all=True, hide=0, desc=""):
    v = {"name": name, "label": label, "type": "query", "datasource": DS, "query": {"query": query, "refId": "PrometheusVariableQueryEditor-VariableQuery", "qryType": 1},
         "definition": query, "regex": regex, "multi": multi, "includeAll": include_all, "refresh": 2, "sort": 1, "hide": hide, "description": desc,
         "current": {"text": ["All"], "value": ["$__all"]} if current_all else {}, "options": []}
    if all_value is not None:
        v["allValue"] = all_value
    return v


def variables(queue_multi=True):
    return [
        {"name": "datasource", "label": "Prometheus", "type": "datasource", "query": "prometheus", "current": {}, "hide": 0, "refresh": 1, "regex": "", "options": []},
        var_query("job", "Job", "label_values(qsrv_build_info, job)", multi=False, include_all=False, current_all=False),
        var_query("instance", "Instance", 'label_values(qsrv_build_info{job=~"$job"}, instance)', all_value=".+"),
        var_query("tenant", "Tenant", 'label_values(qsrv_queue_info{job=~"$job", instance=~"$instance"}, queue)', regex="/^([^/]+)\\//", all_value=".+",
                  desc="The part of queue names before the slash."),
        var_query("queue", "Queue", 'label_values(qsrv_queue_info{job=~"$job", instance=~"$instance", queue=~"($tenant)/.+"}, queue)', multi=queue_multi,
                  include_all=queue_multi, all_value=".+", current_all=queue_multi),
    ]


def alert_annotations():
    return {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
        {"datasource": DS, "enable": True, "iconColor": "red", "name": "qsrv alerts firing",
         "expr": 'ALERTS{alertname=~"Qsrv.*", alertstate="firing"}', "step": "60s",
         "titleFormat": "{{alertname}}", "textFormat": "{{queue}}{{instance}} severity={{severity}}", "tagKeys": "severity,alertname", "useValueForTime": False},
    ]}


def dashboard(uid, title, desc, panels, queue_multi=True, links_to=()):
    return {
        "uid": uid, "title": title, "description": desc, "tags": ["qsrv"], "timezone": "browser", "editable": True,
        "graphTooltip": 1, "refresh": "30s", "time": {"from": "now-6h", "to": "now"}, "schemaVersion": 39, "version": 1,
        "fiscalYearStartMonth": 0, "liveNow": False, "weekStart": "",
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m", "15m"]},
        "templating": {"list": variables(queue_multi)},
        "annotations": alert_annotations(),
        "links": [{"title": "qsrv dashboards", "type": "dashboards", "tags": ["qsrv"], "asDropdown": True, "includeVars": True, "keepTime": True, "icon": "external link"}],
        "panels": panels,
    }


# --- overview -------------------------------------------------------------------


def overview():
    g, P = Grid(), []
    P.append(row(g, "Health"))
    P.append(stat(g, "Up", [target(f'min(up{{{SEL}}})', instant=True)], w=3, graph=False,
                  mappings=[{"type": "value", "options": {"0": {"text": "Down", "color": "red"}, "1": {"text": "Up", "color": "green"}}}],
                  th=thresholds((None, "red"), (1, "green")), color_mode="background", desc="Prometheus can scrape qsrv."))
    P.append(stat(g, "Uptime", [target(f'min(time() - qsrv_start_time_seconds{{{SEL}}})', instant=True)], unit="s", w=3, graph=False, decimals=0))
    P.append(stat(g, "Version", [target(f'qsrv_build_info{{{SEL}}}', legend="{{version}}", instant=True)], w=4, graph=False, text_mode="name",
                  desc="qsrv version, SQLite driver (cgo or WASM) and Go version: hover the value."))
    P.append(stat(g, "Alerts firing", [target('count(ALERTS{alertname=~"Qsrv.*", alertstate="firing"}) or vector(0)', instant=True)], w=3, graph=False,
                  th=thresholds((None, "green"), (1, "red")), color_mode="background",
                  links=[{"title": "Alerts", "url": "/d/qsrv-alerts?${__url_time_range}&${__all_variables}"}],
                  desc="qsrv alerts firing now (from deploy/prometheus/alerts.yml)."))
    P.append(stat(g, "Waiting", [target(f'sum(qsrv_queue_messages{{{QSEL}, state!="in_flight"}})')], w=4,
                  desc="Messages in the selected queues not being processed: ready, delayed, or waiting behind their ordering group."))
    P.append(stat(g, "Oldest message", [target(f'max(qsrv_queue_oldest_message_age_seconds{{{QSEL}}} unless on (queue) qsrv:dlq:is_dlq) or vector(0)')],
                  unit="s", w=4, th=AGE_TH, decimals=0,
                  desc="Longest any message has waited, over the selected queues, dead letter queues excluded. Above 5m (amber) or 1h (red), something is not being consumed."))
    P.append(stat(g, "Disk free", [target(f'min(qsrv_disk_free_bytes{{{SEL}}} / qsrv_disk_size_bytes{{{SEL}}})')], unit="percentunit", w=3, decimals=1,
                  th=thresholds((None, "red"), (0.05, "#e0a020"), (0.15, "green"))))

    P.append(row(g, "Queues"))
    P.append(ts(g, "Oldest message age", [target(f'qsrv_queue_oldest_message_age_seconds{{{QSEL}}} unless on (queue) qsrv:dlq:is_dlq', "{{queue}}")],
                unit="s", th=AGE_TH, show_th=True, legend_calcs=["lastNotNull", "max"],
                desc="Per queue (dead letter queues excluded). The most useful signal: a line that only goes up is a queue nobody consumes, or a group blocked by a failing message."))
    P.append(ts(g, "Messages by queue", [target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}}})', "{{queue}}")], stack=True, legend_calcs=["lastNotNull", "max"],
                desc="Live messages (ready, delayed, in flight, waiting) per queue."))
    P.append(ts(g, "Messages by state", [target(f'sum by (state) (qsrv_queue_messages{{{QSEL}}})', "{{state}}")], stack=True, overrides=STATE_COLORS,
                desc="ready: deliverable now. delayed: scheduled or backing off after a failure. in_flight: leased to a consumer. waiting: behind the head of its ordering group."))
    P.append(ts(g, "Throughput", [target(f'sum by (event) (rate(qsrv_messages_total{{{QSEL}, event=~"sent|delivered|acked"}}[$__rate_interval]))', "{{event}}")],
                unit="ops", desc="Messages per second sent, delivered (including redeliveries) and acked."))
    P.append(ts(g, "Failures", [target(f'sum by (event) (increase(qsrv_messages_total{{{QSEL}, event=~"nacked|expired|dead_lettered|dropped"}}[$__interval]))', "{{event}}", interval="1m")],
                bars=True, stack=True,
                overrides=[color_override("dead_lettered", "red"), color_override("dropped", "dark-red"), color_override("expired", "#e0a020"), color_override("nacked", "#8b96a5")],
                desc="nacked: a consumer reported a failure. expired: a lease ran out (consumer crashed or too slow). dead_lettered: retries exhausted, moved to the DLQ. dropped: retries exhausted, no DLQ."))
    P.append(ts(g, "Dead letter queues", [target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}}}) and on (queue) qsrv:dlq:is_dlq', "{{queue}}")],
                legend_calcs=["lastNotNull"], desc="Messages waiting in dead letter queues: failures someone has to look at (admin UI, or qctl replay). Needs the recording rule in alerts.yml."))

    # Per-queue table: current values, one row per queue.
    tbl_targets = [
        target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}, state="ready"}})', ref="A", instant=True, fmt="table"),
        target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}, state="delayed"}})', ref="B", instant=True, fmt="table"),
        target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}, state="in_flight"}})', ref="C", instant=True, fmt="table"),
        target(f'sum by (queue) (qsrv_queue_messages{{{QSEL}, state="waiting"}})', ref="D", instant=True, fmt="table"),
        target(f'max by (queue) (qsrv_queue_oldest_message_age_seconds{{{QSEL}}})', ref="E", instant=True, fmt="table"),
        target(f'sum by (queue) (rate(qsrv_messages_total{{{QSEL}, event="acked"}}[5m]))', ref="F", instant=True, fmt="table"),
        target(f'sum by (queue) (increase(qsrv_messages_total{{{QSEL}, event=~"dead_lettered|dropped"}}[1h]))', ref="G", instant=True, fmt="table"),
    ]
    names = {"Value #A": "Ready", "Value #B": "Delayed", "Value #C": "In flight", "Value #D": "Waiting in group",
             "Value #E": "Oldest", "Value #F": "Acked/s (5m)", "Value #G": "Failed (1h)", "queue": "Queue"}
    g.newline()
    P.append({
        "type": "table", "title": "Queues now", "id": g.id(), "gridPos": g.place(24, 10), "datasource": DS, "targets": tbl_targets,
        "description": "Current state of every selected queue. Click a queue for its own dashboard.",
        "transformations": [
            {"id": "merge", "options": {}},
            {"id": "organize", "options": {"excludeByName": {"Time": True}, "renameByName": names,
                                            "indexByName": {"queue": 0, "Value #A": 1, "Value #D": 2, "Value #B": 3, "Value #C": 4, "Value #E": 5, "Value #F": 6, "Value #G": 7}}},
        ],
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}, "noValue": "0"},
                        "overrides": [
                            {"matcher": {"id": "byName", "options": "Queue"}, "properties": [
                                {"id": "links", "value": [{"title": "Queue dashboard", "url": "/d/qsrv-queue?var-queue=${__value.raw}&${datasource:queryparam}&${job:queryparam}&${instance:queryparam}&${__url_time_range}"}]}]},
                            {"matcher": {"id": "byName", "options": "Oldest"}, "properties": [
                                {"id": "unit", "value": "s"}, {"id": "decimals", "value": 0}, {"id": "thresholds", "value": AGE_TH},
                                {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
                            {"matcher": {"id": "byName", "options": "Acked/s (5m)"}, "properties": [{"id": "decimals", "value": 2}]},
                            {"matcher": {"id": "byName", "options": "Failed (1h)"}, "properties": [
                                {"id": "decimals", "value": 0}, {"id": "thresholds", "value": thresholds((None, "text"), (1, "red"))},
                                {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
                        ]},
        "options": {"showHeader": True, "cellHeight": "sm", "sortBy": [{"displayName": "Oldest", "desc": True}], "footer": {"show": False}},
    })

    P.append(row(g, "Storage"))
    P.append(ts(g, "Commit latency", [
        target(f'histogram_quantile(0.5, sum by (le) (rate(qsrv_store_commit_duration_seconds_bucket{{{SEL}}}[$__rate_interval])))', "p50", "A"),
        target(f'histogram_quantile(0.99, sum by (le) (rate(qsrv_store_commit_duration_seconds_bucket{{{SEL}}}[$__rate_interval])))', "p99", "B")],
        unit="s", w=8, desc="Transaction + fsync. Every send and ack waits for it: this is the disk's health. A few ms on SSD; tens of ms on network disks."))
    P.append(ts(g, "Group commit", [
        target(f'sum(rate(qsrv_store_commit_duration_seconds_count{{{SEL}}}[$__rate_interval]))', "commits/s", "A"),
        target(f'sum(rate(qsrv_store_commit_batch_ops_sum{{{SEL}}}[$__rate_interval])) / sum(rate(qsrv_store_commit_batch_ops_count{{{SEL}}}[$__rate_interval]))', "ops per commit", "B")],
        w=8, desc="Operations share transactions and fsyncs when they arrive together. More ops per commit = the disk is busy and batching works.",
        overrides=[{"matcher": {"id": "byName", "options": "ops per commit"}, "properties": [{"id": "custom.axisPlacement", "value": "right"}]}]))
    P.append(ts(g, "Writes waiting for the disk", [target(f'sum(qsrv_store_pending_writes{{{SEL}}})', "pending")], w=8,
                th=thresholds((None, "green"), (500, "red")), show_th=True,
                desc="Operations queued for the writer. Sustained above 0 means the disk can't keep up."))
    P.append(ts(g, "Disk", [
        target(f'qsrv_disk_free_bytes{{{SEL}}}', "free {{instance}}", "A"),
        target(f'predict_linear(qsrv_disk_free_bytes{{{SEL}}}[6h], 3 * 24 * 3600)', "free in 3 days at this rate", "B")],
        unit="bytes", w=8, desc="Free space on the data dir's filesystem, and where it is heading. Below -min-free-disk qsrv refuses new messages."))
    P.append(ts(g, "Database files", [target(f'qsrv_db_size_bytes{{{SEL}}}', "{{file}}")], unit="bytes", w=8,
                desc="qsrv.db (live messages + history) and its WAL. A growing WAL means checkpoints are blocked by a long reader."))
    P.append(ts(g, "Refusing sends (disk low)", [target(f'max(qsrv_storage_low{{{SEL}}})', "storage low")], w=8, min0=True,
                th=thresholds((None, "green"), (1, "red")), show_th=True,
                desc="1 while free disk is below -min-free-disk: sends get 503 storage_full, consumers keep draining."))

    P.append(row(g, "Backups"))
    P.append(stat(g, "Last snapshot", [target(f'min(time() - qsrv_backup_last_success_timestamp_seconds{{{SEL}}} > 0)', instant=True)], unit="s", w=6, graph=False, decimals=0,
                  desc="How long ago the last complete snapshot was taken. No data: snapshots are off (-backup-dir) or none finished yet."))
    P.append(stat(g, "Snapshot interval", [target(f'max(qsrv_backup_interval_seconds{{{SEL}}})', instant=True)], unit="s", w=6, graph=False))
    P.append(stat(g, "Last snapshot size", [target(f'max(qsrv_backup_last_size_bytes{{{SEL}}})')], unit="bytes", w=6))
    P.append(stat(g, "Snapshot failures since start", [target(f'max(qsrv_backup_failures{{{SEL}}})', instant=True)], w=6, graph=False,
                  th=thresholds((None, "green"), (1, "red"))))

    P.append(row(g, "HTTP API"))
    P.append(ts(g, "Requests by route", [target(f'sum by (route) (rate(qsrv_http_requests_total{{{SEL}}}[$__rate_interval]))', "{{route}}")], unit="reqps", w=12))
    P.append(ts(g, "Errors", [
        target(f'sum by (code) (rate(qsrv_http_requests_total{{{SEL}, code=~"429|5.."}}[$__rate_interval]))', "{{code}}")],
        unit="reqps", w=12, bars=False,
        desc="429: a queue is full (max_messages). 503: shutting down or storage_full. 500: internal error, check the logs."))
    P.append(ts(g, "Latency p99 by route", [
        target(f'histogram_quantile(0.99, sum by (le, route) (rate(qsrv_http_request_duration_seconds_bucket{{{SEL}, route!~".*/receive"}}[$__rate_interval])))', "{{route}}")],
        unit="s", w=24, desc="Receive is left out: with long polling it waits on purpose."))

    P.append(row(g, "Process"))
    P.append(ts(g, "Memory", [
        target(f'qsrv_process_resident_memory_bytes{{{SEL}}}', "resident {{instance}}", "A"),
        target(f'qsrv_go_heap_inuse_bytes{{{SEL}}}', "Go heap {{instance}}", "B")], unit="bytes", w=12,
        desc="Grows with live messages (~200 B each), not with history or body size."))
    P.append(ts(g, "Goroutines", [target(f'qsrv_go_goroutines{{{SEL}}}', "{{instance}}")], w=12, desc="Roughly one per open connection or long poll."))
    return dashboard("qsrv-overview", "qsrv / Overview", "Health, queues, storage, backups and HTTP of qsrv.", P)


# --- one queue ------------------------------------------------------------------


def queue_dash():
    g, P = Grid(), []
    one = 'job=~"$job", instance=~"$instance", queue="$queue"'
    P.append(row(g, "Now"))
    for st, color in (("ready", "#0b7a75"), ("waiting", "#8b96a5"), ("delayed", "#e0a020"), ("in_flight", "#6f62f5")):
        P.append(stat(g, {"in_flight": "In flight", "waiting": "Waiting in group"}.get(st, st.capitalize()),
                      [target(f'sum(qsrv_queue_messages{{{one}, state="{st}"}})')], w=4, th=thresholds((None, color))))
    P.append(stat(g, "Oldest message", [target(f'max(qsrv_queue_oldest_message_age_seconds{{{one}}}) or vector(0)')], unit="s", w=4, th=AGE_TH, decimals=0))
    P.append(stat(g, "Capacity used", [target(f'sum(qsrv_queue_messages{{{one}}}) / max(qsrv_queue_max_messages{{{one}}})')], unit="percentunit", w=4,
                  th=thresholds((None, "green"), (0.8, "#e0a020"), (0.95, "red")), decimals=0,
                  desc="Live messages over max_messages. No data: the queue has no limit."))
    P.append(stat(g, "Dead letter queue", [target(f'qsrv_queue_info{{{one}, dead_letter_queue!=""}}', legend="{{dead_letter_queue}}", instant=True)], w=8, graph=False,
                  text_mode="name", desc="Where messages go when retries run out. No data: none, they are dropped."))
    P.append(stat(g, "Retries", [target(f'qsrv_queue_info{{{one}}}', legend="{{max_attempts}} attempts, ordering {{ordering}}", instant=True)], w=8, graph=False, text_mode="name",
                  desc="max_attempts: deliveries before giving up (0 = forever, 1 = no retries)."))
    P.append(stat(g, "Failed in the last hour", [target(f'sum(increase(qsrv_messages_total{{{one}, event=~"dead_lettered|dropped"}}[1h])) or vector(0)', instant=True)], w=8, graph=False, decimals=0,
                  th=thresholds((None, "green"), (1, "red"))))

    P.append(row(g, "Over time"))
    P.append(ts(g, "Messages by state", [target(f'sum by (state) (qsrv_queue_messages{{{one}}})', "{{state}}")], stack=True, overrides=STATE_COLORS))
    P.append(ts(g, "Oldest message age", [target(f'max(qsrv_queue_oldest_message_age_seconds{{{one}}})', "oldest")], unit="s", th=AGE_TH, show_th=True))
    P.append(ts(g, "Throughput", [target(f'sum by (event) (rate(qsrv_messages_total{{{one}, event=~"sent|dead_letter_in|replay_in|delivered|acked"}}[$__rate_interval]))', "{{event}}")],
                unit="ops", desc="In: sent, dead_letter_in (arriving from queues that use this one as DLQ), replay_in. Out: delivered, acked."))
    P.append(ts(g, "Failures", [target(f'sum by (event) (increase(qsrv_messages_total{{{one}, event=~"nacked|expired|dead_lettered|dropped"}}[$__interval]))', "{{event}}", interval="1m")],
                bars=True, stack=True,
                overrides=[color_override("dead_lettered", "red"), color_override("dropped", "dark-red"), color_override("expired", "#e0a020"), color_override("nacked", "#8b96a5")]))
    P.append(ts(g, "Redelivery ratio", [target(
        f'sum(rate(qsrv_messages_total{{{one}, event="redelivered"}}[$__rate_interval])) / sum(rate(qsrv_messages_total{{{one}, event="delivered"}}[$__rate_interval]))', "redelivered / delivered")],
        unit="percentunit", th=thresholds((None, "green"), (0.2, "red")), show_th=True,
        desc="Share of deliveries that are retries. High: handlers failing, or leases expiring (visibility_timeout too short)."))
    P.append(ts(g, "Ordering groups", [target(f'max(qsrv_queue_groups{{{one}}})', "groups")],
                desc="Ordering groups with live messages: each delivers one message at a time."))
    return dashboard("qsrv-queue", "qsrv / Queue", "One queue in detail: state, throughput, failures, retries.", P, queue_multi=False)


# --- alerts ---------------------------------------------------------------------

RUNBOOK = """
| Alert | Means | Do |
|---|---|---|
| **QsrvDown** | Prometheus can't scrape qsrv | Check the process or container, the host, and `/healthz` |
| **QsrvRestartLoop** | More than 2 restarts in 30 minutes | Logs at startup: a storage error, a full disk or a bad flag |
| **QsrvHTTP5xx** | Internal errors | Logs, `unhandled error` lines |
| **QsrvMessagesTooOld** / **Critical** | A queue's oldest message waits over 10 min / 1 h | Are consumers running? A group blocked by a message that keeps failing? Admin UI → the queue |
| **QsrvNoConsumers** | Ready messages and nothing delivered in 10 min | Start or fix the consumers of that queue |
| **QsrvDeadLetters** | Messages ran out of retries | Look at them in the DLQ (admin UI), fix the cause, `qctl replay` |
| **QsrvDLQNotEmpty** | A dead letter queue holds messages | Same: inspect, fix, replay or delete |
| **QsrvHighRedeliveryRatio** | Many deliveries are retries | Handlers failing, or `visibility_timeout` shorter than the work |
| **QsrvLeaseExpiries** | Leases running out | Consumers crashing or too slow: raise `visibility_timeout` or extend leases |
| **QsrvQueueNearFull** | A queue is close to `max_messages` | Consumers are behind; sends will get 429 |
| **QsrvSendsRejected** | Sends getting 429 | A queue is full |
| **QsrvDiskSpaceLow** / **Critical** / **WillFill** | Disk filling up | Lower `retention`, grow the volume, check the WAL |
| **QsrvStorageFull** | qsrv refuses sends (below `-min-free-disk`) | Free space now; sends resume within seconds |
| **QsrvBackupStale** | No snapshot in two intervals | Logs `snapshot failed`: backup disk full or not writable |
| **QsrvSlowCommits** | fsync is slow | Disk saturated or degraded (EBS burst credits, noisy neighbour) |
| **QsrvWriterBacklog** | Writes queueing for the disk | Disk can't keep up with the load |
| **QsrvWALGrowing** | The WAL is not checkpointed | A long reader (big replay, a backup tool) holds it |
| **QsrvMemoryHigh** | Resident memory high | Many live messages: consumers are behind |

Thresholds are in `deploy/prometheus/alerts.yml` (marked `TUNE`).
"""


def alerts_dash():
    g, P = Grid(), []
    firing = 'ALERTS{alertname=~"Qsrv.*", alertstate="firing"}'
    pending = 'ALERTS{alertname=~"Qsrv.*", alertstate="pending"}'
    P.append(stat(g, "Firing", [target(f'count({firing}) or vector(0)', instant=True)], w=6, graph=False, th=thresholds((None, "green"), (1, "red")), color_mode="background"))
    P.append(stat(g, "Critical", [target(f'count({firing[:-1]}, severity="critical"}}) or vector(0)', instant=True)], w=6, graph=False,
                  th=thresholds((None, "green"), (1, "dark-red")), color_mode="background"))
    P.append(stat(g, "Pending", [target(f'count({pending}) or vector(0)', instant=True)], w=6, graph=False, th=thresholds((None, "text"), (1, "#e0a020")),
                  desc="Condition true, waiting for its 'for' duration before firing."))
    P.append(stat(g, "Fired in the selected range", [target(f'count(count_over_time({firing}[$__range])) or vector(0)', instant=True)], w=6, graph=False,
                  desc="Distinct alerts (name and labels) that fired at some point in the time range."))
    g.newline()
    P.append({
        "type": "table", "title": "Active now", "id": g.id(), "gridPos": g.place(24, 9), "datasource": DS,
        "targets": [target('ALERTS{alertname=~"Qsrv.*"}', instant=True, fmt="table")],
        "description": "Firing and pending qsrv alerts with their labels.",
        "transformations": [
            {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True, "job": True},
                                            "renameByName": {"alertname": "Alert", "alertstate": "State", "severity": "Severity", "queue": "Queue", "instance": "Instance", "route": "Route", "file": "File", "event": "Event"},
                                            "indexByName": {"alertname": 0, "alertstate": 1, "severity": 2, "queue": 3, "instance": 4}}},
        ],
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}, "noValue": "No qsrv alerts active"},
                        "overrides": [
                            {"matcher": {"id": "byName", "options": "State"}, "properties": [
                                {"id": "mappings", "value": [{"type": "value", "options": {"firing": {"color": "red", "text": "Firing"}, "pending": {"color": "#e0a020", "text": "Pending"}}}]},
                                {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
                            {"matcher": {"id": "byName", "options": "Severity"}, "properties": [
                                {"id": "mappings", "value": [{"type": "value", "options": {"critical": {"color": "dark-red", "text": "critical"}, "warning": {"color": "#e0a020", "text": "warning"}}}]},
                                {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
                        ]},
        "options": {"showHeader": True, "cellHeight": "sm", "sortBy": [{"displayName": "Severity", "desc": False}], "footer": {"show": False}},
    })
    g.newline()
    P.append({
        "type": "state-timeline", "title": "History", "id": g.id(), "gridPos": g.place(24, 10), "datasource": DS,
        "targets": [target(f'max by (alertname, queue, instance) ({firing})', "{{alertname}} {{queue}}")],
        "description": "When each alert was firing over the time range.",
        "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "red"}, "custom": {"fillOpacity": 80, "lineWidth": 0},
                                     "mappings": [{"type": "value", "options": {"1": {"text": "firing", "color": "red"}}}], "noValue": "Nothing fired in this range"},
                        "overrides": []},
        "options": {"showValue": "never", "mergeValues": True, "alignValue": "left", "rowHeight": 0.8, "legend": {"showLegend": False}},
    })
    g.newline()
    P.append(text(g, "What each alert means", RUNBOOK.strip(), h=16))
    return dashboard("qsrv-alerts", "qsrv / Alerts", "qsrv alerts: firing now, history, and what to do.", P)


if __name__ == "__main__":
    out = sys.argv[1]
    for name, d in (("qsrv-overview.json", overview()), ("qsrv-queue.json", queue_dash()), ("qsrv-alerts.json", alerts_dash())):
        with open(f"{out}/{name}", "w") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)
            f.write("\n")
        print(name, len(d["panels"]), "panels")
