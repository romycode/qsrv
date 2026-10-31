#!/usr/bin/env bash
# Traffic for the monitoring check: every kind of event the dashboards and
# alerts look at (sends, acks, groups, delays, dead letters, nacks, lease
# expiries, a full queue). Needs QSRV_ADDR, QSRV_API_KEY and qctl in PATH.
set -euo pipefail
q() { qctl "$@" >/dev/null; }

# setup: the queues, so Prometheus scrapes their counters at 0 before any
# event (as in production); traffic: the events.
case "${1:-}" in
setup)
  q create orders-dlq
  q create -max-attempts 2 -backoff 0s -dlq orders-dlq orders
  q create -visibility 1s -backoff 0s -max-attempts 10 flaky
  q create -max-messages 5 tiny
  ;;
traffic)
  for i in $(seq 1 30); do echo "{\"order\": $i}" | q send -group "customer-$((i % 4))" orders; done
  for i in $(seq 1 5); do echo "later $i" | q send -delay 1h orders; done
  for i in $(seq 1 5); do echo "x" | q send tiny; done
  echo "x" | qctl send tiny >/dev/null 2>&1 || true            # 429: queue full
  for i in $(seq 1 10); do qctl recv -ack orders >/dev/null || true; done
  for i in 1 2 3 4; do                                          # nack twice -> dead letter
    l=$(qctl recv orders | sed -n 's/.*"lease_id": *"\([^"]*\)".*/\1/p') && [ -n "$l" ] && qctl nack orders "$l" >/dev/null || true
  done
  for i in 1 2 3; do echo "job $i" | q send flaky; done
  qctl recv flaky >/dev/null; qctl recv flaky >/dev/null        # leases expire -> expired, redelivered
  sleep 2
  qctl recv -wait 2s flaky >/dev/null || true
  ;;
*)
  echo "usage: $0 setup|traffic" >&2; exit 2 ;;
esac
echo "load $1 done"
