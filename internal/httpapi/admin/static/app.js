// Alpine components of the qsrv admin. Data comes from /admin/api/v1/...
// (the API's GET endpoints, authenticated by the session cookie) and is
// only ever shown with x-text, never as HTML.

const API = "/admin/api";

async function getJSON(path) {
  const res = await fetch(API + path, { credentials: "same-origin", headers: { Accept: "application/json" } });
  if (res.status === 401) {
    window.location.assign("/admin/login");
    throw new Error("Your session ended. Sign in again.");
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "qsrv answered " + res.status + ".");
  return data;
}

const queuePath = (name) => "/v1/queues/" + encodeURIComponent(name);

const STATE_LABELS = {
  ready: "Ready",
  waiting: "Waiting in group",
  delayed: "Delayed",
  in_flight: "In flight",
  acked: "Acked",
  dropped: "Dropped",
  dead_lettered: "Dead lettered",
};

// Shared formatting, mixed into every component.
const fmt = {
  queueURL(name) {
    return "/admin/queue?name=" + encodeURIComponent(name);
  },
  fmtNum(n) {
    return n === undefined || n === null ? "–" : Number(n).toLocaleString();
  },
  fmtBytes(n) {
    if (n === undefined || n === null) return "";
    if (n < 1024) return n + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(n < 10240 ? 1 : 0) + " KB";
    return (n / 1024 / 1024).toFixed(1) + " MB";
  },
  fmtTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" });
  },
  // fmtDur turns a Go duration ("2160h0m0s", "1m30s", "500ms") into "90 days", "1m 30s".
  fmtDur(d) {
    if (!d) return "";
    let secs = 0;
    for (const [, n, unit] of d.matchAll(/([\d.]+)(h|ms|m|s|µs|us|ns)/g)) {
      secs += Number(n) * ({ h: 3600, m: 60, s: 1, ms: 1e-3, "µs": 1e-6, us: 1e-6, ns: 1e-9 }[unit]);
    }
    if (secs === 0) return "0s";
    if (secs < 1) return Math.round(secs * 1000) + "ms";
    const days = Math.floor(secs / 86400), h = Math.floor((secs % 86400) / 3600), m = Math.floor((secs % 3600) / 60), s = Math.round(secs % 60);
    if (days > 0) return days + (days === 1 ? " day" : " days") + (h ? " " + h + "h" : "");
    return [h && h + "h", m && m + "m", s && s + "s"].filter(Boolean).join(" ");
  },
  clock(d) {
    return d ? d.toLocaleTimeString() : "";
  },
  // age: how long ago, in the largest sensible unit.
  age(iso) {
    if (!iso) return "";
    const ms = Date.now() - new Date(iso).getTime();
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return s + "s";
    const m = Math.floor(s / 60);
    if (m < 60) return m + "m";
    const h = Math.floor(m / 60);
    if (h < 48) return h + "h " + (m % 60) + "m";
    return Math.floor(h / 24) + "d";
  },
  ageSeconds(iso) {
    return iso ? (Date.now() - new Date(iso).getTime()) / 1000 : 0;
  },
  // ageClass flags old messages: something isn't being consumed.
  ageClass(q) {
    const s = this.ageSeconds(q.oldest_enqueued_at);
    if (s > 3600) return "age-bad";
    if (s > 300) return "age-warn";
    return "";
  },
  stateLabel(s) {
    return STATE_LABELS[s] || s;
  },
};

document.addEventListener("alpine:init", () => {
  Alpine.data("dashboard", () => ({
    ...fmt,
    ov: null,
    error: "",
    filter: "",
    admin: false,
    lastUpdate: null,
    timer: null,

    init() {
      this.admin = this.$el.dataset.admin === "true";
      this.load();
      this.timer = setInterval(() => { if (!document.hidden) this.load(); }, 3000);
    },
    destroy() {
      clearInterval(this.timer);
    },
    async load() {
      try {
        const ov = await getJSON("/v1/overview");
        this.ov = ov;
        this.error = "";
        this.lastUpdate = new Date();
      } catch (e) {
        this.error = "Couldn't load the queues: " + e.message;
      }
    },
    waiting(q) {
      return Math.max(0, q.messages - q.ready - q.delayed - q.in_flight);
    },
    deadLetters(q) {
      return q.is_dead_letter_queue_of ? q.messages : 0;
    },
    segStyle(q, n) {
      return { width: q.messages ? (n / q.messages) * 100 + "%" : "0" };
    },
    barLabel(q) {
      return `${q.ready} ready, ${q.delayed} delayed, ${q.in_flight} in flight, ${this.waiting(q)} waiting in a group`;
    },
    filtered() {
      if (!this.ov) return [];
      const f = this.filter.trim().toLowerCase();
      return f ? this.ov.queues.filter((q) => q.name.toLowerCase().includes(f)) : this.ov.queues;
    },
    groups() {
      if (!this.ov) return [];
      const qs = this.filtered();
      if (!this.admin) return this.ov.queues.length ? [{ tenant: "", queues: qs }] : [];
      const by = new Map();
      for (const q of this.ov.queues) {
        const i = q.name.indexOf("/");
        const t = i < 0 ? "" : q.name.slice(0, i);
        if (!by.has(t)) by.set(t, []);
      }
      for (const q of qs) {
        const i = q.name.indexOf("/");
        by.get(i < 0 ? "" : q.name.slice(0, i)).push(q);
      }
      return [...by.entries()]
        .filter(([, list]) => list.length > 0 || !this.filter)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([tenant, queues]) => ({ tenant, queues }));
    },
    topics() {
      return this.ov ? this.ov.topics : [];
    },
    summary() {
      if (!this.ov) return "";
      const qs = this.ov.queues;
      const waiting = qs.reduce((n, q) => n + q.ready + q.delayed + this.waiting(q), 0);
      const inflight = qs.reduce((n, q) => n + q.in_flight, 0);
      const dead = qs.filter((q) => this.deadLetters(q) > 0).length;
      let s = `${qs.length} ${qs.length === 1 ? "queue" : "queues"}, ${waiting.toLocaleString()} ${waiting === 1 ? "message" : "messages"} waiting and ${inflight.toLocaleString()} being processed.`;
      if (dead > 0) s += ` ${dead} dead letter ${dead === 1 ? "queue holds" : "queues hold"} failed messages.`;
      return s;
    },
  }));

  Alpine.data("queuePage", () => ({
    ...fmt,
    name: "",
    info: null,
    summary: null,
    error: "",
    view: "live",
    states: [],
    messages: [],
    next: 0,
    loaded: false,
    loading: false,
    selected: null,
    copied: false,
    timer: null,

    init() {
      this.name = this.$el.dataset.queue;
      this.loadInfo();
      this.reload();
      this.timer = setInterval(() => { if (!document.hidden) this.loadInfo(); }, 5000);
    },
    destroy() {
      clearInterval(this.timer);
    },
    async loadInfo() {
      try {
        const [info, ov] = await Promise.all([getJSON(queuePath(this.name)), getJSON("/v1/overview")]);
        this.info = info;
        this.summary = ov.queues.find((q) => q.name === this.name) || null;
        this.error = "";
      } catch (e) {
        this.error = "Couldn't load the queue: " + e.message;
      }
    },
    waitingInfo() {
      if (!this.info) return undefined;
      const s = this.info.stats;
      return Math.max(0, s.messages - s.ready - s.delayed - s.in_flight);
    },
    lede() {
      const s = this.summary;
      if (!s) return "";
      if (s.messages === 0) return "Empty: every message has been processed.";
      return `${s.messages.toLocaleString()} ${s.messages === 1 ? "message" : "messages"} in the queue.`;
    },
    attemptsText() {
      const n = this.info?.retry?.max_attempts;
      if (n === undefined) return "";
      if (n === 0) return "Unlimited: retried until processed";
      if (n === 1) return "1: no retries";
      return n + ": 1 delivery and " + (n - 1) + " retries";
    },
    backoffText() {
      const r = this.info?.retry;
      if (!r) return "";
      if (r.max_attempts === 1) return "Not used";
      if (r.initial_backoff === "0s") return "None: retried immediately";
      return `${this.fmtDur(r.initial_backoff)}, ×${r.multiplier} each attempt, up to ${this.fmtDur(r.max_backoff)}`;
    },
    viewStates() {
      return this.view === "live" ? ["ready", "waiting", "delayed", "in_flight"] : ["acked", "dropped", "dead_lettered"];
    },
    setView(v) {
      if (this.view === v) return;
      this.view = v;
      this.states = [];
      this.reload();
    },
    toggleState(s) {
      this.states = this.states.includes(s) ? this.states.filter((x) => x !== s) : [...this.states, s];
      this.reload();
    },
    stateText(m) {
      if (m.state === "dead_lettered" && m.moved_to) return "Moved to " + m.moved_to;
      return this.stateLabel(m.state);
    },
    async fetchPage(cursor) {
      const p = new URLSearchParams({ view: this.view, limit: "50" });
      if (this.states.length) p.set("state", this.states.join(","));
      if (cursor) p.set("cursor", String(cursor));
      return getJSON(queuePath(this.name) + "/messages?" + p);
    },
    async reload() {
      this.loading = true;
      try {
        const page = await this.fetchPage(0);
        this.messages = page.messages;
        this.next = page.next_cursor || 0;
        this.error = "";
      } catch (e) {
        this.error = "Couldn't load messages: " + e.message;
      } finally {
        this.loading = false;
        this.loaded = true;
      }
    },
    async loadMore() {
      if (!this.next) return;
      this.loading = true;
      try {
        const page = await this.fetchPage(this.next);
        this.messages = this.messages.concat(page.messages);
        this.next = page.next_cursor || 0;
      } catch (e) {
        this.error = "Couldn't load more messages: " + e.message;
      } finally {
        this.loading = false;
      }
    },
    emptyText() {
      if (this.states.length) return "No messages in that state.";
      return this.view === "live"
        ? "No messages waiting or in flight."
        : "No processed messages kept. Acked, dropped and dead-lettered messages show here for the queue's history retention.";
    },
    async open(m) {
      this.copied = false;
      try {
        this.selected = await getJSON(queuePath(this.name) + "/messages/" + encodeURIComponent(m.id));
      } catch (e) {
        this.error = "Couldn't open the message: " + e.message;
      }
    },
    close() {
      this.selected = null;
    },
    bodyKind() {
      const m = this.selected;
      if (!m) return "";
      if (m.body_base64 !== undefined && m.body === undefined) return "Body (binary, shown as base64)";
      try {
        JSON.parse(m.body);
        return "Body (JSON)";
      } catch (e) {
        return "Body";
      }
    },
    bodyText() {
      const m = this.selected;
      if (!m) return "";
      if (m.body === undefined) return m.body_base64 || "";
      try {
        return JSON.stringify(JSON.parse(m.body), null, 2);
      } catch (e) {
        return m.body;
      }
    },
    async copyBody() {
      try {
        await navigator.clipboard.writeText(this.selected.body ?? this.selected.body_base64 ?? "");
        this.copied = true;
        setTimeout(() => (this.copied = false), 1500);
      } catch (e) {
        this.error = "Couldn't copy: the browser blocked the clipboard.";
      }
    },
  }));
});
