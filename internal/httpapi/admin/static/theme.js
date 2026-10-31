// Runs before the page paints: applies the saved theme (light or dark), or
// leaves the system preference in charge. The header button toggles it.
(function () {
  var KEY = "qsrv-theme";
  var root = document.documentElement;
  function saved() {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  }
  function current() {
    var t = root.dataset.theme;
    if (t) return t;
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  var t = saved();
  if (t === "light" || t === "dark") root.dataset.theme = t;

  document.addEventListener("click", function (e) {
    var btn = e.target.closest && e.target.closest("[data-theme-toggle]");
    if (!btn) return;
    var next = current() === "dark" ? "light" : "dark";
    root.dataset.theme = next;
    try { localStorage.setItem(KEY, next); } catch (err) {}
  });
})();
