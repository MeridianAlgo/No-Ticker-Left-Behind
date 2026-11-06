// TickerAtlas client-side search. Loads api/index.json once and searches in memory.
(function () {
  "use strict";

  var input = document.getElementById("q");
  var list = document.getElementById("results");
  var meta = document.getElementById("result-meta");
  var chips = Array.prototype.slice.call(document.querySelectorAll(".chip[data-type]"));
  if (!input || !list) return;

  var MAX = 60;
  var FEATURED = ["stock:AAPL", "stock:MSFT", "stock:NVDA", "etf:SPY", "etf:QQQ", "crypto:BTC", "crypto:ETH", "crypto:SOL", "stock:TSLA", "crypto:DOGE"];
  var rows = [];
  var byId = {};
  var type = "all";
  var active = -1;

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function highlight(text, q) {
    if (!q) return esc(text);
    var i = String(text).toLowerCase().indexOf(q);
    if (i < 0) return esc(text);
    return esc(text.slice(0, i)) + "<mark>" + esc(text.slice(i, i + q.length)) + "</mark>" + esc(text.slice(i + q.length));
  }

  function href(r) {
    return (r.c === "crypto" ? "crypto/" : "stocks/") + encodeURIComponent(r.s) + ".html";
  }

  function badgeClass(c) {
    return c === "stock" || c === "etf" || c === "crypto" ? c : "other";
  }

  function matchesType(r) {
    if (type === "all") return true;
    if (type === "stock") return r.c !== "etf" && r.c !== "crypto";
    return r.c === type;
  }

  function score(r, q) {
    if (r.tl === q) return 1000;
    if (r.tl.indexOf(q) === 0) return 800 - r.tl.length;
    if (r.nl.indexOf(q) === 0) return 600;
    if (r.nl.indexOf(" " + q) >= 0) return 500;
    if (q.length >= 2 && r.tl.indexOf(q) >= 0) return 300;
    if (q.length >= 3 && r.nl.indexOf(q) >= 0) return 200;
    return 0;
  }

  function render(items, q, total) {
    active = -1;
    if (!items.length) {
      list.innerHTML = "";
      meta.textContent = q ? "No matches for “" + q + "”." : "";
      return;
    }
    meta.textContent = q
      ? (total > items.length ? "Top " + items.length + " of " + total.toLocaleString() + " matches" : total.toLocaleString() + (total === 1 ? " match" : " matches"))
      : "Popular tickers — start typing to search " + rows.length.toLocaleString() + " assets";
    var html = "";
    for (var i = 0; i < items.length; i++) {
      var r = items[i];
      var sub = [r.x, r.k].filter(Boolean).join(" · ");
      html +=
        '<li><a href="' + href(r) + '">' +
        '<span class="t">' + highlight(r.t, q) + "</span>" +
        '<span class="n"><div>' + highlight(r.n, q) + "</div>" + (sub ? '<div class="sub">' + esc(sub) + "</div>" : "") + "</span>" +
        '<span class="badge ' + badgeClass(r.c) + '">' + esc(r.c) + "</span></a></li>";
    }
    list.innerHTML = html;
  }

  function search() {
    var q = input.value.trim().toLowerCase();
    if (!q) {
      render(FEATURED.map(function (id) { return byId[id]; }).filter(function (r) { return r && matchesType(r); }), "", 0);
      return;
    }
    var hits = [];
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      if (!matchesType(r)) continue;
      var s = score(r, q);
      if (s > 0) hits.push([s, r]);
    }
    hits.sort(function (a, b) {
      if (b[0] !== a[0]) return b[0] - a[0];
      // listed securities before crypto on ties; enriched rows (with sector) first
      var ac = a[1].c === "crypto" ? 1 : 0, bc = b[1].c === "crypto" ? 1 : 0;
      if (ac !== bc) return ac - bc;
      var ak = a[1].k ? 0 : 1, bk = b[1].k ? 0 : 1;
      if (ak !== bk) return ak - bk;
      return a[1].t.length - b[1].t.length || (a[1].t < b[1].t ? -1 : 1);
    });
    render(hits.slice(0, MAX).map(function (h) { return h[1]; }), q, hits.length);
  }

  function syncUrl() {
    try {
      var p = new URLSearchParams();
      if (input.value.trim()) p.set("q", input.value.trim());
      if (type !== "all") p.set("type", type);
      var qs = p.toString();
      history.replaceState(null, "", qs ? "?" + qs : location.pathname);
    } catch (e) { /* ignore */ }
  }

  function setType(t) {
    type = t;
    chips.forEach(function (c) { c.setAttribute("aria-pressed", c.getAttribute("data-type") === t ? "true" : "false"); });
  }

  var timer = null;
  input.addEventListener("input", function () {
    clearTimeout(timer);
    timer = setTimeout(function () { search(); syncUrl(); }, 60);
  });
  chips.forEach(function (c) {
    c.addEventListener("click", function () { setType(c.getAttribute("data-type")); search(); syncUrl(); });
  });
  input.addEventListener("keydown", function (e) {
    var links = list.querySelectorAll("a");
    if (!links.length) return;
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (active >= 0 && links[active]) links[active].classList.remove("active");
      active = e.key === "ArrowDown" ? Math.min(links.length - 1, active + 1) : Math.max(0, active - 1);
      links[active].classList.add("active");
      links[active].scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter") {
      var target = links[active >= 0 ? active : 0];
      if (target) location.href = target.href;
    }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "/" && document.activeElement !== input) { e.preventDefault(); input.focus(); }
  });

  try {
    var params = new URLSearchParams(location.search);
    if (params.get("q")) input.value = params.get("q");
    if (params.get("type")) setType(params.get("type"));
  } catch (e) { /* ignore */ }

  meta.textContent = "Loading index…";
  fetch("api/index.json")
    .then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); })
    .then(function (data) {
      var f = data.fields;
      var it = f.indexOf("ticker"), ina = f.indexOf("name"), ic = f.indexOf("asset_class"),
          ix = f.indexOf("exchange"), ik = f.indexOf("sector"), is = f.indexOf("slug");
      rows = data.rows.map(function (row) {
        var r = { t: row[it], n: row[ina] || row[it], c: row[ic], x: row[ix] || "", k: row[ik] || "", s: row[is] || row[it] };
        r.tl = String(r.t).toLowerCase();
        r.nl = String(r.n).toLowerCase();
        byId[r.c + ":" + r.t] = r;
        return r;
      });
      search();
    })
    .catch(function (err) {
      meta.textContent = "Could not load the ticker index (" + err.message + "). Download it directly from api/index.json.";
    });
})();
