/*
 * dashboard_new.js
 * Loads /api/dashboard-data (dashboard_data_new.json, schema_version 2) and fills
 * sections 04-09 of index_new.html: KPIs, 10 charts, "View exact data" tables,
 * and the "About this export" block.
 *
 * Place at: dashboard/static/dashboard_new.js
 * Requires: Chart.js (already loaded in index_new.html before this deferred script).
 */
(() => {
  "use strict";

  const ENDPOINT = "/api/dashboard-data";

  const COLOR = {
    green: "#52e0aa",
    red: "#ff778b",
    blue: "#78a8ff",
    neutral: "#9baec7",
    amber: "#f5c76b",
    text: "#afc0d3",
    subtle: "#71869e",
    grid: "rgba(173,195,218,.10)",
  };
  const VERDICT_COLOR = {
    ACCURATE: COLOR.green,
    APPROXIMATE: COLOR.amber,
    INACCURATE: COLOR.red,
  };

  const $ = (id) => document.getElementById(id);
  const isNum = (v) => typeof v === "number" && Number.isFinite(v);
  const fmt = (v) => (isNum(v) ? v.toLocaleString("en-US") : "—");
  const fmt1 = (v) => (isNum(v) ? v.toFixed(1) : "—");
  const pct = (a, b) => (isNum(a) && isNum(b) && b > 0 ? `${((a / b) * 100).toFixed(1)}%` : "—");
  const arr = (v) => (Array.isArray(v) ? v : []);

  // ---------- small DOM helpers (textContent only: usernames etc. are untrusted) ----------
  function setText(id, text) {
    const el = $(id);
    if (el) el.textContent = text;
  }

  function setStatus(message, state) {
    const el = $("dashboard-status");
    if (!el) return;
    el.textContent = message;
    if (state) el.setAttribute("data-state", state);
    else el.removeAttribute("data-state");
  }

  function renderTable(hostId, caption, headers, rows) {
    const host = $(hostId);
    if (!host) return;
    host.replaceChildren();
    const table = document.createElement("table");
    const cap = document.createElement("caption");
    cap.textContent = caption;
    table.appendChild(cap);

    const thead = document.createElement("thead");
    const hr = document.createElement("tr");
    headers.forEach((h) => {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = h;
      hr.appendChild(th);
    });
    thead.appendChild(hr);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = typeof cell === "number" ? fmt(cell) : cell == null ? "—" : String(cell);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    host.appendChild(table);
  }

  function emptyState(canvasId, message) {
    const canvas = $(canvasId);
    if (!canvas) return;
    const existing = typeof Chart !== "undefined" ? Chart.getChart(canvas) : null;
    if (existing) existing.destroy();
    canvas.hidden = true;
    const note = canvas.parentElement && canvas.parentElement.querySelector(".export-empty");
    if (note) {
      note.hidden = false;
      note.textContent = message;
    }
  }

  function drawChart(canvasId, config) {
    const canvas = $(canvasId);
    if (!canvas) return;
    if (typeof Chart === "undefined") {
      emptyState(canvasId, "Chart library could not be loaded. See the data table below.");
      return;
    }
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
    canvas.hidden = false;
    const note = canvas.parentElement && canvas.parentElement.querySelector(".export-empty");
    if (note) note.hidden = true;
    new Chart(canvas, config);
  }

  // ---------- shared chart option builders ----------
  const tooltipBase = {
    backgroundColor: "#21344a",
    titleColor: "#f0f6fd",
    bodyColor: "#f0f6fd",
    padding: 11,
    cornerRadius: 9,
  };

  function axisPair({ horizontal = false, stacked = false, max = null, valueTitle = "" } = {}) {
    const category = {
      stacked,
      border: { display: false },
      grid: { display: false },
      ticks: { color: COLOR.text, font: { size: 11, weight: "600" } },
    };
    const value = {
      stacked,
      beginAtZero: true,
      border: { display: false },
      grid: { color: COLOR.grid, drawTicks: false },
      ticks: { color: COLOR.subtle, precision: 0, padding: 8, callback: (v) => fmt(v) },
      title: valueTitle ? { display: true, text: valueTitle, color: COLOR.subtle } : { display: false },
    };
    if (max != null) value.max = max;
    return horizontal ? { x: value, y: category } : { x: category, y: value };
  }

  function barOptions({ horizontal = false, stacked = false, legend = false, max = null, valueTitle = "", label } = {}) {
    return {
      indexAxis: horizontal ? "y" : "x",
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 500 },
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: legend
          ? { position: "bottom", labels: { color: COLOR.text, boxWidth: 12, usePointStyle: true } }
          : { display: false },
        tooltip: {
          ...tooltipBase,
          displayColors: legend,
          callbacks: label ? { label } : {},
        },
      },
      scales: axisPair({ horizontal, stacked, max, valueTitle }),
    };
  }

  function pieChart(canvasId, labels, values, colorFor) {
    const total = values.reduce((a, b) => a + (isNum(b) ? b : 0), 0);
    drawChart(canvasId, {
      type: "doughnut",
      data: {
        labels,
        datasets: [
          {
            data: values,
            backgroundColor: labels.map(colorFor),
            borderColor: "#111c2c",
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "58%",
        animation: { duration: 500 },
        plugins: {
          legend: { position: "bottom", labels: { color: COLOR.text, boxWidth: 12, usePointStyle: true } },
          tooltip: {
            ...tooltipBase,
            callbacks: {
              label: (c) => ` ${c.label}: ${fmt(c.parsed)} (${pct(c.parsed, total)})`,
            },
          },
        },
      },
    });
  }

  function datasetsToStacked(block, colorFor) {
    return arr(block.datasets).map((d, i) => ({
      label: d.label,
      data: arr(d.data),
      backgroundColor: colorFor(d.label, i),
      borderRadius: 4,
      maxBarThickness: 46,
    }));
  }

  const SERIES_FALLBACK = [COLOR.neutral, COLOR.green, COLOR.blue, COLOR.amber];
  const verdictColor = (label, i) => VERDICT_COLOR[label] || SERIES_FALLBACK[i % SERIES_FALLBACK.length];
  const coverageColor = (label) => (label === "Scored" ? COLOR.green : COLOR.neutral);

  function hasValues(values) {
    return arr(values).some((v) => isNum(v) && v > 0);
  }

  // ---------- section renderers ----------
  function renderKpis(meta) {
    setText("export-total", fmt(meta.total_claims));
    setText("export-scored", fmt(meta.scored_claims));
    setText("export-unscored", fmt(meta.not_scored_claims));
    setText("export-rate", pct(meta.scored_claims, meta.total_claims));
  }

  function renderVerdict(ov) {
    const b = ov.verdict_breakdown || {};
    const labels = arr(b.labels);
    const values = arr(b.values);
    const total = values.reduce((a, v) => a + (isNum(v) ? v : 0), 0);
    if (!hasValues(values)) emptyState("export-chart-verdict", "No scored claims in this snapshot.");
    else pieChart("export-chart-verdict", labels, values, (l) => VERDICT_COLOR[l] || COLOR.neutral);
    renderTable(
      "export-table-verdict",
      "Verdict distribution (scored claims)",
      ["Verdict", "Claims", "Share of scored"],
      labels.map((l, i) => [l, values[i], pct(values[i], total)])
    );
  }

  function renderCheckable(ov) {
    const b = ov.checkable_share || {};
    const labels = arr(b.labels);
    const values = arr(b.values);
    const total = values.reduce((a, v) => a + (isNum(v) ? v : 0), 0);
    if (!hasValues(values)) emptyState("export-chart-checkable", "No claims in this snapshot.");
    else pieChart("export-chart-checkable", labels, values, coverageColor);
    renderTable(
      "export-table-checkable",
      "Verification coverage (all extracted claims)",
      ["Group", "Claims", "Share of all claims"],
      labels.map((l, i) => [l, values[i], pct(values[i], total)])
    );
  }

  function renderTypes(ov) {
    const b = ov.claim_types || {};
    const labels = arr(b.labels);
    const values = arr(b.values);
    if (!hasValues(values)) emptyState("export-chart-types", "No claim types in this snapshot.");
    else {
      drawChart("export-chart-types", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "Claims", data: values, backgroundColor: COLOR.blue, borderRadius: 6, maxBarThickness: 30 },
          ],
        },
        options: barOptions({ horizontal: true, label: (c) => ` ${fmt(c.parsed.x)} claims` }),
      });
    }
    const total = values.reduce((a, v) => a + (isNum(v) ? v : 0), 0);
    renderTable(
      "export-table-types",
      "Claims by type (all claims)",
      ["Claim type", "Claims", "Share"],
      labels.map((l, i) => [l, values[i], pct(values[i], total)])
    );
  }

  function stackedBlock(canvasId, tableId, caption, firstHeader, block) {
    const labels = arr(block.labels);
    const datasets = arr(block.datasets);
    const any = datasets.some((d) => hasValues(d.data));
    if (!any) emptyState(canvasId, "No scored claims for this breakdown.");
    else {
      drawChart(canvasId, {
        type: "bar",
        data: { labels, datasets: datasetsToStacked(block, verdictColor) },
        options: barOptions({
          stacked: true,
          legend: true,
          label: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y)}`,
        }),
      });
    }
    renderTable(
      tableId,
      caption,
      [firstHeader, ...datasets.map((d) => d.label), "Total"],
      labels.map((l, i) => {
        const vals = datasets.map((d) => arr(d.data)[i]);
        const total = vals.reduce((a, v) => a + (isNum(v) ? v : 0), 0);
        return [l, ...vals, total];
      })
    );
  }

  function renderSamples(data) {
    const rows = arr(data.subreddit_scores);
    const pts = rows.filter((r) => isNum(r.scored_claims) && r.scored_claims > 0 && isNum(r.verification_score));
    if (!pts.length) emptyState("export-chart-samples", "No subreddit has any scored claims.");
    else {
      drawChart("export-chart-samples", {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Subreddit",
              data: pts.map((r) => ({ x: r.scored_claims, y: r.verification_score, name: r.subreddit })),
              backgroundColor: COLOR.green,
              borderColor: "#0b3d30",
              pointRadius: 7,
              pointHoverRadius: 9,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: { duration: 500 },
          plugins: {
            legend: { display: false },
            tooltip: {
              ...tooltipBase,
              displayColors: false,
              callbacks: {
                label: (c) =>
                  ` r/${c.raw.name}: ${fmt(c.raw.x)} scored claims, trust ${fmt1(c.raw.y)}`,
              },
            },
          },
          scales: {
            x: {
              beginAtZero: true,
              grid: { color: COLOR.grid },
              border: { display: false },
              ticks: { color: COLOR.subtle, precision: 0 },
              title: { display: true, text: "Scored claims", color: COLOR.subtle },
            },
            y: {
              min: 0,
              max: 100,
              grid: { color: COLOR.grid },
              border: { display: false },
              ticks: { color: COLOR.subtle },
              title: { display: true, text: "Trust score", color: COLOR.subtle },
            },
          },
        },
      });
    }
    renderTable(
      "export-table-samples",
      "Subreddit evidence (all subreddits; blank trust = no scored claims)",
      ["Subreddit", "All claims", "Scored", "Accurate", "Approximate", "Inaccurate", "Trust score"],
      rows.map((r) => [
        r.subreddit,
        r.claims,
        r.scored_claims,
        r.accurate,
        r.approximate,
        r.inaccurate,
        isNum(r.verification_score) ? fmt1(r.verification_score) : "—",
      ])
    );
  }

  function renderTrust(ov) {
    const rows = arr(ov.trust_by_subreddit);
    if (!rows.length) emptyState("export-chart-trust", "No subreddit has any scored claims.");
    else {
      drawChart("export-chart-trust", {
        type: "bar",
        data: {
          labels: rows.map((r) => `r/${r.SUBREDDIT}`),
          datasets: [
            {
              label: "Trust score",
              data: rows.map((r) => r.TRUST_SCORE),
              backgroundColor: rows.map((r) =>
                r.TRUST_SCORE >= 60 ? COLOR.green : r.TRUST_SCORE >= 30 ? COLOR.amber : COLOR.red
              ),
              borderRadius: 6,
              maxBarThickness: 30,
            },
          ],
        },
        options: {
          ...barOptions({
            horizontal: true,
            max: 100,
            valueTitle: "Trust score (0–100)",
          }),
          plugins: {
            legend: { display: false },
            tooltip: {
              ...tooltipBase,
              displayColors: false,
              callbacks: {
                label: (c) => ` Trust ${fmt1(c.parsed.x)}`,
                afterLabel: (c) => {
                  const r = rows[c.dataIndex];
                  return ` ${fmt(r.SCORED_CLAIMS)} scored of ${fmt(r.TOTAL_CLAIMS)} claims`;
                },
              },
            },
          },
        },
      });
    }
    renderTable(
      "export-table-trust",
      "Subreddit trust scores (at least one scored claim)",
      ["Subreddit", "Scored", "Accurate", "Approximate", "Inaccurate", "Trust score", "All claims"],
      rows.map((r) => [
        r.SUBREDDIT,
        r.SCORED_CLAIMS,
        r.ACCURATE,
        r.APPROXIMATE,
        r.INACCURATE,
        fmt1(r.TRUST_SCORE),
        r.TOTAL_CLAIMS,
      ])
    );
  }

  function renderUsers(ov) {
    const rows = arr(ov.user_reliability);
    const userColor = (r) =>
      r.TRUST_LABEL === "RELIABLE" ? COLOR.green : r.TRUST_LABEL === "SPREADER" ? COLOR.red : COLOR.neutral;
    if (!rows.length) emptyState("export-chart-users", "No users with at least 3 scored claims.");
    else {
      drawChart("export-chart-users", {
        type: "bar",
        data: {
          labels: rows.map((r) => r.AUTHOR),
          datasets: [
            {
              label: "Trust score",
              data: rows.map((r) => r.TRUST_SCORE),
              backgroundColor: rows.map(userColor),
              borderRadius: 6,
              maxBarThickness: 30,
            },
          ],
        },
        options: {
          ...barOptions({ horizontal: true, max: 100, valueTitle: "Trust score (0–100)" }),
          plugins: {
            legend: { display: false },
            tooltip: {
              ...tooltipBase,
              displayColors: false,
              callbacks: {
                label: (c) => ` Trust ${fmt1(c.parsed.x)} (${rows[c.dataIndex].TRUST_LABEL})`,
                afterLabel: (c) => {
                  const r = rows[c.dataIndex];
                  return ` ${fmt(r.ACCURATE)} accurate / ${fmt(r.INACCURATE)} inaccurate of ${fmt(r.SCORED_CLAIMS)} scored`;
                },
              },
            },
          },
        },
      });
    }
    renderTable(
      "export-table-users",
      "Lowest and highest scoring users (min. scored claims applies)",
      ["User", "Label", "Scored", "Accurate", "Approximate", "Inaccurate", "Trust score"],
      rows.map((r) => [
        r.AUTHOR,
        r.TRUST_LABEL,
        r.SCORED_CLAIMS,
        r.ACCURATE,
        r.APPROXIMATE,
        r.INACCURATE,
        fmt1(r.TRUST_SCORE),
      ])
    );
  }

  function renderStatuses(data) {
    const b = data.status_counts || {};
    const labels = arr(b.labels);
    const values = arr(b.values);
    if (!hasValues(values)) emptyState("export-chart-statuses", "No status data in this snapshot.");
    else {
      drawChart("export-chart-statuses", {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "Claims", data: values, backgroundColor: COLOR.blue, borderRadius: 6, maxBarThickness: 30 },
          ],
        },
        options: barOptions({ horizontal: true, label: (c) => ` ${fmt(c.parsed.x)} claims` }),
      });
    }
    renderTable(
      "export-table-statuses",
      "Pipeline STATUS counts",
      ["Status", "Claims"],
      labels.map((l, i) => [l, values[i]])
    );
  }

  function renderMeta(meta) {
    const dl = $("export-metadata");
    if (dl) {
      dl.replaceChildren();
      const items = [
        ["Generated", meta.generated_at],
        ["Source file", meta.source_file],
        ["Summary source", meta.summary_source],
        ["Date range", meta.date_start && meta.date_end ? `${meta.date_start} → ${meta.date_end}` : null],
        ["Total claims", isNum(meta.total_claims) ? fmt(meta.total_claims) : null],
        ["Scored claims", isNum(meta.scored_claims) ? fmt(meta.scored_claims) : null],
        ["Not scored", isNum(meta.not_scored_claims) ? fmt(meta.not_scored_claims) : null],
        ["Missing dates", isNum(meta.missing_dates) ? fmt(meta.missing_dates) : null],
        ["Min. claims per user", isNum(meta.min_claims_user) ? fmt(meta.min_claims_user) : null],
        ["Trust formula", meta.trust_formula],
      ];
      items.forEach(([k, v]) => {
        if (v == null || v === "") return;
        const dt = document.createElement("dt");
        dt.textContent = k;
        const dd = document.createElement("dd");
        dd.textContent = String(v);
        dl.append(dt, dd);
      });
    }
    const ul = $("export-notes");
    if (ul) {
      ul.replaceChildren();
      arr(meta.notes).forEach((n) => {
        const li = document.createElement("li");
        li.textContent = String(n);
        ul.appendChild(li);
      });
    }
  }

  function render(data) {
    const meta = data.meta || {};
    const ov = data.overview || {};

    renderKpis(meta);
    renderVerdict(ov);
    renderCheckable(ov);
    renderTypes(ov);
    stackedBlock(
      "export-chart-typeAccuracy",
      "export-table-typeAccuracy",
      "Accuracy by claim type (scored claims)",
      "Claim type",
      ov.accuracy_by_type || {}
    );
    stackedBlock(
      "export-chart-coins",
      "export-table-coins",
      "Accuracy by coin (scored claims)",
      "Coin",
      ov.accuracy_by_coin || {}
    );
    renderSamples(data);
    renderTrust(ov);
    renderUsers(ov);
    stackedBlock(
      "export-chart-coverage",
      "export-table-coverage",
      "Checkability by claim type",
      "Claim type",
      data.coverage_by_type || {}
    );
    // coverage uses Scored / Not scored colours rather than verdict colours
    const cov = $("export-chart-coverage");
    if (cov && typeof Chart !== "undefined") {
      const chart = Chart.getChart(cov);
      if (chart) {
        chart.data.datasets.forEach((d) => {
          d.backgroundColor = coverageColor(d.label);
        });
        chart.update("none");
      }
    }
    renderStatuses(data);
    renderMeta(meta);

    const range = meta.date_start && meta.date_end ? `${meta.date_start} to ${meta.date_end}` : "date range unavailable";
    setStatus(
      `Snapshot loaded: ${fmt(meta.scored_claims)} scored of ${fmt(meta.total_claims)} claims · ${range}` +
        (meta.generated_at ? ` · generated ${meta.generated_at.slice(0, 10)}` : ""),
      null
    );
  }

  async function load() {
    setStatus("Loading market-verification snapshot…", null);
    try {
      const res = await fetch(ENDPOINT, { cache: "no-store", headers: { Accept: "application/json" } });
      let data = null;
      try {
        data = await res.json();
      } catch (_) {
        /* non-JSON response */
      }
      if (!res.ok) {
        const reason = (data && data.error) || `Server returned ${res.status}.`;
        throw new Error(reason);
      }
      if (!data || typeof data !== "object" || !data.overview) {
        throw new Error("The dashboard export has an unexpected format.");
      }
      render(data);
    } catch (err) {
      console.error("[CryptoTruth] Could not load dashboard data:", err);
      setStatus(`Could not load the market-verification snapshot. ${err.message}`, "error");
      [
        "verdict",
        "checkable",
        "types",
        "typeAccuracy",
        "coins",
        "samples",
        "trust",
        "users",
        "coverage",
        "statuses",
      ].forEach((k) => emptyState(`export-chart-${k}`, "Data unavailable."));
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", load);
  else load();
})();
