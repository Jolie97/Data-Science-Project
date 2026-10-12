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
    grid: "rgba(173,195,218,.10)"
  };
  const VERDICT_COLOR = {
    ACCURATE: COLOR.green,
    APPROXIMATE: COLOR.amber,
    INACCURATE: COLOR.red
  };

  const $ = (id) => document.getElementById(id);
  const isNum = (v) => typeof v === "number" && Number.isFinite(v);
  const fmt = (v) => (isNum(v) ? v.toLocaleString("en-US") : "—");
  const fmt1 = (v) => (isNum(v) ? v.toFixed(1) : "—");
  const pct = (a, b) =>
    isNum(a) && isNum(b) && b > 0 ? `${((a / b) * 100).toFixed(1)}%` : "—";
  const arr = (v) => (Array.isArray(v) ? v : []);

  // These controls only slice/sort fields returned by /api/dashboard-data.
  // No extra endpoints, third-party data or generated example records.
  const explorer = {
    claimType: "",
    subreddit: "",
    coverageSeries: "",
    userOrder: "original",
    userLimit: null
  };
  const tableSorts = new Map();
  let latestSnapshot = null;

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

  function cellSortValue(cell) {
    if (typeof cell === "number") return Number.isFinite(cell) ? cell : null;
    if (cell == null || cell === "—") return null;
    const text = String(cell).trim();
    if (/^-?\d+(?:\.\d+)?%?$/.test(text)) return Number(text.replace("%", ""));
    return text.toLocaleLowerCase();
  }

  function renderTable(hostId, caption, headers, rows) {
    const host = $(hostId);
    if (!host) return;
    host.replaceChildren();
    const table = document.createElement("table");
    const cap = document.createElement("caption");
    cap.textContent = caption;
    table.appendChild(cap);
    const sort = tableSorts.get(hostId);

    const thead = document.createElement("thead");
    const hr = document.createElement("tr");
    headers.forEach((h, i) => {
      const th = document.createElement("th");
      th.scope = "col";
      th.setAttribute("aria-sort", sort && sort.index === i
        ? (sort.ascending ? "ascending" : "descending") : "none");
      if (rows.length > 1) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = h;
        btn.title = `Sort by ${h}`;
        btn.addEventListener("click", () => {
          const previous = tableSorts.get(hostId);
          tableSorts.set(hostId, {
            index: i,
            ascending: previous && previous.index === i ? !previous.ascending : true
          });
          renderTable(hostId, caption, headers, rows);
        });
        th.appendChild(btn);
      } else {
        th.textContent = h;
      }
      hr.appendChild(th);
    });
    thead.appendChild(hr);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    const sortedRows = rows.map((row, i) => ({ row, i }));
    if (sort && sort.index < headers.length) {
      sortedRows.sort((a, b) => {
        const av = cellSortValue(a.row[sort.index]);
        const bv = cellSortValue(b.row[sort.index]);
        if (av == null || bv == null) return av == null && bv == null ? a.i - b.i : av == null ? 1 : -1;
        const compare = typeof av === "number" && typeof bv === "number"
          ? av - bv : String(av).localeCompare(String(bv));
        return (sort.ascending ? compare : -compare) || a.i - b.i;
      });
    }
    if (!sortedRows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = headers.length;
      td.className = "ct-no-results";
      td.textContent = "No data for this filter.";
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    sortedRows.forEach(({ row }) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = typeof cell === "number"
          ? fmt(cell) : cell == null ? "—" : String(cell);
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
    const existing =
      typeof Chart !== "undefined" ? Chart.getChart(canvas) : null;
    if (existing) existing.destroy();
    canvas.hidden = true;
    const note =
      canvas.parentElement &&
      canvas.parentElement.querySelector(".export-empty");
    if (note) {
      note.hidden = false;
      note.textContent = message;
    }
  }

  function drawChart(canvasId, config) {
    const canvas = $(canvasId);
    if (!canvas) return;
    if (typeof Chart === "undefined") {
      emptyState(
        canvasId,
        "Chart library could not be loaded. See the data table below."
      );
      return;
    }
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
    canvas.hidden = false;
    const note =
      canvas.parentElement &&
      canvas.parentElement.querySelector(".export-empty");
    if (note) note.hidden = true;

    // Clicking an existing plotted point opens its own exact-JSON-value table.
    // The table remains available without a chart or JavaScript chart library.
    config.options = config.options || {};
    const previousOnClick = config.options.onClick;
    config.options.onClick = (event, points, chart) => {
      if (previousOnClick) previousOnClick(event, points, chart);
      if (!points.length) return;
      const details = canvas.closest(".export-panel")?.querySelector("details.export-table");
      if (!details) return;
      details.open = true;
      const ix = points[0].index;
      const rawLabel = chart.data.labels?.[ix] ??
        chart.data.datasets?.[points[0].datasetIndex]?.data?.[ix]?.name;
      const label = String(rawLabel ?? "").replace(/^r\//, "");
      details.querySelectorAll("tbody tr").forEach((tr) => {
        tr.classList.toggle("ct-selected-row",
          tr.firstElementChild?.textContent?.trim() === label);
      });
    };
    new Chart(canvas, config);
  }

  // ---------- shared chart option builders ----------
  const tooltipBase = {
    backgroundColor: "#21344a",
    titleColor: "#f0f6fd",
    bodyColor: "#f0f6fd",
    padding: 11,
    cornerRadius: 9
  };

  function axisPair({
    horizontal = false,
    stacked = false,
    max = null,
    valueTitle = ""
  } = {}) {
    const category = {
      stacked,
      border: { display: false },
      grid: { display: false },
      ticks: { color: COLOR.text, font: { size: 11, weight: "600" } }
    };
    const value = {
      stacked,
      beginAtZero: true,
      border: { display: false },
      grid: { color: COLOR.grid, drawTicks: false },
      ticks: {
        color: COLOR.subtle,
        precision: 0,
        padding: 8,
        callback: (v) => fmt(v)
      },
      title: valueTitle
        ? { display: true, text: valueTitle, color: COLOR.subtle }
        : { display: false }
    };
    if (max != null) value.max = max;
    return horizontal ? { x: value, y: category } : { x: category, y: value };
  }

  function barOptions({
    horizontal = false,
    stacked = false,
    legend = false,
    max = null,
    valueTitle = "",
    label
  } = {}) {
    return {
      indexAxis: horizontal ? "y" : "x",
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 500 },
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: legend
          ? {
              position: "bottom",
              labels: { color: COLOR.text, boxWidth: 12, usePointStyle: true }
            }
          : { display: false },
        tooltip: {
          ...tooltipBase,
          displayColors: legend,
          callbacks: label ? { label } : {}
        }
      },
      scales: axisPair({ horizontal, stacked, max, valueTitle })
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
            borderWidth: 2
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "58%",
        animation: { duration: 500 },
        plugins: {
          legend: {
            position: "bottom",
            labels: { color: COLOR.text, boxWidth: 12, usePointStyle: true }
          },
          tooltip: {
            ...tooltipBase,
            callbacks: {
              label: (c) =>
                ` ${c.label}: ${fmt(c.parsed)} (${pct(c.parsed, total)})`
            }
          }
        }
      }
    });
  }

  function datasetsToStacked(block, colorFor) {
    return arr(block.datasets).map((d, i) => ({
      label: d.label,
      data: arr(d.data),
      backgroundColor: colorFor(d.label, i),
      borderRadius: 4,
      maxBarThickness: 46
    }));
  }

  const SERIES_FALLBACK = [COLOR.neutral, COLOR.green, COLOR.blue, COLOR.amber];
  const verdictColor = (label, i) =>
    VERDICT_COLOR[label] || SERIES_FALLBACK[i % SERIES_FALLBACK.length];
  const coverageColor = (label) =>
    label === "Scored" ? COLOR.green : COLOR.neutral;

  function hasValues(values) {
    return arr(values).some((v) => isNum(v) && v > 0);
  }

  // ---------- independent, schema-gated snapshot exploration ----------
  function controlBar(sectionId) {
    const section = $(sectionId);
    const grid = section?.querySelector(":scope > .cards-grid");
    if (!grid) return null;
    let bar = section.querySelector(":scope > .ct-explore-controls");
    if (!bar) {
      bar = document.createElement("div");
      bar.className = "ct-explore-controls";
      grid.before(bar);
    }
    return bar;
  }

  function selectControl(sectionId, name, choices, selected, onChange) {
    const bar = controlBar(sectionId);
    if (!bar || choices.length <= 1) return;
    const label = document.createElement("label");
    label.textContent = `${name}: `;
    const select = document.createElement("select");
    select.setAttribute("aria-label", name);
    choices.forEach(({ value, label: text }) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = text;
      select.appendChild(option);
    });
    select.value = selected;
    select.addEventListener("change", () => onChange(select.value));
    label.appendChild(select);
    bar.appendChild(label);
  }

  function refreshSnapshot() {
    if (latestSnapshot) render(latestSnapshot);
  }

  function initializeChartSelectors() {
    // Section titles are taken from the existing HTML, not invented events/data.
    ["analysis", "subreddits", "topics", "scoring", "data-notes"].forEach((id) => {
      const section = $(id);
      const grid = section?.querySelector(":scope > .cards-grid");
      if (!grid || grid.querySelectorAll(":scope > article.panel").length < 2) return;
      const panels = Array.from(grid.querySelectorAll(":scope > article.panel"));
      const bar = document.createElement("div");
      bar.className = "ct-chart-choices";
      bar.setAttribute("role", "group");
      bar.setAttribute("aria-label", "Choose chart to display");
      const label = document.createElement("span");
      label.className = "ct-control-label";
      label.textContent = "Display:";
      bar.appendChild(label);
      const buttons = [];
      ["All charts", ...panels.map((p) => p.querySelector("h3")?.textContent?.trim() || "Chart")]
        .forEach((text, selectedIndex) => {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.textContent = text;
          btn.setAttribute("aria-pressed", String(selectedIndex === 0));
          btn.addEventListener("click", () => {
            buttons.forEach((b, i) => b.setAttribute("aria-pressed", String(i === selectedIndex)));
            panels.forEach((panel, i) => { panel.hidden = selectedIndex !== 0 && selectedIndex !== i + 1; });
            grid.classList.toggle("ct-single-choice", selectedIndex !== 0);
            requestAnimationFrame(() => {
              panels.forEach((panel) => {
                if (!panel.hidden && typeof Chart !== "undefined") {
                  panel.querySelectorAll("canvas").forEach((canvas) => Chart.getChart(canvas)?.resize());
                }
              });
            });
          });
          buttons.push(btn);
          bar.appendChild(btn);
        });
      grid.before(bar);
    });
  }

  function initializeSentimentFilter() {
    // The existing Flask template reads these exact labels/counts from
    // results_file1.json, then builds chartPrototype1 before DOMContentLoaded.
    const chart = typeof Chart !== "undefined" ? Chart.getChart($("chartPrototype1")) : null;
    if (!chart || !chart.data.labels?.length) return;
    const labels = [...chart.data.labels];
    const amounts = [...arr(chart.data.datasets[0]?.data)];
    const fills = [...arr(chart.data.datasets[0]?.backgroundColor)];
    if (!amounts.some((v) => isNum(v) && v > 0)) return;
    const bar = controlBar("analysis");
    if (!bar) return;
    const choices = [{ value: "", label: "All sentiment labels" },
      ...labels.map((value) => ({ value, label: value }))];
    selectControl("analysis", "Sentiment label", choices, "", (value) => {
      const indices = labels.map((label, i) => ({ label, i }))
        .filter((item) => !value || item.label === value).map((item) => item.i);
      chart.data.labels = indices.map((i) => labels[i]);
      chart.data.datasets[0].data = indices.map((i) => amounts[i]);
      chart.data.datasets[0].backgroundColor = indices.map((i) => fills[i]);
      chart.data.datasets[0].hoverBackgroundColor = indices.map((i) => fills[i]);
      chart.update();
    });
  }

  function initializeControls(data) {
    const ov = data.overview || {};
    initializeChartSelectors();
    initializeSentimentFilter();
    const types = arr(ov.claim_types?.labels);
    if (types.length) {
      selectControl("topics", "Claim type",
        [{ value: "", label: "All claim types" }, ...types.map((v) => ({ value: v, label: v }))],
        explorer.claimType, (v) => { explorer.claimType = v; refreshSnapshot(); });
    }
    const subreddits = arr(data.subreddit_scores).map((r) => r.subreddit).filter(Boolean);
    if (subreddits.length) {
      const values = [...new Set(subreddits)];
      for (const id of ["claims", "scoring"]) {
        selectControl(id, "Subreddit",
          [{ value: "", label: "All subreddits" }, ...values.map((v) => ({ value: v, label: `r/${v}` }))],
          explorer.subreddit, (v) => {
            explorer.subreddit = v;
            document.querySelectorAll('#claims .ct-explore-controls select, #scoring .ct-explore-controls select')
              .forEach((select) => {
                if (select.getAttribute("aria-label") === "Subreddit") select.value = v;
              });
            refreshSnapshot();
          });
      }
      // No author-to-subreddit mapping exists in this export. User scores stay unfiltered.
      const scoringBar = controlBar("scoring");
      if (scoringBar) {
        const note = document.createElement("span");
        note.className = "ct-control-label";
        note.textContent = "Subreddit filter applies to subreddit charts, not user scores.";
        scoringBar.appendChild(note);
      }
    }
    const series = arr(data.coverage_by_type?.datasets).map((r) => r.label).filter(Boolean);
    if (series.length >= 2) {
      selectControl("data-notes", "Coverage series",
        [{ value: "", label: "Both series" }, ...series.map((v) => ({ value: v, label: v }))],
        explorer.coverageSeries, (v) => { explorer.coverageSeries = v; refreshSnapshot(); });
    }
    const users = arr(ov.user_reliability);
    if (users.length > 1 && users.every((r) => isNum(r.TRUST_SCORE) && isNum(r.SCORED_CLAIMS))) {
      const panel = $("export-chart-users")?.closest(".export-panel");
      if (panel) {
        const bar = document.createElement("div");
        bar.className = "ct-explore-controls";
        panel.querySelector(".export-chart").before(bar);
        const sortLabel = document.createElement("label");
        sortLabel.textContent = "User order: ";
        const select = document.createElement("select");
        select.setAttribute("aria-label", "User order");
        [["original", "Original JSON order"], ["trust_desc", "Trust score: high to low"],
          ["trust_asc", "Trust score: low to high"], ["scored_desc", "Scored claims: most first"]]
          .forEach(([value, label]) => {
            const opt = document.createElement("option"); opt.value = value; opt.textContent = label;
            select.appendChild(opt);
          });
        select.value = explorer.userOrder;
        select.addEventListener("change", () => { explorer.userOrder = select.value; refreshSnapshot(); });
        sortLabel.appendChild(select);
        bar.appendChild(sortLabel);

        explorer.userLimit = users.length;
        const limitLabel = document.createElement("label");
        limitLabel.textContent = "Top N: ";
        const range = document.createElement("input");
        range.type = "range";
        range.min = "1";
        range.max = String(users.length);
        range.step = "1";
        range.value = String(users.length);
        range.setAttribute("aria-label", "Number of user rows to show");
        const output = document.createElement("output");
        output.textContent = range.value;
        range.addEventListener("input", () => {
          explorer.userLimit = Number(range.value);
          output.textContent = range.value;
          refreshSnapshot();
        });
        limitLabel.append(range, output);
        bar.appendChild(limitLabel);
      }
    }
  }

  function filteredBlock(block, filterValue, seriesValue = "") {
    const labels = arr(block?.labels);
    const indices = labels.map((_, i) => i).filter((i) => !filterValue || labels[i] === filterValue);
    return {
      labels: indices.map((i) => labels[i]),
      datasets: arr(block?.datasets).filter((d) => !seriesValue || d.label === seriesValue)
        .map((d) => ({ ...d, data: indices.map((i) => arr(d.data)[i]) }))
    };
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
    if (!hasValues(values))
      emptyState("export-chart-verdict", "No scored claims in this snapshot.");
    else
      pieChart(
        "export-chart-verdict",
        labels,
        values,
        (l) => VERDICT_COLOR[l] || COLOR.neutral
      );
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
    if (!hasValues(values))
      emptyState("export-chart-checkable", "No claims in this snapshot.");
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
    const allLabels = arr(b.labels);
    const allValues = arr(b.values);
    const indices = allLabels.map((_, i) => i)
      .filter((i) => !explorer.claimType || allLabels[i] === explorer.claimType);
    const labels = indices.map((i) => allLabels[i]);
    const values = indices.map((i) => allValues[i]);
    if (!hasValues(values))
      emptyState("export-chart-types", "No data for this filter.");
    else {
      drawChart("export-chart-types", {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "Claims",
              data: values,
              backgroundColor: COLOR.blue,
              borderRadius: 6,
              maxBarThickness: 30
            }
          ]
        },
        options: barOptions({
          horizontal: true,
          label: (c) => ` ${fmt(c.parsed.x)} claims`
        })
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
    if (!any) emptyState(canvasId, "No data for this filter.");
    else {
      drawChart(canvasId, {
        type: "bar",
        data: { labels, datasets: datasetsToStacked(block, verdictColor) },
        options: barOptions({
          stacked: true,
          legend: true,
          label: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y)}`
        })
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
    const rows = arr(data.subreddit_scores).filter(
      (r) => !explorer.subreddit || r.subreddit === explorer.subreddit
    );
    const pts = rows.filter(
      (r) =>
        isNum(r.scored_claims) &&
        r.scored_claims > 0 &&
        isNum(r.verification_score)
    );
    if (!pts.length)
      emptyState("export-chart-samples", "No scored data for this filter.");
    else {
      drawChart("export-chart-samples", {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Subreddit",
              data: pts.map((r) => ({
                x: r.scored_claims,
                y: r.verification_score,
                name: r.subreddit
              })),
              backgroundColor: COLOR.green,
              borderColor: "#0b3d30",
              pointRadius: 7,
              pointHoverRadius: 9
            }
          ]
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
                  ` r/${c.raw.name}: ${fmt(c.raw.x)} scored claims, trust ${fmt1(c.raw.y)}`
              }
            }
          },
          scales: {
            x: {
              beginAtZero: true,
              grid: { color: COLOR.grid },
              border: { display: false },
              ticks: { color: COLOR.subtle, precision: 0 },
              title: {
                display: true,
                text: "Scored claims",
                color: COLOR.subtle
              }
            },
            y: {
              min: 0,
              max: 100,
              grid: { color: COLOR.grid },
              border: { display: false },
              ticks: { color: COLOR.subtle },
              title: { display: true, text: "Trust score", color: COLOR.subtle }
            }
          }
        }
      });
    }
    renderTable(
      "export-table-samples",
      "Subreddit evidence (all subreddits; blank trust = no scored claims)",
      [
        "Subreddit",
        "All claims",
        "Scored",
        "Accurate",
        "Approximate",
        "Inaccurate",
        "Trust score"
      ],
      rows.map((r) => [
        r.subreddit,
        r.claims,
        r.scored_claims,
        r.accurate,
        r.approximate,
        r.inaccurate,
        isNum(r.verification_score) ? fmt1(r.verification_score) : "—"
      ])
    );
  }

  function renderTrust(ov) {
    const rows = arr(ov.trust_by_subreddit).filter(
      (r) => !explorer.subreddit || r.SUBREDDIT === explorer.subreddit
    );
    if (!rows.length)
      emptyState("export-chart-trust", "No scored data for this filter.");
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
                r.TRUST_SCORE >= 60
                  ? COLOR.green
                  : r.TRUST_SCORE >= 30
                    ? COLOR.amber
                    : COLOR.red
              ),
              borderRadius: 6,
              maxBarThickness: 30
            }
          ]
        },
        options: {
          ...barOptions({
            horizontal: true,
            max: 100,
            valueTitle: "Trust score (0–100)"
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
                }
              }
            }
          }
        }
      });
    }
    renderTable(
      "export-table-trust",
      "Subreddit trust scores (at least one scored claim)",
      [
        "Subreddit",
        "Scored",
        "Accurate",
        "Approximate",
        "Inaccurate",
        "Trust score",
        "All claims"
      ],
      rows.map((r) => [
        r.SUBREDDIT,
        r.SCORED_CLAIMS,
        r.ACCURATE,
        r.APPROXIMATE,
        r.INACCURATE,
        fmt1(r.TRUST_SCORE),
        r.TOTAL_CLAIMS
      ])
    );
  }

  function renderUsers(ov) {
    // This is the exporter-selected set, not an estimate of all authors.
    const rows = [...arr(ov.user_reliability)];
    if (explorer.userOrder === "trust_desc") rows.sort((a, b) => b.TRUST_SCORE - a.TRUST_SCORE);
    if (explorer.userOrder === "trust_asc") rows.sort((a, b) => a.TRUST_SCORE - b.TRUST_SCORE);
    if (explorer.userOrder === "scored_desc") rows.sort((a, b) => b.SCORED_CLAIMS - a.SCORED_CLAIMS);
    if (explorer.userLimit != null) rows.splice(explorer.userLimit);
    const userColor = (r) =>
      r.TRUST_LABEL === "RELIABLE"
        ? COLOR.green
        : r.TRUST_LABEL === "SPREADER"
          ? COLOR.red
          : COLOR.neutral;
    if (!rows.length)
      emptyState(
        "export-chart-users",
        "No users with at least 3 scored claims."
      );
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
              maxBarThickness: 30
            }
          ]
        },
        options: {
          ...barOptions({
            horizontal: true,
            max: 100,
            valueTitle: "Trust score (0–100)"
          }),
          plugins: {
            legend: { display: false },
            tooltip: {
              ...tooltipBase,
              displayColors: false,
              callbacks: {
                label: (c) =>
                  ` Trust ${fmt1(c.parsed.x)} (${rows[c.dataIndex].TRUST_LABEL})`,
                afterLabel: (c) => {
                  const r = rows[c.dataIndex];
                  return ` ${fmt(r.ACCURATE)} accurate / ${fmt(r.INACCURATE)} inaccurate of ${fmt(r.SCORED_CLAIMS)} scored`;
                }
              }
            }
          }
        }
      });
    }
    renderTable(
      "export-table-users",
      "Lowest and highest scoring users (min. scored claims applies)",
      [
        "User",
        "Label",
        "Scored",
        "Accurate",
        "Approximate",
        "Inaccurate",
        "Trust score"
      ],
      rows.map((r) => [
        r.AUTHOR,
        r.TRUST_LABEL,
        r.SCORED_CLAIMS,
        r.ACCURATE,
        r.APPROXIMATE,
        r.INACCURATE,
        fmt1(r.TRUST_SCORE)
      ])
    );
  }

  function renderStatuses(data) {
    const b = data.status_counts || {};
    const labels = arr(b.labels);
    const values = arr(b.values);
    if (!hasValues(values))
      emptyState("export-chart-statuses", "No status data in this snapshot.");
    else {
      drawChart("export-chart-statuses", {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "Claims",
              data: values,
              backgroundColor: COLOR.blue,
              borderRadius: 6,
              maxBarThickness: 30
            }
          ]
        },
        options: barOptions({
          horizontal: true,
          label: (c) => ` ${fmt(c.parsed.x)} claims`
        })
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
        [
          "Date range",
          meta.date_start && meta.date_end
            ? `${meta.date_start} → ${meta.date_end}`
            : null
        ],
        [
          "Total claims",
          isNum(meta.total_claims) ? fmt(meta.total_claims) : null
        ],
        [
          "Scored claims",
          isNum(meta.scored_claims) ? fmt(meta.scored_claims) : null
        ],
        [
          "Not scored",
          isNum(meta.not_scored_claims) ? fmt(meta.not_scored_claims) : null
        ],
        [
          "Missing dates",
          isNum(meta.missing_dates) ? fmt(meta.missing_dates) : null
        ],
        [
          "Min. claims per user",
          isNum(meta.min_claims_user) ? fmt(meta.min_claims_user) : null
        ],
        ["Trust formula", meta.trust_formula]
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
      filteredBlock(ov.accuracy_by_type || {}, explorer.claimType)
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
      filteredBlock(data.coverage_by_type || {}, "", explorer.coverageSeries)
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

    const range =
      meta.date_start && meta.date_end
        ? `${meta.date_start} to ${meta.date_end}`
        : "date range unavailable";
    setStatus(
      `Snapshot loaded: ${fmt(meta.scored_claims)} scored of ${fmt(meta.total_claims)} claims · ${range}` +
        (meta.generated_at
          ? ` · generated ${meta.generated_at.slice(0, 10)}`
          : ""),
      null
    );
  }

  async function load() {
    setStatus("Loading market-verification snapshot…", null);
    try {
      const res = await fetch(ENDPOINT, {
        cache: "no-store",
        headers: { Accept: "application/json" }
      });
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
      latestSnapshot = data;
      initializeControls(data);
      render(data);
    } catch (err) {
      console.error("[CryptoTruth] Could not load dashboard data:", err);
      setStatus(
        `Could not load the market-verification snapshot. ${err.message}`,
        "error"
      );
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
        "statuses"
      ].forEach((k) => emptyState(`export-chart-${k}`, "Data unavailable."));
    }
  }

  if (document.readyState === "loading")
    document.addEventListener("DOMContentLoaded", load);
  else load();
})();
