/* CryptoTruth sample explorer. All analytical values come from fetched JSON.
   No browser randomness, no chatbot code, no writes to real result files. */
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const cards = new Map();
  const chartColors = [
    "#52e0aa",
    "#78a8ff",
    "#b9a0ff",
    "#f2c66d",
    "#ff778b",
    "#7dd4df"
  ];
  const verdictColors = ["#52e0aa", "#ff778b", "#9baec7"];
  const number = (n) =>
    Number.isFinite(n)
      ? new Intl.NumberFormat("en-AU", { maximumFractionDigits: 0 }).format(n)
      : "—";
  const decimal = (n) => (Number.isFinite(n) ? n.toFixed(3) : "—");
  const percent = (n, d) => (d ? `${((100 * n) / d).toFixed(1)}%` : "—");
  const mean = (rows) =>
    rows.length
      ? rows.reduce((s, r) => s + r.sentiment, 0) / rows.length
      : null;
  const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;
  let data,
    dates = [],
    activeDates = [],
    selected = [],
    pending = false;
  const state = {
    year: "all",
    coin: "all",
    sub: "all",
    kind: "all",
    threshold: 0,
    from: 0,
    to: 0
  };
  function el(tag, text) {
    const e = document.createElement(tag);
    e.textContent = text;
    return e;
  }
  function group(rows, key) {
    const out = new Map();
    for (const r of rows) {
      const k = typeof key === "function" ? key(r) : r[key];
      if (!out.has(k)) out.set(k, []);
      out.get(k).push(r);
    }
    return out;
  }
  function table(
    host,
    headers,
    rows,
    caption = "Exact values for the selected sample"
  ) {
    const t = document.createElement("table");
    t.append(el("caption", caption));
    const h = document.createElement("thead"),
      tr = document.createElement("tr");
    headers.forEach((x) => {
      const th = el("th", x);
      th.scope = "col";
      tr.append(th);
    });
    h.append(tr);
    t.append(h);
    const body = document.createElement("tbody");
    for (const row of rows) {
      const r = document.createElement("tr");
      for (const value of row) r.append(el("td", value ?? "—"));
      body.append(r);
    }
    if (!rows.length) {
      const r = document.createElement("tr"),
        td = el("td", "No matching data. Broaden the filters or reset.");
      td.colSpan = headers.length;
      r.append(td);
      body.append(r);
    }
    t.append(body);
    host.replaceChildren(t);
  }
  function axis(title, min, max) {
    return {
      title: { display: Boolean(title), text: title, color: "#97a9bd" },
      border: { display: false },
      grid: { color: "rgba(173,195,218,.09)" },
      ticks: { color: "#afc0d3", font: { size: 10 } },
      ...(min !== undefined ? { min } : {}),
      ...(max !== undefined ? { max } : {})
    };
  }
  function options() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: reducedMotion ? false : { duration: 260 },
      interaction: { mode: "nearest", intersect: false },
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            color: "#afc0d3",
            usePointStyle: true,
            boxWidth: 8,
            padding: 15,
            font: { size: 10 }
          }
        },
        tooltip: {
          backgroundColor: "#17283d",
          titleColor: "#f0f6fd",
          bodyColor: "#dce7f4",
          borderColor: "#31455e",
          borderWidth: 1,
          padding: 12,
          cornerRadius: 9
        }
      }
    };
  }
  function renderChart(
    key,
    config,
    hasData,
    emptyMessage = "No matching data. Broaden the filters or reset."
  ) {
    const card = cards.get(key),
      canvas = $(`ix-chart-${key}`),
      wrap = $(`ix-wrap-${key}`),
      message = wrap.querySelector(".ix-empty");
    message.hidden = true;
    canvas.hidden = false;
    if (!hasData || typeof Chart === "undefined") {
      if (card.chart) {
        card.chart.destroy();
        card.chart = null;
      }
      canvas.hidden = true;
      message.textContent = !hasData
        ? emptyMessage
        : "The chart library is unavailable. Use the table to inspect the data.";
      message.hidden = false;
      return;
    }
    try {
      if (card.chart && card.chart.config.type === config.type) {
        card.chart.data = config.data;
        card.chart.options = config.options;
        card.chart.update(card.table ? "none" : undefined);
      } else {
        if (card.chart) card.chart.destroy();
        card.chart = new Chart(canvas, config);
      }
    } catch (e) {
      console.error(`Chart ${key}:`, e);
      if (card.chart) {
        card.chart.destroy();
        card.chart = null;
      }
      canvas.hidden = true;
      message.hidden = false;
      message.textContent =
        "Unable to draw this chart. The exact-data table remains available.";
    }
  }
  function categorical(
    key,
    labels,
    values,
    colors = chartColors,
    unit = "Records"
  ) {
    const type = cards.get(key).type,
      o = options(),
      c = colors.slice();
    o.plugins.legend.display = type !== "bar";
    o.plugins.tooltip.callbacks = {
      label: (ctx) =>
        `${ctx.label}: ${number(ctx.raw)} ${unit.toLowerCase()}${
          type !== "bar"
            ? " · " +
              percent(
                ctx.raw,
                values.reduce((a, b) => a + b, 0)
              )
            : ""
        }`
    };
    if (type === "bar") {
      o.indexAxis = "y";
      o.scales = {
        x: { ...axis(unit, 0), ticks: { precision: 0, color: "#97a9bd" } },
        y: { ...axis(""), grid: { display: false } }
      };
    }
    renderChart(
      key,
      {
        type,
        data: {
          labels,
          datasets: [
            {
              label: unit,
              data: values,
              backgroundColor: labels.map((_, i) => c[i % c.length]),
              borderColor: "#101b2a",
              borderWidth: type === "bar" ? 0 : 3,
              borderRadius: type === "bar" ? 5 : 0,
              hoverOffset: 6,
              maxBarThickness: 26
            }
          ]
        },
        options: o
      },
      values.some((v) => v > 0)
    );
    table(
      $(`ix-table-${key}`),
      ["Category", unit, "Share"],
      labels.map((l, i) => [
        l,
        number(values[i]),
        percent(
          values[i],
          values.reduce((a, b) => a + b, 0)
        )
      ])
    );
  }
  function timeline(rows, months) {
    const groups = group(rows, "coin"),
      monthGroups = new Map(
        [...groups].map(([coin, r]) => [
          coin,
          group(r, (x) => x.date.slice(0, 7))
        ])
      );
    const coins = data._meta.coins.filter((c) => groups.has(c));
    const o = options();
    o.scales = { x: axis("Month"), y: axis("Mean sentiment", -1, 1) };
    o.interaction = { mode: "index", intersect: false };
    o.plugins.tooltip.callbacks = {
      label: (c) =>
        `${c.dataset.label}: ${decimal(c.raw)} · ${number(monthGroups.get(c.dataset.label).get(months[c.dataIndex])?.length || 0)} records`
    };
    const sets = coins.map((c) => ({
      label: c,
      data: months.map((m) => mean(monthGroups.get(c).get(m) || [])),
      borderColor:
        chartColors[data._meta.coins.indexOf(c) % chartColors.length],
      backgroundColor:
        chartColors[data._meta.coins.indexOf(c) % chartColors.length],
      tension: 0.25,
      spanGaps: false,
      pointRadius: 2,
      borderWidth: 2,
      maxBarThickness: 12
    }));
    renderChart(
      "timeline",
      {
        type: cards.get("timeline").type,
        data: { labels: months, datasets: sets },
        options: o
      },
      rows.length > 0
    );
    table(
      $("ix-table-timeline"),
      ["Month", ...coins],
      months.map((m, i) => [m, ...sets.map((s) => decimal(s.data[i]))])
    );
  }
  function alignment(rows) {
    const posts = new Map(
      rows.filter((r) => r.type === "post").map((r) => [r.id, r])
    );
    const replies = group(
      rows.filter((r) => r.type === "comment" && posts.has(r.parent_id)),
      "parent_id"
    );
    const pairs = [...replies].map(([id, r]) => ({
      subreddit: posts.get(id).subreddit,
      x: posts.get(id).sentiment,
      y: mean(r),
      replyCount: r.length
    }));
    const groups = group(pairs, "subreddit");
    const points = [...groups]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([sub, r]) => ({
        subreddit: sub,
        x: r.reduce((s, p) => s + p.x, 0) / r.length,
        y: r.reduce((s, p) => s + p.y, 0) / r.length,
        pairs: r.length,
        replies: r.reduce((s, p) => s + p.replyCount, 0)
      }));
    const type = cards.get("divergence").type,
      o = options();
    let datasets, labels;
    if (type === "scatter") {
      o.scales = {
        x: axis("Mean parent-post sentiment", -1, 1),
        y: axis("Mean reply sentiment", -1, 1)
      };
      o.plugins.legend.display = false;
      o.plugins.tooltip.callbacks = {
        title: (c) => c[0]?.raw.subreddit || "",
        label: (c) => [
          `Posts: ${decimal(c.raw.x)}`,
          `Replies: ${decimal(c.raw.y)}`,
          `${number(c.raw.pairs)} paired posts · ${number(c.raw.replies)} comments`
        ]
      };
      datasets = [
        {
          label: "Subreddit",
          data: points,
          backgroundColor: points.map(
            (_, i) => chartColors[i % chartColors.length]
          ),
          pointRadius: 8,
          pointHoverRadius: 11,
          borderColor: "#080f1b",
          borderWidth: 2
        }
      ];
    } else {
      labels = points.map((p) => p.subreddit);
      o.scales = { x: axis("Subreddit"), y: axis("Mean sentiment", -1, 1) };
      datasets = [
        {
          label: "Parent posts",
          data: points.map((p) => p.x),
          backgroundColor: "#78a8ff"
        },
        {
          label: "Replies",
          data: points.map((p) => p.y),
          backgroundColor: "#52e0aa"
        }
      ];
    }
    renderChart(
      "divergence",
      { type, data: { labels, datasets }, options: o },
      points.length > 0,
      "No matched post/comment pairs. Select All content types and broaden the filters."
    );
    table(
      $("ix-table-divergence"),
      [
        "Subreddit",
        "Parent mean",
        "Reply mean",
        "Post − reply",
        "Paired posts",
        "Replies"
      ],
      points.map((p) => [
        p.subreddit,
        decimal(p.x),
        decimal(p.y),
        decimal(p.x - p.y),
        number(p.pairs),
        number(p.replies)
      ])
    );
  }
  function engagement(rows) {
    const type = cards.get("engagement").type,
      o = options();
    if (type === "scatter") {
      const count = Math.min(rows.length, 1200),
        points = Array.from(
          { length: count },
          (_, i) => rows[Math.floor((i * rows.length) / count)]
        );
      $("ix-note-engagement").textContent =
        `Showing ${number(count)} of ${number(rows.length)} matched records; evenly spaced selection, with no jitter. X: sentiment. Y: upvotes + replies.`;
      o.scales = { x: axis("Sentiment", -1, 1), y: axis("Engagement", 0) };
      o.animation = false;
      o.plugins.tooltip.callbacks = {
        title: (c) => (c[0] ? `${c[0].raw.date} · ${c[0].raw.coin}` : ""),
        label: (c) => [
          `r/${c.raw.subreddit} · ${c.raw.type}`,
          `Sentiment: ${decimal(c.raw.x)}`,
          `Engagement: ${number(c.raw.y)}`,
          `Verdict: ${c.raw.verdict}`,
          c.raw.title
        ]
      };
      const groups = group(points, "coin");
      const datasets = data._meta.coins
        .filter((c) => groups.has(c))
        .map((c) => ({
          label: c,
          data: groups
            .get(c)
            .map((r) => ({ ...r, x: r.sentiment, y: r.engagement })),
          backgroundColor:
            chartColors[data._meta.coins.indexOf(c) % chartColors.length] +
            "aa",
          pointRadius: 3,
          pointHoverRadius: 6
        }));
      renderChart(
        "engagement",
        { type, data: { datasets }, options: o },
        rows.length > 0
      );
      table(
        $("ix-table-engagement"),
        [
          "ID",
          "Date",
          "Coin",
          "Subreddit",
          "Type",
          "Sentiment",
          "Engagement",
          "Verdict"
        ],
        points.map((r) => [
          r.id,
          r.date,
          r.coin,
          r.subreddit,
          r.type,
          decimal(r.sentiment),
          number(r.engagement),
          r.verdict
        ]),
        `Same ${number(count)} plotted records of ${number(rows.length)} matched. Download filtered CSV for all matching records.`
      );
    } else {
      $("ix-note-engagement").textContent =
        "Mean engagement by coin, calculated from every matching record. Engagement = upvotes + replies.";
      const groups = group(rows, "coin"),
        labels = data._meta.coins.filter((c) => groups.has(c)),
        values = labels.map(
          (c) =>
            groups.get(c).reduce((s, r) => s + r.engagement, 0) /
            groups.get(c).length
        );
      o.scales = { x: axis("Coin"), y: axis("Mean engagement", 0) };
      o.plugins.legend.display = false;
      o.plugins.tooltip.callbacks = {
        label: (c) =>
          `${Number(c.raw).toFixed(1)} mean engagement · ${number(groups.get(c.label).length)} records`
      };
      renderChart(
        "engagement",
        {
          type,
          data: {
            labels,
            datasets: [
              {
                label: "Mean engagement",
                data: values,
                backgroundColor: labels.map(
                  (c) =>
                    chartColors[
                      data._meta.coins.indexOf(c) % chartColors.length
                    ]
                ),
                borderRadius: 5
              }
            ]
          },
          options: o
        },
        rows.length > 0
      );
      table(
        $("ix-table-engagement"),
        ["Coin", "Mean engagement", "Records"],
        labels.map((c, i) => [
          c,
          values[i].toFixed(1),
          number(groups.get(c).length)
        ])
      );
    }
  }
  function communities(rows) {
    const rank = $("ix-rank").value;
    const entries = [...group(rows, "subreddit")]
      .map(([name, r]) => ({
        name,
        volume: r.length,
        sentiment: mean(r),
        coverage:
          (r.filter((x) => x.verdict !== "unverified").length / r.length) * 100
      }))
      .sort((a, b) => b[rank] - a[rank] || a.name.localeCompare(b.name));
    const labels = entries.map((r) => "r/" + r.name),
      values = entries.map((r) => r[rank]),
      o = options();
    const title = {
      volume: "Records",
      sentiment: "Mean sentiment",
      coverage: "Verification coverage (%)"
    }[rank];
    o.indexAxis = "y";
    o.scales = {
      x: axis(
        title,
        rank === "sentiment" ? -1 : 0,
        rank === "sentiment" ? 1 : rank === "coverage" ? 100 : undefined
      ),
      y: { ...axis(""), grid: { display: false } }
    };
    o.plugins.legend.display = false;
    o.plugins.tooltip.callbacks = {
      label: (c) =>
        `${title}: ${rank === "sentiment" ? decimal(c.raw) : Number(c.raw).toFixed(rank === "volume" ? 0 : 1)} · ${number(entries[c.dataIndex].volume)} records`
    };
    renderChart(
      "leaderboard",
      {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: title,
              data: values,
              backgroundColor: values.map((v) =>
                rank === "sentiment" && v < 0 ? "#ff778b" : "#52e0aa"
              ),
              borderRadius: 5,
              maxBarThickness: 25
            }
          ]
        },
        options: o
      },
      rows.length > 0
    );
    table(
      $("ix-table-leaderboard"),
      ["Subreddit", "Records", "Mean sentiment", "Verification coverage"],
      entries.map((r) => [
        "r/" + r.name,
        number(r.volume),
        decimal(r.sentiment),
        r.coverage.toFixed(1) + "%"
      ])
    );
  }
  function verification(rows, months) {
    const groups = group(rows, (r) => r.date.slice(0, 7));
    const totals = months.map((m) => {
      const r = groups.get(m) || [],
        resolved = r.filter((x) => x.verdict !== "unverified").length,
        supported = r.filter((x) => x.verdict === "supported").length;
      return { total: r.length, resolved, supported };
    });
    const o = options();
    o.scales = { x: axis("Month"), y: axis("Rate (%)", 0, 100) };
    o.interaction = { mode: "index", intersect: false };
    o.plugins.tooltip.callbacks = {
      label: (c) =>
        `${c.dataset.label}: ${c.raw === null ? "Unavailable" : Number(c.raw).toFixed(1) + "%"} · ${number(totals[c.dataIndex].total)} records`
    };
    const datasets = [
      {
        label: "Verification coverage",
        data: totals.map((t) =>
          t.total ? (100 * t.resolved) / t.total : null
        ),
        borderColor: "#78a8ff",
        backgroundColor: "#78a8ff"
      },
      {
        label: "Supported / resolved",
        data: totals.map((t) =>
          t.resolved ? (100 * t.supported) / t.resolved : null
        ),
        borderColor: "#52e0aa",
        backgroundColor: "#52e0aa"
      }
    ].map((s) => ({
      ...s,
      pointRadius: 2,
      tension: 0.2,
      borderWidth: 2,
      spanGaps: false
    }));
    renderChart(
      "verification",
      {
        type: cards.get("verification").type,
        data: { labels: months, datasets },
        options: o
      },
      rows.length > 0
    );
    table(
      $("ix-table-verification"),
      [
        "Month",
        "Records",
        "Resolved",
        "Supported",
        "Coverage",
        "Supported / resolved"
      ],
      months.map((m, i) => {
        const t = totals[i];
        return [
          m,
          number(t.total),
          number(t.resolved),
          number(t.supported),
          percent(t.resolved, t.total),
          percent(t.supported, t.resolved)
        ];
      })
    );
  }
  function update() {
    pending = false;
    if (!data) return;
    const start = activeDates[state.from],
      end = activeDates[state.to];
    selected = data.records.filter(
      (r) =>
        r.date >= start &&
        r.date <= end &&
        (state.coin === "all" || r.coin === state.coin) &&
        (state.sub === "all" || r.subreddit === state.sub) &&
        (state.kind === "all" || r.type === state.kind) &&
        Math.abs(r.sentiment) >= state.threshold
    );
    const rows = selected,
      n = rows.length,
      posts = rows.filter((r) => r.type === "post").length,
      comments = n - posts;
    const supported = rows.filter((r) => r.verdict === "supported").length,
      unsupported = rows.filter((r) => r.verdict === "unsupported").length,
      unverified = n - supported - unsupported,
      resolved = supported + unsupported;
    const cutoff = data._meta.sentiment_cutoff,
      bullish = rows.filter((r) => r.sentiment >= cutoff).length,
      bearish = rows.filter((r) => r.sentiment <= -cutoff).length,
      neutral = n - bullish - bearish;
    const totalEngagement = rows.reduce((s, r) => s + r.engagement, 0);
    for (const [k, v] of Object.entries({
      posts: number(posts),
      comments: number(comments),
      sentiment: decimal(mean(rows)),
      support: percent(supported, resolved),
      coverage: percent(resolved, n),
      engagement: number(totalEngagement)
    }))
      $(`ix-kpi-${k}`).textContent = v;
    $("ix-from-label").textContent = start;
    $("ix-to-label").textContent = end;
    $("ix-threshold-label").textContent = state.threshold.toFixed(2);
    $("ix-from").setAttribute("aria-valuetext", start);
    $("ix-to").setAttribute("aria-valuetext", end);
    $("ix-filter-hint").textContent = `${number(n)} records matched`;
    $("ix-status").textContent =
      `${number(posts)} posts + ${number(comments)} comments matched (${number(n)} / ${number(data.records.length)} records) · ${start} → ${end}`;
    $("ix-download").disabled = !n;
    categorical(
      "sentiment",
      ["Bullish", "Bearish", "Neutral"],
      [bullish, bearish, neutral],
      verdictColors
    );
    categorical(
      "verdict",
      ["Supported", "Unsupported", "Unverified"],
      [supported, unsupported, unverified],
      verdictColors
    );
    for (const [key, labels, values] of [
      [
        "sentiment",
        ["Bullish", "Bearish", "Neutral"],
        [bullish, bearish, neutral]
      ],
      [
        "verdict",
        ["Supported", "Unsupported", "Unverified"],
        [supported, unsupported, unverified]
      ]
    ])
      table(
        $(`ix-${key}-labels`),
        ["Label", "Records", "Share"],
        labels.map((l, i) => [l, number(values[i]), percent(values[i], n)])
      );
    const months = [
      ...new Set(
        activeDates.slice(state.from, state.to + 1).map((d) => d.slice(0, 7))
      )
    ];
    timeline(rows, months);
    const grouping = $("ix-topic-group").value,
      topics = [...group(rows, grouping)].sort(
        (a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0])
      );
    $("ix-title-topics").textContent =
      grouping === "coin" ? "Assets in focus" : "Topics in focus";
    categorical(
      "topics",
      topics.map((t) => t[0]),
      topics.map((t) => t[1].length)
    );
    alignment(rows);
    engagement(rows);
    communities(rows);
    verification(rows, months);
    categorical(
      "coverage",
      ["Posts", "Comments"],
      [posts, comments],
      ["#78a8ff", "#52e0aa"]
    );
    tableMetadata(rows, posts, comments);
  }
  function tableMetadata(rows, posts, comments) {
    const m = data._meta,
      pairs = [
        ["Source", "dashboard_mock_data.json"],
        ["Data status", m.label + " · entirely synthetic"],
        ["Full period", `${m.date_start} → ${m.date_end}`],
        ["Full dataset", `${number(m.row_count)} records`],
        ["Selected", `${number(posts)} posts / ${number(comments)} comments`],
        ["Seed", String(m.seed)],
        ["Engagement", m.engagement_definition],
        ["Claim unit", m.claim_definition]
      ];
    $("ix-metadata").replaceChildren(
      ...pairs.flatMap(([k, v]) => [el("dt", k), el("dd", v)])
    );
  }
  function schedule() {
    if (!pending) {
      pending = true;
      requestAnimationFrame(update);
    }
  }
  function clearEvent() {
    document
      .querySelectorAll("#ix-events button")
      .forEach((b) => b.setAttribute("aria-pressed", "false"));
  }
  function resetDates() {
    activeDates = dates.filter(
      (d) => state.year === "all" || d.startsWith(state.year)
    );
    state.from = 0;
    state.to = activeDates.length - 1;
    for (const [id, value] of [
      ["ix-from", 0],
      ["ix-to", state.to]
    ]) {
      const input = $(id);
      input.max = String(state.to);
      input.value = String(value);
    }
  }
  function reset() {
    Object.assign(state, {
      year: "all",
      coin: "all",
      sub: "all",
      kind: "all",
      threshold: 0
    });
    $("ix-year").value = "all";
    $("ix-coin").value = "all";
    $("ix-sub").value = "all";
    $("ix-threshold").value = "0";
    document
      .querySelectorAll("[data-kind]")
      .forEach((b) =>
        b.setAttribute("aria-pressed", String(b.dataset.kind === "all"))
      );
    resetDates();
    clearEvent();
    schedule();
  }
  function setup() {
    for (const node of document.querySelectorAll("[data-card]")) {
      const key = node.dataset.card,
        first = node.querySelector("[data-chart-type]");
      cards.set(key, {
        type: first.dataset.chartType,
        chart: null,
        table: false
      });
      node.querySelectorAll("[data-chart-type]").forEach((button) =>
        button.addEventListener("click", () => {
          cards.get(key).type = button.dataset.chartType;
          node
            .querySelectorAll("[data-chart-type]")
            .forEach((b) =>
              b.setAttribute("aria-pressed", String(b === button))
            );
          schedule();
        })
      );
      node.querySelector("[data-table]").addEventListener("click", (e) => {
        const card = cards.get(key);
        card.table = !card.table;
        e.currentTarget.textContent = card.table ? "Show chart" : "Show table";
        e.currentTarget.setAttribute("aria-pressed", String(card.table));
        $(`ix-table-${key}`).hidden = !card.table;
        $(`ix-wrap-${key}`).hidden = card.table;
        if (!card.table && card.chart)
          requestAnimationFrame(() => card.chart.resize());
      });
    }
    for (const [id, items, label] of [
      ["ix-year", [...new Set(dates.map((d) => d.slice(0, 4)))], (v) => v],
      ["ix-coin", data._meta.coins, (v) => v],
      ["ix-sub", data._meta.subreddits, (v) => "r/" + v]
    ])
      for (const v of items) {
        const option = el("option", label(v));
        option.value = v;
        $(id).append(option);
      }
    $("ix-year").addEventListener("change", () => {
      state.year = $("ix-year").value;
      resetDates();
      clearEvent();
      schedule();
    });
    for (const [id, key] of [
      ["ix-coin", "coin"],
      ["ix-sub", "sub"]
    ])
      $(id).addEventListener("change", () => {
        state[key] = $(id).value;
        schedule();
      });
    for (const [id, key] of [
      ["ix-from", "from"],
      ["ix-to", "to"]
    ])
      $(id).addEventListener("input", () => {
        state[key] = Number($(id).value);
        if (state.from > state.to) {
          if (key === "from") {
            state.to = state.from;
            $("ix-to").value = String(state.to);
          } else {
            state.from = state.to;
            $("ix-from").value = String(state.from);
          }
        }
        clearEvent();
        schedule();
      });
    $("ix-threshold").addEventListener("input", () => {
      state.threshold = Number($("ix-threshold").value);
      schedule();
    });
    document.querySelectorAll("[data-kind]").forEach((b) =>
      b.addEventListener("click", () => {
        state.kind = b.dataset.kind;
        document
          .querySelectorAll("[data-kind]")
          .forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
        schedule();
      })
    );
    $("ix-reset").addEventListener("click", reset);
    $("ix-topic-group").addEventListener("change", schedule);
    $("ix-rank").addEventListener("change", schedule);
    data.events.forEach((event) => {
      const b = el("button", event.label);
      b.type = "button";
      b.setAttribute("aria-pressed", "false");
      b.addEventListener("click", () => {
        state.year = "all";
        $("ix-year").value = "all";
        resetDates();
        state.from = Math.max(
          0,
          dates.findIndex((d) => d >= event.start)
        );
        const end = dates.findIndex((d) => d > event.end);
        state.to =
          end === -1 ? dates.length - 1 : Math.max(state.from, end - 1);
        $("ix-from").value = String(state.from);
        $("ix-to").value = String(state.to);
        clearEvent();
        b.setAttribute("aria-pressed", "true");
        schedule();
      });
      $("ix-events").append(b);
    });
    $("ix-download").addEventListener("click", () => {
      const keys = [
        "id",
        "date",
        "coin",
        "subreddit",
        "type",
        "parent_id",
        "topic",
        "sentiment",
        "verdict",
        "confidence",
        "upvotes",
        "reply_count",
        "engagement"
      ];
      const quote = (v) => '"' + String(v ?? "").replaceAll('"', '""') + '"';
      const csv = [
        keys.join(","),
        ...selected.map((r) => keys.map((k) => quote(r[k])).join(","))
      ].join("\r\n");
      const url = URL.createObjectURL(
          new Blob([csv], { type: "text/csv;charset=utf-8" })
        ),
        a = document.createElement("a");
      a.href = url;
      a.download = "CryptoTruth_SAMPLE_filtered.csv";
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    $("ix-controls").disabled = false;
    resetDates();
    update();
  }
  function validate(d) {
    if (
      !d ||
      d._meta?.is_mock !== true ||
      d._meta.schema_version !== 1 ||
      !Array.isArray(d.records) ||
      !Array.isArray(d.events) ||
      !d.records.length
    )
      throw Error("Invalid or unlabelled sample JSON.");
    const m = d._meta;
    if (
      !["coins", "subreddits", "topics"].every((k) => Array.isArray(m[k])) ||
      !Number.isFinite(m.sentiment_cutoff) ||
      m.sentiment_cutoff <= 0 ||
      m.sentiment_cutoff > 1 ||
      m.row_count !== d.records.length
    )
      throw Error("Invalid sample metadata.");
    const ids = new Set();
    for (const r of d.records) {
      if (
        typeof r.id !== "string" ||
        ids.has(r.id) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(r.date) ||
        !m.coins.includes(r.coin) ||
        !m.subreddits.includes(r.subreddit) ||
        !m.topics.includes(r.topic) ||
        !["post", "comment"].includes(r.type) ||
        !["supported", "unsupported", "unverified"].includes(r.verdict) ||
        !Number.isFinite(r.sentiment) ||
        Math.abs(r.sentiment) > 1 ||
        !["upvotes", "reply_count", "engagement"].every(
          (k) => Number.isInteger(r[k]) && r[k] >= 0
        ) ||
        r.engagement !== r.upvotes + r.reply_count
      )
        throw Error("Invalid sample record.");
      ids.add(r.id);
    }
    return d;
  }
  async function load() {
    const abort = new AbortController(),
      timer = setTimeout(() => abort.abort(), 30000);
    try {
      const response = await fetch("/api/dashboard-mock-data", {
        cache: "no-cache",
        signal: abort.signal,
        headers: { Accept: "application/json" }
      });
      if (!response.ok)
        throw Error(`Sample JSON unavailable (HTTP ${response.status}).`);
      data = validate(await response.json());
      dates = [...new Set(data.records.map((r) => r.date))].sort();
      setup();
    } catch (error) {
      $("ix-status").dataset.error = "true";
      $("ix-status").textContent =
        `${error.name === "AbortError" ? "Sample data request timed out." : error.message} Check dashboard_mock_data.json and reload. The claim checker is independent.`;
      document.querySelectorAll(".ix-chart-wrap").forEach((w) => {
        w.querySelector("canvas").hidden = true;
        const p = w.querySelector(".ix-empty");
        p.hidden = false;
        p.textContent =
          "Sample data unavailable. No substitute values are shown.";
      });
    } finally {
      clearTimeout(timer);
    }
  }
  load();
})();
