/* Real saved results only. Never synthesizes Reddit records or market values. */
const CryptoTruthData = (() => {
  const VERDICTS = ["ACCURATE", "APPROXIMATE", "INACCURATE"];
  const sum = values => values.reduce((a, b) => a + b, 0);
  const series = counts => ({labels: Object.keys(counts), values: Object.values(counts)});
  const countBy = (rows, key) => {
    const counts = Object.create(null);
    rows.forEach(r => { const label = r[key] ?? "Unknown"; counts[label] = (counts[label] || 0) + 1; });
    return series(counts);
  };
  const ratio = (n, d) => d ? n / d * 100 : null;
  function matrix(rows, key, categories, getCategory) {
    const labels = [...new Set(rows.map(r => r[key] ?? "Unknown"))].sort();
    return {labels, datasets: categories.map(label => ({label, data: labels.map(group =>
      rows.filter(r => (r[key] ?? "Unknown") === group && getCategory(r) === label).length)}))};
  }
  function aggregate(rows) {
    const scored = rows.filter(r => r.scored);
    const verdictOf = r => VERDICTS.includes(r.verdict) ? r.verdict : "Unclassified";
    const categories = scored.some(r => !VERDICTS.includes(r.verdict)) ? [...VERDICTS, "Unclassified"] : VERDICTS;
    const accurate = scored.filter(r => r.verdict === "ACCURATE").length;
    const approximate = scored.filter(r => r.verdict === "APPROXIMATE").length;
    const subreddits = [...new Set(rows.map(r => r.subreddit ?? "Unknown"))].sort().map(subreddit => {
      const members = rows.filter(r => (r.subreddit ?? "Unknown") === subreddit);
      const checked = members.filter(r => r.scored);
      const n = verdict => checked.filter(r => r.verdict === verdict).length;
      return {subreddit, claims:members.length, scored_claims:checked.length, accurate:n("ACCURATE"),
        approximate:n("APPROXIMATE"), inaccurate:n("INACCURATE"),
        verification_score:checked.length ? Math.round((n("ACCURATE") + .5*n("APPROXIMATE"))/checked.length*1000)/10 : null};
    });
    const months = [...new Set(rows.filter(r => r.date).map(r => r.date.slice(0,7)))].sort();
    const monthly = months.map(month => {
      const items = rows.filter(r => r.date?.startsWith(month));
      const checked = items.filter(r => r.scored);
      const correct = checked.filter(r => r.verdict === "ACCURATE").length;
      return {month, claims:items.length, scored:checked.length, accurate:correct,
        coverage:ratio(checked.length,items.length), accuracy:ratio(correct,checked.length)};
    });
    return {meta:{total_claims:rows.length, scored_claims:scored.length, not_scored_claims:rows.length-scored.length},
      overview:{verdict_breakdown:{labels:categories, values:categories.map(c => scored.filter(r => verdictOf(r)===c).length)},
        checkable_share:{labels:["Not checkable","Checkable"], values:[rows.length-scored.length,scored.length]},
        claim_types:countBy(rows,"claim_type"), accuracy_by_coin:matrix(scored,"coin",categories,verdictOf),
        accuracy_by_type:matrix(scored,"claim_type",categories,verdictOf)},
      coverage_by_type:matrix(rows,"claim_type",["Not scored","Scored"],r => r.scored ? "Scored" : "Not scored"),
      status_counts:countBy(rows,"status"), subreddit_scores:subreddits, monthly,
      score:ratio(accurate+.5*approximate,scored.length)};
  }
  function filter(rows, state) {
    return rows.filter(r => (state.coin==="all" || r.coin===state.coin || (state.coin==="Unknown" && r.coin==null)) &&
      (state.sub==="all" || r.subreddit===state.sub || (state.sub==="Unknown" && r.subreddit==null)) &&
      (state.kind==="all" || (state.kind==="scored")===r.scored) &&
      (state.year==="all" || r.date?.startsWith(state.year)) &&
      (!state.from || (r.date && r.date>=state.from)) &&
      (!state.to || (r.date && r.date<=state.to)));
  }
  function selectMatrix(input, coin) {
    if (!input) return {labels:[],datasets:[]};
    const indices = input.labels.map((_,i)=>i).filter(i=>coin==="all" || input.labels[i]===coin);
    return {labels:indices.map(i=>input.labels[i]), datasets:input.datasets.map(d=>({label:d.label,data:indices.map(i=>d.data[i])}))};
  }
  function validate(data) {
    if (!data || data.schema_version!==2 || !data.meta || !data.overview) throw Error("The real dashboard export is missing or incompatible.");
    for (const k of ["total_claims","scored_claims","not_scored_claims"])
      if (!Number.isInteger(data.meta[k]) || data.meta[k]<0) throw Error("Invalid claim totals in the export.");
    if(data.meta.total_claims!==data.meta.scored_claims+data.meta.not_scored_claims) throw Error("Inconsistent claim totals in the export.");
    function checkSeries(s) {
      if(!s || !Array.isArray(s.labels) || !Array.isArray(s.values) || s.labels.length!==s.values.length ||
        !s.labels.every(x=>typeof x==="string") || !s.values.every(x=>Number.isFinite(x)&&x>=0)) throw Error("Invalid chart counts in the export.");
    }
    for(const key of ["verdict_breakdown","checkable_share","claim_types"]) checkSeries(data.overview[key]);
    checkSeries(data.status_counts);
    for(const m of [data.overview.accuracy_by_coin,data.overview.accuracy_by_type,data.coverage_by_type]) {
      if(!m || !Array.isArray(m.datasets)) throw Error("Missing chart breakdown in the export.");
      m.datasets.forEach(d=>checkSeries({labels:m.labels,values:d.data}));
    }
    if(!Array.isArray(data.subreddit_scores)) throw Error("Missing community breakdown in the export.");
    if(data.claim_records !== undefined) {
      if(!Array.isArray(data.claim_records) || data.claim_records.length!==data.meta.total_claims) throw Error("Claim record count does not match the export.");
      for(const r of data.claim_records) {
        if(!r || typeof r.scored!=="boolean" || !["date","coin","subreddit","claim_type","status","verdict"].every(k => r[k]===null || typeof r[k]==="string") ||
          (r.date!==null && (!/^\d{4}-\d{2}-\d{2}$/.test(r.date) || !Number.isFinite(Date.parse(r.date)))))
          throw Error("Invalid claim record in the export.");
      }
      if(data.claim_records.filter(r=>r.scored).length!==data.meta.scored_claims) throw Error("Scored record count does not match the export.");
    }
    return data;
  }
  return {VERDICTS, sum, ratio, aggregate, filter, selectMatrix, validate};
})();
if (typeof module !== "undefined" && module.exports) module.exports = CryptoTruthData;

if (typeof document !== "undefined") (() => {
  "use strict";
  const $ = id => document.getElementById(id);
  const cards = new Map();
  const chartColors = ["#52e0aa","#78a8ff","#b9a0ff","#f2c66d","#ff778b","#7dd4df"];
  const verdictColors = ["#52e0aa","#f2c66d","#ff778b","#9baec7"];
  const number = n => Number.isFinite(n) ? new Intl.NumberFormat("en-AU",{maximumFractionDigits:0}).format(n) : "—";
  const decimal = n => Number.isFinite(n) ? n.toFixed(1) : "—";
  const percent = (n,d) => d ? `${(n/d*100).toFixed(1)}%` : "—";
  const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;
  const M = CryptoTruthData;
  let data, selected=[], current, dates=[], activeDates=[], pending=false, rowMode=false;
  const state = {year:"all",coin:"all",sub:"all",kind:"all",from:null,to:null};
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
    caption = "Exact values for the selected data"
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


  function empty(key, message) {
    renderChart(key, {type:cards.get(key).type,data:{labels:[],datasets:[]},options:options()},false,message);
    table($(`ix-table-${key}`),["Availability"],[[message]]);
  }
  function drawMatrix(key, value) {
    const o=options();
    o.indexAxis="y";
    o.scales={x:{...axis("Claims",0),stacked:true},y:{...axis(""),stacked:true}};
    const colors=key==="verification" ? ["#9baec7","#52e0aa"] : verdictColors;
    renderChart(key,{type:"bar",data:{labels:value.labels,datasets:value.datasets.map((d,i)=>({
      ...d,backgroundColor:colors[i%colors.length],maxBarThickness:26}))},options:o},value.datasets.some(d=>d.data.some(v=>v>0)));
    table($(`ix-table-${key}`),["Category",...value.datasets.map(d=>d.label)],value.labels.map((l,i)=>[l,...value.datasets.map(d=>number(d.data[i]))]));
  }
  function communityRows() {
    const min=Number($("ix-threshold").value),rank=$("ix-rank").value;
    const metric=r=>rank==="trust" ? r.verification_score : rank==="coverage" ? M.ratio(r.scored_claims,r.claims) : r.claims;
    return current.subreddit_scores.filter(r=>(rowMode || state.sub==="all" || r.subreddit===state.sub) && r.scored_claims>=min)
      .sort((a,b)=>(metric(b)??-1)-(metric(a)??-1) || a.subreddit.localeCompare(b.subreddit));
  }
  function drawCommunities() {
    const rows=communityRows(),rank=$("ix-rank").value;
    const values=rows.map(r=>rank==="trust" ? r.verification_score : rank==="coverage" ? M.ratio(r.scored_claims,r.claims) : r.claims);
    const label=rank==="trust" ? "Verification score" : rank==="coverage" ? "Coverage (%)" : "Claims";
    const o=options();o.indexAxis="y";o.plugins.legend.display=false;
    o.scales={x:axis(label,0,rank==="volume" ? undefined : 100),y:axis("")};
    renderChart("leaderboard",{type:"bar",data:{labels:rows.map(r=>"r/"+r.subreddit),datasets:[{label,data:values,backgroundColor:"#52e0aa",maxBarThickness:26}]},options:o},values.some(Number.isFinite));
    table($("ix-table-leaderboard"),["Subreddit","Claims","Scored","Coverage","Verification score"],rows.map(r=>[r.subreddit,number(r.claims),number(r.scored_claims),percent(r.scored_claims,r.claims),decimal(r.verification_score)]));
  }
  function savedSentiment() {
    const values=data.legacy_results?.sentiment;
    if(!values || typeof values!=="object" || Array.isArray(values)) return null;
    const entries=Object.entries(values);
    return entries.length && entries.every(([k,v])=>typeof k==="string" && Number.isFinite(v) && v>=0) ? {labels:entries.map(x=>x[0]),values:entries.map(x=>x[1])} : null;
  }
  function update() {
    pending=false;
    selected=rowMode ? M.filter(data.claim_records,state) : [];
    current=rowMode ? M.aggregate(selected) : data;
    const o=current.overview,meta=current.meta;
    const count=name=>o.verdict_breakdown.values[o.verdict_breakdown.labels.indexOf(name)] ?? 0;
    const accurate=count("ACCURATE"),approximate=count("APPROXIMATE");
    for(const [id,text] of [["ix-kpi-posts",number(meta.total_claims)],["ix-kpi-comments",number(meta.scored_claims)],
      ["ix-kpi-sentiment",number(meta.not_scored_claims)],["ix-kpi-support",percent(accurate,meta.scored_claims)],
      ["ix-kpi-coverage",percent(meta.scored_claims,meta.total_claims)],["ix-kpi-engagement",decimal(M.ratio(accurate+.5*approximate,meta.scored_claims))]]) $(id).textContent=text;
    const sentiment=savedSentiment();
    if(sentiment) {
      categorical("sentiment",sentiment.labels,sentiment.values,chartColors,"Results");
      table($("ix-sentiment-labels"),["Label","Results","Share"],sentiment.labels.map((l,i)=>[l,number(sentiment.values[i]),percent(sentiment.values[i],M.sum(sentiment.values))]));
    } else {
      empty("sentiment","No saved sentiment results are available.");
      table($("ix-sentiment-labels"),["Availability"],[["No saved sentiment results are available."]]);
    }
    categorical("verdict",o.verdict_breakdown.labels,o.verdict_breakdown.values,verdictColors,"Claims");
    table($("ix-verdict-labels"),["Status","Claims","Share"],current.status_counts.labels.map((l,i)=>[l,number(current.status_counts.values[i]),percent(current.status_counts.values[i],meta.total_claims)]));
    const coinMatrix=M.selectMatrix(o.accuracy_by_coin,rowMode ? "all" : state.coin);
    const coinMode=$("ix-topic-group").value==="coin";
    const grouped=coinMode ? {labels:coinMatrix.labels,values:coinMatrix.labels.map((_,i)=>M.sum(coinMatrix.datasets.map(d=>d.data[i])))} : o.claim_types;
    $("ix-title-topics").textContent=coinMode ? "Scored claims by coin" : "Claims by type";
    categorical("topics",grouped.labels,grouped.values,chartColors,"Claims");
    drawMatrix("divergence",coinMatrix);
    drawMatrix("engagement",o.accuracy_by_type);
    drawCommunities();
    drawMatrix("verification",current.coverage_by_type);
    categorical("coverage",["Unscored","Scored"],[meta.not_scored_claims,meta.scored_claims],["#9baec7","#52e0aa"],"Claims");
    $("ix-threshold-label").textContent=$("ix-threshold").value;
    $("ix-from-label").textContent=state.from || data.meta.date_start || "—";
    $("ix-to-label").textContent=state.to || data.meta.date_end || "—";
    $("ix-status").textContent=rowMode ? `${number(selected.length)} of ${number(data.meta.total_claims)} claims selected. Sentiment remains a separate result set.` :
      `${number(meta.total_claims)} claims in the saved summary. Coin selection: asset charts only. Subreddit selection: community ranking only. Other totals remain overall.`;
    const metadata=[["Source",data.meta.source_file],["Generated",data.meta.generated_at],["Full period",`${data.meta.date_start || "Unknown"} → ${data.meta.date_end || "Unknown"}`],
      ["Full dataset",`${number(data.meta.total_claims)} claims`],["Current scope",rowMode ? `${number(selected.length)} claims` : "Saved overall summary"],
      ["Missing dates",number(data.meta.missing_dates)],["Sentiment cohort",sentiment ? `${number(M.sum(sentiment.values))} saved results` : "Unavailable"],
      ["Verification score",data.meta.trust_formula || "(accurate + 0.5 × approximate) / scored × 100"]];
    $("ix-metadata").replaceChildren(...metadata.flatMap(([k,v])=>[el("dt",k),el("dd",v??"Unavailable")]));
    $("ix-download").disabled=false;
  }
  function schedule() {if(!pending){pending=true;requestAnimationFrame(update);}}
  function resetDates() {
    activeDates=dates.filter(d=>state.year==="all" || d.startsWith(state.year));
    state.from=null;state.to=null;
    $("ix-from").max=$("ix-to").max=String(Math.max(0,activeDates.length-1));
    $("ix-from").value="0";$("ix-to").value=String(Math.max(0,activeDates.length-1));
  }
  function reset() {
    Object.assign(state,{year:"all",coin:"all",sub:"all",kind:"all",from:null,to:null});
    for(const id of ["ix-year","ix-coin","ix-sub"]) $(id).value="all";
    $("ix-threshold").value="0";$("ix-topic-group").value="type";$("ix-rank").value="volume";
    document.querySelectorAll("[data-kind]").forEach(b=>b.setAttribute("aria-pressed",String(b.dataset.kind==="all")));
    resetDates();schedule();
  }
  function download() {
    let rows;
    if(rowMode) rows=[["date","coin","subreddit","claim_type","status","scored","verdict"],...selected.map(r=>[r.date,r.coin,r.subreddit,r.claim_type,r.status,r.scored,r.verdict])];
    else {
      rows=[["scope","category","measure","value"]];
      for(const [key,label] of [["verdict_breakdown","Overall market verdict"],["claim_types","Overall claim types"],["checkable_share","Overall checkability"]])
        data.overview[key].labels.forEach((l,i)=>rows.push([label,l,"Claims",data.overview[key].values[i]]));
      current.status_counts.labels.forEach((l,i)=>rows.push(["Overall verification status",l,"Claims",current.status_counts.values[i]]));
      const coin=M.selectMatrix(data.overview.accuracy_by_coin,state.coin);
      coin.labels.forEach((l,i)=>coin.datasets.forEach(d=>rows.push(["Selected coin charts",l,d.label,d.data[i]])));
      for(const [matrix,scope] of [[data.overview.accuracy_by_type,"Overall accuracy by type"],[data.coverage_by_type,"Overall coverage by type"]])
        matrix.labels.forEach((l,i)=>matrix.datasets.forEach(d=>rows.push([scope,l,d.label,d.data[i]])));
      const sent=savedSentiment();if(sent) sent.labels.forEach((l,i)=>rows.push(["Separate sentiment cohort",l,"Results",sent.values[i]]));
      communityRows().forEach(r=>["claims","scored_claims","verification_score"].forEach(k=>rows.push(["Selected community ranking",r.subreddit,k,r[k]])));
    }
    const quote=value=>{let s=String(value??"");if(/^[=+@\-\t\r]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};
    const url=URL.createObjectURL(new Blob([rows.map(r=>r.map(quote).join(",")).join("\r\n")],{type:"text/csv;charset=utf-8"}));
    const a=document.createElement("a");a.href=url;a.download=rowMode ? "CryptoTruth_filtered_claims.csv" : "CryptoTruth_current_summaries.csv";a.click();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
  }
  function setup() {
    rowMode=Array.isArray(data.claim_records);
    dates=rowMode ? [...new Set(data.claim_records.map(r=>r.date).filter(Boolean))].sort() : [];
    for(const node of document.querySelectorAll("[data-card]")) {
      const key=node.dataset.card;
      cards.set(key,{type:node.querySelector("[data-chart-type]").dataset.chartType,chart:null,table:false});
      node.querySelectorAll("[data-chart-type]").forEach(button=>button.addEventListener("click",()=>{
        cards.get(key).type=button.dataset.chartType;
        node.querySelectorAll("[data-chart-type]").forEach(b=>b.setAttribute("aria-pressed",String(b===button)));schedule();
      }));
      node.querySelector("[data-table]").addEventListener("click",e=>{
        const card=cards.get(key);card.table=!card.table;
        e.currentTarget.textContent=card.table ? "Show chart" : "Show table";
        e.currentTarget.setAttribute("aria-pressed",String(card.table));
        $(`ix-table-${key}`).hidden=!card.table;$(`ix-wrap-${key}`).hidden=card.table;
        if(!card.table && card.chart) requestAnimationFrame(()=>card.chart.resize());
      });
    }
    const coins=rowMode ? [...new Set(data.claim_records.map(r=>r.coin??"Unknown"))].sort() : data.overview.accuracy_by_coin.labels;
    const subs=rowMode ? [...new Set(data.claim_records.map(r=>r.subreddit??"Unknown"))].sort() : data.subreddit_scores.map(r=>r.subreddit);
    for(const [id,items] of [["ix-year",[...new Set(dates.map(d=>d.slice(0,4)))]],["ix-coin",coins],["ix-sub",subs]])
      items.forEach(v=>{const opt=el("option",v);opt.value=v;$(id).append(opt);});
    $("ix-controls").disabled=false;
    for(const id of ["ix-year","ix-from","ix-to"]) $(id).disabled=!rowMode || !dates.length;
    document.querySelectorAll("[data-kind]").forEach(b=>{b.disabled=!rowMode;b.addEventListener("click",()=>{
      state.kind=b.dataset.kind;document.querySelectorAll("[data-kind]").forEach(x=>x.setAttribute("aria-pressed",String(x===b)));schedule();
    });});
    $("ix-filter-hint").textContent=rowMode ? "Filter verified claims" : "Explore saved breakdowns";
    $("ix-scope").textContent=rowMode ? "Year, dates, coin, subreddit and coverage combine across claim charts. Sentiment uses its separate saved results. Undated claims remain included until a date filter is applied." :
      "This export contains summaries. Coin filters apply only to asset charts; subreddit filters apply only to community rankings. Date and scored/unscored filters need claim records and are disabled.";
    $("ix-year").addEventListener("change",()=>{state.year=$("ix-year").value;resetDates();schedule();});
    for(const [id,key] of [["ix-coin","coin"],["ix-sub","sub"]]) $(id).addEventListener("change",()=>{state[key]=$(id).value;schedule();});
    for(const id of ["ix-from","ix-to"]) $(id).addEventListener("input",()=>{
      if(Number($("ix-from").value)>Number($("ix-to").value)) $(id==="ix-from"?"ix-to":"ix-from").value=$(id).value;
      state.from=activeDates[Number($("ix-from").value)]??null;state.to=activeDates[Number($("ix-to").value)]??null;schedule();
    });
    $("ix-threshold").max=String(Math.max(1,...data.subreddit_scores.map(r=>r.scored_claims)));
    $("ix-threshold").addEventListener("input",schedule);
    for(const id of ["ix-topic-group","ix-rank"]) $(id).addEventListener("change",schedule);
    $("ix-reset").addEventListener("click",reset);$("ix-download").addEventListener("click",download);
    const events=[
      ["Hot · TOKEN2049 Singapore","7–8 October 2026","https://www.token2049.com/singapore/faqs"],
      ["EDCON Kuala Lumpur","17–18 October 2026","https://www.edcon.io/en/"],
      ["Ripple Swell","27–29 October 2026","https://swell.ripple.com/"],
      ["Hot · Devcon 8 Mumbai","3–6 November 2026","https://devcon.org/en/"],
      ["Bitcoin Amsterdam","5–6 November 2026","https://www.bitcoin.amsterdam/"],
      ["Hot · Solana Breakpoint","15–17 November 2026","https://solana.com/breakpoint"]];
    events.forEach(([label,date,url])=>{const b=el("button",label);b.type="button";b.dataset.hot=String(label.startsWith("Hot · "));b.title=`${date} · Open official event page`;
      b.setAttribute("aria-label",`${label}, ${date}, opens official page in a new tab`);b.addEventListener("click",()=>window.open(url,"_blank","noopener,noreferrer"));$("ix-events").append(b);});
    resetDates();update();
  }
  async function load() {
    const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),30000);
    try {
      const response=await fetch("/api/dashboard-data",{cache:"no-cache",signal:controller.signal,headers:{Accept:"application/json"}});
      if(!response.ok) throw Error(`Dashboard export unavailable (HTTP ${response.status}).`);
      data=M.validate(await response.json());setup();
    } catch(error) {
      $("ix-status").dataset.error="true";$("ix-status").textContent=error.name==="AbortError" ? "The dashboard request timed out. Reload to retry." : error.message;
      $("ix-controls").disabled=true;$("ix-download").disabled=true;
      document.querySelectorAll(".ix-chart-wrap").forEach(w=>{w.querySelector("canvas").hidden=true;const p=w.querySelector(".ix-empty");p.hidden=false;p.textContent="Real dashboard data is unavailable. No replacement values are shown.";});
    } finally {clearTimeout(timer);}
  }
  load();
})();
