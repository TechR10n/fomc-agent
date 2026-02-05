let timelinePayload = null;
let timelineChart = null;
let awsObsPayload = null;
let awsMetricChart = null;
let costChart = null;
let awsCorrelationChart = null;
const ET_TIME_ZONE = "America/New_York";

function actionBadge(action) {
  if (action === "updated") return '<span class="badge badge-green">updated</span>';
  if (action === "added") return '<span class="badge badge-blue">added</span>';
  if (action === "deleted") return '<span class="badge badge-red">deleted</span>';
  return `<span class="badge badge-gray">${action}</span>`;
}

function statusBadge(status) {
  if (status === "on_time") return '<span class="badge badge-green">on time</span>';
  if (status === "late") return '<span class="badge badge-amber">late</span>';
  if (status === "early") return '<span class="badge badge-blue">early</span>';
  if (status === "missing") return '<span class="badge badge-red">missing</span>';
  if (status === "upcoming") return '<span class="badge badge-gray">upcoming</span>';
  return `<span class="badge badge-gray">${status}</span>`;
}

function formatBytes(bytes) {
  if (bytes === null || bytes === undefined) return "—";
  const n = Number(bytes);
  if (Number.isNaN(n)) return "—";
  bytes = n;
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / 1048576).toFixed(1) + " MB";
}

function parseTime(s) {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function formatEt(iso) {
  const d = parseTime(iso);
  if (!d) return "—";
  return new Intl.DateTimeFormat(undefined, {
    timeZone: ET_TIME_ZONE,
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(d);
}

function utcDateKey(d) {
  // "YYYY-MM-DD" in UTC
  return d.toISOString().slice(0, 10);
}

function daysBetween(startUtc, endUtc) {
  const ms = endUtc.getTime() - startUtc.getTime();
  return Math.floor(ms / 86400000);
}

function buildDayKeys(startUtc, endUtc) {
  const totalDays = daysBetween(startUtc, endUtc);
  const keys = [];
  for (let i = 0; i <= totalDays; i++) {
    const d = new Date(startUtc.getTime() + i * 86400000);
    keys.push(utcDateKey(d));
  }
  return keys;
}

function getSeriesList(events, releases) {
  const series = [];
  for (const e of (events || [])) series.push(e.series);
  for (const r of (releases || [])) series.push(r.series);
  return Array.from(new Set(series)).filter(Boolean).sort();
}

function filteredEvents() {
  if (!timelinePayload) return [];
  const series = document.getElementById("filter-series").value;
  const events = timelinePayload.events || [];
  if (!series || series === "all") return events;
  return events.filter(e => e.series === series);
}

function filteredReleases() {
  if (!timelinePayload) return [];
  const series = document.getElementById("filter-series").value;
  const releases = timelinePayload.releases || [];
  if (!series || series === "all") return releases;
  return releases.filter(r => r.series === series);
}

function updateSummary(events) {
  document.getElementById("stat-events").textContent = events.length.toLocaleString();

  const seriesTouched = new Set(events.map(e => e.series).filter(Boolean));
  document.getElementById("stat-series").textContent = seriesTouched.size.toLocaleString();

  const latest = events.length ? parseTime(events[0].event_time) : null;
  document.getElementById("stat-latest").textContent = latest ? latest.toLocaleDateString() : "—";
}

function renderEventsTable(events) {
  const el = document.getElementById("events");
  if (!events.length) {
    el.innerHTML = '<p class="muted">No change events in this window.</p>';
    return;
  }

  el.innerHTML = `
    <table>
      <tr><th>Time</th><th>Series</th><th>File</th><th>Status</th><th>Size</th></tr>
      ${events.map(e => {
        const t = parseTime(e.event_time);
        const seriesUrl = `https://download.bls.gov/pub/time.series/${e.series}/`;
        const timeLabel = t ? formatEt(e.event_time) : "—";
        return `
          <tr>
            <td class="muted">${timeLabel}</td>
            <td><a href="${seriesUrl}" target="_blank" rel="noopener"><code>${e.series}</code></a></td>
            <td><code>${e.file}</code></td>
            <td>${actionBadge(e.action)}</td>
            <td class="muted">${formatBytes(e.bytes)}</td>
          </tr>
        `;
      }).join("")}
    </table>
    <p class="muted">Note: “deleted” events have no BLS source timestamp, so the time shown is when the pipeline detected the deletion.</p>
  `;
}

function renderChart(events) {
  const status = document.getElementById("status-timeline");

  if (!timelinePayload) return;
  const windowDays = timelinePayload.window_days || 60;
  const lookaheadDays = timelinePayload.lookahead_days || 0;
  const generatedAt = parseTime(timelinePayload.generated_at);
  const endUtc = generatedAt ? new Date(generatedAt.getTime() + lookaheadDays * 86400000) : new Date();
  const startUtc = new Date(endUtc.getTime());
  startUtc.setUTCDate(startUtc.getUTCDate() - windowDays - lookaheadDays);

  const dayKeys = buildDayKeys(startUtc, endUtc);
  const counts = {};
  for (const e of events) {
    const dt = parseTime(e.event_time);
    if (!dt) continue;
    const key = utcDateKey(dt);
    counts[key] = (counts[key] || 0) + 1;
  }

  const labels = dayKeys.map(k => {
    const d = new Date(k + "T00:00:00Z");
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  });
  const values = dayKeys.map(k => counts[k] || 0);

  const releases = filteredReleases();
  const releaseCounts = {};
  for (const r of releases) {
    const dt = parseTime(r.scheduled_time);
    if (!dt) continue;
    const key = utcDateKey(dt);
    releaseCounts[key] = (releaseCounts[key] || 0) + 1;
  }
  const releaseValues = dayKeys.map(k => releaseCounts[k] || 0);

  const ctx = document.getElementById("timeline-chart");
  if (timelineChart) {
    timelineChart.destroy();
    timelineChart = null;
  }

  timelineChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Changed files",
        data: values,
        backgroundColor: "rgba(59, 130, 246, 0.25)",
        borderColor: "rgb(59, 130, 246)",
        borderWidth: 1,
      }, {
        type: "line",
        label: "Scheduled releases",
        data: releaseValues,
        borderColor: "rgb(245, 158, 11)",
        backgroundColor: "rgba(245, 158, 11, 0.15)",
        pointRadius: 2,
        pointHoverRadius: 4,
        tension: 0.2,
      }],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: true, labels: { boxWidth: 12 } },
        title: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return dayKeys[idx] || "";
            },
          },
        },
      },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });

  status.textContent = `Loaded ${events.length} change event(s) and ${releases.length} scheduled release(s).`;
}

function computeReleaseStatus(item, now) {
  const scheduled = parseTime(item.scheduled_time);
  const actual = parseTime(item.actual_time);
  if (scheduled && scheduled > now && !actual) return "upcoming";
  if (!actual) return "missing";
  const delay = item.delay_minutes;
  if (delay === null || delay === undefined) return "unknown";
  if (Math.abs(delay) <= 5) return "on_time";
  if (delay > 5) return "late";
  if (delay < -5) return "early";
  return "unknown";
}

function formatDelay(delayMinutes) {
  if (delayMinutes === null || delayMinutes === undefined) return "—";
  const m = Number(delayMinutes);
  if (Number.isNaN(m)) return "—";
  const rounded = Math.round(m);
  if (rounded === 0) return "0m";
  return (rounded > 0 ? `+${rounded}m` : `${rounded}m`);
}

function renderReleasesTable(releases) {
  const card = document.getElementById("releases-card");
  const el = document.getElementById("releases");
  if (!card || !el) return;

  if (!releases.length) {
    card.style.display = "none";
    return;
  }
  card.style.display = "block";

  const now = new Date();
  el.innerHTML = `
    <table>
      <tr><th>Scheduled (ET)</th><th>Actual (ET)</th><th>Δ</th><th>Release</th><th>Series</th><th>Status</th></tr>
      ${releases.map(r => {
        const status = computeReleaseStatus(r, now);
        const scheduled = formatEt(r.scheduled_time);
        const actual = r.actual_time ? formatEt(r.actual_time) : "—";
        const delta = formatDelay(r.delay_minutes);
        const releaseName = r.release || "—";
        const link = r.url ? `<a href="${r.url}" target="_blank" rel="noopener">${releaseName}</a>` : releaseName;
        return `
          <tr>
            <td class="muted">${scheduled}</td>
            <td class="muted">${actual}</td>
            <td class="muted">${delta}</td>
            <td>${link}</td>
            <td><code>${r.series}</code></td>
            <td>${statusBadge(status)}</td>
          </tr>
        `;
      }).join("")}
    </table>
  `;
}

function renderAll() {
  const events = filteredEvents();
  const releases = filteredReleases();
  updateSummary(events);
  renderChart(events);
  renderReleasesTable(releases);
  renderEventsTable(events);
  renderAwsCorrelationChart();
}

function groupSeries(series) {
  const groups = {};
  for (const s of (series || [])) {
    const g = s.group || "Other";
    if (!groups[g]) groups[g] = [];
    groups[g].push(s);
  }
  return groups;
}

function findSeriesById(series, id) {
  if (!id) return null;
  for (const s of (series || [])) {
    if (s.id === id) return s;
  }
  return null;
}

function lastNonNullValue(values, dates) {
  if (!Array.isArray(values) || !Array.isArray(dates)) return null;
  const n = Math.min(values.length, dates.length);
  for (let i = n - 1; i >= 0; i--) {
    const v = values[i];
    if (v === null || v === undefined) continue;
    const num = Number(v);
    if (Number.isNaN(num)) continue;
    return { date: dates[i], value: num };
  }
  return null;
}

function formatMetricValue(value, unit) {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (Number.isNaN(num)) return "—";
  if (unit === "Bytes") return formatBytes(num);
  if (unit === "Milliseconds") return `${Math.round(num).toLocaleString()} ms`;
  if (unit === "Seconds") return `${Math.round(num).toLocaleString()} s`;
  if (unit === "Count") return Math.round(num).toLocaleString();
  return num.toLocaleString();
}

function formatMoney(value, currency) {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (Number.isNaN(num)) return "—";
  const ccy = currency || "USD";
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: ccy,
      maximumFractionDigits: 4,
    }).format(num);
  } catch {
    return `${ccy} ${num.toFixed(4)}`;
  }
}

function pickPreferredName(names, preferred) {
  const list = Array.isArray(names) ? names : [];
  for (const p of (preferred || [])) {
    if (list.includes(p)) return p;
  }
  return list.length ? list[0] : null;
}

function toNumericAligned(values, dates) {
  const out = [];
  const arr = Array.isArray(values) ? values : [];
  const n = Array.isArray(dates) ? dates.length : 0;
  for (let i = 0; i < n; i++) {
    const v = i < arr.length ? arr[i] : null;
    if (v === null || v === undefined) {
      out.push(null);
      continue;
    }
    const num = Number(v);
    out.push(Number.isNaN(num) ? null : num);
  }
  return out;
}

function normalizeToPercent(values) {
  const arr = Array.isArray(values) ? values : [];
  let max = 0;
  for (const v of arr) {
    if (v === null || v === undefined) continue;
    const num = Number(v);
    if (Number.isNaN(num)) continue;
    if (num > max) max = num;
  }
  if (!max) {
    return { max: 0, values: arr.map(v => (v === null || v === undefined ? null : 0)) };
  }
  return {
    max,
    values: arr.map(v => {
      if (v === null || v === undefined) return null;
      const num = Number(v);
      if (Number.isNaN(num)) return null;
      return (num / max) * 100;
    }),
  };
}

function buildDailyChangeCounts(events, dates) {
  const counts = {};
  for (const e of (events || [])) {
    const dt = parseTime(e.event_time);
    if (!dt) continue;
    const key = utcDateKey(dt);
    counts[key] = (counts[key] || 0) + 1;
  }
  return (dates || []).map(d => counts[d] || 0);
}

function renderAwsCorrelationChart() {
  const status = document.getElementById("status-aws-correlation");
  const canvas = document.getElementById("aws-correlation-chart");
  if (!status || !canvas) return;

  if (!timelinePayload) {
    status.textContent = "Waiting for timeline data…";
    return;
  }
  if (!awsObsPayload) {
    status.textContent = "Waiting for AWS observability data…";
    return;
  }

  const dates = awsObsPayload?.metric_dates || [];
  if (!Array.isArray(dates) || !dates.length) {
    status.textContent = "No AWS metric dates available.";
    return;
  }

  const labels = dates.map(d => new Date(d + "T00:00:00Z").toLocaleDateString(undefined, { month: "short", day: "numeric" }));

  const events = filteredEvents();
  const changesRaw = buildDailyChangeCounts(events, dates);
  const changesNorm = normalizeToPercent(changesRaw);

  const resources = awsObsPayload?.resources || {};
  const queue = pickPreferredName(resources.sqs_queues, ["fomc-analytics-queue"]);
  const fetcherFn = pickPreferredName(resources.lambda_functions, ["fomc-data-fetcher"]);
  const analyticsFn = pickPreferredName(resources.lambda_functions, ["fomc-analytics-processor"]);

  const series = awsObsPayload?.metrics?.series || [];

  const datasets = [];
  datasets.push({
    type: "bar",
    label: "Source changes (BLS files)",
    data: changesNorm.values,
    rawValues: changesRaw,
    rawUnit: "Count",
    backgroundColor: "rgba(59, 130, 246, 0.25)",
    borderColor: "rgb(59, 130, 246)",
    borderWidth: 1,
    yAxisID: "y",
  });

  const wanted = [
    {
      id: queue ? `sqs.${queue}.NumberOfMessagesSent.Sum` : null,
      label: "SQS messages sent",
      color: "rgb(147, 51, 234)",
      unit: "Count",
      dash: null,
    },
    {
      id: analyticsFn ? `lambda.${analyticsFn}.Invocations.Sum` : null,
      label: "Analytics invocations",
      color: "rgb(16, 185, 129)",
      unit: "Count",
      dash: null,
    },
    {
      id: analyticsFn ? `lambda.${analyticsFn}.Duration.Sum` : null,
      label: "Analytics duration (sum)",
      color: "rgb(245, 158, 11)",
      unit: "Milliseconds",
      dash: [6, 4],
    },
    {
      id: fetcherFn ? `lambda.${fetcherFn}.Duration.Sum` : null,
      label: "Fetcher duration (sum)",
      color: "rgb(100, 116, 139)",
      unit: "Milliseconds",
      dash: [6, 4],
    },
  ];

  const missing = [];
  for (const w of wanted) {
    if (!w.id) {
      missing.push(w.label);
      continue;
    }
    const s = findSeriesById(series, w.id);
    const raw = toNumericAligned(s?.values, dates);
    const hasAny = raw.some(v => v !== null && v !== undefined && !Number.isNaN(Number(v)));
    if (!hasAny) {
      missing.push(w.label);
      continue;
    }
    const norm = normalizeToPercent(raw);
    datasets.push({
      type: "line",
      label: w.label,
      data: norm.values,
      rawValues: raw,
      rawUnit: w.unit,
      borderColor: w.color,
      backgroundColor: w.color.replace("rgb(", "rgba(").replace(")", ", 0.10)"),
      borderDash: w.dash || undefined,
      pointRadius: 2,
      pointHoverRadius: 4,
      tension: 0.2,
      spanGaps: true,
      yAxisID: "y",
    });
  }

  if (awsCorrelationChart) {
    awsCorrelationChart.destroy();
    awsCorrelationChart = null;
  }

  awsCorrelationChart = new Chart(canvas, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: true, labels: { boxWidth: 12 } },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return dates[idx] || "";
            },
            label: (ctx) => {
              const ds = ctx.dataset || {};
              const idx = ctx.dataIndex;
              const rawValues = ds.rawValues || [];
              const rawUnit = ds.rawUnit || "Count";
              const raw = idx < rawValues.length ? rawValues[idx] : null;
              const pct = ctx.parsed?.y;
              const pctNum = pct === null || pct === undefined ? null : Number(pct);
              const pctLabel = pctNum === null || Number.isNaN(pctNum) ? "—" : `${pctNum.toFixed(0)}%`;
              return `${ds.label}: ${formatMetricValue(raw, rawUnit)} (${pctLabel})`;
            },
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          max: 100,
          ticks: { callback: v => `${v}%` },
        },
      },
    },
  });

  const start = dates[0];
  const end = dates[dates.length - 1];
  const suffix = missing.length ? ` · Missing: ${missing.join(", ")}` : "";
  status.textContent = `Normalized view (${start} → ${end})${suffix}`;
}

function setAwsMetricSelectOptions(series) {
  const select = document.getElementById("aws-metric-select");
  if (!select) return;
  if (!Array.isArray(series) || !series.length) {
    select.innerHTML = '<option value="">No metrics available</option>';
    return;
  }

  const groups = groupSeries(series);
  const groupNames = Object.keys(groups).sort();

  select.innerHTML = groupNames.map(g => {
    const items = (groups[g] || []).slice().sort((a, b) => (a.label || "").localeCompare(b.label || ""));
    const options = items.map(s => `<option value="${s.id}">${s.label}${s.stat ? ` (${s.stat})` : ""}</option>`).join("");
    return `<optgroup label="${g}">${options}</optgroup>`;
  }).join("");

  const preferred = [
    "lambda.fomc-data-fetcher.Duration.Sum",
    "lambda.fomc-analytics-processor.Invocations.Sum",
    "sqs.fomc-analytics-queue.NumberOfMessagesSent.Sum",
    "lambda.fomc-data-fetcher.Invocations.Sum",
  ];
  const ids = new Set(series.map(s => s.id));
  const fallback = series[0]?.id;
  const initial = preferred.find(id => ids.has(id)) || fallback;
  if (initial) select.value = initial;
}

function renderAwsMetricChart(metricId) {
  const status = document.getElementById("status-aws-metrics");
  const canvas = document.getElementById("aws-metrics-chart");
  if (!status || !canvas) return;

  const dates = awsObsPayload?.metric_dates || [];
  const series = awsObsPayload?.metrics?.series || [];
  const s = findSeriesById(series, metricId);
  if (!s) {
    status.textContent = "Select a metric to view.";
    return;
  }

  const labels = dates.map(d => new Date(d + "T00:00:00Z").toLocaleDateString(undefined, { month: "short", day: "numeric" }));
  const values = Array.isArray(s.values) ? s.values.map(v => (v === null || v === undefined ? null : Number(v))) : [];
  const hasAny = values.some(v => v !== null && !Number.isNaN(v));

  if (awsMetricChart) {
    awsMetricChart.destroy();
    awsMetricChart = null;
  }

  awsMetricChart = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: `${s.group} — ${s.label}`,
        data: values,
        borderColor: "rgb(59, 130, 246)",
        backgroundColor: "rgba(59, 130, 246, 0.15)",
        pointRadius: 2,
        pointHoverRadius: 4,
        tension: 0.2,
        spanGaps: true,
      }],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: true, labels: { boxWidth: 12 } },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return dates[idx] || "";
            },
            label: (ctx) => {
              const v = ctx.parsed?.y;
              return `${s.label}: ${formatMetricValue(v, s.unit)}`;
            },
          },
        },
      },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });

  const latest = lastNonNullValue(values, dates);
  if (!hasAny) {
    status.textContent = `No CloudWatch datapoints found for ${s.group} — ${s.label}.`;
  } else if (latest) {
    status.textContent = `Latest: ${latest.date} · ${formatMetricValue(latest.value, s.unit)}`;
  } else {
    status.textContent = `Loaded metric: ${s.group} — ${s.label}`;
  }
}

function renderAwsMetricsTable() {
  const container = document.getElementById("aws-metrics-table");
  const status = document.getElementById("status-aws-metrics");
  if (!container) return;

  const dates = awsObsPayload?.metric_dates || [];
  const series = (awsObsPayload?.metrics?.series || []).slice();
  if (!series.length) {
    container.innerHTML = '<p class="muted">No AWS metrics available.</p>';
    return;
  }

  series.sort((a, b) => {
    const g = (a.group || "").localeCompare(b.group || "");
    if (g !== 0) return g;
    return (a.label || "").localeCompare(b.label || "");
  });

  const rows = series.map(s => {
    const latest = lastNonNullValue(s.values || [], dates);
    return {
      group: s.group || "—",
      label: s.label || s.metric || "—",
      stat: s.stat || "—",
      unit: s.unit || "—",
      latestDate: latest?.date || null,
      latestValue: latest?.value ?? null,
    };
  });

  container.innerHTML = `
    <table>
      <tr><th>Resource</th><th>Metric</th><th>Stat</th><th>Latest</th><th>Date</th></tr>
      ${rows.map(r => `
        <tr>
          <td><code>${r.group}</code></td>
          <td>${r.label}</td>
          <td class="muted">${r.stat}</td>
          <td class="muted">${formatMetricValue(r.latestValue, r.unit)}</td>
          <td class="muted">${r.latestDate || "—"}</td>
        </tr>
      `).join("")}
    </table>
  `;

  const errs = awsObsPayload?.errors || [];
  if (Array.isArray(errs) && errs.length && status) {
    status.textContent = `Loaded AWS observability with ${errs.length} error(s). See console for details.`;
    try { console.warn("AWS observability export errors:", errs); } catch {}
  }
}

function renderCostChart() {
  const status = document.getElementById("status-cost");
  const canvas = document.getElementById("cost-chart");
  if (!status || !canvas) return;

  const cost = awsObsPayload?.cost || null;
  const dates = cost?.dates || [];
  if (!Array.isArray(dates) || !dates.length) {
    status.textContent = "No cost data available.";
    return;
  }

  const labels = dates.map(d => new Date(d + "T00:00:00Z").toLocaleDateString(undefined, { month: "short", day: "numeric" }));
  const actual = Array.isArray(cost.actual) ? cost.actual.map(v => (v === null || v === undefined ? null : Number(v))) : [];
  const predicted = Array.isArray(cost.predicted) ? cost.predicted.map(v => (v === null || v === undefined ? null : Number(v))) : [];
  const currency = cost.currency || "USD";

  if (costChart) {
    costChart.destroy();
    costChart = null;
  }

  costChart = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Actual cost",
        data: actual,
        borderColor: "rgb(59, 130, 246)",
        backgroundColor: "rgba(59, 130, 246, 0.15)",
        pointRadius: 2,
        pointHoverRadius: 4,
        tension: 0.2,
        spanGaps: true,
      }, {
        label: "Predicted cost",
        data: predicted,
        borderColor: "rgb(245, 158, 11)",
        backgroundColor: "rgba(245, 158, 11, 0.10)",
        borderDash: [6, 4],
        pointRadius: 2,
        pointHoverRadius: 4,
        tension: 0.2,
        spanGaps: true,
      }],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: true, labels: { boxWidth: 12 } },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return dates[idx] || "";
            },
            label: (ctx) => `${ctx.dataset.label}: ${formatMoney(ctx.parsed?.y, currency)}`,
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: v => formatMoney(v, currency),
          },
        },
      },
    },
  });

  const latestActual = lastNonNullValue(actual, dates);
  const latestPred = lastNonNullValue(predicted, dates);
  if (latestActual && latestPred) {
    status.textContent = `Latest actual: ${latestActual.date} · ${formatMoney(latestActual.value, currency)} · Next predicted: ${latestPred.date} · ${formatMoney(latestPred.value, currency)}`;
  } else if (latestActual) {
    status.textContent = `Latest actual: ${latestActual.date} · ${formatMoney(latestActual.value, currency)}`;
  } else if (latestPred) {
    status.textContent = `Next predicted: ${latestPred.date} · ${formatMoney(latestPred.value, currency)}`;
  } else {
    status.textContent = "Loaded cost series.";
  }
}

function renderAwsObservability() {
  const series = awsObsPayload?.metrics?.series || [];
  setAwsMetricSelectOptions(series);
  const select = document.getElementById("aws-metric-select");
  if (select) {
    select.addEventListener("change", () => renderAwsMetricChart(select.value));
    renderAwsMetricChart(select.value);
  }
  renderAwsMetricsTable();
  renderCostChart();
  renderAwsCorrelationChart();
}

async function loadAwsObservability() {
  const statusMetrics = document.getElementById("status-aws-metrics");
  const statusCost = document.getElementById("status-cost");
  const statusCorrelation = document.getElementById("status-aws-correlation");
  try {
    const resp = await fetch("data/aws_observability.json", { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    awsObsPayload = await resp.json();
    renderAwsObservability();
  } catch (e) {
    if (statusMetrics) {
      statusMetrics.textContent =
        `AWS observability data not found. Generate it with: python3 tools/build_aws_observability.py --out site/data/aws_observability.json (${e})`;
    }
    if (statusCost) {
      statusCost.textContent =
        `AWS cost data not found. Generate it with: python3 tools/build_aws_observability.py --out site/data/aws_observability.json (${e})`;
    }
    if (statusCorrelation) {
      statusCorrelation.textContent =
        `AWS observability data not found. Generate it with: python3 tools/build_aws_observability.py --out site/data/aws_observability.json (${e})`;
    }
  }
}

async function loadTimeline() {
  const status = document.getElementById("status-timeline");
  try {
    const resp = await fetch("data/bls_timeline.json", { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    timelinePayload = await resp.json();

    const events = (timelinePayload.events || []).slice();
    // Ensure newest-first ordering
    events.sort((a, b) => (a.event_time < b.event_time ? 1 : -1));
    timelinePayload.events = events;

    const releases = (timelinePayload.releases || []).slice();
    releases.sort((a, b) => (a.scheduled_time > b.scheduled_time ? 1 : -1));
    timelinePayload.releases = releases;

    const windowDays = timelinePayload.window_days || 60;
    document.getElementById("subtitle").textContent =
      `Trailing ${windowDays} days of changes, using the “Last Modified” timestamps shown on each BLS time-series directory page.`;

    // Build filter list
    const select = document.getElementById("filter-series");
    const seriesList = getSeriesList(events, releases);
    select.innerHTML = `<option value="all">All series</option>` +
      seriesList.map(s => `<option value="${s}">${s}</option>`).join("");
    select.addEventListener("change", renderAll);

    renderAll();
  } catch (e) {
    status.textContent = `Failed to load timeline: ${e}`;
    const statusCorrelation = document.getElementById("status-aws-correlation");
    if (statusCorrelation) statusCorrelation.textContent = `Waiting for timeline data… (${e})`;
  }
}

loadTimeline();
loadAwsObservability();
