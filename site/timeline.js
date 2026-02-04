let timelinePayload = null;
let timelineChart = null;
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
  }
}

loadTimeline();
