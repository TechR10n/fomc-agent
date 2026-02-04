/**
 * Generic dual-axis chart renderer for BLS time series data.
 *
 * Supports two JSON shapes:
 *   1. Legacy (timeseries.json): { title, points: [{year, population, bls_value}] }
 *   2. Generic: { title, description, y_left: {label, key, color}, y_right: {label, key, color}, points: [...] }
 */

const CHARTS = [
  { file: "data/timeseries.json", id: "timeseries", legacy: true },
  { file: "data/productivity_vs_compensation.json", id: "productivity_vs_compensation" },
  { file: "data/productivity_vs_costs.json", id: "productivity_vs_costs" },
  { file: "data/manufacturing_vs_nonfarm.json", id: "manufacturing_vs_nonfarm" },
];

async function loadChart(config) {
  const status = document.getElementById(`status-${config.id}`);
  const card = document.getElementById(`card-${config.id}`);
  try {
    const resp = await fetch(config.file, { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const payload = await resp.json();
    const points = payload.points || [];

    // Insert description if present
    if (payload.description && card) {
      const desc = document.createElement("p");
      desc.className = "description";
      desc.textContent = payload.description;
      card.insertBefore(desc, card.querySelector("canvas"));
    }

    let chartConfig;

    if (config.legacy) {
      // Original timeseries.json shape
      chartConfig = {
        type: "line",
        data: {
          labels: points.map(p => p.year),
          datasets: [
            {
              label: "US Population",
              data: points.map(p => p.population),
              yAxisID: "yLeft",
              borderColor: "rgb(59, 130, 246)",
              backgroundColor: "rgba(59, 130, 246, 0.15)",
              spanGaps: true,
            },
            {
              label: "PRS30006032 (Q01) value",
              data: points.map(p => p.bls_value),
              yAxisID: "yRight",
              borderColor: "rgb(16, 185, 129)",
              backgroundColor: "rgba(16, 185, 129, 0.15)",
              spanGaps: true,
            },
          ],
        },
        options: {
          responsive: true,
          interaction: { mode: "index", intersect: false },
          plugins: {
            title: { display: true, text: payload.title || "Time Series" },
          },
          scales: {
            yLeft: {
              type: "linear", position: "left",
              ticks: { callback: v => v.toLocaleString() },
              title: { display: true, text: "Population" },
            },
            yRight: {
              type: "linear", position: "right",
              grid: { drawOnChartArea: false },
              title: { display: true, text: "BLS value" },
            },
          },
        },
      };
    } else {
      // Generic dual-axis shape
      const yL = payload.y_left;
      const yR = payload.y_right;
      chartConfig = {
        type: "line",
        data: {
          labels: points.map(p => p.year),
          datasets: [
            {
              label: yL.label,
              data: points.map(p => p[yL.key]),
              yAxisID: "yLeft",
              borderColor: yL.color,
              backgroundColor: yL.color + "26",
              spanGaps: true,
            },
            {
              label: yR.label,
              data: points.map(p => p[yR.key]),
              yAxisID: "yRight",
              borderColor: yR.color,
              backgroundColor: yR.color + "26",
              spanGaps: true,
            },
          ],
        },
        options: {
          responsive: true,
          interaction: { mode: "index", intersect: false },
          plugins: {
            title: { display: true, text: payload.title || "Time Series" },
          },
          scales: {
            yLeft: {
              type: "linear", position: "left",
              ticks: { callback: v => v.toLocaleString() },
              title: { display: true, text: yL.label },
            },
            yRight: {
              type: "linear", position: "right",
              grid: { drawOnChartArea: false },
              title: { display: true, text: yR.label },
            },
          },
        },
      };
    }

    const ctx = document.getElementById(`chart-${config.id}`);
    new Chart(ctx, chartConfig);
    status.textContent = `Loaded ${points.length} points from ${config.file}`;
  } catch (e) {
    status.textContent = `Failed to load ${config.file}: ${e}`;
  }
}

// Load all charts concurrently
Promise.all(CHARTS.map(loadChart));
