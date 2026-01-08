// tooltip ภายนอก: โชว์ "ทุกแกน" ของ dataset ที่ hover อยู่ และขึ้นหัวข้อเป็นชื่อ legend
const externalTooltipPlugin = {
  id: 'dqExternalTooltip',
  afterInit(chart, args, opts) {
    const el = document.createElement('div');
    el.className = 'dq-tooltip';
    el.style.opacity = '0';
    chart.canvas.parentNode.style.position = 'relative';
    chart.canvas.parentNode.appendChild(el);
    chart.$dqTooltip = el;
  },

  afterEvent(chart, args) {
    const el = chart.$dqTooltip;
    if (!el) return;

    const active = chart.getActiveElements();
    if (!active.length) {
      el.style.opacity = '0';
      return;
    }

    const {datasetIndex, index} = active[0];
    const ds = chart.data.datasets[datasetIndex];

    // --- generate content ---
    let rows = '';
    chart.data.labels.forEach((label, i) => {
      const val = Number(ds.data[i]).toFixed(2) ?? 0;
      rows += `<tr><td>${label}</td><td>${val} %</td></tr>`;
    });

    el.innerHTML = `
      <h4>${ds.label}</h4>
      <table>${rows}</table>
    `;

    // --- position near the hovered point ---
    const meta = chart.getDatasetMeta(datasetIndex);
    const point = meta.data[index]; // element ของจุดที่ hover
    const rect = chart.canvas.getBoundingClientRect();

    const x = point.x + rect.left;
    const y = point.y + rect.top;

    el.style.left = (x - rect.left + 10) + 'px';
    el.style.top  = (y - rect.top  + 10) + 'px';
    el.style.opacity = '1';
    
  }
};

const centerTextPlugin = (value = 1000) => (
  {
    id: 'centerText',
    afterDraw(chart) {
      const { ctx, chartArea } = chart;
      const x = (chartArea.left + chartArea.right) / 2;
      const y = (chartArea.top + chartArea.bottom) / 2;

      ctx.save();
      ctx.font = 'bold 40px Arial';
      ctx.fillStyle = '#000';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(value.toLocaleString(), x, y - 10);

      // ctx.font = '14px Arial';
      // ctx.fillText('Count Timeliness = 0', x, y + 30);
      // ctx.restore();
    }
  }
)
function radarChart(ele, data) {
  return new Chart(ele, {
    type: 'radar',
    data: data,
    options: {
      responsive: true,
      scales: {
        r: {
          suggestedMin: 0,
          suggestedMax: 100,
          // ถ้าต้องการให้ตัวเลขบนแกนเป็น 2 ตำแหน่งด้วย
          ticks: {
            // callback: (v) => Number(v).toFixed(2)
            callback: (v) => `${v} %`
          },
          angleLines: { color: 'rgba(0,0,0,0.08)' }, // ปิดเส้นเชื่อมแกน
          grid: { color: 'rgba(0,0,0,0.06)' }, // ปิดเส้นกริด
        }
      },
      interaction: {
        mode: 'nearest',
        intersect: true
      },
      plugins: {
        tooltip: { enabled: false }, // ปิด tooltip ปกติ
          // callbacks: {
            // ให้ tooltip แสดง 2 ตำแหน่ง
            // label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.raw).toFixed(2)} %`
            // label: (ctx) => `${ctx.label}: ${ctx.formattedValue} %`
            
          // }
        legend: { display: true }
      },
    },
    plugins: [externalTooltipPlugin] // ใช้ plugin ที่สร้างขึ้น
  });
}
function donut(canvasId, centerId, yes, no, label=['ได้','ไม่ได้']) {
  document.getElementById(centerId).textContent = yes;
  return new Chart(document.getElementById(canvasId), {
    type: 'doughnut',
    data: { labels: label, datasets: [{ data: [yes, no], backgroundColor: ['#32C8C8', '#FFC850']}] },
    options: { 
      cutout: '70%', 
      plugins: { 
        legend: { display: false }, 
        tooltip: {
          callbacks: {
            label: function (context) {
              const value = context.raw;
              const percent = ((value / (yes+no)) * 100).toFixed(1);
              return ` ${percent}% (${value})`;
            }
          }
        } 
      }
    }
  });
}

// Availability – donuts
donut('chart-dl', 'center-dl', M.availability.downloadable.yes, M.availability.downloadable.no, ['ดาวน์โหลดได้','ดาวน์โหลดไม่ได้']);
donut('chart-api', 'center-api', M.availability.access_api.yes, M.availability.access_api.no, ['เข้าถึงผ่าน API ได้','เข้าถึงผ่าน API ไม่ได้']);

/* =========================
 * Timeliness (Chart.js 4.5.0)
 * ========================= */

(() => {
  const dqCharts = {}; // เก็บ instance

  function getCtx(id) {
    const el = document.getElementById(id);
    if (!el || !(el instanceof HTMLCanvasElement)) return null;
    return el.getContext('2d');
  }

  function destroyIfExist(key) {
    if (dqCharts[key]) {
      dqCharts[key].destroy();
      dqCharts[key] = null;
    }
  }

  function toPct(v) {
    if (v == null || isNaN(v)) return 0;
    const n = Number(v);
    return n <= 1 ? Math.round(n * 100) : Math.round(n);
  }

  function renderTimeliness() {
    let data;
    try { data = T } 
    catch (e) { console.error('Timeliness API error', e); return; }

    /* ---------- 1) Freshness ---------- */
    const freshnessPct = toPct(data.total_late_update);
    // const badgeF = document.getElementById('badge-freshness');
    // if (badgeF) badgeF.textContent = `Dataset AVG Freshness: ${freshnessPct}%`;

    const fctx = getCtx('chart-freshness');
    if (fctx) {
      destroyIfExist('freshness');

      dqCharts.freshness = new Chart(fctx, {
        type: 'bar',
        data :{
          labels: [''],
          datasets: [{
            data: [freshnessPct],
            backgroundColor: 'rgba(30, 213, 30, 0.72)',
            borderRadius: 0,
            barThickness: 20000,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: { enabled: false}
          },
          scales: {
            x: {
              display: false,
            },
            y: {
              min: 0,
              max: 10000,
              display: false,
            }
          }
        },
        plugins: [centerTextPlugin(freshnessPct)]
      });
      
      
      // const grad = fctx.createLinearGradient(0, 0, fctx.canvas.width, 0);
      // grad.addColorStop(0, '#2e7d32');
      // grad.addColorStop(1, '#a5d6a7');

      // dqCharts.freshness = new Chart(fctx, {
      //   type: 'bar',
      //   data: {
      //     labels: [''],
      //     datasets: [
      //       { 
      //         data: [freshnessPct],
      //         backgroundColor: (ctx) => {
      //           const v = ctx.raw;
      //           if (v < 25) return 'rgba(197, 245, 186, 0.7)';
      //           else if (v < 50) return 'rgba(142, 240, 142, 0.7)';
      //           else if (v < 75) return 'rgba(60, 179, 113, 0.8)';
      //           else return 'rgba(0, 100, 0, 0.9)';
      //         },
      //     }]
      //   },
      //   options: {
      //     indexAxis: 'y',
      //     responsive: true,
      //     maintainAspectRatio: true,
      //     plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => `${c.raw}%` } } },
      //     scales: {
      //       x: {
      //         beginAtZero: true, max: 100,
      //         reverse: true,
      //         ticks: { callback: (v) => `${v}%` },
      //         grid: { display: true }
      //       },
      //       y: { display: false }
      //     }
      //   }
      // });
    }

    /* ---------- 2) Acceptable Latency ---------- */
    const order = ['ล่าช้าค่อนข้างมาก','ล่าช้าป่านกลาง','ล่าช้าเล็กน้อย'];
    const counts = order.map(k => Number(data.latency_buckets?.[k] || 0));
    // const badgeL = document.getElementById('badge-latmax');
    // if (badgeL) badgeL.textContent = `Dataset Acceptable Latency MAX: ${data.max_latency ?? 0}`;

    const lctx = getCtx('chart-latency');
    if (lctx) {
      destroyIfExist('latency');
      const colors = ['#f27c21ff', '#ec9654ff', '#e2b38fff'];
      dqCharts.latency = new Chart(lctx, {
        type: 'bar',
        data: { labels: order, datasets: [{ data: counts, backgroundColor: colors }] },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `${ctx.raw}` } } },
          scales: {
            x: { beginAtZero: true, ticks: { precision: 0 } },
            y: { ticks: { autoSkip: false } }
          }
        }
      });
    }

    /* ---------- 3) Outdated ---------- */
    const octx = getCtx('chart-outdated');
    if (octx) {
      destroyIfExist('outdated');
      dqCharts.outdated = new Chart(octx, {
        type: 'bar',
        data: { labels: [''], datasets: [{ data: [Number(data.outdated_count || 0)], backgroundColor: '#e53935' }] },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => `${c.raw}` } } },
          scales: {
            x: { beginAtZero: true, ticks: { precision: 0 } },
            y: { display: false }
          }
        }
      });
    }
  }
  renderTimeliness();
})();
(async () => {

  const ctx = document.getElementById('dqmFormatBar').getContext('2d');
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'จำนวนไฟล์',
        data,
        borderWidth: 2,
        borderRadius: 10,
        // backgroundColor: '#7bd489' // โทนเขียวคล้ายตัวอย่าง
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (c) => `${c.parsed.y ?? 0}` } }
      },
      scales: {
        x: { grid: { display: false } },
        y: { beginAtZero: true, ticks: { precision: 0 } }
      }
    }
  });
})();

