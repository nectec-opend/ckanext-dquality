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

const centerValuePlugin = (value=null, color=null, fontOptions=null) => ({
  id: 'centerValue',
  afterDatasetsDraw(chart) {
    const { ctx } = chart;
    const meta = chart.getDatasetMeta(0);
    if (!meta || !meta.data.length) return;

    const bar = meta.data[0];
    const x = bar.x;
    const y = bar.y + (bar.base - bar.y) / 2;

    ctx.save();
    ctx.font = fontOptions ? fontOptions : 'bold 16px Arial';
    ctx.fillStyle = color ? color : '#000';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(value.toLocaleString(), x, y);
    ctx.restore();
  }
});

// function centerValueHorizontalPlugin(legacyValue = null) {
//   return {
//     id: 'centerValueHorizontal',
//     afterDatasetsDraw(chart) {
//       const opt = chart.options.plugins?.centerValue || {};
//       if (opt.enabled === false) return;

//       const datasetIndex = opt.datasetIndex ?? 0;
//       const color = opt.color ?? '#000';
//       const font = opt.font ?? 'bold 13px sans-serif';

//       const meta = chart.getDatasetMeta(datasetIndex);
//       const dataset = chart.data.datasets[datasetIndex];
//       if (!meta || !dataset) return;

//       const { ctx } = chart;
//       ctx.save();
//       ctx.fillStyle = color;
//       ctx.font = font;
//       ctx.textAlign = 'center';
//       ctx.textBaseline = 'middle';

//       meta.data.forEach((bar, i) => {
//         let value =
//           legacyValue !== null && legacyValue !== undefined
//             ? legacyValue
//             : dataset.data?.[i];

//         if (value === null || value === undefined) return;

//         const n = Number(value);
//         if (!Number.isFinite(n)) return;

//         const text = String(Math.round(n));

//         // ctx.fillText(text, bar.x / 2, bar.y);
//         const x =
//           n === 0
//             ? (chartArea.left + chartArea.right) / 2 // กลาง chart
//             : bar.x / 2;                              // กลางแท่งเดิม

//         ctx.fillText(text, x, bar.y);
//       });

//       ctx.restore();
//     }
//   };
// }

function centerValueHorizontalPlugin(legacyValue = null) {

  // ⭐ helper กัน font ผิด
  function safeFont(font) {
    if (typeof font !== 'string') return '13px sans-serif';
    return font.replace(/\bunderline\b/g, '').trim();
  }

  return {
    id: 'centerValueHorizontal',
    afterDatasetsDraw(chart) {
      const opt = chart.options.plugins?.centerValue || {};
      if (opt.enabled === false) return;

      const datasetIndex = opt.datasetIndex ?? 0;
      const color = opt.color ?? '#000';
      const font = opt.font ?? '13px sans-serif';

      const meta = chart.getDatasetMeta(datasetIndex);
      const dataset = chart.data.datasets[datasetIndex];
      const chartArea = chart.chartArea;
      if (!meta || !dataset || !chartArea) return;

      const { ctx } = chart;
      ctx.save();
      ctx.fillStyle = color;
      ctx.font = safeFont(font);
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';

      const MIN_BAR_LENGTH = 40;

      meta.data.forEach((bar, i) => {
        let value =
          legacyValue !== null && legacyValue !== undefined
            ? legacyValue
            : dataset.data?.[i];

        const n = Number(value);
        if (!Number.isFinite(n)) return;

        let x;
        const barLength = bar.x - chartArea.left;

        if (n === 0) {
          // x = (chartArea.left + chartArea.right) / 2; 
          return; // ไม่แสดงค่า 0
        } else if (barLength < MIN_BAR_LENGTH) {
          x = chartArea.left + MIN_BAR_LENGTH / 2;
        } else {
          x = chartArea.left + barLength / 2;
        }

        ctx.fillText(String(Math.round(n)), x, bar.y);
      });

      ctx.restore();
    }
  };
}



const fullBgPlugin = (color) => ({
  id: 'fullBg',
  beforeDraw(chart) {
    const { ctx, chartArea } = chart;
    if (!chartArea) return;

    ctx.save();
    ctx.fillStyle = color;
    ctx.fillRect(
      chartArea.left,
      chartArea.top,
      chartArea.width,
      chartArea.height
    );
    ctx.restore();
  }
});


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
    const freshnessPct = toPct(data.uptodate);

    const fctx = getCtx('chart-freshness');
    if (fctx) {
      destroyIfExist('freshness');
      const safeMax = freshnessPct > 0 ? freshnessPct: 1;
      dqCharts.freshness = new Chart(fctx, {
        type: 'bar',
        data: {
          labels: ['จำนวนชุดข้อมูลที่เป็นปัจจุบัน'],
          datasets: freshnessPct > 0 ? [{
            data: [freshnessPct],
            backgroundColor: 'rgba(62, 138, 62, 0.88)',
            barThickness: 'flex',
            borderRadius: 10,
            borderSkipped: false,
            categoryPercentage: 1.0,
            barPercentage: 1.0
          }] : []
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: { enabled: true },
            title: {
              display: true,
              text: 'จำนวนชุดข้อมูลที่เป็นปัจจุบัน',
              position: 'bottom',
              align: 'center',
              padding: { top: 10, bottom: 0 },
              font: { size: 14, weight: 'normal', style: 'underline' }
            }
          },
          scales: {
            y: {
              display: false,
              grid: {
                display: false
              }
            },
            x: {
              min: 0,
              display: true,
              max: safeMax,
              ticks: {
                stepSize: safeMax,
                callback: (v) => v.toLocaleString()
              },
              grid: {
                drawBorder: false
              }
            }
          }
        },
        plugins: freshnessPct > 0 ? [centerValueHorizontalPlugin(freshnessPct)] : []
        // plugins: freshnessPct > 0 ? [centerValuePlugin(freshnessPct)] : []
      });
    }

    /* ---------- 2) Acceptable Latency ---------- */
    const order = ['ล่าช้าค่อนข้างมาก','ล่าช้าป่านกลาง','ล่าช้าเล็กน้อย'];
    const counts = order.map(k => Number(data.latency_buckets?.[k] || 0));
    const totalLatency = data.total_late_update || 0;

    const ttlctx = getCtx('cart-total-late-update');
    if (ttlctx) {
      destroyIfExist('totalLateUpdate');
      const safeMax = totalLatency > 0 ? totalLatency: 1;
      dqCharts.totalLateUpdate = new Chart(ttlctx, {
        type: 'bar',
        data: {
          labels: ['จำนวนข้อมูลที่อยู่ในรอบการปรับปรุงทั้งหมด'],
          datasets: totalLatency > 0 ? [{
            data: [totalLatency],
            backgroundColor: '#f37412ff',
            borderRadius: 8,
            borderSkipped: false,
            barThickness: 'flex',
            categoryPercentage: 1.0,
            barPercentage: 1.0
          }] : []
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: { enabled: true },
            title: {
              display: true,
              text: 'จำนวนข้อมูลที่อยู่ในรอบการปรับปรุงทั้งหมด',
              position: 'top',
              align: 'center',
              padding: { top: 0, bottom: 0 },
              font: { size: 14, weight: 'normal', style: 'underline' }
            }
          },
          scales: {
            x: {
              min: 0,
              display: true,
              max: safeMax,
              ticks: {
                stepSize: safeMax,
                callback: v => v.toLocaleString()
              },
              grid: {
                drawBorder: false
              }
            },
            y: {
              display: false,
              grid: {
                display: false
              }
            }
          }
        },
        plugins: totalLatency > 0 ? [centerValueHorizontalPlugin(totalLatency)] : []
      });
    }

    const lctx = getCtx('chart-latency');
    if (lctx) {
      destroyIfExist('latency');
      const colors = ['#f27c21ff', '#ec9654ff', '#e2b38fff'];
      dqCharts.latency = new Chart(lctx, {
        type: 'bar',
        data: { labels: order, datasets: [{ data: counts, backgroundColor: colors, borderRadius: 5 }] },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `${ctx.raw}` } },
            centerValue: {
              enabled: true,
              datasetIndex: 0
            }
          },
          scales: {
            x: { beginAtZero: true, ticks: { precision: 0 }, display: true },
            y: { ticks: { autoSkip: false } }
          }
        },
        plugins: [centerValueHorizontalPlugin()]
      });
    }

    /* ---------- 3) Outdated ---------- */
    const octx = getCtx('chart-outdated');
    if (octx) {
      destroyIfExist('outdated');
      dqCharts.outdated = new Chart(octx, {
        type: 'bar',
        data: { labels: ['ล่าช้ามากเกินรอบ\nปรับปรุง 1 เท่าขึ้นไป'], datasets: [{ data: [Number(data.outdated_count || 0)], backgroundColor: '#e53935', borderRadius: 5 }] },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => `${c.raw}` } },
            title: {
              display: true,
              text: 'จำนวนข้อมูลที่ปรับปรุงล่าช้ามากเกินรอบ',
              position: 'bottom',
              align: 'center',
              padding: { top: 10, bottom: 0 },
              font: { size: 14, weight: 'normal', style: 'underline' }
            },
          },
          scales: {
            x: { beginAtZero: true, ticks: { precision: 0 }, display: true },
            y: { display: false }
          }
        },
        plugins: data.outdated_count > 0 ? [centerValueHorizontalPlugin(data.outdated_count)] : []
      });
    }

    const nctx = getCtx('chart-noschedules');
    if (nctx) {
      destroyIfExist('noschedules');
      dqCharts.noschedules = new Chart(nctx, {
        type: 'bar',
        data: { labels: ['ไม่ระบุ'], datasets: [{ data: [Number(data.no_schedules || 0)], backgroundColor: '#e3dfdfff', borderRadius: 5 }] },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => `${c.raw}` } }, 
            title: {
              display: true,
              text: 'จำนวนข้อมูลที่ไม่ได้ระบุรอบการปรับปรุง',
              position: 'bottom',
              align: 'center',
              padding: { top: 10, bottom: 0 },
              font: { size: 14, weight: 'normal', style: 'underline' }
            },
          },
          scales: {
            x: { beginAtZero: true, ticks: { precision: 0 } , display: true },
            y: { display: false }
          }
        },
        plugins: data.no_schedules > 0 ? [centerValueHorizontalPlugin(data.no_schedules)] : []
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

