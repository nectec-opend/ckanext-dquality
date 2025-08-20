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
function donut(canvasId, centerId, yes, no) {
  document.getElementById(centerId).textContent = yes;
  return new Chart(document.getElementById(canvasId), {
    type: 'doughnut',
    data: { datasets: [{ data: [yes, no] }] },
    options: { cutout: '70%', plugins: { legend: { display: false } } }
  });
}
new Chart(document.getElementById('chart-validity'), {
  type: 'bar',
  data: {
    labels: ['blank header', 'duplicate header', 'extra value'],
    datasets: [{ data: [M.validity.blank_header, M.validity.duplicate_header, M.validity.extra_value] }]
  },
  options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { precision: 0 } } } }
});

// Relevancy – horizontal bar
new Chart(document.getElementById('chart-relevancy'), {
  type: 'bar',
  data: {
    labels: ['จำนวนการดาวน์โหลด (Download)', 'จำนวนการเข้าชม (View)'],
    datasets: [{ data: [M.relevancy.downloads, M.relevancy.views] }]
  },
  options: {
    indexAxis: 'y', responsive: true, plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true, ticks: { precision: 0 } } }
  }
});

// Availability – donuts
donut('chart-dl', 'center-dl', M.availability.downloadable.yes, M.availability.downloadable.no);
donut('chart-api', 'center-api', M.availability.access_api.yes, M.availability.access_api.no);