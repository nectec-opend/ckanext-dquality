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
              callback: (v) => Number(v).toFixed(2)
            }
          }
        },
        plugins: {
          tooltip: {
            callbacks: {
              // ให้ tooltip แสดง 2 ตำแหน่ง
              label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.raw).toFixed(2)} %`
            }
          }
        }
      }
    });
  }