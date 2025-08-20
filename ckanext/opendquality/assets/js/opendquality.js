"use strict";
ckan.module("opendquality-radar-chart", function ($) {
  return {
    initialize: function () {
      const el = this.el;
      console.log('Initializing radar chart module', radar_chart_config);
      radarChart(el, radar_chart_config);
      // const labels = JSON.parse(JSON.stringify(this.options.radar_labels))
      // console.log('Initializing radar chart module', el);
      // console.log('legend_anme:', this.options.legend_name);
      // console.log('radar_data:', this.options.radar_data);
      // // console.log('radar_labels:', labels);
      // // console.log(typeof(labels));
      // console.log('radar_labels:', radar_labels);
      // console.log(this.options.radar_labels);
      // console.log(typeof(this.options.radar_labels));
      // const data = JSON.parse(el.dataset.moduleRadarData);
      // const labels = JSON.parse(el.dataset.moduleRadarLabels);
      // const legendName = el.dataset.moduleLegendName;

      // // สร้าง tooltip สำหรับแสดงข้อมูลเมื่อ hover
      // this.createTooltip(el);

      // // สร้างกราฟ radar
      // this.chart = radarChart(el, {
      //   labels: labels,
      //   datasets: data.map((ds) => ({
      //     label: ds.label || legendName,
      //     data: ds.data,
      //     backgroundColor: ds.color || 'rgba(0, 123, 255, 0.2)',
      //     borderColor: ds.color || 'rgba(0, 123, 255, 1)',
      //     borderWidth: 1
      //   }))
      // });
    },

    // createTooltip: function (el) {
    //   el.classList.add('dq-tooltip');
    //   el.style.opacity = '0';
    //   el.style.position = 'absolute';
    //   el.style.pointerEvents = 'none'; // ป้องกันไม่ให้ tooltip รับการคลิก
    //   el.style.zIndex = 1000; // ให้ tooltip อยู่ด้านบนสุด
    //   el.style.transition = 'opacity 0.2s ease-in-out'; // เพิ่มการเปลี่ยนแปลงความโปร่งใส
    //   el.innerHTML = ''; // ล้างเนื้อหาเริ่มต้น

    //   // เพิ่ม tooltip ลงใน parent node ของ canvas
    //   const chart = this.chart;
    //   chart.canvas.parentNode.style.position = 'relative';
    //   chart.canvas.parentNode.appendChild(el);
    //   chart.$dqTooltip = el; // เก็บ tooltip ใน chart object เพื่อใช้งานใน afterEvent
    // },

    // afterEvent: function (args) {
    //   const chart = this.chart;
    //   if (!chart) return;

    //   // เรียกใช้ฟังก์ชันจัดการ tooltip
    //   radarTooltip.afterEvent(chart, args);
    // }
  };
});

ckan.module("qa-datatables", function ($) {
  return {
    initialize: function () {
      const el = this.el;
      new DataTable(el, {
        responsive: true,
        language: {
          url: "//cdn.datatables.net/plug-ins/2.0.0/i18n/th.json"
        }
      });
    }
  }
});