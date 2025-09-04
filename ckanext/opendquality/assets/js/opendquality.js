"use strict";
ckan.module("opendquality-radar-chart", function ($) {
  return {
    initialize: function () {
      const el = this.el;
      radarChart(el, radar_chart_config);
    },
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