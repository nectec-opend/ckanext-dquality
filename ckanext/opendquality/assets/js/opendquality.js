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
        pageLength: 50,
        lengthMenu: [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "ทั้งหมด"] ],
        language: {
          url: "//cdn.datatables.net/plug-ins/2.0.0/i18n/th.json"
        },
        layout: {
          topStart: ['pageLength', 'info'],
          topEnd: ['paging', 'search'],        // ✅ แสดง paging ด้านบนขวา
          bottomStart: ['info'],
          bottomEnd: ['paging']      // ✅ แสดง paging ด้านล่างขวา (เหมือนเดิม)
        }
      });
    }
  }
});

$(document).ready(function(){
  const CHILDREN_BY_PARENT = window.CHILDREN_BY_PARENT || {};
  const SUB_DEFAULT_LABEL  = window.SUB_DEFAULT_LABEL || '-- เลือกหน่วยงานย่อย --';

  const $main = $('#org_main');
  const $sub  = $('#org_sub');

  if (!$main.length || !$sub.length){
    console.warn('Cannot find #org_main or #org_sub');
    return;
  }

  function rebuildSub(parentId) {
    const items = CHILDREN_BY_PARENT[parentId] || [];

    $sub.empty();
    $sub.append($('<option>', {value: '', text: SUB_DEFAULT_LABEL}));
    items.forEach( it => {
      $sub.append($('<option>', {value: it.id, text: it.title, selected: it.id === window.SUB_SELECTED}));
    });

    if ($sub.data('select2')) {
      $sub.select2('destroy');
      $sub.select2({width: 'resolve'});
    }

    if (items.length === 0) {
      $sub.prop('disabled', true);
    } else {
      $sub.prop('disabled', false);
    }
  }
  $main.on('change', function(){
    const parentId = $(this).val();
    rebuildSub(parentId);
  });
  
  if ($main.val()) {
    rebuildSub($main.val());
  } else {
    $sub.prop('disabled', true);
  }
})

document.getElementById('org-filter-form').addEventListener('submit', function (e) {
  e.preventDefault();
  const mainSel  = document.getElementById('org_main');
  const subSel  = document.getElementById('org_sub');
  if (mainSel.value.trim() && !subSel.value.trim()) {
    alert('กรุณาเลือกหน่วยงานย่อยก่อน');
    if (subSel) subSel.focus();
    return false;
  }
  e.target.submit();
});
