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
  const SUB_DEFAULT_LABEL  = window.SUB_DEFAULT_LABEL || '-- เลือกหน่วยงาน --';

  const $main = $('#org_main');
  const $sub  = $('#org_sub');
  const $ver = $('#ver_selected');

  if ( !$sub.length || !$ver.length) {
    console.warn('Cannot find #org_sub or #ver_selected');
    return;
  }
  
  const $verGroup = $ver.closest('.control-group, .form-group');
  const $verLabel = $('label[for="ver_selected"]');

  function toggleVer(show){
    if (show) {
      $verGroup.show();
      $verLabel.show();
      $ver.prop('disabled', false);
      if ($ver.data('select2')) $ver.select2('destroy');
      $ver.select2({ width: 'resolve' });
    } else {
      if ($ver.data('select2')) $ver.select2('destroy');
      $ver.prop('disabled', true).empty();
      $verGroup.hide();
      $verLabel.hide(); // เผื่อบางธีม label อยู่นอก group
    }
  }


  function rebuildVersion(subId) {
    const items = versions[subId] || [];
    $ver.empty();
    items.forEach( it => {
      $ver.append($('<option>', {value: it.value, text: it.label, selected: String(it.value) === String(selected_version)}));
    });
    toggleVer(items.length > 0);
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

    toggleVer(false);
  }
  $main.on('change', function(){
    rebuildSub($(this).val());
  });

  $sub.on('change', function(){
    const subId = $(this).val();
    console.log('Selected subId:', subId);

    if (subId) {
      rebuildVersion(subId);
    } else {
      toggleVer(false); // ยังไม่ได้เลือกหน่วยงานย่อย -> ซ่อน
    }
  });

  if ($main.length && $main.val()) {
    rebuildSub($main.val());
  } else if ($main.length && !$main.val()) {
    $sub.prop('disabled', true);
  }


  toggleVer(false);

  if ($sub.val()) {
    rebuildVersion($sub.val());
  }
})

document.getElementById('org-filter-form').addEventListener('submit', function (e) {
  e.preventDefault();
  const mainSel  = document.getElementById('org_main');
  const subSel  = document.getElementById('org_sub');
  const verSel  = document.getElementById('ver_selected');
  if (mainSel && mainSel.value.trim() && !subSel.value.trim()) {
    alert('กรุณาเลือกหน่วยงานย่อยก่อน');
    if (subSel) subSel.focus();
    return false;
  }
  e.target.submit();
});
