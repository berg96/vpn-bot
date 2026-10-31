// Перенос rs_device_id со старого origin (radarshield.mooo.com) на .net.
// localStorage чужого домена fetch'ем не читается — только скрытый iframe
// на /legacy-device-bridge + postMessage. На РФ-провайдерах с TCP-блоком
// origin UK iframe тоже не откроется: тогда остаётся /pay по логину или ссылка из бота.
(function () {
  'use strict';

  var KEY = 'rs_device_id';
  var DONE = 'rs_legacy_device_done';
  var TRIED = 'rs_legacy_device_tried';
  var TIMEOUT_MS = 4000;

  var resolveReady;
  window.rsDeviceReady = new Promise(function (r) { resolveReady = r; });

  function finish() {
    try { resolveReady(); } catch (_) {}
  }

  var legacyOrigin = (window.RS_LEGACY_ORIGIN || '').replace(/\/$/, '');
  var legacyHost = '';
  var siteHost = '';
  try { legacyHost = new URL(legacyOrigin).hostname; } catch (_) {}
  try {
    siteHost = new URL(window.RS_SITE_URL || location.origin).hostname;
  } catch (_) {
    siteHost = location.hostname;
  }

  // Мост только на новом домене. На mooo нечего мигрировать.
  if (!legacyOrigin || !legacyHost || location.hostname === legacyHost) {
    finish();
    return;
  }
  if (siteHost && location.hostname !== siteHost && location.hostname !== 'www.' + siteHost) {
    finish();
    return;
  }

  try {
    if (localStorage.getItem(DONE)) { finish(); return; }
    if (sessionStorage.getItem(TRIED)) { finish(); return; }
    sessionStorage.setItem(TRIED, '1');
  } catch (_) {
    finish();
    return;
  }

  var settled = false;
  var iframe = null;

  function cleanup() {
    window.removeEventListener('message', onMessage);
    if (iframe && iframe.parentNode) {
      try { iframe.parentNode.removeChild(iframe); } catch (_) {}
    }
    iframe = null;
  }

  function apply(id) {
    if (settled) return;
    settled = true;
    cleanup();
    if (id && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
      try {
        var prev = localStorage.getItem(KEY) || '';
        localStorage.setItem(KEY, id);
        localStorage.setItem(DONE, '1');
        document.querySelectorAll('#device_id, .device_id').forEach(function (el) {
          el.value = id;
        });
        if (prev !== id) {
          try {
            window.dispatchEvent(new CustomEvent('rs:device-migrated', {
              detail: { id: id, prev: prev }
            }));
          } catch (_) {}
        }
      } catch (_) {}
    }
    finish();
  }

  function onMessage(ev) {
    if (!ev || ev.origin !== legacyOrigin) return;
    var data = ev.data;
    if (!data || data.type !== 'rs-legacy-device') return;
    apply(typeof data.id === 'string' ? data.id : '');
  }

  window.addEventListener('message', onMessage);
  iframe = document.createElement('iframe');
  iframe.src = legacyOrigin + '/legacy-device-bridge';
  iframe.title = '';
  iframe.setAttribute('aria-hidden', 'true');
  iframe.setAttribute('tabindex', '-1');
  iframe.referrerPolicy = 'no-referrer';
  iframe.style.cssText = 'position:absolute;width:0;height:0;border:0;clip:rect(0,0,0,0);';
  (document.documentElement || document.body).appendChild(iframe);
  setTimeout(function () { apply(''); }, TIMEOUT_MS);
})();
