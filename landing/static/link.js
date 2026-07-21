// Browser ↔ account linking (Этап 4 support-чата).
// Стабильный browser_id браузера + тихая фоновая привязка к TG-аккаунту,
// когда страница открыта из бота (несёт подписанный uid в window.RS_LINK).
(function () {
  'use strict';

  function uuid() {
    return ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, function (c) {
      return (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4)).toString(16);
    });
  }

  // browser_id = тот же rs_device_id, что лендинг уже кладёт в localStorage
  // и сохраняет на лиде при создании триала. Не плодим второй идентификатор.
  function browserId() {
    var k = 'rs_device_id';
    var v = '';
    try { v = localStorage.getItem(k) || ''; } catch (_) {}
    if (!v) {
      v = uuid();
      try { localStorage.setItem(k, v); } catch (_) {}
    }
    return v;
  }
  window.rsBrowserId = browserId;

  function autoLink() {
    var L = window.RS_LINK;
    if (!L || !L.uid || !L.sig) return;
    try {
      fetch('/api/link-browser', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          browser_id: browserId(),
          uid: L.uid,
          sig: L.sig,
          source: L.source || 'page',
        }),
      }).then(function (r) {
        // Чат (support.js) запрашивает /api/whois независимо и на первом заходе
        // обычно опережает эту привязку — тогда он не узнаёт человека до
        // перезагрузки. Сообщаем ему, что аккаунт появился, чтобы перезапросил.
        if (r && r.ok) {
          try { window.dispatchEvent(new CustomEvent('rs:browser-linked')); } catch (_) {}
        }
      }).catch(function () {});
    } catch (_) {}
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', autoLink);
  } else {
    autoLink();
  }
})();
