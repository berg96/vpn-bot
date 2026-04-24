// Кнопка «Копировать» + QR-рендер. Используется только на success-странице.
(function() {
  var expEl = document.getElementById('expires-local');
  if (expEl && expEl.dataset.iso) {
    var dt = new Date(expEl.dataset.iso);
    expEl.textContent = dt.toLocaleString(undefined, {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
    var cdEl = document.getElementById('countdown');
    if (cdEl) {
      var tick = function() {
        var ms = dt - new Date();
        if (ms <= 0) { cdEl.textContent = 'истекло'; return; }
        var h = Math.floor(ms / 3600000);
        var m = Math.floor((ms % 3600000) / 60000);
        var s = Math.floor((ms % 60000) / 1000);
        var parts = [];
        if (h > 0) parts.push(h + 'ч');
        if (h > 0 || m > 0) parts.push(m + 'м');
        parts.push(s + 'с');
        cdEl.textContent = parts.join(' ');
        setTimeout(tick, 1000);
      };
      tick();
    }
  }

  var subUrl = window.SUB_URL;
  if (!subUrl) return;

  var btn = document.getElementById('copy-btn');
  if (btn) {
    btn.addEventListener('click', function() {
      var done = function() { btn.textContent = 'Скопировано ✓'; };
      var fail = function() { btn.textContent = 'Не получилось — выдели и Ctrl+C'; };
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(subUrl).then(done).catch(fail);
      } else {
        var ta = document.createElement('textarea');
        ta.value = subUrl; document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); done(); } catch (e) { fail(); }
        document.body.removeChild(ta);
      }
    });
  }

  var qrEl = document.getElementById('qr');
  if (qrEl && window.QRCode) {
    // QR кодирует install-ссылку, а не sub_url напрямую —
    // при сканировании камерой телефон сразу откроет приложение с импортом.
    var qrPayload = window.INSTALL_URL || subUrl;
    new QRCode(qrEl, {
      text: qrPayload, width: 240, height: 240,
      colorDark: '#000', colorLight: '#fff',
      correctLevel: QRCode.CorrectLevel.M,
    });
  }
})();
