// Постоянная ссылка подписки для УЗНАННОГО браузера.
// Ключ — rs_device_id (browser_id), который лендинг уже кладёт в localStorage
// (app.js) и привязывает к аккаунту на pay/install (link.js). Эндпоинт
// /api/my-subscription отдаёт ссылку ТОЛЬКО привязанному браузеру — по tg_id
// её взять нельзя (см. коммент эндпоинта). Свежий/непривязанный браузер здесь
// просто ничего не увидит.
(function () {
  function ready(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }
  ready(function () {
    var card = document.getElementById('my-sub-card');
    if (!card) return;
    var bid = '';
    try { bid = localStorage.getItem('rs_device_id') || ''; } catch (_) {}
    if (!bid) return; // нет device_id — привязки заведомо нет, не дёргаем сеть

    fetch('/api/my-subscription?browser_id=' + encodeURIComponent(bid))
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d || !d.found || !d.sub_url) return;
        var urlEl = document.getElementById('my-sub-url');
        var openEl = document.getElementById('my-sub-open');
        var noteEl = document.getElementById('my-sub-note');
        var copyBtn = document.getElementById('my-sub-copy');

        urlEl.textContent = d.sub_url;
        openEl.href = 'clash://install-config?url=' + encodeURIComponent(d.sub_url);
        if (d.status && d.status.indexOf('activ') !== 0 && noteEl) {
          noteEl.textContent = 'Мы узнали ваш браузер. Подписка неактивна — ' +
            'продлите, и эта же ссылка снова заработает.';
        }
        card.hidden = false;

        copyBtn.addEventListener('click', function () {
          var restore = function () {
            var t = copyBtn.getAttribute('data-label') || 'Копировать';
            copyBtn.textContent = 'Скопировано ✓';
            setTimeout(function () { copyBtn.textContent = t; }, 1500);
          };
          copyBtn.setAttribute('data-label', copyBtn.textContent);
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(d.sub_url).then(restore, restore);
          } else {
            try {
              var range = document.createRange();
              range.selectNode(urlEl);
              var sel = window.getSelection();
              sel.removeAllRanges();
              sel.addRange(range);
              document.execCommand('copy');
              sel.removeAllRanges();
            } catch (_) {}
            restore();
          }
        });
      })
      .catch(function () {});
  });
})();
