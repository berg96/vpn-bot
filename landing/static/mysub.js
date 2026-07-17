// Блок «Моя подписка» — только для УЗНАННОГО браузера. Макет Claude Design (17.07).
//
// Ключ — rs_device_id (browser_id), который лендинг кладёт в localStorage (app.js)
// и привязывает к аккаунту на pay/install (link.js). /api/my-subscription отдаёт
// ссылку ТОЛЬКО привязанному браузеру — по tg_id её взять нельзя (см. коммент
// эндпоинта). Свежий/непривязанный браузер здесь не увидит ничего: секция остаётся
// hidden, никаких плейсхолдеров и «залогиньтесь» (решение дизайнера).
//
// Ссылка по умолчанию ЗАМАСКИРОВАНА: это фактически ключ доступа, а читать её
// глазами не нужно — есть «Открыть в приложении» и «Скопировать». Раскрытие —
// по явному клику (шоулдер-сёрфинг, скриншоты).
(function () {
  var ICON = {
    eye: '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/>',
    eyeoff: '<path d="M9.9 4.2A9.5 9.5 0 0 1 12 4c6.5 0 10 7 10 7a17 17 0 0 1-3.3 4M6.6 6.6A17 17 0 0 0 2 12s3.5 7 10 7a9.4 9.4 0 0 0 4.5-1.1"/><path d="m4 4 16 16"/>',
    open: '<path d="M14 3h7v7"/><path d="M21 3 10 14"/><path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"/>',
    copy: '<rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/>',
    check: '<path d="M20 6 9 17l-5-5"/>'
  };
  function svg(name, w) {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="' +
      (w || 2) + '" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
      ICON[name] + '</svg>';
  }
  function ready(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }
  // Маска показывает домен (человек видит, что ссылка наша), прячет только токен.
  function masked(url) {
    var m = /^(https?:\/\/[^/]+\/[a-z]+\/)/i.exec(url);
    return (m ? m[1] : '') + '••••••••';
  }
  var MONTHS = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
  function fmtDate(ts) {
    if (!ts) return '';
    var d = new Date(ts * 1000);
    if (isNaN(d.getTime())) return '';
    return d.getDate() + ' ' + MONTHS[d.getMonth()] + ' ' + d.getFullYear();
  }

  ready(function () {
    var section = document.getElementById('my-sub-section');
    if (!section) return;
    var bid = '';
    try { bid = localStorage.getItem('rs_device_id') || ''; } catch (_) {}
    if (!bid) return; // нет device_id — привязки заведомо нет, не дёргаем сеть

    fetch('/api/my-subscription?browser_id=' + encodeURIComponent(bid))
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d || !d.found || !d.sub_url) return;

        var expired = !!(d.status && d.status.indexOf('activ') !== 0);
        var revealed = false;

        var urlEl = document.getElementById('my-sub-url');
        var revealBtn = document.getElementById('my-sub-reveal');
        var copyBtn = document.getElementById('my-sub-copy');
        var primary = document.getElementById('my-sub-primary');
        var badge = document.getElementById('my-sub-badge');
        var nick = document.getElementById('my-sub-nick');
        var subline = document.getElementById('my-sub-sub');
        var hint = document.getElementById('my-sub-hint');
        var expnote = document.getElementById('my-sub-expnote');

        nick.textContent = d.username ? '@' + String(d.username).replace(/^@/, '') : 'подписки';
        var when = fmtDate(d.expire_at);
        badge.textContent = expired ? 'неактивна' : 'активна';
        badge.className = 'mysub-badge ' + (expired ? 'exp' : 'ok');
        subline.innerHTML = expired
          ? (when ? 'подписка действовала до ' + when : 'подписка неактивна')
          : '<span class="dot"></span>узнали этот браузер' + (when ? ' · активна до ' + when : '');

        function paintUrl() {
          urlEl.textContent = revealed ? d.sub_url : masked(d.sub_url);
          revealBtn.innerHTML = svg(revealed ? 'eyeoff' : 'eye') +
            '<span class="mysub-linkbtn-lbl">' + (revealed ? 'скрыть' : 'показать') + '</span>';
          revealBtn.setAttribute('aria-label', revealed ? 'Скрыть ссылку' : 'Показать ссылку');
        }
        paintUrl();
        revealBtn.addEventListener('click', function () { revealed = !revealed; paintUrl(); });

        if (expired) {
          expnote.hidden = false;
          primary.textContent = 'Продлить подписку';
          primary.href = '/pay';
          hint.textContent = 'После оплаты доступ вернётся автоматически. Ссылку можно скопировать уже сейчас.';
        } else {
          primary.innerHTML = svg('open') + 'Открыть в приложении';
          primary.href = 'clash://install-config?url=' + encodeURIComponent(d.sub_url);
          // Установлено ли приложение — детектить нечем, поэтому запасной путь
          // всегда на виду, а не выплывает после неудачи.
          hint.innerHTML = 'Не открылось? Приложение ещё не установлено — ' +
            '<a href="/#apps">скачайте клиент</a>, затем скопируйте ссылку и вставьте вручную.';
        }

        // Две кнопки копирования: в поле ссылки (десктоп) и крупная внизу под
        // primary (мобилка, где иконка в поле — слишком мелкая тап-цель).
        // Видимостью рулит CSS по брейкпоинту; логика копирования — одна.
        function doCopy(cb) {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(d.sub_url).then(cb, cb);
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
            cb();
          }
        }
        function bindCopy(btn, idle, copied) {
          btn.innerHTML = idle;
          btn.addEventListener('click', function () {
            doCopy(function () {
              btn.classList.add('is-copied');
              btn.innerHTML = copied;
              setTimeout(function () {
                btn.classList.remove('is-copied');
                btn.innerHTML = idle;
              }, 1500);
            });
          });
        }
        bindCopy(copyBtn, svg('copy'), svg('check', 2.4));
        var copyBottom = document.getElementById('my-sub-copy-bottom');
        if (copyBottom) bindCopy(copyBottom, svg('copy'), svg('check', 2.4));

        section.hidden = false;

        // Тихий вход в шапке — по макету. Ставим только когда браузер узнан:
        // остальным эта ссылка вела бы в пустоту.
        var navCta = document.querySelector('.nav-cta');
        if (navCta && !document.getElementById('nav-mysub')) {
          var a = document.createElement('a');
          a.id = 'nav-mysub';
          a.className = 'nav-mysub';
          a.href = '#my-sub-section';
          a.innerHTML = '<span class="dot"></span>Моя подписка';
          navCta.insertBefore(a, navCta.firstChild);
        }
      })
      .catch(function () {});
  });
})();
