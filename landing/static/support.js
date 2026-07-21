// Support widget — RadarShield landing
(function () {
  'use strict';
  if (window.__rsChatLoaded) return;
  window.__rsChatLoaded = true;

  var API_BASE = '/api/support';
  var COOKIE_KEY = 'rs_chat_id';
  var COOKIE_DAYS = 90;
  var LS_HISTORY = 'rs_chat_history_v1';
  var LS_UNREAD = 'rs_chat_unread_v1';
  var LS_PROMO_SEEN = 'rs_chat_promo_seen_v1';
  var LS_LAST_OP = 'rs_chat_last_op_v1';     // последний preview op-сообщения для бабла
  var LS_WELCOME_SEEN = 'rs_chat_welcome_v1'; // virtual «1 непрочитанное приветствие»
  var LS_IDENTITY_ACK = 'rs_chat_identity_ack_v1'; // uid, по которому юзер уже ответил «это я»
  var SS_PROMO_ACTIVE = 'rs_chat_promo_active_v1'; // sessionStorage: промо ещё «живёт» между переходами
  var MAX_HISTORY = 200;
  var MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
  // image/* + pdf — даёт нативный picker на iOS с пунктом «Photo Library».
  // Подробный MIME-список (image/jpeg, image/png, …) ломает iOS picker
  // и оставляет только «Browse».
  var FILE_ACCEPT = 'image/*,application/pdf';
  var MAX_PENDING_FILES = 5;
  var PROMO_DELAY_MS = 3000;
  var PROMO_AUTOHIDE_MS = 10000;
  var PROMO_SNOOZE_MS = 24 * 60 * 60 * 1000; // крестик → не показывать сутки
  var PROMO_TEXT = 'Поможем настроить или ответим на любой вопрос — пишите!';
  var OP_PREVIEW_CHARS = 60;

  function getCookie(name) {
    var m = document.cookie.match('(?:^|; )' + name + '=([^;]*)');
    return m ? decodeURIComponent(m[1]) : '';
  }
  function setCookie(name, value, days) {
    var d = new Date();
    d.setTime(d.getTime() + days * 86400000);
    document.cookie = name + '=' + encodeURIComponent(value) +
      '; expires=' + d.toUTCString() +
      '; path=/; SameSite=Lax' +
      (location.protocol === 'https:' ? '; Secure' : '');
  }

  // browser_id = тот же rs_device_id, что лендинг кладёт в localStorage и
  // сохраняет на лиде. Через него vpn-bot /api/whois узнаёт аккаунт юзера.
  function browserId() {
    var k = 'rs_device_id';
    var v = '';
    try { v = localStorage.getItem(k) || ''; } catch (_) {}
    if (!v) {
      try {
        v = ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, function (c) {
          return (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4)).toString(16);
        });
        localStorage.setItem(k, v);
      } catch (_) {}
    }
    return v;
  }

  function loadHistory() {
    try { return JSON.parse(localStorage.getItem(LS_HISTORY) || '[]'); }
    catch (_) { return []; }
  }
  function saveHistory(arr) {
    try {
      var trimmed = arr.slice(-MAX_HISTORY);
      localStorage.setItem(LS_HISTORY, JSON.stringify(trimmed));
    } catch (_) {}
  }

  function fmtTime(ts) {
    var d = new Date(ts * 1000);
    var hh = String(d.getHours()).padStart(2, '0');
    var mm = String(d.getMinutes()).padStart(2, '0');
    return hh + ':' + mm;
  }

  // ── DOM ───────────────────────────────────────────────────────────────────
  var fab, panel, body, input, sendBtn, attachBtn, fileInput, pendingHost, badge, statusDot, promo, foot, closeCard, identityStrip;
  var pendingFiles = []; // [{file, localUrl, isImage, el}]
  var chatKey = '';
  var es = null;
  var unread = parseInt(localStorage.getItem(LS_UNREAD) || '0', 10) || 0;
  var seenMsgIds = {};
  var promoTimers = [];
  var ratingValue = 0;
  var threadClosed = false;
  var identity = null; // {uid, username, acked} — распознанный по browser_id аккаунт

  function buildDOM() {
    fab = document.createElement('button');
    fab.className = 'rs-chat-fab';
    fab.setAttribute('aria-label', 'Открыть чат поддержки');
    fab.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/>' +
      '</svg>' +
      '<span class="rs-chat-badge" aria-hidden="true"></span>';
    fab.addEventListener('click', open);
    document.body.appendChild(fab);
    badge = fab.querySelector('.rs-chat-badge');
    renderBadge();

    promo = document.createElement('div');
    promo.className = 'rs-chat-promo';
    promo.setAttribute('role', 'button');
    promo.setAttribute('tabindex', '0');
    promo.innerHTML =
      '<button class="rs-promo-close" type="button" aria-label="Закрыть подсказку">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg>' +
      '</button>' +
      '<span class="rs-promo-text"></span>';
    promo.querySelector('.rs-promo-text').textContent = PROMO_TEXT;
    promo.addEventListener('click', function (e) {
      if (e.target.closest('.rs-promo-close')) {
        hidePromo(true);
      } else {
        hidePromo(true);
        open();
      }
    });
    document.body.appendChild(promo);

    panel = document.createElement('aside');
    panel.className = 'rs-chat-panel';
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-label', 'Чат поддержки RadarShield');
    panel.innerHTML =
      '<div class="rs-chat-head">' +
        '<h4>Поддержка</h4>' +
        '<span class="rs-chat-status">подключаемся…</span>' +
        '<button class="rs-chat-close" aria-label="Закрыть">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg>' +
        '</button>' +
      '</div>' +
      '<div class="rs-identity-strip"></div>' +
      '<div class="rs-chat-body" aria-live="polite"></div>' +
      '<div class="rs-chat-close-card">' +
        '<h5>Спасибо за обращение!</h5>' +
        '<p>Надеемся, мы решили ваш вопрос. Оцените, пожалуйста, поддержку:</p>' +
        '<div class="rs-stars" role="radiogroup" aria-label="Оценка от 1 до 5"></div>' +
        '<textarea class="rs-comment" rows="2" placeholder="Комментарий (необязательно)" maxlength="2000"></textarea>' +
        '<div class="rs-close-actions">' +
          '<button class="rs-btn rs-dismiss" type="button">Без оценки</button>' +
          '<button class="rs-btn primary rs-submit-rate" type="button" disabled>Отправить</button>' +
        '</div>' +
        '<button class="rs-btn rs-unresolved" type="button">❌ Вопрос не решён</button>' +
      '</div>' +
      '<div class="rs-pending-files is-empty"></div>' +
      '<form class="rs-chat-foot">' +
        '<button class="rs-chat-attach" type="button" aria-label="Прикрепить файл">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>' +
        '</button>' +
        '<input class="rs-chat-file" type="file" accept="' + FILE_ACCEPT + '" multiple hidden>' +
        '<textarea class="rs-chat-input" rows="1" placeholder="Опишите проблему…" maxlength="4000"></textarea>' +
        '<button class="rs-chat-send" type="submit" aria-label="Отправить">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>' +
        '</button>' +
      '</form>';
    document.body.appendChild(panel);

    body = panel.querySelector('.rs-chat-body');
    identityStrip = panel.querySelector('.rs-identity-strip');
    input = panel.querySelector('.rs-chat-input');
    sendBtn = panel.querySelector('.rs-chat-send');
    attachBtn = panel.querySelector('.rs-chat-attach');
    fileInput = panel.querySelector('.rs-chat-file');
    pendingHost = panel.querySelector('.rs-pending-files');
    statusDot = panel.querySelector('.rs-chat-status');
    foot = panel.querySelector('.rs-chat-foot');
    closeCard = panel.querySelector('.rs-chat-close-card');

    panel.querySelector('.rs-chat-close').addEventListener('click', close);
    foot.addEventListener('submit', onSend);
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(e); }
    });
    input.addEventListener('input', autoresize);
    // Mobile keyboard fix: при focus и при изменении viewport (клавиатура
    // поднялась/скрылась) — прокрутить чат к низу, чтобы последние сообщения
    // и поле ввода всегда были видны.
    input.addEventListener('focus', function () {
      setTimeout(scrollDown, 280);
    });
    if (window.visualViewport) {
      window.visualViewport.addEventListener('resize', function () {
        if (isPanelOpen()) scrollDown();
      });
    }
    attachBtn.addEventListener('click', function () {
      if (!chatKey || threadClosed) return;
      fileInput.click();
    });
    fileInput.addEventListener('change', onFilePick);

    buildStars(closeCard.querySelector('.rs-stars'));
    closeCard.querySelector('.rs-submit-rate').addEventListener('click', submitRating);
    closeCard.querySelector('.rs-dismiss').addEventListener('click', function () {
      finalizeClose(false);
    });
    closeCard.querySelector('.rs-unresolved').addEventListener('click', submitUnresolved);
  }

  function buildStars(host) {
    var STAR_SVG = '<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M12 2l2.95 6.6 7.05.7-5.3 4.9 1.55 7.05L12 17.6 5.75 21.25 7.3 14.2 2 9.3l7.05-.7L12 2z"/></svg>';
    for (var i = 1; i <= 5; i++) {
      (function (val) {
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'rs-star';
        b.dataset.val = String(val);
        b.setAttribute('role', 'radio');
        b.setAttribute('aria-label', val + ' из 5');
        b.innerHTML = STAR_SVG;
        b.addEventListener('mouseenter', function () { highlightStars(val); });
        b.addEventListener('focus',      function () { highlightStars(val); });
        b.addEventListener('click', function () {
          ratingValue = val;
          highlightStars(val, true);
          closeCard.querySelector('.rs-comment').classList.add('is-on');
          closeCard.querySelector('.rs-submit-rate').disabled = false;
        });
        host.appendChild(b);
      })(i);
    }
    host.addEventListener('mouseleave', function () { highlightStars(ratingValue); });
  }

  function highlightStars(n, persist) {
    var stars = closeCard.querySelectorAll('.rs-star');
    stars.forEach(function (s, idx) {
      s.classList.toggle('is-hot', idx < n);
      if (persist) s.setAttribute('aria-checked', idx < n ? 'true' : 'false');
    });
  }

  function showCloseCard() {
    if (!closeCard) return;
    closeCard.classList.add('is-on');
    foot.style.display = 'none';
    pushSys('— чат закрыт оператором —');
    scrollDown();
  }

  function hideCloseCard() {
    if (!closeCard) return;
    closeCard.classList.remove('is-on');
    foot.style.display = '';
    ratingValue = 0;
    highlightStars(0, true);
    closeCard.querySelector('.rs-comment').value = '';
    closeCard.querySelector('.rs-comment').classList.remove('is-on');
    closeCard.querySelector('.rs-submit-rate').disabled = true;
    var ur = closeCard.querySelector('.rs-unresolved');
    if (ur) ur.disabled = false;
  }

  function submitRating() {
    if (!ratingValue || !chatKey) return;
    var btn = closeCard.querySelector('.rs-submit-rate');
    btn.disabled = true;
    var comment = closeCard.querySelector('.rs-comment').value.trim() || null;
    fetch(API_BASE + '/rate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_key: chatKey, rating: ratingValue, comment: comment }),
    }).then(function (r) {
      if (!r.ok) throw new Error('rate ' + r.status);
      pushSys('Спасибо за оценку!');
      finalizeClose(true);
    }).catch(function () {
      btn.disabled = false;
      pushSys('Не удалось отправить оценку. Попробуйте ещё раз.');
    });
  }

  function submitUnresolved() {
    if (!chatKey) return;
    var btn = closeCard.querySelector('.rs-unresolved');
    btn.disabled = true;
    var comment = closeCard.querySelector('.rs-comment').value.trim() || null;
    // rating=0 — серверный сигнал «вопрос не решён»: reopen + alert операторам.
    fetch(API_BASE + '/rate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_key: chatKey, rating: 0, comment: comment }),
    }).then(function (r) {
      if (!r.ok) throw new Error('unresolved ' + r.status);
      // Сервер пришлёт SSE reopened → виджет сам скроет close-card, разблокирует ввод.
      threadClosed = false;
      hideCloseCard();
      pushSys('Тред переоткрыт. Опишите, пожалуйста, что именно осталось не решённым.');
    }).catch(function () {
      btn.disabled = false;
      pushSys('Не удалось отправить. Попробуйте ещё раз.');
    });
  }

  function finalizeClose(rated) {
    // Юзер взаимодействовал с closing card → можно чистить локальную историю.
    // Cookie оставляем: при новом обращении тот же chat_key → backend
    // переоткрывает прежний топик, оператор видит того же пользователя.
    try { localStorage.removeItem(LS_HISTORY); } catch (_) {}
    seenMsgIds = {};
    threadClosed = false;
    hideCloseCard();
    body.innerHTML = '';
    pushSys(rated
      ? 'Готово. Если понадобится — пишите снова, мы откроем тему.'
      : 'Чат завершён. Если понадобится — пишите снова.');
  }

  function autoresize() {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 110) + 'px';
  }

  function isPromoSnoozed() {
    // LS_PROMO_SEEN хранит timestamp последнего dismiss. Старые значения "1"
    // считаем как t=1 → давно протухло → snooze=false → промо снова покажется.
    var v = localStorage.getItem(LS_PROMO_SEEN);
    if (!v) return false;
    var ts = parseInt(v, 10) || 0;
    return (Date.now() - ts) < PROMO_SNOOZE_MS;
  }

  function showPromo() {
    if (!promo) return;
    if (isPanelOpen()) return;

    var lastOp = '';
    try { lastOp = localStorage.getItem(LS_LAST_OP) || ''; } catch (_) {}
    var hasOpReply = unread > 0 && lastOp;

    var text;
    if (hasOpReply) {
      // Новый ответ оператора перебивает snooze — обязательно напоминаем.
      text = '✉️ Поддержка ответила: ' + truncate(lastOp, OP_PREVIEW_CHARS);
    } else if (isPromoSnoozed()) {
      return;
    } else if (identity && identity.username) {
      text = 'Поможем с подпиской @' + identity.username + ' — пишите!';
    } else {
      text = PROMO_TEXT;
    }
    var span = promo.querySelector('.rs-promo-text');
    if (span) span.textContent = text;

    promo.classList.add('is-on');
    try { sessionStorage.setItem(SS_PROMO_ACTIVE, '1'); } catch (_) {}
    // Autohide → промо «прожил» свой показ, на след странице тоже не покажем
    // (sessionStorage сбросится). Снова покажется только в новой вкладке/после
    // 24ч snooze, либо при новом ответе оператора.
    promoTimers.push(setTimeout(function () { hidePromo(false); }, PROMO_AUTOHIDE_MS));
  }

  function hidePromo(persistDismissal) {
    if (!promo) return;
    promo.classList.remove('is-on');
    promoTimers.forEach(clearTimeout);
    promoTimers = [];
    try { sessionStorage.removeItem(SS_PROMO_ACTIVE); } catch (_) {}
    if (persistDismissal) {
      try { localStorage.setItem(LS_PROMO_SEEN, String(Date.now())); } catch (_) {}
    }
  }

  function schedulePromo() {
    var lastOp = '';
    try { lastOp = localStorage.getItem(LS_LAST_OP) || ''; } catch (_) {}
    var hasOpReply = unread > 0 && lastOp;

    if (hasOpReply) {
      // Не ждём 3 сек — оператор уже написал, показываем сразу.
      showPromo();
      return;
    }
    if (isPromoSnoozed()) return;
    // Cross-page continuation: если на прошлой странице промо ещё горел,
    // на новой показываем мгновенно — юзер не успел прочитать.
    var continuation = false;
    try { continuation = sessionStorage.getItem(SS_PROMO_ACTIVE) === '1'; } catch (_) {}
    if (continuation) {
      showPromo();
      return;
    }
    promoTimers.push(setTimeout(showPromo, PROMO_DELAY_MS));
  }

  function truncate(s, n) {
    s = String(s || '');
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
  }

  function ensureWelcomeBadge() {
    // Виртуальное «1 непрочитанное приветствие» для новых посетителей —
    // даёт стимул кликнуть FAB ещё до взаимодействия. Сбрасывается при
    // первом открытии чата (open() ставит LS_WELCOME_SEEN=1).
    if (localStorage.getItem(LS_WELCOME_SEEN) === '1') return;
    if (isPromoSnoozed()) return;
    if (unread > 0) return;
    unread = 1;
    renderBadge();
  }

  function renderBadge() {
    if (!badge) return;
    if (unread > 0) {
      badge.textContent = unread > 9 ? '9+' : String(unread);
      badge.classList.add('is-on');
    } else {
      badge.classList.remove('is-on');
    }
  }

  function setStatus(online, text) {
    if (!statusDot) return;
    statusDot.textContent = text || (online ? 'на связи' : 'не в сети');
    statusDot.classList.toggle('is-online', !!online);
  }

  function renderHistory() {
    body.innerHTML = '';
    var h = loadHistory();
    if (h.length === 0) {
      pushSys('Здравствуйте! Опишите проблему — оператор ответит в течение нескольких минут.');
    } else {
      h.forEach(function (m) { renderMsg(m, false); });
    }
    scrollDown();
  }

  // Вставляет текст в элемент, оборачивая http(s)-ссылки в кликабельные <a>.
  // Без innerHTML — только textNode/createElement, поэтому XSS-безопасно.
  function appendText(parent, text) {
    var re = /(https?:\/\/[^\s]+)/g;
    var last = 0, mm;
    while ((mm = re.exec(text)) !== null) {
      if (mm.index > last) {
        parent.appendChild(document.createTextNode(text.slice(last, mm.index)));
      }
      var a = document.createElement('a');
      a.href = mm[0];
      a.textContent = mm[0];
      a.target = '_blank';
      a.rel = 'noopener noreferrer';
      parent.appendChild(a);
      last = mm.index + mm[0].length;
    }
    if (last < text.length) {
      parent.appendChild(document.createTextNode(text.slice(last)));
    }
  }

  function renderMsg(m, animate) {
    var el = document.createElement('div');
    el.className = 'rs-chat-msg ' + (m.kind || 'sys');
    if (m.batch && m.batch.length > 0) {
      el.classList.add('has-file');
      if (m.batch.length === 1) {
        el.appendChild(renderFile(m.batch[0]));
      } else {
        el.appendChild(renderBatch(m.batch));
      }
      if (m.text) {
        var capBatch = document.createElement('div');
        capBatch.className = 'rs-file-caption';
        appendText(capBatch, m.text);
        el.appendChild(capBatch);
      }
    } else if (m.file) {
      el.classList.add('has-file');
      el.appendChild(renderFile(m.file));
      if (m.text) {
        var caption = document.createElement('div');
        caption.className = 'rs-file-caption';
        appendText(caption, m.text);
        el.appendChild(caption);
      }
    } else if (m.text) {
      appendText(el, m.text);
    }
    if (m.ts && m.kind !== 'sys') {
      var ts = document.createElement('span');
      ts.className = 'ts';
      ts.textContent = fmtTime(m.ts);
      el.appendChild(ts);
    }
    body.appendChild(el);
    if (animate) scrollDown();
    return el;
  }

  function renderBatch(files) {
    var grid = document.createElement('div');
    grid.className = 'rs-file-batch';
    var imageFiles = files.filter(function (f) { return f.kind === 'image'; });
    var cols = Math.min(files.length, 2);
    grid.style.gridTemplateColumns = 'repeat(' + cols + ', 1fr)';
    var imgIdx = 0;
    files.forEach(function (file) {
      var cell = document.createElement('div');
      cell.className = 'rs-file-batch-cell';
      if (file.kind === 'image') {
        var thisImgIdx = imgIdx++;
        cell.appendChild(renderFile(file, imageFiles, thisImgIdx));
      } else {
        cell.appendChild(renderFile(file));
      }
      grid.appendChild(cell);
    });
    return grid;
  }

  function renderFile(file, siblings, sibIdx) {
    // siblings — массив images сообщения (для lightbox переключателя)
    if (file.kind === 'image') {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'rs-file-img';
      btn.setAttribute('aria-label', 'Открыть изображение');
      var img = document.createElement('img');
      img.src = file.url;
      img.alt = file.name || 'screenshot';
      img.loading = 'lazy';
      btn.appendChild(img);
      var lbFiles = siblings || [file];
      var lbIdx = (sibIdx !== undefined) ? sibIdx : 0;
      btn.addEventListener('click', function () { openLightbox(lbFiles, lbIdx); });
      return btn;
    }
    var doc = document.createElement('a');
    doc.href = file.url;
    doc.target = '_blank';
    doc.rel = 'noopener';
    if (file.name) doc.download = file.name;
    doc.className = 'rs-file-doc';
    var icon = document.createElement('span');
    icon.className = 'rs-file-icon';
    icon.textContent = '📎';
    var name = document.createElement('span');
    name.className = 'rs-file-name';
    name.textContent = file.name || 'файл';
    doc.appendChild(icon);
    doc.appendChild(name);
    if (file.size) {
      var sz = document.createElement('span');
      sz.className = 'rs-file-size';
      sz.textContent = formatSize(file.size);
      doc.appendChild(sz);
    }
    return doc;
  }

  function formatSize(n) {
    if (n < 1024) return n + ' B';
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
    return (n / 1024 / 1024).toFixed(1) + ' MB';
  }

  function openLightbox(files, startIdx) {
    // files — [{url, name, kind}], startIdx — начальный индекс
    if (!files || !files.length) return;
    var idx = startIdx || 0;
    var multi = files.length > 1;

    var lb = document.createElement('div');
    lb.className = 'rs-chat-lightbox';

    // --- header ---
    var header = '<div class="rs-lb-header">';
    if (multi) header += '<span class="rs-lb-counter">' + (idx + 1) + ' / ' + files.length + '</span>';
    header += '<div class="rs-lb-actions">';
    header += '<a class="rs-lb-download" target="_blank" rel="noopener" aria-label="Скачать">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12m0 0l-4-4m4 4l4-4M5 21h14"/></svg></a>';
    header += '<button class="rs-lb-close" type="button" aria-label="Закрыть">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg></button>';
    header += '</div></div>';

    // --- nav arrows ---
    var nav = multi
      ? '<button class="rs-lb-prev" type="button" aria-label="Предыдущее">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M15 18l-6-6 6-6"/></svg></button>' +
        '<button class="rs-lb-next" type="button" aria-label="Следующее">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18l6-6-6-6"/></svg></button>'
      : '';

    // --- thumb strip ---
    var thumbHtml = '';
    if (multi) {
      thumbHtml = '<div class="rs-lb-thumbs">';
      files.forEach(function (f, i) {
        thumbHtml += '<button class="rs-lb-thumb' + (i === idx ? ' is-active' : '') +
          '" type="button" data-i="' + i + '">';
        if (f.kind === 'image') {
          thumbHtml += '<img src="' + f.url + '" alt="">';
        } else {
          thumbHtml += '<span>📎</span>';
        }
        thumbHtml += '</button>';
      });
      thumbHtml += '</div>';
    }

    lb.innerHTML = header + nav + '<img class="rs-lb-img" alt="">' + thumbHtml;

    // refs
    var img = lb.querySelector('.rs-lb-img');
    var counter = lb.querySelector('.rs-lb-counter');
    var dl = lb.querySelector('.rs-lb-download');
    var thumbs = lb.querySelectorAll('.rs-lb-thumb');

    function render(newIdx) {
      idx = (newIdx + files.length) % files.length;
      var f = files[idx];
      img.src = f.url;
      img.alt = f.name || '';
      if (dl) { dl.href = f.url; dl.setAttribute('download', f.name || 'image'); }
      if (counter) counter.textContent = (idx + 1) + ' / ' + files.length;
      thumbs.forEach(function (th, i) { th.classList.toggle('is-active', i === idx); });
    }

    function closeFn() {
      lb.classList.remove('is-on');
      setTimeout(function () { if (lb.parentNode) lb.parentNode.removeChild(lb); }, 180);
      document.removeEventListener('keydown', onKey);
      if (history.state && history.state.rsLightbox) history.replaceState({ rsChat: true }, '');
    }
    lb._closeFn = closeFn;

    function onKey(e) {
      if (e.key === 'Escape') { closeFn(); return; }
      if (multi) {
        if (e.key === 'ArrowLeft') render(idx - 1);
        if (e.key === 'ArrowRight') render(idx + 1);
      }
    }

    // touch swipe
    var touchX = 0;
    lb.addEventListener('touchstart', function (e) { touchX = e.touches[0].clientX; }, { passive: true });
    lb.addEventListener('touchend', function (e) {
      var dx = e.changedTouches[0].clientX - touchX;
      if (Math.abs(dx) > 48 && multi) render(dx < 0 ? idx + 1 : idx - 1);
    });

    lb.addEventListener('click', function (e) {
      if (e.target.closest('.rs-lb-close')) { closeFn(); return; }
      if (e.target.closest('.rs-lb-prev')) { render(idx - 1); return; }
      if (e.target.closest('.rs-lb-next')) { render(idx + 1); return; }
      var th = e.target.closest('.rs-lb-thumb');
      if (th) { render(parseInt(th.dataset.i, 10)); return; }
      if (e.target === lb) closeFn();
    });

    document.addEventListener('keydown', onKey);
    document.body.appendChild(lb);
    render(idx);
    requestAnimationFrame(function () { lb.classList.add('is-on'); });

    // Android Back закроет lightbox, не чат.
    if (window.history && history.pushState) {
      history.pushState({ rsChat: true, rsLightbox: true }, '');
    }
  }

  function pushSys(text) {
    renderMsg({ kind: 'sys', text: text, ts: Math.floor(Date.now() / 1000) }, false);
  }

  function pushAndStore(m) {
    var h = loadHistory();
    h.push(m);
    saveHistory(h);
    renderMsg(m, true);
  }

  function scrollDown() {
    requestAnimationFrame(function () { body.scrollTop = body.scrollHeight; });
  }

  // ── Identity (распознавание аккаунта по browser_id) ───────────────────────
  function fetchIdentity() {
    var bid = browserId();
    if (!bid) return Promise.resolve();
    return fetch('/api/whois?browser_id=' + encodeURIComponent(bid))
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d || !d.found || !d.accounts || !d.accounts.length) return;
        var acc = d.accounts[0];
        if (!acc.username) return;
        var acked = acc.confirmed === 1;
        try { acked = acked || localStorage.getItem(LS_IDENTITY_ACK) === String(acc.uid); }
        catch (_) {}
        identity = { uid: acc.uid, username: acc.username, acked: acked };
      })
      .catch(function () {});
  }

  function renderIdentityStrip() {
    if (!identityStrip) return;
    if (!identity || identity.acked) {
      identityStrip.classList.remove('is-on');
      return;
    }
    identityStrip.innerHTML =
      '<span class="rs-id-q">Вопрос по аккаунту <b>@' + identity.username + '</b>?</span>' +
      '<div class="rs-id-btns">' +
        '<button class="rs-id-yes" type="button">Да, это я</button>' +
        '<button class="rs-id-no" type="button">Нет</button>' +
      '</div>';
    identityStrip.classList.add('is-on');
    identityStrip.querySelector('.rs-id-yes')
      .addEventListener('click', function () { identityFeedback(1); });
    identityStrip.querySelector('.rs-id-no')
      .addEventListener('click', function () { identityFeedback(-1); });
  }

  function identityFeedback(confirmed) {
    if (!identity) return;
    var uid = identity.uid;
    var uname = identity.username;
    fetch('/api/link-browser/feedback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ browser_id: browserId(), uid: uid, confirmed: confirmed }),
    }).catch(function () {});
    if (identityStrip) identityStrip.classList.remove('is-on');
    if (confirmed === 1) {
      try { localStorage.setItem(LS_IDENTITY_ACK, String(uid)); } catch (_) {}
      identity.acked = true;
      pushSys('Спасибо! Будем знать, что вопрос по аккаунту @' + uname + '.');
    } else {
      identity = null;
    }
  }

  // ── API ───────────────────────────────────────────────────────────────────
  function apiInit() {
    var existing = getCookie(COOKIE_KEY);
    return fetch(API_BASE + '/init', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(existing ? { chat_key: existing } : {}),
    })
      .then(function (r) {
        if (!r.ok) throw new Error('init ' + r.status);
        return r.json();
      })
      .then(function (data) {
        chatKey = data.chat_key;
        setCookie(COOKIE_KEY, chatKey, COOKIE_DAYS);
        return chatKey;
      });
  }

  function apiSend(text) {
    return fetch(API_BASE + '/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_key: chatKey, text: text, browser_id: browserId() }),
    }).then(function (r) {
      if (!r.ok) throw new Error('send ' + r.status);
      return r.json();
    });
  }

  function openStream() {
    if (es) { try { es.close(); } catch (_) {} }
    es = new EventSource(API_BASE + '/stream/' + encodeURIComponent(chatKey));
    es.addEventListener('open', function () {
      setStatus(true);
    });
    es.addEventListener('error', function () {
      setStatus(false, 'переподключение…');
      // EventSource сам реконнектится; статус восстановит open-event.
    });
    es.addEventListener('message', function (ev) {
      try {
        var data = JSON.parse(ev.data);
        if (!data || !data.type) return;
        if (data.type === 'operator') {
          if (data.msg_id && seenMsgIds[data.msg_id]) return;
          if (data.msg_id) seenMsgIds[data.msg_id] = true;
          pushAndStore({ kind: 'op', text: data.text, ts: data.ts });
          try { localStorage.setItem(LS_LAST_OP, data.text || ''); } catch (_) {}
          if (!isPanelOpen()) {
            unread += 1;
            localStorage.setItem(LS_UNREAD, String(unread));
            renderBadge();
            // На текущей странице показываем промо немедленно (с preview ответа).
            // На след. странице schedulePromo() сделает это при boot.
            showPromo();
          }
        } else if (data.type === 'operator_file') {
          if (data.msg_id && seenMsgIds[data.msg_id]) return;
          if (data.msg_id) seenMsgIds[data.msg_id] = true;
          var fileMsg = {
            kind: 'op', ts: data.ts,
            file: { url: data.url, name: data.name, mime: data.mime, kind: data.kind, size: data.size },
          };
          if (data.text) fileMsg.text = data.text;
          pushAndStore(fileMsg);
          var opPreview = data.text
            ? data.text
            : (data.kind === 'image' ? '📷 Фото' : '📎 ' + (data.name || 'файл'));
          try { localStorage.setItem(LS_LAST_OP, opPreview); } catch (_) {}
          if (!isPanelOpen()) {
            unread += 1;
            localStorage.setItem(LS_UNREAD, String(unread));
            renderBadge();
            showPromo();
          }
        } else if (data.type === 'operator_batch') {
          if (data.msg_id && seenMsgIds[data.msg_id]) return;
          if (data.msg_id) seenMsgIds[data.msg_id] = true;
          var bMsg = { kind: 'op', ts: data.ts, batch: data.files };
          if (data.text) bMsg.text = data.text;
          pushAndStore(bMsg);
          var opBatchPrev = data.text || (data.files && data.files.length
            ? (data.files[0].kind === 'image' ? '📷 Фото ×' + data.files.length : '📎 Файлы ×' + data.files.length)
            : '');
          try { localStorage.setItem(LS_LAST_OP, opBatchPrev); } catch (_) {}
          if (!isPanelOpen()) {
            unread += 1;
            localStorage.setItem(LS_UNREAD, String(unread));
            renderBadge();
            showPromo();
          }
        } else if (data.type === 'closed') {
          threadClosed = true;
          if (isPanelOpen()) {
            showCloseCard();
          } else {
            unread += 1;
            localStorage.setItem(LS_UNREAD, String(unread));
            renderBadge();
          }
        } else if (data.type === 'reopened') {
          threadClosed = false;
          hideCloseCard();
        }
      } catch (_) {}
    });
  }

  // ── Open / Close / Send ───────────────────────────────────────────────────
  function isPanelOpen() { return panel && panel.classList.contains('is-open'); }

  function open() {
    if (!panel) return;
    hidePromo(true);
    panel.classList.add('is-open');
    fab.classList.add('is-hidden');
    // Android Back закроет виджет, а не уйдёт со страницы.
    if (window.history && history.pushState) {
      history.pushState({ rsChat: true }, '');
    }
    unread = 0;
    localStorage.setItem(LS_UNREAD, '0');
    try {
      localStorage.setItem(LS_WELCOME_SEEN, '1');
      localStorage.removeItem(LS_LAST_OP);
    } catch (_) {}
    renderBadge();
    setStatus(false, 'подключаемся…');
    if (!chatKey) {
      apiInit().then(function () {
        renderHistory();
        openStream();
        if (threadClosed) showCloseCard();
      }).catch(function () {
        setStatus(false, 'нет связи');
        pushSys('Не удалось подключиться. Попробуйте позже или напишите в Telegram.');
      });
    } else {
      renderHistory();
      if (!es) openStream();
      else setStatus(true);
      if (threadClosed) showCloseCard();
    }
    if (identity) renderIdentityStrip();
    else fetchIdentity().then(renderIdentityStrip);
    setTimeout(function () {
      if (!threadClosed) input && input.focus();
    }, 80);
  }

  function close() {
    if (!panel) return;
    panel.classList.remove('is-open');
    fab.classList.remove('is-hidden');
    // Если виджет закрыли вручную — убираем наш history entry,
    // чтобы следующий Back не остался «висеть» пустым.
    if (history.state && history.state.rsChat) {
      history.replaceState(null, '');
    }
  }

  window.addEventListener('popstate', function () {
    // Сначала проверяем lightbox — Back должен закрыть его, а не чат.
    var lb = document.querySelector('.rs-chat-lightbox.is-on');
    if (lb && lb._closeFn) { lb._closeFn(); return; }
    if (isPanelOpen()) close();
  });

  function onOutsideClick(e) {
    if (!isPanelOpen()) return;
    if (panel.contains(e.target)) return;
    if (fab.contains(e.target)) return;
    // Lightbox поверх панели (z-index 10000) — клики там не закрывают чат.
    if (e.target.closest && e.target.closest('.rs-chat-lightbox')) return;
    close();
  }

  function onSend(e) {
    e.preventDefault();
    var text = (input.value || '').trim();
    if (!text && pendingFiles.length === 0) return;
    if (!chatKey) return;
    sendBtn.disabled = true;

    if (pendingFiles.length > 0) {
      var files = pendingFiles.slice();
      pendingFiles = [];
      renderPendingFiles();
      input.value = '';
      autoresize();
      sendFilesAsBatch(files, text);
      return;
    }

    var ts = Math.floor(Date.now() / 1000);
    pushAndStore({ kind: 'user', text: text, ts: ts });
    input.value = '';
    autoresize();
    apiSend(text)
      .catch(function () {
        pushSys('Сообщение не отправлено. Проверьте интернет и попробуйте ещё раз.');
      })
      .finally(function () {
        sendBtn.disabled = false;
        input.focus();
      });
  }

  function onFilePick(e) {
    var files = Array.from ? Array.from(e.target.files || []) : [].slice.call(e.target.files || []);
    e.target.value = '';
    if (!chatKey || files.length === 0) return;
    files.forEach(function (f) {
      if (f.size > MAX_UPLOAD_BYTES) {
        pushSys('Файл «' + f.name + '» слишком большой (максимум 10 МБ).');
        return;
      }
      if (pendingFiles.length >= MAX_PENDING_FILES) {
        pushSys('Не больше ' + MAX_PENDING_FILES + ' файлов за раз.');
        return;
      }
      addPendingFile(f);
    });
    input.focus();
  }

  function addPendingFile(f) {
    var isImage = (f.type || '').indexOf('image/') === 0;
    var entry = {
      file: f,
      localUrl: URL.createObjectURL(f),
      isImage: isImage,
    };
    pendingFiles.push(entry);
    renderPendingFiles();
  }

  function removePendingFile(idx) {
    var entry = pendingFiles[idx];
    if (entry) {
      try { URL.revokeObjectURL(entry.localUrl); } catch (_) {}
    }
    pendingFiles.splice(idx, 1);
    renderPendingFiles();
  }

  function renderPendingFiles() {
    if (!pendingHost) return;
    pendingHost.innerHTML = '';
    pendingHost.classList.toggle('is-empty', pendingFiles.length === 0);
    pendingFiles.forEach(function (entry, idx) {
      var item = document.createElement('div');
      item.className = 'rs-pending-item';
      if (entry.isImage) {
        var img = document.createElement('img');
        img.src = entry.localUrl;
        img.alt = entry.file.name || '';
        item.appendChild(img);
      } else {
        var icon = document.createElement('span');
        icon.className = 'rs-pending-icon';
        icon.textContent = '📎';
        item.appendChild(icon);
        var name = document.createElement('span');
        name.className = 'rs-pending-name';
        name.textContent = entry.file.name;
        item.appendChild(name);
      }
      var rm = document.createElement('button');
      rm.type = 'button';
      rm.className = 'rs-pending-remove';
      rm.setAttribute('aria-label', 'Убрать файл');
      rm.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg>';
      rm.addEventListener('click', function (e) {
        e.stopPropagation(); // не пузырить до document → onOutsideClick не закроет панель
        removePendingFile(idx);
      });
      item.appendChild(rm);
      pendingHost.appendChild(item);
    });
  }

  function sendFilesAsBatch(entries, caption) {
    var ts = Math.floor(Date.now() / 1000);
    var localFiles = entries.map(function (e) {
      return { url: e.localUrl, name: e.file.name, mime: e.file.type,
               kind: e.isImage ? 'image' : 'file', size: e.file.size };
    });
    var msg = { kind: 'user', ts: ts, batch: localFiles };
    if (caption) msg.text = caption;
    var el = renderMsg(msg, true);
    el.classList.add('is-pending');

    var fd = new FormData();
    fd.append('chat_key', chatKey);
    fd.append('browser_id', browserId());
    if (caption) fd.append('caption', caption);
    entries.forEach(function (e) { fd.append('file', e.file, e.file.name); });

    fetch(API_BASE + '/upload_batch', { method: 'POST', body: fd })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error('batch ' + r.status + ': ' + t); });
        return r.json();
      })
      .then(function (data) {
        msg.batch = data.files;
        el.classList.remove('is-pending');
        // Перерисуем bubble с серверными URL
        var newEl = renderMsg(msg, false);
        body.removeChild(el);
        body.appendChild(newEl);
        entries.forEach(function (e) { try { URL.revokeObjectURL(e.localUrl); } catch (_) {} });
        var h = loadHistory();
        h.push(msg);
        saveHistory(h);
        scrollDown();
      })
      .catch(function (err) {
        console.warn('batch upload failed:', err);
        el.classList.remove('is-pending');
        el.classList.add('is-error');
        pushSys('Не удалось загрузить файлы. Проверьте интернет и попробуйте ещё раз.');
      })
      .finally(function () {
        sendBtn.disabled = false;
        input.focus();
      });
  }

  function uploadFile(f, localUrl, caption) {
    var isImage = (f.type || '').indexOf('image/') === 0;
    var ownsUrl = !localUrl;
    if (!localUrl) localUrl = URL.createObjectURL(f);
    var ts = Math.floor(Date.now() / 1000);
    var msg = {
      kind: 'user', ts: ts,
      file: { url: localUrl, name: f.name, mime: f.type, kind: isImage ? 'image' : 'file', size: f.size },
    };
    if (caption) msg.text = caption;
    var el = renderMsg(msg, true);
    el.classList.add('is-pending');

    var fd = new FormData();
    fd.append('chat_key', chatKey);
    fd.append('browser_id', browserId());
    fd.append('file', f);
    if (caption) fd.append('caption', caption);

    return fetch(API_BASE + '/upload', { method: 'POST', body: fd })
      .then(function (r) {
        if (!r.ok) return r.text().then(function (t) { throw new Error('upload ' + r.status + ': ' + t); });
        return r.json();
      })
      .then(function (data) {
        msg.file.url = data.url;
        msg.file.name = data.name;
        msg.file.mime = data.mime;
        msg.file.size = data.size;
        msg.file.kind = data.kind;
        var img = el.querySelector('img');
        if (img) img.src = data.url;
        // Перепривяжем lightbox-кнопку (после смены url подмены не нужно — handler читает file.url замыкания, но
        // оно уже видит свежие значения)
        el.classList.remove('is-pending');
        var h = loadHistory();
        h.push(msg);
        saveHistory(h);
        try { if (ownsUrl) URL.revokeObjectURL(localUrl); } catch (_) {}
      })
      .catch(function (err) {
        console.warn('upload failed:', err);
        el.classList.remove('is-pending');
        el.classList.add('is-error');
        pushSys('Не удалось загрузить файл «' + (f.name || '') + '».');
      });
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  function boot() {
    buildDOM();
    ensureWelcomeBadge();
    document.addEventListener('click', onOutsideClick);
    var existing = getCookie(COOKIE_KEY);
    if (existing) {
      // Ленивая прелоадка: подтянем chat_key и стрим, чтобы видеть сообщения,
      // даже когда панель ещё закрыта (бейджик непрочитанных).
      apiInit().then(function () { openStream(); }).catch(function () {});
    }
    // Распознаём аккаунт по browser_id — для текста облачка и полоски «это вы?».
    fetchIdentity().then(renderIdentityStrip);
    // Переход по персональной ссылке из рассылки: link.js привязывает браузер уже
    // после нашего запроса выше, поэтому ждём его сигнал и перезапрашиваем —
    // иначе человек был бы узнан только со следующей загрузки страницы.
    window.addEventListener('rs:browser-linked', function () {
      fetchIdentity().then(renderIdentityStrip);
    });
    schedulePromo();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
