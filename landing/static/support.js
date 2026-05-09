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
  var MAX_HISTORY = 200;
  var PROMO_DELAY_MS = 8000;
  var PROMO_AUTOHIDE_MS = 12000;
  var PROMO_TEXT = 'Поможем настроить или ответим на любой вопрос — пишите!';

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
  var fab, panel, body, input, sendBtn, badge, statusDot, promo, foot, closeCard;
  var chatKey = '';
  var es = null;
  var unread = parseInt(localStorage.getItem(LS_UNREAD) || '0', 10) || 0;
  var seenMsgIds = {};
  var promoTimers = [];
  var ratingValue = 0;
  var threadClosed = false;

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
      '<span></span>';
    promo.querySelector('span').textContent = PROMO_TEXT;
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
      '</div>' +
      '<form class="rs-chat-foot">' +
        '<textarea class="rs-chat-input" rows="1" placeholder="Опишите проблему…" maxlength="4000"></textarea>' +
        '<button class="rs-chat-send" type="submit" aria-label="Отправить">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>' +
        '</button>' +
      '</form>';
    document.body.appendChild(panel);

    body = panel.querySelector('.rs-chat-body');
    input = panel.querySelector('.rs-chat-input');
    sendBtn = panel.querySelector('.rs-chat-send');
    statusDot = panel.querySelector('.rs-chat-status');
    foot = panel.querySelector('.rs-chat-foot');
    closeCard = panel.querySelector('.rs-chat-close-card');

    panel.querySelector('.rs-chat-close').addEventListener('click', close);
    foot.addEventListener('submit', onSend);
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(e); }
    });
    input.addEventListener('input', autoresize);

    buildStars(closeCard.querySelector('.rs-stars'));
    closeCard.querySelector('.rs-submit-rate').addEventListener('click', submitRating);
    closeCard.querySelector('.rs-dismiss').addEventListener('click', function () {
      finalizeClose(false);
    });
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

  function showPromo() {
    if (!promo) return;
    if (localStorage.getItem(LS_PROMO_SEEN) === '1') return;
    if (isPanelOpen()) return;
    promo.classList.add('is-on');
    promoTimers.push(setTimeout(function () { hidePromo(false); }, PROMO_AUTOHIDE_MS));
  }

  function hidePromo(persistDismissal) {
    if (!promo) return;
    promo.classList.remove('is-on');
    promoTimers.forEach(clearTimeout);
    promoTimers = [];
    if (persistDismissal) {
      try { localStorage.setItem(LS_PROMO_SEEN, '1'); } catch (_) {}
    }
  }

  function schedulePromo() {
    if (localStorage.getItem(LS_PROMO_SEEN) === '1') return;
    promoTimers.push(setTimeout(showPromo, PROMO_DELAY_MS));
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

  function renderMsg(m, animate) {
    var el = document.createElement('div');
    el.className = 'rs-chat-msg ' + (m.kind || 'sys');
    if (m.text) {
      el.textContent = m.text;
    }
    if (m.ts && m.kind !== 'sys') {
      var ts = document.createElement('span');
      ts.className = 'ts';
      ts.textContent = fmtTime(m.ts);
      el.appendChild(ts);
    }
    body.appendChild(el);
    if (animate) scrollDown();
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
      body: JSON.stringify({ chat_key: chatKey, text: text }),
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
          if (!isPanelOpen()) {
            unread += 1;
            localStorage.setItem(LS_UNREAD, String(unread));
            renderBadge();
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
    unread = 0;
    localStorage.setItem(LS_UNREAD, '0');
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
    setTimeout(function () {
      if (!threadClosed) input && input.focus();
    }, 80);
  }

  function close() {
    if (!panel) return;
    panel.classList.remove('is-open');
    fab.classList.remove('is-hidden');
  }

  function onSend(e) {
    e.preventDefault();
    var text = (input.value || '').trim();
    if (!text) return;
    if (!chatKey) return;
    sendBtn.disabled = true;
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

  // ── Boot ──────────────────────────────────────────────────────────────────
  function boot() {
    buildDOM();
    var existing = getCookie(COOKIE_KEY);
    if (existing) {
      // Ленивая прелоадка: подтянем chat_key и стрим, чтобы видеть сообщения,
      // даже когда панель ещё закрыта (бейджик непрочитанных).
      apiInit().then(function () { openStream(); }).catch(function () {});
    }
    schedulePromo();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
