// Общий JS для лендинга: device_id, fingerprint, auto-highlight платформы.
(function() {
  function uuid() {
    return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, function(c) {
      return (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c/4)).toString(16);
    });
  }

  var did = localStorage.getItem('rs_device_id');
  if (!did) { did = uuid(); localStorage.setItem('rs_device_id', did); }
  // Заполняем все поля device_id на странице (форма в hero + форма в CTA-band)
  document.querySelectorAll('#device_id, .device_id').forEach(function(el) {
    el.value = did;
  });

  function detectOS() {
    var ua = navigator.userAgent;
    if (/Android/i.test(ua)) return 'android';
    if (/iPhone|iPad|iPod/i.test(ua)) return 'ios';
    if (/Macintosh|Mac OS X/i.test(ua)) return 'macos';
    if (/Windows/i.test(ua)) return 'windows';
    return null;
  }
  var os = detectOS();
  if (os) {
    var card = document.querySelector('.plat-card[data-os="' + os + '"]');
    if (card && !card.classList.contains('highlight')) {
      card.classList.add('highlight');
      var badge = document.createElement('span');
      badge.className = 'plat-badge';
      badge.textContent = '★ Рекомендуем';
      card.insertBefore(badge, card.firstChild);
      var note = document.createElement('span');
      note.className = 'plat-note';
      note.textContent = 'Мы определили вашу платформу — этот вариант подойдёт вам лучше всего.';
      card.insertBefore(note, card.querySelector('.plat-link') || null);
    }
  }
})();
