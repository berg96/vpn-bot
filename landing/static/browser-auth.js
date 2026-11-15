(function () {
  var form = document.getElementById('browser-link-form');
  var input = document.getElementById('browser-link-code');
  var status = document.getElementById('browser-link-status');
  form.addEventListener('submit', async function (event) {
    event.preventDefault();
    var button = form.querySelector('button');
    button.disabled = true;
    status.textContent = 'Проверяем код...';
    try {
      var response = await fetch('/api/browser-auth', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({code: input.value.trim()})
      });
      var data = await response.json();
      if (!response.ok || !data.ok) {
        status.textContent = 'Код недействителен или истёк. Получи новый код в боте.';
      } else {
        input.value = '';
        window.location.href = '/#my-sub-section';
        window.location.reload();
      }
    } catch (_) {
      status.textContent = 'Не удалось проверить код. Попробуй ещё раз.';
    } finally {
      button.disabled = false;
    }
  });
})();
