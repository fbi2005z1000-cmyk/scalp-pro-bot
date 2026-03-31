(function () {
  class GlossaryPanel {
    constructor(container) {
      this.container = container;
      this.data = {};
    }

    setData(data) {
      this.data = data || {};
      this.render();
    }

    render() {
      const entries = Object.entries(this.data);
      if (!entries.length) {
        this.container.innerHTML = '<div class="term">Chưa tải dữ liệu thuật ngữ.</div>';
        return;
      }

      this.container.innerHTML = entries
        .map(([term, value]) => {
          return `<div class="term">
            <div class="term-title" data-tip="${this.escape(value.detail || '')}">${term}</div>
            <div class="term-short">${this.escape(value.short || '')}</div>
          </div>`;
        })
        .join('');
    }

    escape(str) {
      return String(str)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }
  }

  window.GlossaryPanel = GlossaryPanel;
})();
