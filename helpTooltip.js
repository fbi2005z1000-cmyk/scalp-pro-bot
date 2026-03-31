(function () {
  class HelpTooltip {
    constructor() {
      this.tooltip = document.createElement('div');
      this.tooltip.className = 'tooltip';
      document.body.appendChild(this.tooltip);
      this.active = false;
    }

    bind(selector = '[data-tip]') {
      document.addEventListener('mouseover', (e) => {
        const target = e.target.closest(selector);
        if (!target) return;

        const text = target.getAttribute('data-tip');
        if (!text) return;

        this.tooltip.textContent = text;
        this.tooltip.style.opacity = '1';
        this.active = true;
        this.move(e.clientX, e.clientY);
      });

      document.addEventListener('mousemove', (e) => {
        if (!this.active) return;
        this.move(e.clientX, e.clientY);
      });

      document.addEventListener('mouseout', (e) => {
        const target = e.target.closest(selector);
        if (!target) return;
        this.hide();
      });
    }

    move(x, y) {
      this.tooltip.style.left = `${x + 14}px`;
      this.tooltip.style.top = `${y + 14}px`;
    }

    hide() {
      this.active = false;
      this.tooltip.style.opacity = '0';
    }
  }

  window.HelpTooltip = HelpTooltip;
})();
