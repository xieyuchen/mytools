/**
 * DevTools App - 主应用逻辑
 */
class App {
  static init() {
    this.renderSidebar();
    this.bindEvents();
    Router.init();
    this.registerSW();
  }

  /** 注册 Service Worker */
  static async registerSW() {
    if ('serviceWorker' in navigator) {
      try {
        await navigator.serviceWorker.register('./public/sw.js');
      } catch (e) {
        console.log('SW registration skipped:', e.message);
      }
    }
  }

  /** 渲染侧边栏导航 */
  static renderSidebar() {
    const nav = document.getElementById('sidebar-nav');
    if (!nav) return;

    let html = '';
    html += `<div class="nav-item active" data-route="" onclick="Router.navigate('')">
      <span class="icon">🏠</span><span>首页</span>
    </div>`;

    const categories = ToolRegistry.getCategories();
    for (const [category, tools] of categories) {
      html += `<div class="nav-section-title">${category}</div>`;
      for (const tool of tools) {
        html += `<div class="nav-item" data-route="${tool.id}" onclick="Router.navigate('${tool.id}')">
          <span class="icon">${tool.icon}</span><span>${tool.name}</span>
        </div>`;
      }
    }

    nav.innerHTML = html;
  }

  /** 高亮当前导航项 */
  static setActiveNav(toolId) {
    document.querySelectorAll('.nav-item').forEach((item) => {
      item.classList.toggle('active', item.dataset.route === (toolId || ''));
    });
  }

  /** 渲染首页工具网格 */
  static renderHome() {
    const page = document.getElementById('page-content');
    const tools = ToolRegistry.getAll();

    let html = `
      <h2 class="page-title">工具箱</h2>
      <p class="page-desc">选择一个工具开始使用，所有工具支持离线运行</p>
      <div class="tools-grid">
    `;

    for (const tool of tools) {
      html += `
        <div class="tool-card" onclick="Router.navigate('${tool.id}')">
          <span class="card-icon">${tool.icon}</span>
          <div class="card-name">${tool.name}</div>
          <div class="card-desc">${tool.desc}</div>
        </div>
      `;
    }

    html += '</div>';
    page.innerHTML = html;
    this.setActiveNav('');
    document.getElementById('topbar-title').textContent = 'DevTools';
  }

  /** 渲染工具页面 */
  static renderTool(toolId) {
    const tool = ToolRegistry.getById(toolId);
    const page = document.getElementById('page-content');

    if (!tool) {
      page.innerHTML = `<h2 class="page-title">工具未找到</h2>
        <p class="page-desc">请返回首页选择工具</p>`;
      return;
    }

    page.innerHTML = `
      <h2 class="page-title">${tool.icon} ${tool.name}</h2>
      <p class="page-desc">${tool.desc}</p>
      <div id="tool-root" class="tool-container"></div>
    `;

    const root = document.getElementById('tool-root');
    tool.render(root);
    this.setActiveNav(toolId);
    document.getElementById('topbar-title').textContent = tool.name;
  }

  /** 绑定全局事件 */
  static bindEvents() {
    // 移动端菜单按钮
    document.getElementById('menu-btn')?.addEventListener('click', () => {
      document.querySelector('.sidebar')?.classList.toggle('open');
      document.querySelector('.sidebar-overlay')?.classList.toggle('show');
    });

    // 点击遮罩关闭侧边栏
    document.querySelector('.sidebar-overlay')?.addEventListener('click', () => {
      document.querySelector('.sidebar')?.classList.remove('open');
      document.querySelector('.sidebar-overlay')?.classList.remove('show');
    });
  }

  /** 工具函数：复制到剪贴板 */
  static async copyText(text) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // fallback
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      return true;
    }
  }
}

// DOM 加载后初始化
document.addEventListener('DOMContentLoaded', () => App.init());
