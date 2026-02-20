/**
 * 简易 Hash 路由
 * 路由格式: #/tool/{toolId}
 */
class Router {
  static init() {
    window.addEventListener('hashchange', () => Router.handleRoute());
    // 初始路由
    Router.handleRoute();
  }

  static navigate(toolId) {
    window.location.hash = toolId ? `#/tool/${toolId}` : '#/';
  }

  static handleRoute() {
    const hash = window.location.hash || '#/';
    const match = hash.match(/^#\/tool\/(.+)$/);

    if (match) {
      const toolId = match[1];
      App.renderTool(toolId);
    } else {
      App.renderHome();
    }

    // 关闭移动端侧边栏
    document.querySelector('.sidebar')?.classList.remove('open');
    document.querySelector('.sidebar-overlay')?.classList.remove('show');
  }
}

window.Router = Router;
