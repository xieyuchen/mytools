/**
 * 工具注册中心
 *
 * 所有工具通过 ToolRegistry.register() 注册，框架自动处理：
 * - 首页工具卡片渲染
 * - 侧边栏导航项
 * - 路由跳转
 *
 * 添加新工具只需：
 * 1. 在 src/tools/ 下创建 JS 文件
 * 2. 调用 ToolRegistry.register({ ... })
 * 3. 在 index.html 中引入该文件
 */
class ToolRegistry {
  static _tools = [];
  static _categories = new Map();

  /**
   * 注册一个工具
   * @param {Object} tool
   * @param {string} tool.id       - 唯一标识，用于路由 (如 'json-formatter')
   * @param {string} tool.name     - 显示名称
   * @param {string} tool.icon     - emoji 图标
   * @param {string} tool.category - 分类名 (如 '编码解码', '文本处理')
   * @param {string} tool.desc     - 简短描述
   * @param {Function} tool.render - 渲染函数，接收容器 DOM 元素
   */
  static register(tool) {
    this._tools.push(tool);
    if (!this._categories.has(tool.category)) {
      this._categories.set(tool.category, []);
    }
    this._categories.get(tool.category).push(tool);
  }

  static getAll() {
    return this._tools;
  }

  static getById(id) {
    return this._tools.find((t) => t.id === id);
  }

  static getCategories() {
    return this._categories;
  }
}

window.ToolRegistry = ToolRegistry;
