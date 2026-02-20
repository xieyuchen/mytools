# DevTools - 个人工具箱

跨平台 PWA 个人工具箱，支持 iPhone、iPad、Web 浏览器和所有移动端设备。

## 特性

- **跨平台** - 一套代码，iPhone / iPad / Web / Android 全平台运行
- **离线可用** - Service Worker 缓存，无网络也能使用
- **可安装** - 支持添加到主屏幕，像原生 App 一样使用
- **插件化** - 工具独立注册，添加新工具只需 3 步
- **响应式** - 自动适配不同屏幕尺寸，iPhone 刘海屏安全区适配

## 快速开始

无需构建工具，直接用任何 HTTP 服务器启动：

```bash
# 方式 1: Python
python3 -m http.server 8080

# 方式 2: Node.js
npx serve .

# 方式 3: PHP
php -S localhost:8080
```

然后访问 `http://localhost:8080`

## 添加新工具

1. 在 `src/tools/` 下创建 JS 文件
2. 调用 `ToolRegistry.register()` 注册
3. 在 `index.html` 中添加 `<script>` 引入

示例：

```javascript
// src/tools/my-tool.js
ToolRegistry.register({
  id: 'my-tool',
  name: '我的工具',
  icon: '🔧',
  category: '自定义',
  desc: '工具描述',
  render(container) {
    container.innerHTML = '<div class="card">工具内容</div>';
  },
});
```

## 目录结构

```
mytools/
├── index.html              # 主入口
├── public/
│   ├── manifest.json       # PWA 清单
│   ├── sw.js               # Service Worker
│   └── icons/              # App 图标
├── src/
│   ├── css/
│   │   └── style.css       # 全局样式
│   ├── js/
│   │   ├── app.js          # 主应用逻辑
│   │   ├── router.js       # Hash 路由
│   │   └── toolRegistry.js # 工具注册中心
│   └── tools/
│       └── sample-tool.js  # 示例工具 (JSON 格式化)
└── README.md
```

## iPhone / iPad 安装

1. 用 Safari 打开应用地址
2. 点击分享按钮 → "添加到主屏幕"
3. 即可像原生 App 一样全屏使用
