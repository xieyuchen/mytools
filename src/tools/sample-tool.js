/**
 * 示例工具 - JSON 格式化
 *
 * 这是一个示例工具，展示如何添加新工具到框架中。
 *
 * 添加新工具的步骤：
 * 1. 在 src/tools/ 下创建新的 JS 文件
 * 2. 调用 ToolRegistry.register() 注册工具
 * 3. 在 index.html 的 <!-- 工具脚本 --> 区域添加 <script> 引入
 *
 * register 参数说明：
 * - id:       路由标识 (英文，如 'my-tool')
 * - name:     显示名称
 * - icon:     emoji 图标
 * - category: 分类名称，相同分类的工具会归为一组
 * - desc:     简短描述
 * - render:   渲染函数，接收一个 DOM 容器，在其中构建工具 UI
 */
ToolRegistry.register({
  id: 'json-formatter',
  name: 'JSON 格式化',
  icon: '📋',
  category: '编码转换',
  desc: 'JSON 美化、压缩、验证',

  render(container) {
    container.innerHTML = `
      <div class="card">
        <div class="card-title">输入</div>
        <textarea id="json-input" placeholder='粘贴 JSON 内容，如 {"key": "value"}'></textarea>
      </div>

      <div class="btn-group">
        <button class="btn btn-primary" id="json-format-btn">格式化</button>
        <button class="btn btn-secondary" id="json-compact-btn">压缩</button>
        <button class="btn btn-danger" id="json-clear-btn">清空</button>
      </div>

      <div id="json-error"></div>

      <div class="card">
        <div class="card-title">
          输出
          <button class="btn btn-copy" id="json-copy-btn">📋 复制</button>
        </div>
        <div class="output-area">
          <textarea id="json-output" readonly placeholder="结果将显示在这里"></textarea>
        </div>
      </div>
    `;

    const input = container.querySelector('#json-input');
    const output = container.querySelector('#json-output');
    const error = container.querySelector('#json-error');

    container.querySelector('#json-format-btn').addEventListener('click', () => {
      error.innerHTML = '';
      try {
        const obj = JSON.parse(input.value);
        output.value = JSON.stringify(obj, null, 2);
      } catch (e) {
        error.innerHTML = `<p class="error-msg">JSON 解析错误: ${e.message}</p>`;
      }
    });

    container.querySelector('#json-compact-btn').addEventListener('click', () => {
      error.innerHTML = '';
      try {
        const obj = JSON.parse(input.value);
        output.value = JSON.stringify(obj);
      } catch (e) {
        error.innerHTML = `<p class="error-msg">JSON 解析错误: ${e.message}</p>`;
      }
    });

    container.querySelector('#json-clear-btn').addEventListener('click', () => {
      input.value = '';
      output.value = '';
      error.innerHTML = '';
    });

    container.querySelector('#json-copy-btn').addEventListener('click', async () => {
      if (output.value) {
        await App.copyText(output.value);
        container.querySelector('#json-copy-btn').textContent = '✅ 已复制';
        setTimeout(() => {
          container.querySelector('#json-copy-btn').textContent = '📋 复制';
        }, 1500);
      }
    });
  },
});
