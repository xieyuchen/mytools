/**
 * 待阅读列表工具
 *
 * 使用 localStorage 持久化存储 + GitHub Gist 云同步，支持：
 * - 添加文章（标题 + URL + 可选标签）
 * - 只粘贴链接，后台自动通过 LLM 补充标题和中文摘要
 * - 标记已读 / 未读
 * - 按标签筛选
 * - 删除条目
 * - 通过 GitHub Gist 跨设备同步
 */
ToolRegistry.register({
  id: 'reading-list',
  name: '待阅读列表',
  icon: '📚',
  category: '效率工具',
  desc: '收藏想读的文章，支持标签和已读标记',

  _STORAGE_KEY: 'devtools_reading_list',
  _SYNC_KEY: 'devtools_reading_list_sync',
  _LLM_KEY: 'devtools_reading_list_llm',
  _GIST_FILENAME: 'devtools-reading-list.json',

  _load() {
    try {
      return JSON.parse(localStorage.getItem(this._STORAGE_KEY)) || [];
    } catch {
      return [];
    }
  },

  _save(items) {
    localStorage.setItem(this._STORAGE_KEY, JSON.stringify(items));
  },

  _getSyncConfig() {
    try {
      return JSON.parse(localStorage.getItem(this._SYNC_KEY)) || {};
    } catch {
      return {};
    }
  },

  _saveSyncConfig(config) {
    localStorage.setItem(this._SYNC_KEY, JSON.stringify(config));
  },

  _getLLMConfig() {
    try {
      return JSON.parse(localStorage.getItem(this._LLM_KEY)) || {};
    } catch {
      return {};
    }
  },

  _saveLLMConfig(config) {
    localStorage.setItem(this._LLM_KEY, JSON.stringify(config));
  },

  _isURL(str) {
    return /^https?:\/\//i.test(str.trim());
  },

  async _gistRequest(method, path, token, body) {
    let res;
    try {
      res = await fetch(`https://api.github.com${path}`, {
        method,
        headers: {
          Authorization: `token ${token}`,
          Accept: 'application/vnd.github.v3+json',
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
    } catch (e) {
      throw new Error('网络请求失败，请检查网络连接');
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      if (res.status === 401) throw new Error('Token 无效或已过期');
      if (res.status === 403) throw new Error('Token 权限不足，请确认勾选了 gist 权限（需使用 classic token）');
      if (res.status === 404) throw new Error('Gist 不存在，请检查 ID');
      throw new Error(err.message || `GitHub API 错误 (HTTP ${res.status})`);
    }
    return res.json();
  },

  async _createGist(token) {
    const items = this._load();
    const data = await this._gistRequest('POST', '/gists', token, {
      description: 'DevTools - 待阅读列表同步数据',
      public: false,
      files: {
        [this._GIST_FILENAME]: {
          content: JSON.stringify(items, null, 2),
        },
      },
    });
    return data.id;
  },

  async _pushToGist(token, gistId) {
    const items = this._load();
    await this._gistRequest('PATCH', `/gists/${gistId}`, token, {
      files: {
        [this._GIST_FILENAME]: {
          content: JSON.stringify(items, null, 2),
        },
      },
    });
  },

  async _pullFromGist(token, gistId) {
    const data = await this._gistRequest('GET', `/gists/${gistId}`, token);
    const file = data.files[this._GIST_FILENAME];
    if (!file) throw new Error('Gist 中未找到阅读列表数据');
    return JSON.parse(file.content);
  },

  _mergeItems(local, remote) {
    const map = new Map();
    remote.forEach((item) => map.set(item.id, item));
    local.forEach((item) => {
      const existing = map.get(item.id);
      if (!existing || (item.updatedAt || item.createdAt) >= (existing.updatedAt || existing.createdAt)) {
        map.set(item.id, item);
      }
    });
    return [...map.values()].sort((a, b) => b.createdAt - a.createdAt);
  },

  async _fetchPageContent(url) {
    // Try direct fetch first, then CORS proxy fallback
    const targets = [
      url,
      `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`,
    ];
    for (const target of targets) {
      try {
        const res = await fetch(target, {
          headers: { Accept: 'text/html' },
          signal: AbortSignal.timeout(10000),
        });
        if (!res.ok) continue;
        const html = await res.text();
        if (html.length > 0) return html;
      } catch {
        continue;
      }
    }
    throw new Error('无法获取页面内容');
  },

  _extractTextFromHTML(html) {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    // Remove scripts and styles
    doc.querySelectorAll('script, style, nav, footer, header, aside').forEach((el) => el.remove());
    const title = doc.querySelector('title')?.textContent?.trim() || '';
    // Get main content: prefer article/main, fallback to body
    const main = doc.querySelector('article') || doc.querySelector('main') || doc.body;
    let text = main?.innerText || main?.textContent || '';
    // Truncate to ~4000 chars to stay within LLM context limits
    text = text.replace(/\s+/g, ' ').trim().slice(0, 4000);
    return { title, text };
  },

  async _callLLM(pageTitle, pageText, url) {
    const config = this._getLLMConfig();
    if (!config.apiKey || !config.endpoint) {
      throw new Error('未配置 LLM');
    }

    const model = config.model || 'gpt-4o-mini';
    const prompt = `你是一个阅读助手。根据以下网页内容，返回 JSON 格式结果：
{"title": "文章的原始标题", "summary": "100字以内的中文摘要"}

网页 URL: ${url}
网页 title 标签: ${pageTitle}
网页正文节选:
${pageText}

只返回 JSON，不要返回其他内容。`;

    const res = await fetch(config.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({
        model,
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.3,
      }),
      signal: AbortSignal.timeout(30000),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error?.message || `LLM API 错误 (HTTP ${res.status})`);
    }

    const data = await res.json();
    const content = data.choices?.[0]?.message?.content || '';
    // Extract JSON from response (handle markdown code blocks)
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('LLM 返回格式异常');
    return JSON.parse(jsonMatch[0]);
  },

  async _enrichItem(itemId, renderList, renderFilters) {
    const config = this._getLLMConfig();
    if (!config.apiKey || !config.endpoint) return;

    const items = this._load();
    const item = items.find((i) => i.id === itemId);
    if (!item || !item.url || item.enriched) return;

    try {
      // Mark as enriching
      item.enriching = true;
      this._save(items);
      renderList();

      const html = await this._fetchPageContent(item.url);
      const { title: pageTitle, text: pageText } = this._extractTextFromHTML(html);
      const result = await this._callLLM(pageTitle, pageText, item.url);

      // Re-load to avoid overwriting concurrent changes
      const freshItems = this._load();
      const freshItem = freshItems.find((i) => i.id === itemId);
      if (!freshItem) return;

      // Only overwrite title if the original was just a URL
      if (this._isURL(freshItem.title) && result.title) {
        freshItem.title = result.title;
      }
      freshItem.summary = result.summary || '';
      freshItem.enriched = true;
      delete freshItem.enriching;
      freshItem.updatedAt = Date.now();
      this._save(freshItems);
      renderFilters();
      renderList();
    } catch (e) {
      // Remove enriching flag on failure
      const freshItems = this._load();
      const freshItem = freshItems.find((i) => i.id === itemId);
      if (freshItem) {
        delete freshItem.enriching;
        freshItem.enrichError = e.message;
        this._save(freshItems);
        renderList();
      }
    }
  },

  _getAllTags(items) {
    const tags = new Set();
    items.forEach((item) => {
      if (item.tags) item.tags.forEach((t) => tags.add(t));
    });
    return [...tags].sort();
  },

  _formatDate(ts) {
    const d = new Date(ts);
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${d.getFullYear()}-${month}-${day}`;
  },

  render(container) {
    const self = this;
    let currentFilter = 'all';
    let currentFilterType = 'status';
    let syncing = false;

    container.innerHTML = `
      <div class="card">
        <div class="card-title">添加文章</div>
        <div class="rl-add-form">
          <input type="text" id="rl-url" placeholder="粘贴链接，自动获取标题和摘要" />
          <input type="text" id="rl-title" placeholder="标题（可选，留空自动获取）" />
          <input type="text" id="rl-tags" placeholder="标签，用逗号分隔（可选）" />
          <button class="btn btn-primary" id="rl-add-btn">添加</button>
        </div>
        <div id="rl-error"></div>
      </div>

      <div class="rl-filters" id="rl-filters"></div>

      <div class="card">
        <div class="card-title">
          阅读列表
          <span class="rl-count" id="rl-count"></span>
        </div>
        <div id="rl-list"></div>
        <div id="rl-empty" class="rl-empty" style="display:none">
          暂无内容，添加一篇文章开始吧
        </div>
      </div>

      <div class="card">
        <div class="card-title">设置</div>
        <details id="rl-llm-settings">
          <summary>AI 摘要（配置 LLM API 后粘贴链接自动生成标题和摘要）</summary>
          <div class="rl-add-form" style="margin-top:12px">
            <input type="text" id="rl-llm-endpoint" autocomplete="off" placeholder="API Endpoint（如 https://api.openai.com/v1/chat/completions）" />
            <input type="text" id="rl-llm-key" autocomplete="off" placeholder="API Key" />
            <input type="text" id="rl-llm-model" autocomplete="off" placeholder="模型名（默认 gpt-4o-mini）" />
            <button class="btn btn-primary" id="rl-llm-save-btn">保存</button>
          </div>
          <div id="rl-llm-msg" style="margin-top:8px"></div>
        </details>
        <details id="rl-sync-settings" style="margin-top:12px">
          <summary>云同步（通过 GitHub Gist 跨设备同步）</summary>
          <div id="rl-sync-area" style="margin-top:12px"></div>
        </details>
      </div>
    `;

    const titleInput = container.querySelector('#rl-title');
    const urlInput = container.querySelector('#rl-url');
    const tagsInput = container.querySelector('#rl-tags');
    const errorEl = container.querySelector('#rl-error');
    const listEl = container.querySelector('#rl-list');
    const emptyEl = container.querySelector('#rl-empty');
    const countEl = container.querySelector('#rl-count');
    const filtersEl = container.querySelector('#rl-filters');
    const syncArea = container.querySelector('#rl-sync-area');

    // ---- LLM Settings ----
    (function initLLMSettings() {
      const config = self._getLLMConfig();
      const endpointInput = container.querySelector('#rl-llm-endpoint');
      const keyInput = container.querySelector('#rl-llm-key');
      const modelInput = container.querySelector('#rl-llm-model');
      const msgEl = container.querySelector('#rl-llm-msg');

      if (config.endpoint) endpointInput.value = config.endpoint;
      if (config.apiKey) keyInput.value = config.apiKey;
      if (config.model) modelInput.value = config.model;

      // If already configured, show status
      if (config.endpoint && config.apiKey) {
        msgEl.innerHTML = '<p class="success-msg">已配置</p>';
      }

      container.querySelector('#rl-llm-save-btn').addEventListener('click', () => {
        const endpoint = endpointInput.value.trim();
        const apiKey = keyInput.value.trim();
        const model = modelInput.value.trim();

        if (!endpoint || !apiKey) {
          msgEl.innerHTML = '<p class="error-msg">请填写 Endpoint 和 API Key</p>';
          return;
        }

        self._saveLLMConfig({ endpoint, apiKey, model: model || 'gpt-4o-mini' });
        msgEl.innerHTML = '<p class="success-msg">已保存</p>';
      });
    })();

    // ---- Sync UI ----
    function renderSyncUI() {
      const config = self._getSyncConfig();

      if (config.token && config.gistId) {
        syncArea.innerHTML = `
          <div class="rl-sync-status">
            <span class="rl-sync-dot connected"></span>
            <span>已连接 Gist</span>
            <span class="rl-sync-id" id="rl-gist-id-display" title="点击复制">${config.gistId}</span>
          </div>
          ${config.lastSync ? `<div class="rl-sync-time">上次同步: ${self._formatDate(config.lastSync)}</div>` : ''}
          <div class="btn-group" style="margin-top:12px">
            <button class="btn btn-primary" id="rl-sync-btn">同步</button>
            <button class="btn btn-secondary" id="rl-push-btn">强制上传</button>
            <button class="btn btn-secondary" id="rl-pull-btn">强制下载</button>
            <button class="btn btn-danger" id="rl-disconnect-btn">断开</button>
          </div>
          <div id="rl-sync-msg" style="margin-top:8px"></div>
        `;

        syncArea.querySelector('#rl-gist-id-display').addEventListener('click', () => {
          navigator.clipboard.writeText(config.gistId).then(() => {
            const el = syncArea.querySelector('#rl-gist-id-display');
            const original = el.textContent;
            el.textContent = '已复制!';
            setTimeout(() => { el.textContent = original; }, 1500);
          });
        });
        syncArea.querySelector('#rl-sync-btn').addEventListener('click', async () => {
          await doSync('merge');
        });
        syncArea.querySelector('#rl-push-btn').addEventListener('click', async () => {
          await doSync('push');
        });
        syncArea.querySelector('#rl-pull-btn').addEventListener('click', async () => {
          await doSync('pull');
        });
        syncArea.querySelector('#rl-disconnect-btn').addEventListener('click', () => {
          self._saveSyncConfig({});
          renderSyncUI();
        });
      } else {
        syncArea.innerHTML = `
          <div class="rl-add-form">
            <input type="text" id="rl-token-input" autocomplete="off" placeholder="GitHub Personal Access Token" />
            <input type="text" id="rl-gist-id-input" autocomplete="off" placeholder="Gist ID（留空则自动创建）" />
            <button class="btn btn-primary" id="rl-connect-btn">连接</button>
          </div>
          <div id="rl-sync-msg" style="margin-top:8px"></div>
          <details class="rl-sync-help">
            <summary>如何获取 Token？</summary>
            <ol>
              <li>打开 GitHub → Settings → Developer settings</li>
              <li>Personal access tokens → Tokens (classic)</li>
              <li>Generate new token，只勾选 <strong>gist</strong> 权限</li>
              <li>复制生成的 token 粘贴到上方</li>
            </ol>
          </details>
        `;

        const tokenInput = syncArea.querySelector('#rl-token-input');
        const gistIdInput = syncArea.querySelector('#rl-gist-id-input');
        const connectMsgEl = syncArea.querySelector('#rl-sync-msg');

        syncArea.querySelector('#rl-connect-btn').addEventListener('click', async () => {
          const token = tokenInput.value.trim();
          const gistId = gistIdInput.value.trim();

          if (!token) {
            connectMsgEl.innerHTML = '<p class="error-msg">请输入 Token</p>';
            return;
          }

          connectMsgEl.innerHTML = '<p class="rl-sync-loading">验证中...</p>';

          try {
            await self._gistRequest('GET', '/gists?per_page=1', token);

            let finalGistId = gistId;
            if (!finalGistId) {
              connectMsgEl.innerHTML = '<p class="rl-sync-loading">创建 Gist...</p>';
              finalGistId = await self._createGist(token);
            } else {
              await self._gistRequest('GET', `/gists/${finalGistId}`, token);
            }

            self._saveSyncConfig({ token, gistId: finalGistId, lastSync: Date.now() });

            if (gistId) {
              renderSyncUI();
              await doSync('merge');
            } else {
              renderSyncUI();
            }
          } catch (e) {
            connectMsgEl.innerHTML = `<p class="error-msg">连接失败: ${e.message}</p>`;
          }
        });
      }
    }

    async function doSync(mode) {
      if (syncing) return;
      syncing = true;
      const config = self._getSyncConfig();
      const msgEl = syncArea.querySelector('#rl-sync-msg');
      const syncBtn = syncArea.querySelector('#rl-sync-btn');
      const pushBtn = syncArea.querySelector('#rl-push-btn');
      const pullBtn = syncArea.querySelector('#rl-pull-btn');
      if (syncBtn) syncBtn.disabled = true;
      if (pushBtn) pushBtn.disabled = true;
      if (pullBtn) pullBtn.disabled = true;
      if (msgEl) msgEl.innerHTML = '<p class="rl-sync-loading">同步中...</p>';

      try {
        if (mode === 'push') {
          await self._pushToGist(config.token, config.gistId);
          if (msgEl) msgEl.innerHTML = '<p class="success-msg">已上传到 Gist</p>';
        } else if (mode === 'pull') {
          const remote = await self._pullFromGist(config.token, config.gistId);
          self._save(remote);
          if (msgEl) msgEl.innerHTML = '<p class="success-msg">已从 Gist 下载</p>';
        } else {
          const remote = await self._pullFromGist(config.token, config.gistId);
          const local = self._load();
          const merged = self._mergeItems(local, remote);
          self._save(merged);
          await self._pushToGist(config.token, config.gistId);
          if (msgEl) msgEl.innerHTML = '<p class="success-msg">同步完成</p>';
        }
        config.lastSync = Date.now();
        self._saveSyncConfig(config);
        renderFilters();
        renderList();
        const timeEl = syncArea.querySelector('.rl-sync-time');
        if (timeEl) timeEl.textContent = '上次同步: ' + self._formatDate(config.lastSync);
      } catch (e) {
        if (msgEl) msgEl.innerHTML = `<p class="error-msg">同步失败: ${e.message}</p>`;
      } finally {
        syncing = false;
        if (syncBtn) syncBtn.disabled = false;
        if (pushBtn) pushBtn.disabled = false;
        if (pullBtn) pullBtn.disabled = false;
      }
    }

    // ---- Filters ----
    function renderFilters() {
      const items = self._load();
      const tags = self._getAllTags(items);
      const unreadCount = items.filter((i) => !i.read).length;
      const readCount = items.filter((i) => i.read).length;

      let html = `
        <button class="rl-filter-btn ${currentFilter === 'all' ? 'active' : ''}"
                data-filter="all" data-type="status">
          全部 <span class="rl-badge">${items.length}</span>
        </button>
        <button class="rl-filter-btn ${currentFilter === 'unread' && currentFilterType === 'status' ? 'active' : ''}"
                data-filter="unread" data-type="status">
          未读 <span class="rl-badge">${unreadCount}</span>
        </button>
        <button class="rl-filter-btn ${currentFilter === 'read' && currentFilterType === 'status' ? 'active' : ''}"
                data-filter="read" data-type="status">
          已读 <span class="rl-badge">${readCount}</span>
        </button>
      `;

      tags.forEach((tag) => {
        const count = items.filter(
          (i) => i.tags && i.tags.includes(tag)
        ).length;
        html += `
          <button class="rl-filter-btn ${currentFilter === tag && currentFilterType === 'tag' ? 'active' : ''}"
                  data-filter="${tag}" data-type="tag">
            #${tag} <span class="rl-badge">${count}</span>
          </button>
        `;
      });

      filtersEl.innerHTML = html;

      filtersEl.querySelectorAll('.rl-filter-btn').forEach((btn) => {
        btn.addEventListener('click', () => {
          currentFilter = btn.dataset.filter;
          currentFilterType = btn.dataset.type;
          renderFilters();
          renderList();
        });
      });
    }

    // ---- List ----
    function renderList() {
      let items = self._load();

      if (currentFilterType === 'status') {
        if (currentFilter === 'unread') items = items.filter((i) => !i.read);
        else if (currentFilter === 'read') items = items.filter((i) => i.read);
      } else if (currentFilterType === 'tag') {
        items = items.filter(
          (i) => i.tags && i.tags.includes(currentFilter)
        );
      }

      if (items.length === 0) {
        listEl.innerHTML = '';
        emptyEl.style.display = 'block';
        countEl.textContent = '';
        return;
      }

      emptyEl.style.display = 'none';
      countEl.textContent = `(${items.length})`;

      listEl.innerHTML = items
        .map(
          (item) => `
        <div class="rl-item ${item.read ? 'rl-read' : ''} ${item.enriching ? 'rl-enriching' : ''}" data-id="${item.id}">
          <div class="rl-item-left">
            <button class="rl-check-btn" data-id="${item.id}" title="${item.read ? '标为未读' : '标为已读'}">
              ${item.read ? '✅' : '⬜'}
            </button>
            <div class="rl-item-info">
              ${
                item.url
                  ? `<a class="rl-item-title" href="${item.url}" target="_blank" rel="noopener">${item.title}</a>`
                  : `<span class="rl-item-title">${item.title}</span>`
              }
              ${item.enriching ? '<div class="rl-item-summary rl-enriching-text">AI 正在生成摘要...</div>' : ''}
              ${item.summary ? `<div class="rl-item-summary">${item.summary}</div>` : ''}
              ${item.enrichError ? `<div class="rl-item-summary rl-enrich-error">摘要获取失败: ${item.enrichError}</div>` : ''}
              <div class="rl-item-meta">
                <span>${self._formatDate(item.createdAt)}</span>
                ${item.tags ? item.tags.map((t) => `<span class="rl-tag">#${t}</span>`).join('') : ''}
              </div>
            </div>
          </div>
          <button class="rl-del-btn" data-id="${item.id}" title="删除">✕</button>
        </div>
      `
        )
        .join('');

      listEl.querySelectorAll('.rl-check-btn').forEach((btn) => {
        btn.addEventListener('click', () => {
          const all = self._load();
          const target = all.find((i) => i.id === btn.dataset.id);
          if (target) {
            target.read = !target.read;
            target.updatedAt = Date.now();
            self._save(all);
            renderFilters();
            renderList();
          }
        });
      });

      listEl.querySelectorAll('.rl-del-btn').forEach((btn) => {
        btn.addEventListener('click', () => {
          const all = self._load().filter((i) => i.id !== btn.dataset.id);
          self._save(all);
          renderFilters();
          renderList();
        });
      });
    }

    // ---- Add ----
    container.querySelector('#rl-add-btn').addEventListener('click', () => {
      let title = titleInput.value.trim();
      let url = urlInput.value.trim();
      const tags = tagsInput.value
        .split(/[,，]/)
        .map((t) => t.trim())
        .filter(Boolean);

      // If no URL but title looks like a URL, swap
      if (!url && self._isURL(title)) {
        url = title;
        title = '';
      }

      if (!url && !title) {
        errorEl.innerHTML = '<p class="error-msg">请至少输入链接或标题</p>';
        return;
      }

      errorEl.innerHTML = '';

      const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
      const items = self._load();
      items.unshift({
        id,
        title: title || url,
        url: url || '',
        tags,
        read: false,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      });
      self._save(items);

      titleInput.value = '';
      urlInput.value = '';
      tagsInput.value = '';
      urlInput.focus();

      renderFilters();
      renderList();

      // Background enrichment if URL provided and no manual title
      if (url && !title) {
        self._enrichItem(id, renderList, renderFilters);
      }
    });

    // Initial render
    renderFilters();
    renderList();
    renderSyncUI();
  },
});
