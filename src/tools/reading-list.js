/**
 * 待阅读列表工具
 *
 * 使用 localStorage 持久化存储 + GitHub Gist 云同步，支持：
 * - 添加文章（标题 + URL + 可选标签）
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
    // Remote first, then local overrides with newer data
    remote.forEach((item) => map.set(item.id, item));
    local.forEach((item) => {
      const existing = map.get(item.id);
      if (!existing || (item.updatedAt || item.createdAt) >= (existing.updatedAt || existing.createdAt)) {
        map.set(item.id, item);
      }
    });
    // Sort by createdAt descending
    return [...map.values()].sort((a, b) => b.createdAt - a.createdAt);
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

    const syncConfig = self._getSyncConfig();

    container.innerHTML = `
      <div class="card">
        <div class="card-title">添加文章</div>
        <div class="rl-add-form">
          <input type="text" id="rl-title" placeholder="文章标题" />
          <input type="text" id="rl-url" placeholder="URL（可选）" />
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
        <div class="card-title">云同步</div>
        <div id="rl-sync-area"></div>
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

    // ---- Sync UI ----
    function renderSyncUI() {
      const config = self._getSyncConfig();

      if (config.token && config.gistId) {
        // Connected state
        syncArea.innerHTML = `
          <div class="rl-sync-status">
            <span class="rl-sync-dot connected"></span>
            <span>已连接 Gist</span>
            <span class="rl-sync-id">${config.gistId.slice(0, 8)}...</span>
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
        // Setup state
        syncArea.innerHTML = `
          <p class="rl-sync-hint">通过 GitHub Gist 跨设备同步你的阅读列表。</p>
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
            // Verify token
            await self._gistRequest('GET', '/gists?per_page=1', token);

            let finalGistId = gistId;
            if (!finalGistId) {
              // Create new gist
              connectMsgEl.innerHTML = '<p class="rl-sync-loading">创建 Gist...</p>';
              finalGistId = await self._createGist(token);
            } else {
              // Verify gist exists
              await self._gistRequest('GET', `/gists/${finalGistId}`, token);
            }

            self._saveSyncConfig({ token, gistId: finalGistId, lastSync: Date.now() });

            // Auto sync after connect (if user provided an existing gist)
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
          // merge
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
        // Update sync time without full re-render (to preserve msgEl)
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
        <div class="rl-item ${item.read ? 'rl-read' : ''}" data-id="${item.id}">
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

    // Add button
    container.querySelector('#rl-add-btn').addEventListener('click', () => {
      const title = titleInput.value.trim();
      if (!title) {
        errorEl.innerHTML =
          '<p class="error-msg">请输入文章标题</p>';
        return;
      }

      errorEl.innerHTML = '';
      const url = urlInput.value.trim();
      const tags = tagsInput.value
        .split(/[,，]/)
        .map((t) => t.trim())
        .filter(Boolean);

      const items = self._load();
      items.unshift({
        id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
        title,
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
      titleInput.focus();

      renderFilters();
      renderList();
    });

    // Initial render
    renderFilters();
    renderList();
    renderSyncUI();
  },
});
