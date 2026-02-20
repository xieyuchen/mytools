/**
 * 待阅读列表工具
 *
 * 使用 localStorage 持久化存储，支持：
 * - 添加文章（标题 + URL + 可选标签）
 * - 标记已读 / 未读
 * - 按标签筛选
 * - 删除条目
 */
ToolRegistry.register({
  id: 'reading-list',
  name: '待阅读列表',
  icon: '📚',
  category: '效率工具',
  desc: '收藏想读的文章，支持标签和已读标记',

  _STORAGE_KEY: 'devtools_reading_list',

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
    let currentFilter = 'all'; // 'all' | 'unread' | 'read' | tag name
    let currentFilterType = 'status'; // 'status' | 'tag'

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
    `;

    const titleInput = container.querySelector('#rl-title');
    const urlInput = container.querySelector('#rl-url');
    const tagsInput = container.querySelector('#rl-tags');
    const errorEl = container.querySelector('#rl-error');
    const listEl = container.querySelector('#rl-list');
    const emptyEl = container.querySelector('#rl-empty');
    const countEl = container.querySelector('#rl-count');
    const filtersEl = container.querySelector('#rl-filters');

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

    function renderList() {
      let items = self._load();

      // Apply filter
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

      // Bind toggle read
      listEl.querySelectorAll('.rl-check-btn').forEach((btn) => {
        btn.addEventListener('click', () => {
          const all = self._load();
          const target = all.find((i) => i.id === btn.dataset.id);
          if (target) {
            target.read = !target.read;
            self._save(all);
            renderFilters();
            renderList();
          }
        });
      });

      // Bind delete
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
      });
      self._save(items);

      titleInput.value = '';
      urlInput.value = '';
      tagsInput.value = '';
      titleInput.focus();

      renderFilters();
      renderList();
    });

    // Enter key to add
    [titleInput, urlInput, tagsInput].forEach((input) => {
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          container.querySelector('#rl-add-btn').click();
        }
      });
    });

    // Initial render
    renderFilters();
    renderList();
  },
});
