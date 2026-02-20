// Dashboard JavaScript
const API_BASE_URL = window.location.origin;

let currentPage = 1;
let currentTab = 'stats';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadOrders();
    loadSyncStatus();
    checkSyncStatus();
    
    // Auto-refresh every 30 seconds
    setInterval(() => {
        if (currentTab === 'stats') {
            loadStats();
        }
        if (currentTab === 'analytics') {
            loadAnalytics();
        }
        if (currentTab === 'orders') loadOrders();
        if (currentTab === 'catalog') loadCatalog();
        loadSyncStatus();
    }, 30000);
    
    // Search on Enter
    document.getElementById('searchInput')?.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') loadOrders();
    });
    
    document.getElementById('catalogSearch')?.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') loadCatalog();
    });
    
});

// Tab switching
function showTab(tab) {
    currentTab = tab;
    
    // Update tab buttons
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('border-purple-500', 'text-purple-600');
        btn.classList.add('border-transparent', 'text-gray-500');
    });
    document.getElementById(`tab-${tab}`).classList.remove('border-transparent', 'text-gray-500');
    document.getElementById(`tab-${tab}`).classList.add('border-purple-500', 'text-purple-600');
    
    // Update content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.add('hidden');
    });
    document.getElementById(`content-${tab}`).classList.remove('hidden');
    
    // Load data for tab
    if (tab === 'stats') {
        loadStats();
    }
    if (tab === 'analytics') {
        loadAnalytics();
    }
    if (tab === 'orders') loadOrders();
    if (tab === 'catalog') loadCatalog();
}

// Load statistics
async function loadStats() {
    try {
        // Загружаем статистику без параметров периода - всегда показываем фиксированные метрики (сегодня/неделя/месяц)
        const url = `${API_BASE_URL}/api/dashboard/stats`;
        
        const response = await fetch(url);
        const data = await response.json();
        
        // Update cards
        document.getElementById('revenue-today').textContent = formatCurrency(data.revenue_today);
        document.getElementById('revenue-week').textContent = formatCurrency(data.revenue_week);
        document.getElementById('conversion-rate').textContent = data.conversion_rate.toFixed(1) + '%';
        document.getElementById('average-check').textContent = formatCurrency(data.average_check);
        
        // Дополнительные метрики: новые заказы
        if (data.new_orders_today !== undefined) {
            document.getElementById('new-orders-today').textContent = data.new_orders_today;
            document.getElementById('new-orders-week').textContent = data.new_orders_week;
            document.getElementById('new-orders-month').textContent = data.new_orders_month;
        }
        
        // Дополнительные метрики: оплаченные заказы
        if (data.paid_orders_today !== undefined) {
            document.getElementById('paid-orders-today').textContent = data.paid_orders_today;
            document.getElementById('paid-orders-week').textContent = data.paid_orders_week;
            document.getElementById('paid-orders-month').textContent = data.paid_orders_month;
        }
        
        // Дополнительные метрики: отмененные заказы
        if (data.cancelled_orders_today !== undefined) {
            document.getElementById('cancelled-orders-today').textContent = data.cancelled_orders_today;
            document.getElementById('cancelled-orders-week').textContent = data.cancelled_orders_week;
            document.getElementById('cancelled-orders-month').textContent = data.cancelled_orders_month;
        }
        
        // Средний размер корзины
        if (data.average_basket_size !== undefined) {
            document.getElementById('average-basket-size').textContent = data.average_basket_size.toFixed(1);
        }
        
        // Повторные покупки
        if (data.repeat_customers_count !== undefined) {
            document.getElementById('repeat-customers').textContent = data.repeat_customers_count;
        }
        
        // Прогноз выручки
        if (data.revenue_forecast !== undefined && data.revenue_forecast !== null) {
            document.getElementById('revenue-forecast').textContent = formatCurrency(data.revenue_forecast);
        } else {
            document.getElementById('revenue-forecast').textContent = '—';
        }
        
        // Отображение сравнения с предыдущим периодом
        if (data.period_comparison) {
            const comp = data.period_comparison;
            const revenueChangeEl = document.getElementById('revenue-today-change');
            const ordersChangeEl = document.getElementById('revenue-week-change');
            
            if (revenueChangeEl) {
                const change = comp.revenue_change;
                const isPositive = change >= 0;
                revenueChangeEl.innerHTML = `
                    <span class="${isPositive ? 'text-green-600' : 'text-red-600'}">
                        <i class="fas fa-${isPositive ? 'arrow-up' : 'arrow-down'}"></i>
                        ${Math.abs(change).toFixed(1)}% vs предыдущий период
                    </span>
                `;
            }
            
            if (ordersChangeEl) {
                const change = comp.orders_change;
                const isPositive = change >= 0;
                ordersChangeEl.innerHTML = `
                    <span class="${isPositive ? 'text-green-600' : 'text-red-600'}">
                        <i class="fas fa-${isPositive ? 'arrow-up' : 'arrow-down'}"></i>
                        ${Math.abs(change).toFixed(1)}% заказов
                    </span>
                `;
            }
            
            // Показываем блок сравнения, если его нет
            let comparisonDiv = document.getElementById('period-comparison');
            if (!comparisonDiv) {
                const statsTab = document.getElementById('content-stats');
                const headerDiv = document.querySelector('#content-stats > div.bg-white.rounded-xl.shadow-md.p-6.mb-6');
                if (statsTab && headerDiv) {
                    comparisonDiv = document.createElement('div');
                    comparisonDiv.id = 'period-comparison';
                    comparisonDiv.className = 'bg-white rounded-xl shadow-md p-6 mb-6';
                    // Вставляем после заголовка
                    headerDiv.insertAdjacentElement('afterend', comparisonDiv);
                }
            }
            
            if (comparisonDiv) {
                comparisonDiv.innerHTML = `
                    <h3 class="text-lg font-semibold text-gray-900 mb-4">Сравнение с предыдущим периодом</h3>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div class="p-4 bg-gray-50 rounded-lg">
                            <p class="text-sm text-gray-600 mb-2">Выручка</p>
                            <p class="text-2xl font-bold text-gray-900">${formatCurrency(comp.current_revenue)}</p>
                            <p class="text-sm mt-1">
                                <span class="${comp.revenue_change >= 0 ? 'text-green-600' : 'text-red-600'}">
                                    <i class="fas fa-${comp.revenue_change >= 0 ? 'arrow-up' : 'arrow-down'}"></i>
                                    ${Math.abs(comp.revenue_change).toFixed(1)}%
                                </span>
                                <span class="text-gray-500 ml-2">(${formatCurrency(comp.previous_revenue)})</span>
                            </p>
                        </div>
                        <div class="p-4 bg-gray-50 rounded-lg">
                            <p class="text-sm text-gray-600 mb-2">Заказы</p>
                            <p class="text-2xl font-bold text-gray-900">${comp.current_orders}</p>
                            <p class="text-sm mt-1">
                                <span class="${comp.orders_change >= 0 ? 'text-green-600' : 'text-red-600'}">
                                    <i class="fas fa-${comp.orders_change >= 0 ? 'arrow-up' : 'arrow-down'}"></i>
                                    ${Math.abs(comp.orders_change).toFixed(1)}%
                                </span>
                                <span class="text-gray-500 ml-2">(${comp.previous_orders})</span>
                            </p>
                        </div>
                    </div>
                `;
            }
        } else {
            // Скрываем блок сравнения, если данных нет
            const comparisonDiv = document.getElementById('period-comparison');
            if (comparisonDiv) {
                comparisonDiv.remove();
            }
            // Очищаем индикаторы изменений
            const revenueChangeEl = document.getElementById('revenue-today-change');
            const ordersChangeEl = document.getElementById('revenue-week-change');
            if (revenueChangeEl) revenueChangeEl.textContent = '';
            if (ordersChangeEl) ordersChangeEl.textContent = '';
        }
        
        // Top products
        const topProductsDiv = document.getElementById('top-products');
        if (data.top_products && data.top_products.length > 0) {
            topProductsDiv.innerHTML = data.top_products.map((product, index) => `
                <div class="flex items-center justify-between p-3 bg-gray-50 rounded-lg fade-in">
                    <div class="flex items-center space-x-3">
                        <div class="w-8 h-8 bg-purple-100 rounded-full flex items-center justify-center text-purple-600 font-bold">
                            ${index + 1}
                        </div>
                        <div>
                            <p class="font-medium text-gray-900">${escapeHtml(product.name)}</p>
                            <p class="text-sm text-gray-500">${product.articul}</p>
                        </div>
                    </div>
                    <div class="text-right">
                        <p class="font-semibold text-gray-900">${product.quantity} шт.</p>
                        <p class="text-sm text-gray-500">${formatCurrency(product.revenue)}</p>
                    </div>
                </div>
            `).join('');
        } else {
            topProductsDiv.innerHTML = '<p class="text-center text-gray-500 py-4">Нет данных</p>';
        }
        
        // Orders chart
        updateOrdersChart(data);
        
    } catch (error) {
        console.error('Error loading stats:', error);
        showError('Ошибка загрузки статистики');
    }
}

// Update orders chart
function updateOrdersChart(data) {
    const ctx = document.getElementById('ordersChart');
    if (!ctx) return;
    
    const chartData = {
        labels: ['Сегодня', 'Неделя', 'Месяц'],
        datasets: [{
            label: 'Количество заказов',
            data: [data.orders_today, data.orders_week, data.orders_month],
            backgroundColor: [
                'rgba(99, 102, 241, 0.5)',
                'rgba(139, 92, 246, 0.5)',
                'rgba(168, 85, 247, 0.5)'
            ],
            borderColor: [
                'rgba(99, 102, 241, 1)',
                'rgba(139, 92, 246, 1)',
                'rgba(168, 85, 247, 1)'
            ],
            borderWidth: 2
        }]
    };
    
    if (window.ordersChartInstance) {
        window.ordersChartInstance.destroy();
    }
    
    window.ordersChartInstance = new Chart(ctx, {
        type: 'bar',
        data: chartData,
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Load orders
async function loadOrders(page = 1) {
    currentPage = page;
    const status = document.getElementById('statusFilter')?.value || '';
    const channel = document.getElementById('channelFilter')?.value || '';
    const search = document.getElementById('searchInput')?.value || '';
    
    try {
        const params = new URLSearchParams({
            page: page.toString(),
            page_size: '20'
        });
        if (status) params.append('status', status);
        if (channel) params.append('channel', channel);
        if (search) params.append('search', search);
        
        const response = await fetch(`${API_BASE_URL}/api/dashboard/orders?${params}`);
        const data = await response.json();
        
        const tbody = document.getElementById('ordersTableBody');
        if (data.items && data.items.length > 0) {
            tbody.innerHTML = data.items.map(order => `
                <tr class="hover:bg-gray-50 fade-in">
                    <td class="px-6 py-4 whitespace-nowrap">
                        <div class="text-sm font-medium text-gray-900">${escapeHtml(order.order_number)}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <div class="text-sm text-gray-900">${escapeHtml(order.customer_name || '—')}</div>
                        <div class="text-sm text-gray-500">${escapeHtml(order.customer_phone || '—')}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <span class="px-2 py-1 text-xs font-medium rounded-full bg-gray-100 text-gray-800">
                            ${getChannelName(order.channel)}
                        </span>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <span class="status-badge status-${order.status}">
                            ${getStatusName(order.status)}
                        </span>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <div class="text-sm font-semibold text-gray-900">${formatCurrency(order.total_amount)}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        ${formatDate(order.created_at)}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                        <button onclick="showOrderDetails('${order.id}')" 
                                class="text-purple-600 hover:text-purple-900">
                            <i class="fas fa-eye"></i>
                        </button>
                    </td>
                </tr>
            `).join('');
            
            // Pagination
            updatePagination(data);
        } else {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="px-6 py-8 text-center text-gray-500">
                        <i class="fas fa-inbox text-4xl mb-2"></i>
                        <p>Заказы не найдены</p>
                    </td>
                </tr>
            `;
        }
    } catch (error) {
        console.error('Error loading orders:', error);
        showError('Ошибка загрузки заказов');
    }
}

// Update pagination
function updatePagination(data) {
    const paginationDiv = document.getElementById('pagination');
    if (!paginationDiv) return;
    
    if (data.pages <= 1) {
        paginationDiv.innerHTML = '';
        return;
    }
    
    let html = '<div class="flex items-center justify-between">';
    html += `<div class="text-sm text-gray-700">Страница ${data.page} из ${data.pages} (всего: ${data.total})</div>`;
    html += '<div class="flex space-x-2">';
    
    if (data.page > 1) {
        html += `<button onclick="loadOrders(${data.page - 1})" class="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">Назад</button>`;
    }
    
    if (data.page < data.pages) {
        html += `<button onclick="loadOrders(${data.page + 1})" class="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">Вперёд</button>`;
    }
    
    html += '</div></div>';
    paginationDiv.innerHTML = html;
}

// Load catalog
async function loadCatalog() {
    const search = document.getElementById('catalogSearch')?.value || '';
    
    try {
        const params = new URLSearchParams({ page: '1', page_size: '50' });
        if (search) params.append('q', search);
        
        const response = await fetch(`${API_BASE_URL}/api/dashboard/catalog?${params}`);
        const data = await response.json();
        
        const tbody = document.getElementById('catalogTableBody');
        if (data.items && data.items.length > 0) {
            tbody.innerHTML = data.items.map(product => `
                <tr class="hover:bg-gray-50 fade-in">
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        ${escapeHtml(product.articul)}
                    </td>
                    <td class="px-6 py-4 text-sm text-gray-900">
                        ${escapeHtml(product.name)}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-900">
                        ${formatCurrency(product.price)}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <span class="px-2 py-1 text-xs font-medium rounded-full ${
                            product.stock > 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                        }">
                            ${product.stock} шт.
                        </span>
                    </td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = `
                <tr>
                    <td colspan="4" class="px-6 py-8 text-center text-gray-500">
                        <i class="fas fa-inbox text-4xl mb-2"></i>
                        <p>Товары не найдены</p>
                    </td>
                </tr>
            `;
        }
    } catch (error) {
        console.error('Error loading catalog:', error);
        showError('Ошибка загрузки каталога');
    }
}

// Show order details
async function showOrderDetails(orderId) {
    try {
        const response = await fetch(`${API_BASE_URL}/api/dashboard/orders/${orderId}`);
        const order = await response.json();
        
        const modal = document.getElementById('orderModal');
        const detailsDiv = document.getElementById('orderDetails');
        
        // Определяем доступные статусы для перехода
        const statusTransitions = {
            'new':              ['validated', 'cancelled'],
            'validated':        ['invoice_created', 'cancelled'],
            'invoice_created':  ['paid', 'cancelled'],
            'paid':             ['order_created_1c', 'cancelled'],
            'order_created_1c': ['tracking_issued', 'cancelled'],
            'tracking_issued':  ['shipped', 'cancelled'],
            'shipped':          [],
            'cancelled':        []
        };
        
        const availableStatuses = statusTransitions[order.status] || [];
        const statusNames = {
            'new':              '🆕 Новый',
            'validated':        '✅ Подтверждён',
            'invoice_created':  '📄 Счёт создан',
            'paid':             '💳 Оплачен',
            'order_created_1c': '📋 Передан на склад',
            'tracking_issued':  '📦 Трек присвоен',
            'shipped':          '🚚 В пути',
            'cancelled':        '❌ Отменён'
        };
        
        detailsDiv.innerHTML = `
            <div class="grid grid-cols-2 gap-4">
                <div>
                    <p class="text-sm text-gray-500">Номер заказа</p>
                    <p class="font-semibold">${escapeHtml(order.order_number)}</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">Статус</p>
                    <div class="flex items-center space-x-2 mt-1">
                        <span class="status-badge status-${order.status}">${getStatusName(order.status)}</span>
                        ${availableStatuses.length > 0 ? `
                            <select id="statusSelect" class="px-3 py-1 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-purple-500 focus:border-purple-500">
                                <option value="">Изменить статус...</option>
                                ${availableStatuses.map(s => `
                                    <option value="${s}">${statusNames[s]}</option>
                                `).join('')}
                            </select>
                            <button onclick="updateOrderStatusFromModal('${order.id}')" 
                                    class="px-3 py-1 bg-purple-600 text-white rounded-lg hover:bg-purple-700 text-sm transition-colors">
                                <i class="fas fa-save mr-1"></i>Сохранить
                            </button>
                        ` : ''}
                    </div>
                </div>
                <div>
                    <p class="text-sm text-gray-500">Канал</p>
                    <p class="font-semibold">${getChannelName(order.channel)}</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">Дата создания</p>
                    <p class="font-semibold">${formatDate(order.created_at)}</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">Клиент</p>
                    <p class="font-semibold">${escapeHtml(order.customer_name || '—')}</p>
                    <p class="text-sm text-gray-500">${escapeHtml(order.customer_phone || '—')}</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">Адрес</p>
                    <p class="font-semibold">${escapeHtml(order.customer_address || '—')}</p>
                </div>
            </div>
            
            <div class="mt-6">
                <h4 class="font-semibold mb-3">Товары:</h4>
                <div class="space-y-2">
                    ${order.items.map(item => `
                        <div class="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                            <div>
                                <p class="font-medium">${escapeHtml(item.product_name)}</p>
                                <p class="text-sm text-gray-500">${escapeHtml(item.product_articul)}</p>
                            </div>
                            <div class="text-right">
                                <p class="font-semibold">${item.quantity} × ${formatCurrency(item.price_at_order)}</p>
                                <p class="text-sm text-gray-500">${formatCurrency(item.total)}</p>
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
            
            <div class="mt-6 pt-6 border-t border-gray-200">
                <div class="flex justify-between items-center">
                    <div>
                        <p class="text-sm text-gray-500">Доставка</p>
                        <p class="font-semibold">${formatCurrency(order.delivery_cost)}</p>
                    </div>
                    <div class="text-right">
                        <p class="text-sm text-gray-500">Итого</p>
                        <p class="text-2xl font-bold text-purple-600">${formatCurrency(order.total_amount)}</p>
                    </div>
                </div>
                ${order.tracking_number ? `
                    <div class="mt-4">
                        <p class="text-sm text-gray-500">Трек-номер</p>
                        <p class="font-semibold">${escapeHtml(order.tracking_number)}</p>
                    </div>
                ` : ''}
                ${order.transaction_id ? `
                    <div class="mt-2">
                        <p class="text-sm text-gray-500">ID транзакции</p>
                        <p class="font-semibold">${escapeHtml(order.transaction_id)}</p>
                    </div>
                ` : ''}
            </div>
            ${order.status === 'order_created_1c' && !order.tracking_number ? `
                <div class="mt-4 pt-4 border-t border-gray-200">
                    <button onclick="generateTrackingNumber('${order.id}')" 
                            class="w-full px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors">
                        <i class="fas fa-truck mr-2"></i>Сгенерировать трек-номер
                    </button>
                </div>
            ` : ''}
        `;
        
        modal.classList.remove('hidden');
    } catch (error) {
        console.error('Error loading order details:', error);
        showError('Ошибка загрузки деталей заказа');
    }
}

// Close order modal
function closeOrderModal() {
    document.getElementById('orderModal').classList.add('hidden');
}

// Update order status from modal
async function updateOrderStatusFromModal(orderId) {
    const statusSelect = document.getElementById('statusSelect');
    const newStatus = statusSelect?.value;
    
    if (!newStatus) {
        showError('Выберите новый статус');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/api/dashboard/orders/${orderId}/status`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status: newStatus })
        });
        
        if (response.ok) {
            showSuccess('Статус обновлён');
            closeOrderModal();
            loadOrders(currentPage);
            if (currentTab === 'stats') loadStats();
        } else {
            const error = await response.json();
            showError(error.error || error.detail || 'Ошибка обновления статуса');
        }
    } catch (error) {
        console.error('Error updating status:', error);
        showError('Ошибка обновления статуса');
    }
}

// Generate tracking number
async function generateTrackingNumber(orderId) {
    if (!confirm('Сгенерировать трек-номер для этого заказа?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/api/orders/${orderId}/generate-tracking`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            showSuccess(`Трек-номер сгенерирован: ${result.tracking_number}`);
            closeOrderModal();
            loadOrders(currentPage);
            if (currentTab === 'stats') loadStats();
        } else {
            const error = await response.json();
            showError(error.error || error.detail || 'Ошибка генерации трек-номера');
        }
    } catch (error) {
        console.error('Error generating tracking number:', error);
        showError('Ошибка генерации трек-номера');
    }
}

// Load sync status
async function loadSyncStatus() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/dashboard/sync-status`);
        const data = await response.json();
        
        const syncStatusDiv = document.getElementById('syncStatus');
        if (syncStatusDiv) {
            const statusColors = {
                'ok': 'bg-green-400',
                'warning': 'bg-yellow-400',
                'error': 'bg-red-400'
            };
            
            let statusText = '';
            if (data.status === 'ok') {
                statusText = `Синхронизация активна (${data.products_count} товаров)`;
            } else if (data.status === 'warning') {
                if (data.last_sync) {
                    const lastSync = new Date(data.last_sync);
                    const hoursAgo = Math.floor((Date.now() - lastSync.getTime()) / (1000 * 60 * 60));
                    statusText = `Синхронизация ${hoursAgo} ч. назад (${data.products_count} товаров)`;
                } else {
                    statusText = `Синхронизация недавно (${data.products_count} товаров)`;
                }
            } else {
                statusText = `Ошибка синхронизации (${data.products_count} товаров)`;
            }
            
            syncStatusDiv.innerHTML = `
                <div class="w-2 h-2 rounded-full ${statusColors[data.status]} ${data.status === 'ok' ? 'animate-pulse' : ''}"></div>
                <span class="text-sm">${statusText}</span>
            `;
        }
    } catch (error) {
        console.error('Error loading sync status:', error);
    }
}

// Check sync status periodically
function checkSyncStatus() {
    setInterval(loadSyncStatus, 60000); // Every minute
}

// Load analytics
async function loadAnalytics() {
    try {
        const periodSelect = document.getElementById('analyticsPeriod');
        const days = periodSelect ? parseInt(periodSelect.value) : 30;
        
        const response = await fetch(`${API_BASE_URL}/api/dashboard/analytics?days=${days}`);
        const data = await response.json();
        
        // Обновление графиков
        updateRevenueChart(data.revenue_by_days);
        updateChannelChart(data.channel_analysis);
        updateFunnelChart(data.sales_funnel);
        updateStatusChart(data.status_distribution);
        updateMetrics(data.metrics);
        
    } catch (error) {
        console.error('Error loading analytics:', error);
        showError('Ошибка загрузки аналитики');
    }
}

// Update revenue chart
function updateRevenueChart(revenueData) {
    const ctx = document.getElementById('revenueChart');
    if (!ctx) return;
    
    const labels = revenueData.map(d => {
        const date = new Date(d.date);
        return date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
    });
    const revenues = revenueData.map(d => d.revenue);
    const orders = revenueData.map(d => d.orders_count);
    
    if (window.revenueChartInstance) {
        window.revenueChartInstance.destroy();
    }
    
    window.revenueChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Выручка (₽)',
                data: revenues,
                borderColor: 'rgb(99, 102, 241)',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                tension: 0.4,
                yAxisID: 'y'
            }, {
                label: 'Заказы (шт)',
                data: orders,
                borderColor: 'rgb(139, 92, 246)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                tension: 0.4,
                yAxisID: 'y1'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    beginAtZero: true
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    beginAtZero: true,
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

// Update channel chart
function updateChannelChart(channelData) {
    const ctx = document.getElementById('channelChart');
    if (!ctx) return;
    
    const channels = Object.keys(channelData);
    const revenues = channels.map(ch => channelData[ch].revenue);
    const orders = channels.map(ch => channelData[ch].orders_count);
    
    if (window.channelChartInstance) {
        window.channelChartInstance.destroy();
    }
    
    window.channelChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: channels.map(ch => getChannelName(ch)),
            datasets: [{
                label: 'Выручка (₽)',
                data: revenues,
                backgroundColor: 'rgba(99, 102, 241, 0.5)',
                borderColor: 'rgb(99, 102, 241)',
                borderWidth: 2
            }, {
                label: 'Заказы (шт)',
                data: orders,
                backgroundColor: 'rgba(139, 92, 246, 0.5)',
                borderColor: 'rgb(139, 92, 246)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Update funnel chart
function updateFunnelChart(funnelData) {
    const ctx = document.getElementById('funnelChart');
    if (!ctx) return;
    
    const statusOrder = ['new', 'validated', 'invoice_created', 'paid', 'order_created_1c', 'tracking_issued', 'shipped', 'cancelled'];
    const statusNames = {
        'new':              '🆕 Новый',
        'validated':        '✅ Подтверждён',
        'invoice_created':  '📄 Счёт создан',
        'paid':             '💳 Оплачен',
        'order_created_1c': '📋 Передан на склад',
        'tracking_issued':  '📦 Трек присвоен',
        'shipped':          '🚚 В пути',
        'cancelled':        '❌ Отменён'
    };
    
    const labels = statusOrder.map(s => statusNames[s]);
    const data = statusOrder.map(s => funnelData[s] || 0);
    
    if (window.funnelChartInstance) {
        window.funnelChartInstance.destroy();
    }
    
    window.funnelChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Количество заказов',
                data: data,
                backgroundColor: [
                    'rgba(59, 130, 246, 0.5)',
                    'rgba(139, 92, 246, 0.5)',
                    'rgba(234, 179, 8, 0.5)',
                    'rgba(34, 197, 94, 0.5)',
                    'rgba(168, 85, 247, 0.5)',
                    'rgba(20, 184, 166, 0.5)',
                    'rgba(99, 102, 241, 0.5)',
                    'rgba(239, 68, 68, 0.5)'
                ],
                borderColor: [
                    'rgb(59, 130, 246)',
                    'rgb(139, 92, 246)',
                    'rgb(234, 179, 8)',
                    'rgb(34, 197, 94)',
                    'rgb(168, 85, 247)',
                    'rgb(20, 184, 166)',
                    'rgb(99, 102, 241)',
                    'rgb(239, 68, 68)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            indexAxis: 'y',
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Update status chart
function updateStatusChart(statusData) {
    const ctx = document.getElementById('statusChart');
    if (!ctx) return;
    
    const statusOrder = ['new', 'validated', 'invoice_created', 'paid', 'order_created_1c', 'tracking_issued', 'shipped', 'cancelled'];
    const statusNames = {
        'new':              '🆕 Новый',
        'validated':        '✅ Подтверждён',
        'invoice_created':  '📄 Счёт создан',
        'paid':             '💳 Оплачен',
        'order_created_1c': '📋 Передан на склад',
        'tracking_issued':  '📦 Трек присвоен',
        'shipped':          '🚚 В пути',
        'cancelled':        '❌ Отменён'
    };
    
    const labels = statusOrder.map(s => statusNames[s]);
    const data = statusOrder.map(s => statusData[s] || 0);
    
    if (window.statusChartInstance) {
        window.statusChartInstance.destroy();
    }
    
    window.statusChartInstance = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: [
                    'rgba(59, 130, 246, 0.7)',
                    'rgba(139, 92, 246, 0.7)',
                    'rgba(234, 179, 8, 0.7)',
                    'rgba(34, 197, 94, 0.7)',
                    'rgba(168, 85, 247, 0.7)',
                    'rgba(20, 184, 166, 0.7)',
                    'rgba(99, 102, 241, 0.7)',
                    'rgba(239, 68, 68, 0.7)'
                ],
                borderColor: [
                    'rgb(59, 130, 246)',
                    'rgb(139, 92, 246)',
                    'rgb(234, 179, 8)',
                    'rgb(34, 197, 94)',
                    'rgb(168, 85, 247)',
                    'rgb(20, 184, 166)',
                    'rgb(99, 102, 241)',
                    'rgb(239, 68, 68)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'right'
                }
            }
        }
    });
}

// Update metrics
function updateMetrics(metrics) {
    const metricsDiv = document.getElementById('metrics');
    if (!metricsDiv) return;
    
    metricsDiv.innerHTML = `
        <div class="space-y-4">
            <div class="p-4 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border border-blue-200">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-xs text-blue-700 font-medium">Среднее время обработки</span>
                    <i class="fas fa-clock text-blue-500"></i>
                </div>
                <p class="text-2xl font-bold text-blue-900">${metrics.avg_processing_hours.toFixed(1)} <span class="text-sm font-normal">ч.</span></p>
            </div>
            <div class="p-4 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border border-green-200">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-xs text-green-700 font-medium">Среднее время доставки</span>
                    <i class="fas fa-truck text-green-500"></i>
                </div>
                <p class="text-2xl font-bold text-green-900">${metrics.avg_delivery_hours.toFixed(1)} <span class="text-sm font-normal">ч.</span></p>
            </div>
            <div class="p-4 bg-gradient-to-r from-purple-50 to-purple-100 rounded-lg border border-purple-200">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-xs text-purple-700 font-medium">Средняя стоимость доставки</span>
                    <i class="fas fa-ruble-sign text-purple-500"></i>
                </div>
                <p class="text-2xl font-bold text-purple-900">${formatCurrency(metrics.avg_delivery_cost)}</p>
            </div>
            <div class="p-4 bg-gradient-to-r from-orange-50 to-orange-100 rounded-lg border border-orange-200">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-xs text-orange-700 font-medium">Заказов с доставкой</span>
                    <i class="fas fa-shipping-fast text-orange-500"></i>
                </div>
                <p class="text-2xl font-bold text-orange-900">${metrics.orders_with_delivery}</p>
            </div>
        </div>
    `;
    
    // Топ городов
    const topCitiesDiv = document.getElementById('top-cities');
    if (topCitiesDiv && metrics.top_cities && metrics.top_cities.length > 0) {
        const maxOrders = Math.max(...metrics.top_cities.map(c => c.orders_count));
        topCitiesDiv.innerHTML = `
            <div class="space-y-3">
                ${metrics.top_cities.map((city, index) => {
                    const percentage = (city.orders_count / maxOrders) * 100;
                    const colors = [
                        'from-yellow-400 to-yellow-600',
                        'from-gray-300 to-gray-500',
                        'from-orange-400 to-orange-600',
                        'from-blue-400 to-blue-600',
                        'from-green-400 to-green-600'
                    ];
                    const colorClass = colors[index] || 'from-purple-400 to-purple-600';
                    
                    return `
                        <div class="flex items-center space-x-4 p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition">
                            <div class="flex-shrink-0">
                                <div class="w-10 h-10 rounded-full bg-gradient-to-r ${colorClass} flex items-center justify-center text-white font-bold shadow-md">
                                    ${index + 1}
                                </div>
                            </div>
                            <div class="flex-1 min-w-0">
                                <div class="flex items-center justify-between mb-1">
                                    <span class="text-sm font-semibold text-gray-900 truncate">${escapeHtml(city.city || 'Не указан')}</span>
                                    <span class="text-sm font-bold text-gray-700 ml-2">${city.orders_count} заказов</span>
                                </div>
                                <div class="w-full bg-gray-200 rounded-full h-2">
                                    <div class="bg-gradient-to-r ${colorClass} h-2 rounded-full transition-all duration-500" style="width: ${percentage}%"></div>
                                </div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    } else if (topCitiesDiv) {
        topCitiesDiv.innerHTML = `
            <div class="text-center py-12">
                <i class="fas fa-map-marker-alt text-gray-300 text-4xl mb-3"></i>
                <p class="text-gray-500">Нет данных о городах</p>
            </div>
        `;
    }
}

// Utility functions
function formatCurrency(amount) {
    return new Intl.NumberFormat('ru-RU', {
        style: 'currency',
        currency: 'RUB',
        minimumFractionDigits: 0
    }).format(amount);
}

function formatDate(dateString) {
    if (!dateString) return '—';
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('ru-RU', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    }).format(date);
}

function getStatusName(status) {
    const names = {
        'new':              '🆕 Новый',
        'validated':        '✅ Подтверждён',
        'invoice_created':  '📄 Счёт создан',
        'paid':             '💳 Оплачен',
        'order_created_1c': '📋 Передан на склад',
        'tracking_issued':  '📦 Трек присвоен',
        'shipped':          '🚚 В пути',
        'cancelled':        '❌ Отменён'
    };
    return names[status] || status;
}

function getChannelName(channel) {
    const names = {
        'telegram': 'Telegram',
        'yandex_mail': 'Яндекс.Почта',
        'yandex_forms': 'Яндекс.Формы'
    };
    return names[channel] || channel;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showError(message) {
    // Simple error notification
    alert('Ошибка: ' + message);
}

function showSuccess(message) {
    // Simple success notification
    alert('Успешно: ' + message);
}

// Refresh button
document.getElementById('refreshBtn')?.addEventListener('click', () => {
    if (currentTab === 'stats') {
        loadStats();
    }
    if (currentTab === 'analytics') {
        loadAnalytics();
    }
    if (currentTab === 'orders') loadOrders();
    if (currentTab === 'catalog') loadCatalog();
    loadSyncStatus();
});

// Export functions
async function exportOrdersExcel() {
    try {
        const statusFilter = document.getElementById('statusFilter')?.value || '';
        const channelFilter = document.getElementById('channelFilter')?.value || '';
        const search = document.getElementById('searchInput')?.value || '';
        
        const params = new URLSearchParams();
        if (statusFilter) params.append('status_filter', statusFilter);
        if (channelFilter) params.append('channel_filter', channelFilter);
        if (search) params.append('search', search);
        
        const url = `${API_BASE_URL}/api/dashboard/export/orders/excel?${params.toString()}`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'orders_export.xlsx';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting to Excel:', error);
        showError('Ошибка при экспорте в Excel: ' + error.message);
    }
}

async function exportOrdersCSV() {
    try {
        const statusFilter = document.getElementById('statusFilter')?.value || '';
        const channelFilter = document.getElementById('channelFilter')?.value || '';
        const search = document.getElementById('searchInput')?.value || '';
        
        const params = new URLSearchParams();
        if (statusFilter) params.append('status_filter', statusFilter);
        if (channelFilter) params.append('channel_filter', channelFilter);
        if (search) params.append('search', search);
        
        const url = `${API_BASE_URL}/api/dashboard/export/orders/csv?${params.toString()}`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'orders_export.csv';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting to CSV:', error);
        showError('Ошибка при экспорте в CSV: ' + error.message);
    }
}

async function exportStatsPDF() {
    try {
        // Экспортируем статистику за месяц (по умолчанию)
        const url = `${API_BASE_URL}/api/dashboard/export/stats/pdf?period=month`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'stats_export.pdf';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting stats to PDF:', error);
        showError('Ошибка при экспорте статистики в PDF: ' + error.message);
    }
}

async function exportAnalyticsPDF() {
    try {
        const days = parseInt(document.getElementById('analyticsPeriod')?.value || '30');
        
        const url = `${API_BASE_URL}/api/dashboard/export/analytics/pdf?days=${days}`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'analytics_export.pdf';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting analytics to PDF:', error);
        showError('Ошибка при экспорте аналитики в PDF: ' + error.message);
    }
}

async function exportCatalogExcel() {
    try {
        const search = document.getElementById('catalogSearch')?.value || '';
        
        const params = new URLSearchParams();
        if (search) params.append('q', search);
        
        const url = `${API_BASE_URL}/api/dashboard/export/catalog/excel?${params.toString()}`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'catalog_export.xlsx';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting catalog to Excel:', error);
        showError('Ошибка при экспорте каталога в Excel: ' + error.message);
    }
}

async function exportCatalogCSV() {
    try {
        const search = document.getElementById('catalogSearch')?.value || '';
        
        const params = new URLSearchParams();
        if (search) params.append('q', search);
        
        const url = `${API_BASE_URL}/api/dashboard/export/catalog/csv?${params.toString()}`;
        
        // Используем fetch для получения файла с правильным типом
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.error || `HTTP ${response.status}`);
        }
        
        // Получаем имя файла из заголовка или генерируем
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'catalog_export.csv';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1].replace(/['"]/g, '');
            }
        }
        
        // Создаем Blob и скачиваем
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        
        // Небольшая задержка перед очисткой, чтобы браузер успел начать скачивание
        setTimeout(() => {
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        }, 100);
    } catch (error) {
        console.error('Error exporting catalog to CSV:', error);
        showError('Ошибка при экспорте каталога в CSV: ' + error.message);
    }
}
