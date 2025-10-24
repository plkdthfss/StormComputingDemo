// ===================== 1. 初始化 ECharts =====================
const amountChart = echarts.init(document.getElementById('amount-chart'));  // 销售额图表
const countChart = echarts.init(document.getElementById('count-chart'));    // 销售量图表
const lineChart = echarts.init(document.getElementById('line-chart'));

// ===================== 2. 初始配置 =====================
// 销售额柱状图配置
const amountOption = {
    tooltip: { trigger: 'axis' },
    grid: { left: '8%', right: '5%', bottom: '10%', containLabel: true },
    xAxis: { type: 'category', data: [] },
    yAxis: { type: 'value', name: '销售额（元）' },
    series: [
        {
            name: '销售额',
            type: 'bar',
            data: [],
            itemStyle: { color: '#5470C6' },
            label: {
                show: true,
                position: 'top',
                formatter: '{c}'
            }
        }
    ]
};

// 销售量柱状图配置
const countOption = {
    tooltip: { trigger: 'axis' },
    grid: { left: '8%', right: '5%', bottom: '10%', containLabel: true },
    xAxis: { type: 'category', data: [] },
    yAxis: { type: 'value', name: '销量（件）' },
    series: [
        {
            name: '销量',
            type: 'bar',
            data: [],
            itemStyle: { color: '#91CC75' },
            label: {
                show: true,
                position: 'top',
                formatter: '{c}'
            }
        }
    ]
};

// 趋势图配置保持不变
const lineOption = {
    title: { text: '销售额趋势', left: 'center' },
    tooltip: { trigger: 'axis' },
    grid: { left: '5%', right: '5%', bottom: '10%', containLabel: true },
    xAxis: { type: 'category', boundaryGap: false, data: [] },
    yAxis: { type: 'value', name: '总销售额（元）' },
    series: [
        {
            name: '总销售额',
            type: 'line',
            smooth: true,
            areaStyle: { color: 'rgba(84,112,198,0.2)' },
            lineStyle: { color: '#5470C6' },
            data: []
        }
    ]
};

// 设置初始配置
amountChart.setOption(amountOption);
countChart.setOption(countOption);
lineChart.setOption(lineOption);

// ===================== 3. 工具函数 =====================
async function fetchAPI(url) {
    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
    } catch (err) {
        console.error(`请求失败: ${url}`, err);
        return null;
    }
}

// ===================== 4. 核心刷新逻辑 =====================
async function updateDashboard() {
    const total = await fetchAPI('/api/total');
    const realtime = await fetchAPI('/api/realtime');
    const timeline = await fetchAPI('/api/timeline');

    if (!total || !realtime || !timeline) {
        console.warn("后端数据不完整，跳过本轮刷新");
        return;
    }

    // ---- 更新顶部统计 ----
    document.getElementById('total-amount').innerText =
        `￥${Number(total.totalAmount || 0).toFixed(2)}`;
    document.getElementById('total-count').innerText =
        `${Number(total.totalCount || 0)} 件`;

    // ---- 处理数据排序 ----
    // 按销售额从高到低排序
    const sortedByAmount = [...realtime].sort((a, b) =>
        parseFloat(b.amount || 0) - parseFloat(a.amount || 0)
    );

    // 按销售量从高到低排序
    const sortedByCount = [...realtime].sort((a, b) =>
        parseInt(b.count || 0) - parseInt(a.count || 0)
    );

    // ---- 更新销售额柱状图 ----
    const amountXData = sortedByAmount.map(i => {
        const match = i.productId.match(/\d+/);
        return match ? `PID${match[0]}` : `PID${i.productId}`;
    });
    const amountYData = sortedByAmount.map(i => parseFloat(i.amount || 0));
    amountChart.setOption({
        xAxis: { data: amountXData },
        series: [{ data: amountYData }]
    });

    // ---- 更新销售量柱状图 ----
    const countXData = sortedByCount.map(i => {
        const match = i.productId.match(/\d+/);
        return match ? `PID${match[0]}` : `PID${i.productId}`;
    });
    const countYData = sortedByCount.map(i => parseInt(i.count || 0));
    countChart.setOption({
        xAxis: { data: countXData },
        series: [{ data: countYData }]
    });

    // ---- 更新折线图（保持不变）----
    const timeData = timeline.map(i => {
        const d = new Date(i.timestamp);
        return isNaN(d.getTime()) ? '' : d.toLocaleTimeString();
    });
    const valueData = timeline.map(i => parseFloat(i.value || 0));

    lineChart.setOption({
        xAxis: { data: timeData },
        series: [{ data: valueData }]
    });
}

// ===================== 5. 自动刷新 =====================
updateDashboard();
setInterval(updateDashboard, 1000);

// ===================== 6. 窗口自适应 =====================
window.addEventListener('resize', () => {
    amountChart.resize();
    countChart.resize();
    lineChart.resize();
});