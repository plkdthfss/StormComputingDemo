// ===================== 1. 初始化 ECharts =====================
const barChart = echarts.init(document.getElementById('bar-chart'));
const lineChart = echarts.init(document.getElementById('line-chart'));

// ===================== 2. 初始配置 =====================
const barOption = {
    title: { text: '各产品销售情况', left: 'center' },
    tooltip: { trigger: 'axis' },
    legend: { data: ['销售额', '销量'], bottom: 0 },
    grid: { left: '5%', right: '5%', bottom: '10%', containLabel: true },
    xAxis: { type: 'category', data: [] },
    yAxis: [
        { type: 'value', name: '销售额（元）' },
        { type: 'value', name: '销量（件）', position: 'right' }
    ],
    series: [
        {
            name: '销售额',
            type: 'bar',
            yAxisIndex: 0,
            data: [],
            itemStyle: { color: '#5470C6' }
        },
        {
            name: '销量',
            type: 'bar',
            yAxisIndex: 1,
            data: [],
            itemStyle: { color: '#91CC75' }
        }
    ]
};

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

barChart.setOption(barOption);
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

    // ---- 更新柱状图 ----
    const xData = realtime.map(i => i.productId);
    const amountData = realtime.map(i => parseFloat(i.amount || 0));
    const countData = realtime.map(i => parseInt(i.count || 0));

    barChart.setOption({
        xAxis: { data: xData },
        series: [
            { name: '销售额', data: amountData },
            { name: '销量', data: countData }
        ]
    });

    // ---- 更新折线图 ----
    const timeData = timeline.map(i => {
        const d = new Date(i.timestamp);
        return isNaN(d.getTime()) ? '' : d.toLocaleTimeString();
    });
    const valueData = timeline.map(i => parseFloat(i.value || 0));

    lineChart.setOption({
        xAxis: { data: timeData },
        series: [{ name: '总销售额', data: valueData }]
    });
}

// ===================== 5. 自动刷新 =====================
updateDashboard();
setInterval(updateDashboard, 1000);

// ===================== 6. 窗口自适应 =====================
window.addEventListener('resize', () => {
    barChart.resize();
    lineChart.resize();
});
