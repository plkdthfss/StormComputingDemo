package com.yjy.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * 在原来金额聚合基础上，新增：各产品销售量 productCount、总销售量 totalCount
 */
public class AggregateBolt extends BaseRichBolt {
    private OutputCollector collector;

    // 金额
    private Map<String, Double> productSales;
    private double totalSales;

    // 销售量（件数/次数）
    private Map<String, Long> productCount;
    private long totalCount;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.productSales = new HashMap<>();
        this.productCount = new HashMap<>();

        Jedis jedis = new Jedis("hadoop102", 6379);

        try {
            // 加载总金额和总销量
            String totalSalesStr = jedis.get("sales:total");
            this.totalSales = totalSalesStr != null ? Double.parseDouble(totalSalesStr) : 0.0;

            String totalCountStr = jedis.get("sales:total:count");
            this.totalCount = totalCountStr != null ? Long.parseLong(totalCountStr) : 0L;

            // 加载各商品累计金额
            Map<String, String> salesMap = jedis.hgetAll("sales:realtime");
            for (Map.Entry<String, String> entry : salesMap.entrySet()) {
                productSales.put(entry.getKey(), Double.parseDouble(entry.getValue()));
            }

            // 加载各商品累计销量
            Map<String, String> countMap = jedis.hgetAll("sales:realtime:count");
            for (Map.Entry<String, String> entry : countMap.entrySet()) {
                productCount.put(entry.getKey(), Long.parseLong(entry.getValue()));
            }

            System.out.println("[AggregateBolt] 从 Redis 加载历史数据完成");
        } catch (Exception e) {
            System.err.println("[AggregateBolt] 加载历史数据失败，使用初始值: " + e.getMessage());
            // 从Redis抓数据失败时用 0 初始化，避免拓扑启动失败
            this.totalSales = 0.0;
            this.totalCount = 0L;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }

    @Override
    public void execute(Tuple input) {
        try {
            String productId = input.getStringByField("productId");
            double productPrice = input.getDoubleByField("productPrice");
            long timestamp = input.getLongByField("timestamp");

            // 金额累计
            double newTotalAmt = productSales.getOrDefault(productId, 0.0) + productPrice;
            productSales.put(productId, newTotalAmt);
            totalSales += productPrice;

            // 销量累计（每条消息 == 1 件）
            long newTotalCnt = productCount.getOrDefault(productId, 0L) + 1L;
            productCount.put(productId, newTotalCnt);
            totalCount += 1L;

            // 发射：金额 + 销量
            collector.emit(new Values(
                    productId,
                    newTotalAmt,    // productTotal (amount)
                    totalSales,     // totalSales (amount)
                    newTotalCnt,    // productCount
                    totalCount,     // totalCount
                    timestamp
            ));

            System.out.printf("[AggregateBolt] %s 金额累计=%.2f, 总额=%.2f | 量累计=%d, 总量=%d%n",
                    productId, newTotalAmt, totalSales, newTotalCnt, totalCount);
        } catch (Exception e) {
            System.err.println("[AggregateBolt] 处理异常: " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 新增两个字段（*顺序要与 emit 对应*）
        declarer.declare(new Fields("productId", "productTotal", "totalSales", "productCount", "totalCount", "timestamp"));
    }
}
