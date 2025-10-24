package com.yjy.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
        this.totalSales = 0.0;
        this.productCount = new HashMap<>();
        this.totalCount = 0L;
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
