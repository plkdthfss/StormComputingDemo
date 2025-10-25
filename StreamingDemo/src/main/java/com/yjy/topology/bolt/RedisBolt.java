package com.yjy.topology.bolt;

import com.yjy.util.ConfigUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * 写入 Redis：
 * 金额（沿用）
 *  - sales:realtime           (Hash)   各商品累计金额
 *  - sales:total              (String) 总金额
 *  - sales:timeline           (List)   总金额曲线（timestamp,amount）
 *
 * 销量（新增，供面板对比 & 排行）
 *  - sales:realtime:count     (Hash)   各商品累计销量
 *  - sales:total:count        (String) 总销量
 *  - sales:timeline:count:{productId} (List) 该商品销量曲线（timestamp,totalCount）
 *  - sales:leaderboard:count  (ZSet)   实时销量排行（score=累计销量）
 */
public class RedisBolt extends BaseRichBolt {
    private transient Jedis jedis;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        jedis = new Jedis(ConfigUtil.getString("redis.hosts"), ConfigUtil.getInt("redis.port"));
        System.out.println("[RedisBolt] 已连接 Redis");
    }

    @Override
    public void execute(Tuple input) {
        try {
            String productId    = input.getStringByField("productId");
            double productTotal = input.getDoubleByField("productTotal");   // 金额累计
            double totalSales   = input.getDoubleByField("totalSales");     // 总金额
            long productCount   = input.getLongByField("productCount");     // 销量累计
            long totalCount     = input.getLongByField("totalCount");       // 总销量
            long timestamp      = input.getLongByField("timestamp");

            // --- 金额：保持你原有的数据结构 ---
            jedis.hset("sales:realtime", productId, String.valueOf(productTotal));
            jedis.set("sales:total", String.valueOf(totalSales));
            jedis.rpush("sales:timeline", timestamp + "," + totalSales);
            jedis.ltrim("sales:timeline", -300, -1);

            // --- 销量：新增结构，供面板直接读取 ---
            // 1) 各产品实时销量（Hash）
            jedis.hset("sales:realtime:count", productId, String.valueOf(productCount));

            // 2) 总销量（String）
            jedis.set("sales:total:count", String.valueOf(totalCount));

            // 3) 各产品销量时间序列（List）
            String perProductTimelineKey = "sales:timeline:count:" + productId;
            jedis.rpush(perProductTimelineKey, timestamp + "," + productCount);
            jedis.ltrim(perProductTimelineKey, -300, -1);

            // 4) 实时销量排行榜（ZSet）
            // 以累计销量作为score，productId作为member，便于实时TopN对比
            jedis.zadd("sales:leaderboard:count", productCount, productId);

            System.out.printf("[RedisBolt] %s | 金额累计=%.2f, 总额=%.2f | 量累计=%d, 总量=%d%n",
                    productId, productTotal, totalSales, productCount, totalCount);
        } catch (Exception e) {
            System.err.println("[RedisBolt] 写入失败: " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
