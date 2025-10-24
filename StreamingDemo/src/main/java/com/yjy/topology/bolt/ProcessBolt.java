package com.yjy.topology.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将 Kafka 消息解析为结构化字段：
 *   productId (String), productPrice (double), timestamp (long)
 * 优先 JSON 解析；失败则回退解析 PaymentInfo.toString() 形式。
 */
public class ProcessBolt extends BaseRichBolt {
    private OutputCollector collector;

    // 形如：PaymentInfo{orderId='O-1', productId='P01', productPrice=19.9, timestamp=1730000000000}
    private static final Pattern TOSTRING_PATTERN = Pattern.compile(
            "productId='([^']+)'\\s*,\\s*productPrice=([0-9]+(?:\\.[0-9]+)?)\\s*,\\s*timestamp=([0-9]+)"
    );

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String raw = input.getStringByField("value");
        try {
            String productId;
            double productPrice;
            long timestamp;

            if (looksLikeJson(raw)) {
                JSONObject obj = JSONObject.parseObject(raw);
                productId    = obj.getString("productId");
                productPrice = obj.getDoubleValue("productPrice");
                timestamp    = obj.getLongValue("timestamp");
            } else {
                // 回退解析 toString() 格式
                Matcher m = TOSTRING_PATTERN.matcher(raw);
                if (!m.find()) {
                    throw new IllegalArgumentException("Unsupported message format: " + raw);
                }
                productId    = m.group(1);
                productPrice = Double.parseDouble(m.group(2));
                timestamp    = Long.parseLong(m.group(3));
            }

            collector.emit(new Values(productId, productPrice, timestamp));
        } catch (Exception e) {
            System.err.println("[ProcessBolt] 解析失败: " + e.getMessage() + " | raw=" + raw);
        }
    }

    private boolean looksLikeJson(String s) {
        return s != null && s.length() >= 2 && s.charAt(0) == '{' && s.charAt(s.length()-1) == '}';
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId", "productPrice", "timestamp"));
    }
}
