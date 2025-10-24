import com.yjy.topology.spout.KafkaSpoutConfigUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaSpoutSmokeTest {

    /** 全局计数器（用于测试断言） */
    public static class MsgCounter {
        public static final AtomicInteger COUNT = new AtomicInteger(0);
    }

    /** 打印并计数的简单 Bolt */
    public static class LoggingBolt extends BaseRichBolt {
        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            // KafkaSpout 默认提供的字段里，“value”是消息体
            String msg = input.getStringByField("value");
            System.out.println("[LoggingBolt] " + msg);
            MsgCounter.COUNT.incrementAndGet();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }

    /**
     * 运行方式：
     * 1) 确保已向 topic 发送消息（你已有 PaymentInfoProducer）
     * 2) 运行本类 main，观察 PASS/FAIL
     */
    public static void main(String[] args) throws Exception {
        final String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        final String topic   = "sales_events"; // 与你的 Producer 保持一致
        final int   seconds  = 20;             // 运行多久判定
        final int   expectN  = 3;              // 期望至少收到多少条算 PASS

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfigUtil.build(brokers, topic)), 1);
        builder.setBolt("print-bolt", new LoggingBolt(), 1).shuffleGrouping("kafka-spout");

        Config cfg = new Config();
        cfg.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-spout-smoke", cfg, builder.createTopology());

        // 运行一段时间后停止并输出计数
        Thread.sleep(seconds * 1000L);
        int got = MsgCounter.COUNT.get();

        cluster.killTopology("kafka-spout-smoke");
        cluster.shutdown();

        if (got >= expectN) {
            System.out.println("[TEST][PASS] Received messages = " + got + " (>= " + expectN + ")");
        } else {
            System.out.println("[TEST][FAIL] Received messages = " + got + " (< " + expectN + ")");
        }
    }
}
