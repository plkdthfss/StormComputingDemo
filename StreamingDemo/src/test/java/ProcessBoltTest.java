import com.yjy.topology.bolt.ProcessBolt;
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

public class ProcessBoltTest {

    /** 验证输出字段是否正确，并计数 */
    public static class LoggingAssertBolt extends BaseRichBolt {
        private final AtomicInteger okCount = new AtomicInteger(0);

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            try {
                String productId = input.getStringByField("productId");
                double productPrice = input.getDoubleByField("productPrice");
                long timestamp = input.getLongByField("timestamp");

                System.out.printf("[Test] productId=%s price=%.2f ts=%d%n",
                        productId, productPrice, timestamp);

                if (productId != null && !productId.isEmpty()
                        && productPrice >= 0
                        && timestamp > 0) {
                    int n = okCount.incrementAndGet();
                    if (n == 3) {
                        System.out.println("[TEST][PASS] 已成功解析 ≥3 条消息");
                    }
                } else {
                    System.out.println("[TEST][WARN] 字段值异常");
                }
            } catch (Exception e) {
                System.out.println("[TEST][FAIL] 下游字段读取错误: " + e.getMessage());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }

    public static void main(String[] args) throws Exception {
        final String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        final String topic   = "sales_events";

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfigUtil.build(brokers, topic)), 1);
        builder.setBolt("process-bolt", new ProcessBolt(), 1).shuffleGrouping("kafka-spout");
        builder.setBolt("assert-bolt", new LoggingAssertBolt(), 1).shuffleGrouping("process-bolt");

        Config cfg = new Config();
        cfg.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("process-bolt-test", cfg, builder.createTopology());

        Thread.sleep(20000);
        cluster.killTopology("process-bolt-test");
        cluster.shutdown();
    }
}
