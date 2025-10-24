import com.yjy.topology.bolt.AggregateBolt;
import com.yjy.topology.bolt.ProcessBolt;
import com.yjy.topology.spout.KafkaSpoutConfigUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试链路：KafkaSpout → ProcessBolt → AggregateBolt
 */
public class AggregateBoltTest {

    public static class LoggingAssertBolt extends BaseRichBolt {
        private final AtomicInteger okCount = new AtomicInteger(0);

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            String productId = input.getStringByField("productId");
            double productTotal = input.getDoubleByField("productTotal");
            double totalSales = input.getDoubleByField("totalSales");
            long ts = input.getLongByField("timestamp");

            System.out.printf("[Test] %s -> 单品累计=%.2f, 总销售额=%.2f, ts=%d%n",
                    productId, productTotal, totalSales, ts);

            if (productId != null && productTotal >= 0 && totalSales >= 0) {
                int n = okCount.incrementAndGet();
                if (n == 3) {
                    System.out.println("[TEST][PASS] AggregateBolt 聚合输出正常");
                }
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
        builder.setBolt("aggregate-bolt", new AggregateBolt(), 1).fieldsGrouping("process-bolt", new org.apache.storm.tuple.Fields("productId"));
        builder.setBolt("test-bolt", new LoggingAssertBolt(), 1).globalGrouping("aggregate-bolt");

        Config cfg = new Config();
        cfg.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("aggregate-bolt-test", cfg, builder.createTopology());

        Thread.sleep(20000);
        cluster.killTopology("aggregate-bolt-test");
        cluster.shutdown();
    }
}
