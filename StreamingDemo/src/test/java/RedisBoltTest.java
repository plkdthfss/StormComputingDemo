import com.yjy.topology.bolt.AggregateBolt;
import com.yjy.topology.bolt.ProcessBolt;
import com.yjy.topology.bolt.RedisBolt;
import com.yjy.topology.spout.KafkaSpoutConfigUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 测试完整链路：
 * KafkaSpout → ProcessBolt → AggregateBolt → RedisBolt
 */
public class RedisBoltTest {
    public static void main(String[] args) throws Exception {
        final String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        final String topic   = "sales_events";

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfigUtil.build(brokers, topic)), 1);
        builder.setBolt("process-bolt", new ProcessBolt(), 1).shuffleGrouping("kafka-spout");
        builder.setBolt("aggregate-bolt", new AggregateBolt(), 1).fieldsGrouping("process-bolt", new org.apache.storm.tuple.Fields("productId"));
        builder.setBolt("redis-bolt", new RedisBolt(), 1).globalGrouping("aggregate-bolt");

        Config cfg = new Config();
        cfg.setDebug(false);
        cfg.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("redis-bolt-test", cfg, builder.createTopology());

        System.out.println("[TEST] 拓扑已启动，等待数据流转...");
        Thread.sleep(20000);  // 等待 20 秒
        cluster.killTopology("redis-bolt-test");
        cluster.shutdown();

        System.out.println("[TEST][PASS] RedisBolt 测试完成，请在 Redis 中查看数据。");
    }
}
