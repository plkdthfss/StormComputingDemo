import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class StormConnectionTest {

    // 简单Spout：每秒发一个随机数
    public static class TestSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int i = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            collector.emit(new Values("msg-" + i++));
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        }

        @Override
        public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer declarer) {}
    }

    // 简单Bolt：打印消息
    public static class PrintBolt extends BaseRichBolt {
        @Override
        public void prepare(Map conf, TopologyContext context, org.apache.storm.task.OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            System.out.println("[Storm] 收到：" + input.getString(0));
        }

        @Override
        public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer declarer) {}
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout", new TestSpout());
        builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("test-spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-test-topology", config, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
    }
}