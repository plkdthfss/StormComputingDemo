package com.yjy.topology;

import com.yjy.topology.bolt.AggregateBolt;
import com.yjy.topology.bolt.ProcessBolt;
import com.yjy.topology.bolt.RedisBolt;
import com.yjy.topology.spout.KafkaSpoutConfigUtil;
import com.yjy.util.ConfigUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 实时销售监控主拓扑入口
 * Kafka → KafkaSpout → ProcessBolt → AggregateBolt → RedisBolt
 */
public class KafkaToRedisTopology {
    public static void main(String[] args) throws Exception {
        // Kafka 集群地址和 topic
        final String brokers = ConfigUtil.getString("kafka.brokers");
        final String topic = ConfigUtil.getString("kafka.topic");

        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfigUtil.build(brokers, topic)), 1);
        builder.setBolt("process-bolt", new ProcessBolt(), 2).shuffleGrouping("kafka-spout");
        builder.setBolt("aggregate-bolt", new AggregateBolt(), 1)
                .fieldsGrouping("process-bolt", new Fields("productId"));
        builder.setBolt("redis-bolt", new RedisBolt(), 1).globalGrouping("aggregate-bolt");

        // Storm 配置
        Config config = new Config();
        config.setDebug(ConfigUtil.getBoolean("storm.debug"));
        config.setNumWorkers(ConfigUtil.getInt("storm.workers"));

        if (args != null && args.length > 0) {
            // 集群模式（Nimbus 提交）
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            System.out.println("[INFO] 拓扑已提交到集群: " + args[0]);
        } else {
            // 本地模式（开发/调试）
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("sales-realtime-monitor", config, builder.createTopology());
            System.out.println("[INFO] 本地模式启动成功！");
            //跑一分钟就结束！！！！！！！！！！！！！！
//            Thread.sleep(60000);
//            cluster.killTopology("sales-realtime-monitor");
//            cluster.shutdown();
//            System.out.println("[INFO] 本地模式结束。");
        }
    }
}
