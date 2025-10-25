package com.yjy.topology.spout;


import com.yjy.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class KafkaSpoutConfigUtil {
    public static KafkaSpoutConfig<String, String> build(String brokers, String topic) {
        return KafkaSpoutConfig.builder(brokers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, ConfigUtil.getString("kafka.group.id"))
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigUtil.getString("kafka.auto.offset.reset"))
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }
}