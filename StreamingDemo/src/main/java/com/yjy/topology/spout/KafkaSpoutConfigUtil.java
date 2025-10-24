package com.yjy.topology.spout;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class KafkaSpoutConfigUtil {
    public static KafkaSpoutConfig<String, String> build(String brokers, String topic) {
        return KafkaSpoutConfig.builder(brokers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-sales-group")
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }
}