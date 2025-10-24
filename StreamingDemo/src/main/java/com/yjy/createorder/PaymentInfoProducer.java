package com.yjy.createorder;

import com.yjy.pojo.PaymentInfo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * Kafka Producer：持续生成订单数据发送到 Kafka
 */
public class PaymentInfoProducer {
    public static void main(String[] args) throws Exception {
        // Kafka 服务地址
        String bootstrapServers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        String topic = "sales_events"; // 你可以改成自己的 topic

        // Kafka 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 不断生成订单
        while (true) {
            PaymentInfo order = PaymentInfo.randomOrder();
            String message = order.toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, order.getOrderId(), message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("数据已发送到kafka" + message);
                } else {
                    exception.printStackTrace();
                }
            });

            Thread.sleep(1000); // 每秒一条
        }
    }
}
