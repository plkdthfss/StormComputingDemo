import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka 消费端：持续读取 sales_events 主题消息，并写入 Redis
 */
public class KafkaToRedisTest {
    public static void main(String[] args) {
        // 1️⃣ Kafka配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sales-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2️⃣ 创建消费者
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Jedis jedis = new Jedis("hadoop102", 6379)) {

            consumer.subscribe(Collections.singletonList("sales_events"));
            System.out.println("[KafkaToRedis] 已订阅 Topic: sales_events");

            // 3️⃣ 持续监听
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    String msg = record.value();
                    System.out.println("[KafkaToRedis] 收到消息：" + msg);

                    // 写入 Redis（列表保存时间序列数据）
                    jedis.rpush("sales:timeline", msg);
                    jedis.ltrim("sales:timeline", -300, -1);

                    // 可选：计算实时总销售额
                    // 可在这里解析 JSON 并计算金额累加
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

