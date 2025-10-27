import redis.clients.jedis.Jedis;

public class RedisConnectionTest {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("hadoop105", 6379)) {
            jedis.set("test:key", "hello_redis");
            String value = jedis.get("test:key");
            System.out.println("[Redis] 连接成功，读取值：" + value);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Redis] 连接失败！");
        }
    }
}
