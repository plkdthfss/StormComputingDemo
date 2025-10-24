package com.yjy.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {

    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(8);
        config.setMaxIdle(4);
        config.setMinIdle(1);
        config.setMaxWaitMillis(1000);
        config.setJmxEnabled(false);  // 禁用 JedisPool 的 JMX 注册

        // 注意：直接使用 Storm 集群 Redis 地址
        return new JedisPool(config, "hadoop102", 6379);
    }
}

