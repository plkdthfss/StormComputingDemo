package com.yjy.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ObjectInputFilter;

@Configuration
public class RedisConfig {

    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(ConfigUtil.getInt("redis.maxTotal"));
        config.setMaxIdle(ConfigUtil.getInt("redis.max-idle"));
        config.setMinIdle(ConfigUtil.getInt("redis.min-idle"));
        config.setMaxWaitMillis(ConfigUtil.getInt("redis.maxWaitMillis"));
        config.setJmxEnabled(ConfigUtil.getBoolean("redis.jmxEnabled"));  // 禁用 JedisPool 的 JMX 注册

        return new JedisPool(config, ConfigUtil.getString("redis.host"), ConfigUtil.getInt("redis.port"));
    }
}

