package com.yjy.repository;

import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

@Repository
public class SalesRepository {

    private final JedisPool pool;

    public SalesRepository(JedisPool pool) {
        this.pool = pool;
    }

    /** 各产品销售额 (Hash) */
    public Map<String, String> getRealtimeAmount() {
        try (Jedis jedis = pool.getResource()) {
            return jedis.hgetAll("sales:realtime");
        }
    }

    /** 各产品销量 (Hash) */
    public Map<String, String> getRealtimeCount() {
        try (Jedis jedis = pool.getResource()) {
            return jedis.hgetAll("sales:realtime:count");
        }
    }

    /** 总销售额 (String) */
    public double getTotalAmount() {
        try (Jedis jedis = pool.getResource()) {
            String v = jedis.get("sales:total");
            return v == null ? 0.0 : Double.parseDouble(v);
        }
    }

    /** 总销售量 (String) */
    public long getTotalCount() {
        try (Jedis jedis = pool.getResource()) {
            String v = jedis.get("sales:total:count");
            return v == null ? 0L : Long.parseLong(v);
        }
    }

    /** 销售额时间序列 (List) */
    public List<Map<String, Object>> getTimelineAmount(int limitLastN) {
        try (Jedis jedis = pool.getResource()) {
            List<String> raw = jedis.lrange("sales:timeline", Math.max(-limitLastN, -1000), -1);
            return parseTimeline(raw);
        }
    }

    /** 各产品销量时间序列 (List) */
    public List<Map<String, Object>> getTimelineCount(String productId, int limitLastN) {
        try (Jedis jedis = pool.getResource()) {
            List<String> raw = jedis.lrange("sales:timeline:count:" + productId, Math.max(-limitLastN, -1000), -1);
            return parseTimeline(raw);
        }
    }

    /** 销量排行榜 (ZSet) */
    public List<Map<String, Object>> getLeaderboardCount(int topN) {
        try (Jedis jedis = pool.getResource()) {
            List<String> top = jedis.zrevrange("sales:leaderboard:count", 0, topN - 1);
            List<Map<String, Object>> list = new ArrayList<>();
            for (String pid : top) {
                Double score = jedis.zscore("sales:leaderboard:count", pid);
                Map<String, Object> row = new HashMap<>();
                row.put("productId", pid);
                row.put("count", score == null ? 0 : score.longValue());
                list.add(row);
            }
            return list;
        }
    }

    private List<Map<String, Object>> parseTimeline(List<String> raw) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String s : raw) {
            String[] arr = s.split(",");
            if (arr.length == 2) {
                Map<String, Object> p = new HashMap<>();
                try {
                    p.put("timestamp", Long.parseLong(arr[0]));
                    p.put("value", Double.parseDouble(arr[1]));
                    out.add(p);
                } catch (NumberFormatException ignored) {}
            }
        }
        return out;
    }
}