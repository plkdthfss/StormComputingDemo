package com.yjy;

import com.yjy.repository.SalesRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

@SpringBootTest(classes = SpringBootServiceApplication.class)
public class SalesRepositoryIntegrationTest {

    @Autowired
    private SalesRepository repo;

    @Test
    void testReadFromStormRedis() {
        Map<String, String> realtime = repo.getRealtimeAmount();
        double total = repo.getTotalAmount();
        long count = repo.getTotalCount();

        System.out.println("✅ [实时销售额 Hash] " + realtime);
        System.out.println("✅ [总销售额 String] " + total);
        System.out.println("✅ [总销量 String] " + count);

        if (realtime.isEmpty()) {
            System.out.println("⚠️ 未读取到数据，请确认 Storm Redis 正在写入 sales:realtime");
        }
    }
}