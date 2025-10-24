package com.yjy.controller;

import com.yjy.repository.SalesRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")  // 允许跨域访问，方便前端 HTML 调试
public class SalesController {

    private final SalesRepository repo;

    public SalesController(SalesRepository repo) {
        this.repo = repo;
    }

    /**
     * 1️⃣ 获取总销售额与总销量
     */
    @GetMapping("/total")
    public Map<String, Object> getTotal() {
        Map<String, Object> map = new HashMap<>();
        map.put("totalAmount", repo.getTotalAmount());
        map.put("totalCount", repo.getTotalCount());
        return map;
    }

    /**
     * 2️⃣ 获取各产品的实时销售额与销量
     * 结果形如：
     * [
     *   {"productId":"PRODUCTID1","count":44,"amount":22705.0},
     *   {"productId":"PRODUCTID2","count":38,"amount":19860.0}
     * ]
     */
    @GetMapping("/realtime")
    public List<Map<String, Object>> getRealtime() {
        Map<String, String> amountMap = repo.getRealtimeAmount();
        Map<String, String> countMap = repo.getRealtimeCount();

        Set<String> productIds = new HashSet<>();
        productIds.addAll(amountMap.keySet());
        productIds.addAll(countMap.keySet());

        List<Map<String, Object>> list = new ArrayList<>();
        for (String pid : productIds) {
            Map<String, Object> item = new HashMap<>();
            item.put("productId", pid);
            item.put("amount", amountMap.getOrDefault(pid, "0"));
            item.put("count", countMap.getOrDefault(pid, "0"));
            list.add(item);
        }
        list.sort(Comparator.comparing(m -> ((String) m.get("productId"))));
        return list;
    }

    /**
     * 3️⃣ 获取销售额趋势（折线图）
     * 结果形如：
     * [
     *   {"timestamp":1761106882997,"value":85398.0},
     *   {"timestamp":1761106898287,"value":86284.0}
     * ]
     */
    @GetMapping("/timeline")
    public List<Map<String, Object>> getTimeline() {
        return repo.getTimelineAmount(20);
    }

    /**
     * 4️⃣ （可选）获取销量排行榜 Top N
     */
    @GetMapping("/leaderboard")
    public List<Map<String, Object>> getLeaderboard(@RequestParam(defaultValue = "5") int topN) {
        return repo.getLeaderboardCount(topN);
    }
}

