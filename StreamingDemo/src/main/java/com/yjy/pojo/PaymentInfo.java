package com.yjy.pojo;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 模拟订单信息实体类
 */
public class PaymentInfo {
    private String orderId;
    private String productId;
    private double productPrice;
    private long timestamp;

    public PaymentInfo() {}

    public PaymentInfo(String orderId, String productId, double productPrice, long timestamp) {
        this.orderId = orderId;
        this.productId = productId;
        this.productPrice = productPrice;
        this.timestamp = timestamp;
    }

    // Getter / Setter
    public String getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public long getTimestamp() {
        return timestamp;
    }

    // 随机生成订单信息
    public static PaymentInfo randomOrder() {
        String orderId = UUID.randomUUID().toString().replace("-", "");
        String productId = "PRODUCTID" + ThreadLocalRandom.current().nextInt(0, 10);
        double price = ThreadLocalRandom.current().nextDouble(50, 1000);
        long ts = System.currentTimeMillis();
        return new PaymentInfo(orderId, productId, price, ts);
    }

    @Override
    public String toString() {
        return "{" +
                "\"orderId\":\"" + orderId + "\"" +
                ", \"productId\":\"" + productId + "\"" +
                ", \"productPrice\":" + (int)productPrice +
                ", \"timestamp\":" + (Long)timestamp +
                "}";
    }
}
