package com.yh.redis;

import org.apache.commons.lang.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;
import redis.clients.jedis.Jedis;

import java.util.UUID;

//使用Jedis实现分布式锁
public class RedissonDistributeLockDemo {
    //主机
    private static final String HOST = "redis://172.16.109.138:6379";

    //分布式锁key
    private static final String LOCK_KEY = "lockKey";

    //库存key
    private static final String STOCK = "stock";

    //端口
    private static final Integer PORT = 6379;

    public static void main(String[] args) {
        //可在高并发场景测试
        doWork();

    }

    public static RedissonClient redisson(){
        Config config = new Config();
        config.setTransportMode(TransportMode.EPOLL);
        config.useSingleServer()
                .setAddress(HOST)
        .setPassword("redis")
        .setDatabase(0);
        return  Redisson.create(config);
    }

    public static String doWork() {
        Jedis jedis = new Jedis(HOST, PORT);
        RedissonClient redissonClient = redisson();
        RLock lock = redissonClient.getLock(LOCK_KEY);
        try {
            lock.lock();
            //业务逻辑-开始
            String stock = jedis.get(STOCK);
            if (StringUtils.isEmpty(stock) && Integer.parseInt(stock) > 0) {
                System.out.println("库存扣减成功，剩余：" + (Integer.parseInt(stock) - 1));
            } else {
                System.out.println("库存不足");
            }
            //业务逻辑-结束
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return "操作成功";
    }
}
