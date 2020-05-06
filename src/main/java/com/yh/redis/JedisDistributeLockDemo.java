package com.yh.redis;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.UUID;

//使用Jedis实现分布式锁
public class JedisDistributeLockDemo {
    //主机
    private static final String HOST = "172.16.109.138";
    //端口
    private static final Integer PORT = 6379;
    //分布式锁key
    private static final String LOCK_KEY = "lockKey";
    //库存key
    private static final String STOCK = "stock";

    public static void main(String[] args) {
        //可在高并发场景测试
        doWork();

    }

    public static String doWork() {
        Jedis jedis = new Jedis(HOST, PORT);
        //定义属于自己的lock
        String clientId = UUID.randomUUID().toString().replaceAll("-", "");
        Long aLong = jedis.setnx(LOCK_KEY, clientId);
        //设置过期时间key，防止锁无法被释放
        jedis.expire(LOCK_KEY, 10);
        try {
            //==0 说明该锁没有被释放
            if (aLong == 0) {
                return "系统繁忙，请稍后再试";
            }
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
            //相等则是该线程加的锁，可以释放；否则不能释放
            if (clientId.equals(jedis.get(LOCK_KEY))) {
                jedis.del(LOCK_KEY);
            }
        }
        return "操作成功";
    }
}
