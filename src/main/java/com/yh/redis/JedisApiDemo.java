package com.yh.redis;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import java.util.List;

/**
 * Jedis APi使用
 */
public class JedisApiDemo {

    public static void main(String[] args) {
        //主机名
        String hostName = "172.16.109.138" ;
        //端口
        int port = 6379;
        //设置redis的密码
        String password = "redis";
        //超时时间：10秒
        int timeOut = 10000;

        //测试联通
        Jedis jedis = MyRedisPoll.getJedis(hostName,port,password,timeOut);
        jedis.set("name","lisi");
        System.out.println("name:"+jedis.get("name"));

        //测试事务
        //事务开启
        Transaction transaction = jedis.multi();
        transaction.lpush("key", "11");
        transaction.lpush("key", "22");
        //业务异常
        int k = 1 / 0;
        transaction.lpush("key", "33");

        //事务执行，出错全部回滚
        List<Object> list = transaction.exec();
        for (Object o : list) {
            System.out.println(o.toString());
        }
        //关闭
        jedis.close();

    }
}


class MyRedisPoll  {

    public static JedisPoolConfig creatPoolConfig (){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(50);
        poolConfig.setMinIdle(5);
        poolConfig.setMaxWaitMillis(60000);
        poolConfig.setTestOnBorrow(true);
        return poolConfig;
    }

    //获取连接池
    public static Jedis getJedis(String hostName,Integer port,String password,Integer timeOut){
        JedisPool pool = new JedisPool(creatPoolConfig(),hostName,port,timeOut,password);
        if (pool == null){
            throw new NullPointerException("JedisPool 为空");
        }
        return pool.getResource() ;
    }


}