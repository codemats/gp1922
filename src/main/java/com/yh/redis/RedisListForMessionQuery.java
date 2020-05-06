package com.yh.redis;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.TimeUnit;

//redis的List实现消息队列
public class RedisListForMessionQuery {
    public static void main(String[] args) {
        
        new Thread(new ProduceList()).start();

        new Thread(new ConsumerList()).start();
    }
}

class  ProduceList implements Runnable{

    private  Jedis jedis = new Jedis(ConstansList.hostName,ConstansList.port);

    public void produce(){
        jedis.auth(ConstansList.password);
        for (int i = 1; i <= 10; i++) {
            if(i % 2 == 0){
                jedis.lpush(ConstansList.messageKey,"左青龙"+i);
                System.out.println("cccc左青龙"+i+"ccc");
            }else {
                jedis.lpush(ConstansList.messageKey,"右青龙"+i);
                System.out.println("ccc右青龙"+i+"cccc");
            }
            try {
                TimeUnit.SECONDS.sleep(1);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        jedis.lpush(ConstansList.messageKey,"100次播放完成");
        jedis.close();
    }

    @Override
    public void run() {
        produce();
    }
}

class ConsumerList implements Runnable{


    @Override
    public void run() {
        try {
            Jedis jedis = new Jedis(ConstansList.hostName,ConstansList.port);
            jedis.auth(ConstansList.password);
            while(true){
               // System.out.println("收到消息："+jedis.lpop(ConstansList.messageKey));  不会阻塞，会去到null值
                List<String> stringList = jedis.blpop(3000,ConstansList.messageKey);
                //是阻止列表弹出原语。您可以看到此命令是LPOP和RPOP的阻止版本，如果指定的键不存在或包含空列表，它们可以阻止
                System.out.println("收到消息："+stringList.get(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

interface ConstansList {

    //主机名
    String hostName = "172.16.109.138";
    //端口
    int port = 6379;
    //设置redis的密码
    String password = "redis";
    //消息的key
    String messageKey = "movie";
}