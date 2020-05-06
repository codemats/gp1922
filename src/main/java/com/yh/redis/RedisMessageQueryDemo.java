package com.yh.redis;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.TimeUnit;

//Redis实现消息队列
public class RedisMessageQueryDemo {

    public static void main(String[] args) {

        //生产者
        Produce produce = new Produce(Constans.channel,Constans.movieTime,Constans.movieName) ;
        new Thread(produce,"produce").start();

        //消费者
        Consumer consumer = new Consumer(Constans.channel) ;
        new Thread(consumer,"consumer").start();

        /*
        打印的结果

        开始播放....
        channel:movie，message：电影[藏龙卧虎]剩余时长:99分钟
        channel:movie，message：电影[藏龙卧虎]剩余时长:98分钟
        channel:movie，message：电影[藏龙卧虎]剩余时长:97分钟
        channel:movie，message：电影[藏龙卧虎]剩余时长:96分钟
        channel:movie，message：电影[藏龙卧虎]剩余时长:95分钟
        channel:movie，message：电影[藏龙卧虎]剩余时长:94分钟
        ... ...

         */

    }
}


//生产者
class Produce implements Runnable{
    private String channel;
    private Integer movieTime;
    private String movieName;

    public Produce(String channel, Integer movieTime, String movieName) {
        if (StringUtils.isEmpty(channel) || movieTime == null) {
            throw new NullPointerException("channel or movieTime 为空");
        }
        this.channel = channel;
        this.movieTime = movieTime;
        this.movieName = movieName;
    }

    //产生消息
    public  void produce() throws Exception {
        Jedis jedis = new Jedis(Constans.hostName, Constans.port);
        jedis.auth(Constans.password);
        for (Integer i = 0; i < movieTime; i++) {
            String info = "电影[" + this.movieName + "]剩余时长:" + (movieTime - i) + "分钟";
            //消息发往redis，同通道channel把消息info发给redis
            jedis.publish(this.channel, info);
            TimeUnit.SECONDS.sleep(1);
        }
        jedis.close();
    }

    @Override
    public void run() {
        try {
            System.out.println("开始播放....");
            produce();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Consumer implements Runnable {
    private String channel;

    public Consumer(String channel) {
        this.channel = channel;
    }


    public void onMessage(){
        Jedis jedis = new Jedis(Constans.hostName, Constans.port);
        jedis.auth(Constans.password);

        //消息的订阅
        JedisPubSub jedisPubSub = new JedisPubSub(){

            //监听收到消息的处理，channel：通道名称，message：收到的消息
            @Override
            public void onMessage(String channel, String message){
                try {
                    System.out.println("channel:"+channel+"，message："+message);
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        //订阅通道的消息
        jedis.subscribe(jedisPubSub, channel);
    }


    @Override
    public void run() {
        onMessage();
    }
}

interface Constans {

    //主机名
    String hostName = "172.16.109.138";
    //端口
    int port = 6379;
    //设置redis的密码
    String password = "redis";

    //通道名称
    String channel = "movie";
    //电影时长
    Integer movieTime = 100 ;
    //电影名称
    String movieName ="藏龙卧虎" ;

}