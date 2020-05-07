package com.yh.zk_lock;


import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;

/**
 * 实现zookeeper锁
 */
public abstract class AbstractZookeeperLock implements ZookeeperLock {

    //zk链接地址
    protected String zk_address = "172.16.109.138:2181" ;
    //zk客户端
    protected ZkClient zkClient = new ZkClient(zk_address) ;
    //zk锁的路径
    protected String lock ;
    //计数器
    protected CountDownLatch countDownLatch ;

    //加锁
    @Override
    public final void lock() {
        if(tryLock()){
            System.out.println(Thread.currentThread().getName()+"\t 获取锁成功....");
        }else {
            waitLock();
            lock();
        }
    }

    //让子类实现
    protected abstract void waitLock() ;

    protected abstract boolean tryLock() ;

    //解锁
    @Override
    public void unlock() {
        if(zkClient != null){
            //zk创建的是临时节点，关闭后临时节点会销毁
            zkClient.close();
            System.out.println(Thread.currentThread().getName()+"\t 解锁成功....");
        }
    }
}
