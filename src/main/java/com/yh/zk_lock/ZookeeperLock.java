package com.yh.zk_lock;

//定义zookeeper的锁的方法
public interface ZookeeperLock {

    /**
     * 锁
     */
    void lock() ;

    /**
     * 解锁
     */
    void unlock() ;


}
