package com.yh.zk_lock;

import org.I0Itec.zkclient.IZkDataListener;

import java.util.concurrent.CountDownLatch;

//具体实现锁
public class ZookeeperDistributeLock extends AbstractZookeeperLock {

    private Integer initalCount = 1;

    public ZookeeperDistributeLock(String lockName) {
        this.lock = lockName;
    }

    @Override
    protected void waitLock() {

        IZkDataListener iZkDataListener = new IZkDataListener() {
            @Override
            //监听数据改变
            public void handleDataChange(String s, Object o) throws Exception { }

            @Override
            //监听数据删除
            public void handleDataDeleted(String s) throws Exception {
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        };

        //监听节点数据改变
        zkClient.subscribeDataChanges(lock, iZkDataListener);

        if(zkClient.exists(lock)){
            countDownLatch = new CountDownLatch(initalCount);
            try {
                //线程阻塞
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //取消订阅
        zkClient.unsubscribeDataChanges(lock,iZkDataListener);

    }

    @Override
    protected boolean tryLock() {
        try {
            //创建临时节点
            zkClient.createEphemeral(lock);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
