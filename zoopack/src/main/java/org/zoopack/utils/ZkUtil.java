package org.zoopack.utils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper base op
 * @author wangxu
 * @date 2016/11/14
 */
public class ZkUtil {

    private static Logger logger = LoggerFactory.getLogger(ZkUtil.class);

    private volatile static ZooKeeper zooKeeper = null;
    private static CountDownLatch latch = new CountDownLatch(1);

    /**
     * Try connecting to zookeeper.
     * @param connectString
     * @param timeout
     * @return zookeeper object
     */
    public static ZooKeeper connect(String connectString, int timeout) {
        if (zooKeeper == null || zooKeeper.getState()== ZooKeeper.States.CLOSED) {
            synchronized (ZkUtil.class) {
                if (zooKeeper == null || zooKeeper.getState() == ZooKeeper.States.CLOSED) {
                    try {
                        zooKeeper = new ZooKeeper(connectString, timeout, new Watcher() {
                            public void process(WatchedEvent watchedEvent) {
                                if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                                    latch.countDown();
                                }
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error("Zookeeper connection failed");
                    }
                }
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zooKeeper;
    }


}
