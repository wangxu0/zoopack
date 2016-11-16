package org.zoopack.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoopack.utils.Constants;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Distributed Lock Object
 * @author wangxu
 * @date 2016/11/14
 */
public class DistributedLock {

    private Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    private String lockRoot;
    private ZooKeeper zooKeeper;

    private String curLockPath;
    private String preLockPath;

    private CountDownLatch latch;

    public DistributedLock(String lockRoot) {
        try {
            latch = new CountDownLatch(1);
            this.lockRoot = lockRoot;
            zooKeeper = new ZooKeeper(Constants.CONNECT_CONFIG, Constants.SESSION_TIMEOUT, new ConnectedWatcher());
            latch.await();
        } catch (IOException e) {
            logger.error("Zookeeper connection failed");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public DistributedLock(String lockRoot, int timeout) {
        try {
            latch = new CountDownLatch(1);
            this.lockRoot = lockRoot;
            zooKeeper = new ZooKeeper(Constants.CONNECT_CONFIG, timeout, new ConnectedWatcher());
            latch.await();
        } catch (IOException e) {
            logger.error("Zookeeper connection is failed");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void lock() {
        if (tryLock()) {
            logger.info(curLockPath + "is trun on.");
        }
        else {
            logger.error("try lock is failed.");
        }
    }

    private boolean tryLock() {
        final CountDownLatch lockLatch = new CountDownLatch(1);
        try {
            curLockPath = zooKeeper.create(lockRoot+"/lock_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            List<String> children = zooKeeper.getChildren(lockRoot, false);
            Collections.sort(children);
            int index = children.indexOf(curLockPath.substring(curLockPath.lastIndexOf("/")+1));
            //not minum,watching the pre lock is deleted event
            if (index > 0) {
                preLockPath = lockRoot + "/" + children.get(index-1);
                Stat stat = zooKeeper.exists(preLockPath, new Watcher() {
                    public void process(WatchedEvent watchedEvent) {
                        if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                            lockLatch.countDown();
                        }
                    }
                });
                if (stat == null) {
                    return true;
                }
            }
            else {
                return true;
            }
            lockLatch.await();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
}

    public void unlock() {
        try {
            zooKeeper.delete(curLockPath, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class ConnectedWatcher implements Watcher {
        public void process(WatchedEvent watchedEvent) {
            if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                latch.countDown();
            }
        }
    }

}
