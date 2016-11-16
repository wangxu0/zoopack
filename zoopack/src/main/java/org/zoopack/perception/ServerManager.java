package org.zoopack.perception;

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
import java.util.concurrent.CountDownLatch;

/**
 * Server Manager for server online and offline(register and delete from server list)
 * @author wangxu
 * @date 2016/11/16
 */
public class ServerManager {
	private Logger logger = LoggerFactory.getLogger(ServerManager.class);
	
	private String serverRoot;
	private ZooKeeper zooKeeper;
	
	private String curServerPath;
	
	private CountDownLatch latch = new CountDownLatch(1);
	
	public ServerManager(String serverRoot, ZooKeeper zooKeeper) {
		this.serverRoot = serverRoot;
		this.zooKeeper = zooKeeper;
	}
	
	public ServerManager(String serverRoot, int timeout) {
		try {
			this.serverRoot = serverRoot;
			this.zooKeeper = new ZooKeeper(Constants.CONNECT_CONFIG, timeout, new ConnectedWatcher());
			latch.await();
		} catch (IOException e) {
			logger.error("Zookeeper connection is failed");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.error("Zookeeper connection is failed");
			e.printStackTrace();
		}
	}
	
	public ServerManager(String serverRoot) {
		try {
			this.serverRoot = serverRoot;
			this.zooKeeper = new ZooKeeper(Constants.CONNECT_CONFIG, Constants.SESSION_TIMEOUT, new ConnectedWatcher());
			latch.await();
		} catch (IOException e) {
			logger.error("Zookeeper connection is failed");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.error("Zookeeper connection is failed");
			e.printStackTrace();
		}
	}
	
	
	public void register(String serverInfo) {
		try {
			Stat stat = zooKeeper.exists(serverRoot, false);
			if(stat == null) {
				zooKeeper.create(serverRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (curServerPath != null && zooKeeper.exists(curServerPath, false) != null) {
				logger.info(serverInfo + " is already online.");
				return ;
			}
			curServerPath = zooKeeper.create(serverRoot+"/"+"server_", serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			logger.info(serverInfo + " is online now.");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public void delete() {
		try {
			if (zooKeeper.exists(curServerPath, false) == null) {
				logger.info("Server is already offline.");
			}
			else {
				zooKeeper.delete(curServerPath, -1);
				logger.info("server is offline now.");
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void update(String serverInfo) {
		delete();
		register(serverInfo);
	}
	
	
	private class ConnectedWatcher implements Watcher {
		public void process(WatchedEvent watchedEvent) {
			if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected) {
				latch.countDown();
			}
		}
	}
}
