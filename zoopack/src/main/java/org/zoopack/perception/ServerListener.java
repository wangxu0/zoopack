package org.zoopack.perception;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoopack.utils.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Listener for server online or offline.
 * @author wangxu
 * @date 2016/11/16
 */
public class ServerListener {
	
	private Logger logger = LoggerFactory.getLogger(ServerListener.class);
	
	private String serverRoot;
	private ZooKeeper zooKeeper;
	
	private volatile List<String> serverList;
	private CountDownLatch latch = new CountDownLatch(1);
	
	public ServerListener(String serverRoot, ZooKeeper zooKeeper) {
		this.serverRoot = serverRoot;
		this.zooKeeper = zooKeeper;
	}
	
	public ServerListener(String serverRoot, int timeout) {
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
	
	public ServerListener(String serverRoot) {
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
	
	public List<String> getServerList() {
		return serverList;
	}
	
	private void updateServerList() {
		try {
			List<String> children = zooKeeper.getChildren(serverRoot, new Watcher() {
				public void process(WatchedEvent watchedEvent) {
					if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
							updateServerList();
					}
				}
			});
			List<String> servers = new ArrayList<String>();
			for (String child : children) {
				byte[] data = zooKeeper.getData(serverRoot + "/" + child, false, null);
				servers.add(new String(data));
			}
			serverList = servers;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
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
