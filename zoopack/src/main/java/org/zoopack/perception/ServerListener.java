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
	
	private String serverRoot; //The server root directory where stored server list.
	private ZooKeeper zooKeeper; //zookeeper client
	
	private volatile List<String> serverList; //server list
	private CountDownLatch latch = new CountDownLatch(1); //make sure zookeeper was connected.
	
	/**
	 * Construct server listner
	 * @param serverRoot
	 * @param zooKeeper
	 */
	public ServerListener(String serverRoot, ZooKeeper zooKeeper) {
		this.serverRoot = serverRoot;
		this.zooKeeper = zooKeeper;
	}
	
	/**
	 * Construct server listner
	 * @param serverRoot
	 * @param timeout
	 */
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
	
	/**
	 * Construct server listner
	 * @param serverRoot
	 */
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
	
	/**
	 * Get newest server list.
	 * @return
	 */
	public List<String> getServerList() {
		return serverList;
	}
	
	/**
	 * Update the server list from zookepper server root directory.
	 */
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
