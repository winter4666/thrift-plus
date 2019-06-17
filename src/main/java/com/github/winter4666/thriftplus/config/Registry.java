package com.github.winter4666.thriftplus.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务注册中心
 * @author wutian
 */
public class Registry {
	
	private static Logger logger = LoggerFactory.getLogger(Registry.class);
	
	private static final String REGISTRY_ROOT = "/thrift_registry";
	
	private ZooKeeper zooKeeper;
	
	private CountDownLatch latch;
	
	/**
	 * 构造服务注册中心
	 * @param connectString ZooKeeper服务地址
	 * @see org.apache.zookeeper.ZooKeeper#ZooKeeper(String, int, Watcher)
	 */
	public Registry(String connectString) {
		try {
			latch = new CountDownLatch(1);
			zooKeeper = new ZooKeeper(connectString, 5000, new Watcher() {
				
				@Override
				public void process(WatchedEvent event) {
					try {
						if(event.getState() == KeeperState.SyncConnected) {
							if(zooKeeper.exists(REGISTRY_ROOT, false) == null) {
								logger.info("create node {}",REGISTRY_ROOT);
								zooKeeper.create(REGISTRY_ROOT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							}
						} else {
							logger.info("node {} has been created",REGISTRY_ROOT);
						}
						latch.countDown();
					} catch (KeeperException | InterruptedException e) {
						logger.error("create node " + REGISTRY_ROOT + " error",e);
					} 
				}
			});
		} catch (IOException e) {
			logger.error("registry init error", e);
		}
	}
	
	/**
	 * 注册服务
	 * @param serviceName
	 * @param ip
	 * @param port
	 * @param id
	 */
	public synchronized void registerServer(Class<?> serviceClass,String ip, int port,String id) {
		try {
			latch.await();
			String serviceRoot = REGISTRY_ROOT + "/" + serviceClass.getSimpleName();
			if(zooKeeper.exists(serviceRoot, false) == null) {
				logger.info("create node {}",serviceRoot);
				zooKeeper.create(serviceRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				logger.info("node {} has been created",serviceRoot);
			}
			zooKeeper.create(serviceRoot + "/" + ip + ":" + port, new NodeData(id, true).toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException("register server error", e);
		} 
	}
	
	public synchronized void getServers(Class<?> serviceClass,ServerListListener listener) {
		try {
			latch.await();
			String serviceRoot = REGISTRY_ROOT + "/" + serviceClass.getSimpleName();
			if(zooKeeper.exists(serviceRoot, false) == null) {
				logger.info("create node {}",serviceRoot);
				zooKeeper.create(serviceRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			getServers(serviceRoot, serviceClass, listener);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 获取服务列表
	 * @param serviceClass
	 */
	private void getServers(final String serviceRoot, Class<?> serviceClass,ServerListListener listener) {
		try {
			List<String> serverList = zooKeeper.getChildren(serviceRoot, new Watcher() {
				
				@Override
				public void process(WatchedEvent event) {
					logger.info("server list changed,event={}",event);
					getServers(serviceRoot,serviceClass,listener);
				}
			});
			List<ServerInfo> serverInfos = new ArrayList<>();
			for(String server : serverList) {
				NodeData nodeData = NodeData.parse(zooKeeper.getData(serviceRoot + "/" + server, false, null));
				if(nodeData.getIsAvailable()) {
					ServerInfo serverInfo = new ServerInfo();
					serverInfo.setHost(server.split(":")[0]);
					serverInfo.setPort(Integer.valueOf(server.split(":")[1]));
					serverInfo.setId(nodeData.getId());
					serverInfos.add(serverInfo);
				}
			}
			listener.onServerListChanged(serverInfos);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 关闭zooKeeper连接
	 */
	public void close() {
		try {
			logger.info("close zooKeeper");
			zooKeeper.close();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 监听服务列表的变化
	 * @author wutian
	 */
	public interface ServerListListener {
		void onServerListChanged(List<ServerInfo> serverInfos);
	}
	
	/**
	 * 节点关联的数据
	 * @author wutian
	 */
	public static class NodeData implements Serializable{
		
		private static final long serialVersionUID = 1L;

		private String id;
		
		private Boolean isAvailable;
		
		public NodeData() {
			
		}
		
		public NodeData(String id, Boolean isAvailable) {
			super();
			this.id = id;
			this.isAvailable = isAvailable;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Boolean getIsAvailable() {
			return isAvailable;
		}

		public void setIsAvailable(Boolean isAvailable) {
			this.isAvailable = isAvailable;
		}
		
		public byte[] toBytes() {
			ObjectOutputStream objectOutputStream = null;
			try {
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
				objectOutputStream.writeObject(this);
				return byteArrayOutputStream.toByteArray();
			} catch (IOException e) {
				throw new RuntimeException("serialize error", e);
			} finally {
				try {
					if(objectOutputStream != null) {
						objectOutputStream.close();
					}
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		public static NodeData parse(byte[] bytes) {
			ObjectInputStream objectInputStream = null;
			try {
				ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
				objectInputStream = new ObjectInputStream(byteArrayInputStream);
				return (NodeData)objectInputStream.readObject();
			} catch (ClassNotFoundException | IOException e) {
				throw new RuntimeException("parse NodeData error", e);
			} finally {
				try {
					if(objectInputStream != null) {
						objectInputStream.close();
					}
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		
	}

}
