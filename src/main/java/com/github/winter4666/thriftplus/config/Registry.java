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
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
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
	 * 构造服务注册中心，默认3分钟session过期时间
	 * @param connectString ZooKeeper服务地址
	 */
	public Registry(String connectString) {
		this(connectString, 3*60*1000);
	}
	
	/**
	 * 构造服务注册中心
	 * @param connectString ZooKeeper服务地址
	 * @param sessionTimeout session过期时间
	 * @see org.apache.zookeeper.ZooKeeper#ZooKeeper(String, int, Watcher)
	 */
	public Registry(String connectString,int sessionTimeout) {
		try {
			latch = new CountDownLatch(1);
			zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
				
				@Override
				public void process(WatchedEvent event) {
					try {
						logger.info("session state changes,event={}",event);
						if(event.getState() == KeeperState.SyncConnected) {
							if(zooKeeper.exists(REGISTRY_ROOT, false) == null) {
								logger.info("create node {}",REGISTRY_ROOT);
								zooKeeper.create(REGISTRY_ROOT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							} else {
								logger.info("node {} has been created",REGISTRY_ROOT);
							}
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
	public synchronized void registerServer(Class<?> serviceClass,String ip, int port,String id, boolean backup) {
		try {
			logger.info("register server,serviceClass={},ip={},port={},id={}",serviceClass,ip,port,id);
			latch.await();
			
			String serviceRoot = REGISTRY_ROOT + "/" + serviceClass.getSimpleName();
			if(zooKeeper.exists(serviceRoot, false) == null) {
				logger.info("create node {}",serviceRoot);
				zooKeeper.create(serviceRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				logger.info("node {} has been created",serviceRoot);
			}
			
			String service = serviceRoot + "/" + ip + ":" + port;
			if(zooKeeper.exists(service, false) != null) {
				logger.warn("node {} is existed,delete it",service);
				try {
					zooKeeper.delete(service, -1);
				} catch (NoNodeException e) {
					logger.warn("node {} may already be deleted",service);
				}
			}
			logger.info("create node {}",service);
			zooKeeper.create(service, new NodeData(id,backup).toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException("register server error", e);
		} 
	}
	
	/**
	 * 移除服务
	 * @param serviceClass
	 * @param ip
	 * @param port
	 */
	public synchronized void removeServer(Class<?> serviceClass,String ip, int port) {
		try {
			logger.info("remove server,serviceClass={},ip={},port={}",serviceClass,ip,port);
			latch.await();
			String serviceRoot = REGISTRY_ROOT + "/" + serviceClass.getSimpleName();
			zooKeeper.delete(serviceRoot + "/" + ip + ":" + port, -1);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException("remove server error", e);
		}
	}
	
	/**
	 * 获取服务列表
	 * @param serviceClass
	 * @param listener
	 */
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
	
	private void getServers(final String serviceRoot, Class<?> serviceClass,ServerListListener listener) {
		try {
			List<String> serverList = zooKeeper.getChildren(serviceRoot, new Watcher() {
				
				@Override
				public void process(WatchedEvent event) {
					logger.info("server list changed,event={}",event);
					States state = zooKeeper.getState();
					if(state.isConnected()) {
						getServers(serviceRoot,serviceClass,listener);
					} else {
						logger.warn("zooKeeper is not connected,state={}",state);
					}
				}
			});
			List<ServerInfo> serverInfos = new ArrayList<>();
			List<ServerInfo> backupServerInfos = new ArrayList<>();
			for(String server : serverList) {
				NodeData nodeData = NodeData.parse(zooKeeper.getData(serviceRoot + "/" + server, false, null));
				ServerInfo serverInfo = new ServerInfo();
				serverInfo.setHost(server.split(":")[0]);
				serverInfo.setPort(Integer.valueOf(server.split(":")[1]));
				serverInfo.setId(nodeData.getId());
				if(nodeData.getBackup()) {
					backupServerInfos.add(serverInfo);
				} else {
					serverInfos.add(serverInfo);
				}
			}
			listener.onServerListChanged(serverInfos,backupServerInfos);
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
		void onServerListChanged(List<ServerInfo> serverInfos,List<ServerInfo> backupServerInfos);
	}
	
	/**
	 * 节点关联的数据
	 * @author wutian
	 */
	public static class NodeData implements Serializable{

		private static final long serialVersionUID = -672419792619990267L;

		private String id;
		
		private Boolean backup;
		
		public NodeData() {
			
		}
		
		public NodeData(String id, Boolean backup) {
			super();
			this.id = id;
			this.backup = backup;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
		
		public Boolean getBackup() {
			return backup;
		}

		public void setBackup(Boolean backup) {
			this.backup = backup;
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
