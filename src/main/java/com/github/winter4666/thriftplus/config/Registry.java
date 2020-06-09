package com.github.winter4666.thriftplus.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
	
	private StateChangesWatcher watcher;
	
	private String connectString;
	
	private int sessionTimeout;
	
	private List<RegisterServerCommand> commands = new CopyOnWriteArrayList<>();
	
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
		watcher = new StateChangesWatcher();
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		initZooKeeper();
	}
	
	private synchronized void initZooKeeper() {
		try {
			logger.info("initZooKeeper,connectString={},sessionTimeout={}",connectString,sessionTimeout);
			latch = new CountDownLatch(1);
			zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
		} catch (IOException e) {
			logger.error("error occurred while initZooKeeper is invoked", e);
		}
	}
	
	/**
	 * 注册服务
	 * @param serviceName
	 * @param ip
	 * @param port
	 * @param id
	 */
	public void registerServer(Class<?> serviceClass,String ip, int port,String id, boolean backup) {
		RegisterServerCommand command = new RegisterServerCommand(serviceClass, ip, port, id, backup);
		commands.add(command);
		command.execute();
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
	 * 加载服务列表
	 * @param serviceClass
	 * @param listener
	 */
	public synchronized void loadServers(Class<?> serviceClass,ServerListListener listener) {
		try {
			latch.await();
			String serviceRoot = REGISTRY_ROOT + "/" + serviceClass.getSimpleName();
			if(zooKeeper.exists(serviceRoot, false) == null) {
				logger.info("create node {}",serviceRoot);
				zooKeeper.create(serviceRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			loadServers(serviceRoot, listener);
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void loadServers(final String serviceRoot, ServerListListener listener) {
		try {
			List<String> serverList = zooKeeper.getChildren(serviceRoot, new Watcher() {
				
				@Override
				public void process(WatchedEvent event) {
					logger.info("server list changed,serviceRoot={},event={}",serviceRoot,event);
					States state = zooKeeper.getState();
					if(state.isConnected()) {
						loadServers(serviceRoot,listener);
					} else {
						logger.warn("zooKeeper is not connected,state={}",state);
					}
				}
			});
			logger.info("loadServers,serviceRoot={},serverList={}",serviceRoot,serverList);
			List<ServerInfo> primaryServerInfos = new ArrayList<>();
			List<ServerInfo> backupServerInfos = new ArrayList<>();
			for(String server : serverList) {
				NodeData nodeData = NodeData.parse(zooKeeper.getData(serviceRoot + "/" + server, false, null));
				ServerInfo serverInfo = new ServerInfo();
				serverInfo.setHost(server.split(":")[0]);
				serverInfo.setPort(Integer.valueOf(server.split(":")[1]));
				serverInfo.setId(nodeData.id);
				if(nodeData.backup) {
					backupServerInfos.add(serverInfo);
				} else {
					primaryServerInfos.add(serverInfo);
				}
			}
			listener.onServerListChanged(primaryServerInfos,backupServerInfos);
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
		void onServerListChanged(List<ServerInfo> primaryServerInfos,List<ServerInfo> backupServerInfos);
	}
	
	private class RegisterServerCommand {
		private Class<?> serviceClass;
		private String ip;
		private int port;
		private String id;
		private boolean backup;
		
		public RegisterServerCommand(Class<?> serviceClass, String ip, int port, String id, boolean backup) {
			super();
			this.serviceClass = serviceClass;
			this.ip = ip;
			this.port = port;
			this.id = id;
			this.backup = backup;
		}

		private void execute() {
			synchronized (Registry.this) {
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
						logger.warn("node {} existed,delete it",service);
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
		}
	}
	
	
	private class StateChangesWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			try {
				logger.info("session state changes,event={}",event);
				switch (event.getState()) {
				case SyncConnected:
					if(zooKeeper.exists(REGISTRY_ROOT, false) == null) {
						logger.info("create node {}",REGISTRY_ROOT);
						zooKeeper.create(REGISTRY_ROOT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} else {
						logger.info("node {} has been created",REGISTRY_ROOT);
					}
					latch.countDown();
					break;
				case Expired:
					initZooKeeper();
					for(RegisterServerCommand command : commands) {
						command.execute();
					}
					break;
				default:
					break;
				}
			} catch (KeeperException | InterruptedException e) {
				logger.error("create node " + REGISTRY_ROOT + " error",e);
			} 
		}
	}
	
	/**
	 * 节点关联的数据
	 * @author wutian
	 */
	private static class NodeData implements Serializable{

		private static final long serialVersionUID = -672419792619990267L;

		private String id;
		
		private Boolean backup;
		
		private NodeData() {
			
		}
		
		private NodeData(String id, Boolean backup) {
			super();
			this.id = id;
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
