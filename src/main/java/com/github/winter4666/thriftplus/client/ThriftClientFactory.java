package com.github.winter4666.thriftplus.client;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.client.ttransport.SimpleTSocketManeger;
import com.github.winter4666.thriftplus.client.ttransport.TTransportManager;
import com.github.winter4666.thriftplus.client.ttransport.recycletsocket.RecycleTSocketManeger;
import com.github.winter4666.thriftplus.client.ttransport.recycletsocket.ThriftPoolConfig;
import com.github.winter4666.thriftplus.config.Registry;
import com.github.winter4666.thriftplus.config.Registry.ServerListListener;
import com.github.winter4666.thriftplus.config.ServerInfo;
import com.github.winter4666.thriftplus.config.ThriftClassUtil;

/**
 * thrift客户端工厂
 * @author wutian
 */
public class ThriftClientFactory<T> {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);
	
	private Registry registry;
	
	private Class<?> serviceClass;
	
	private int socketTimeout = 3000;
	
	private int connectTimeout = 3000;
	
	private Integer maxWorkerThreads;
	
	private ThriftPoolConfig poolConfig;
	
	private List<T> syncClientList = new ArrayList<>();
	
	private Map<String, T> syncClientMap = new HashMap<String, T>();
	
	private List<T> asyncClientList = new ArrayList<>();
	
	private Map<String, T> asyncClientMap = new HashMap<String, T>();
	
	/**
	 * 连接池是比较昂贵的对象，这里做了缓存
	 */
	private Map<String, RecycleTSocketManeger> recycleTSocketManegerTemp = new HashMap<>();
	
	public ThriftClientFactory() {
		
	}
	
	/**
	 * 设置注册中心
	 * @param registry
	 */
	public void setRegistry(Registry registry) {
		this.registry = registry;
	}
	
	/**
	 * 设置thrift生成的service类
	 * @param serviceClass
	 */
	public void setServiceClass(Class<?> serviceClass) {
		this.serviceClass = serviceClass;
	}
	
	/**
	 * 设置thrift连接socketTimeout，默认3000ms
	 * @param socketTimeout
	 * @see org.apache.thrift.transport.TSocket#setSocketTimeout(int)
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}
	
	/**
	 * 设置thrift连接connectTimeout，默认3000ms
	 * @param connectTimeout
	 * @see org.apache.thrift.transport.TSocket#setConnectTimeout(int)
	 */
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	
	/**
	 * 设置thrift异步调用时能使用的最大线程数，默认没有限制
	 * @param maxWorkerThreads
	 */
	public void setMaxWorkerThreads(Integer maxWorkerThreads) {
		this.maxWorkerThreads = maxWorkerThreads;
	}
	
	/**
	 * 设置连接池配置，默认不使用连接池
	 * @param poolConfig
	 */
	public void setPoolConfig(ThriftPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	@SuppressWarnings("unchecked")
	public void init() {
		ExecutorService executorService;
		if(maxWorkerThreads != null && maxWorkerThreads > 0) {
			executorService = Executors.newFixedThreadPool(maxWorkerThreads);
		} else {
			executorService = Executors.newCachedThreadPool();
		}
		
		registry.getServers(serviceClass, new ServerListListener() {
			
			@Override
			public void onServerListChanged(List<ServerInfo> serverInfos) {
				synchronized(ThriftClientFactory.this) {
					logger.info("load clients,serviceClass={}",serviceClass.getSimpleName());
					List<T> syncClientList = new ArrayList<>();
					Map<String, T> syncClientMap = new HashMap<String, T>();
					List<T> asyncClientList = new ArrayList<>();
					Map<String, T> asyncClientMap = new HashMap<String, T>();
					
					Class<T> iface = (Class<T>)ThriftClassUtil.getIface(serviceClass); 
					for(ServerInfo serverInfo : serverInfos) {
						//创建TTransportManager，判定是否需要使用连接池
						TTransportManager manager = null;
						if(poolConfig == null) {
							manager = new SimpleTSocketManeger(serverInfo.getHost(), serverInfo.getPort(), socketTimeout, connectTimeout);
						} else {
							manager = getRecycleTSocketManeger(serverInfo.getHost(), serverInfo.getPort(), socketTimeout, connectTimeout,poolConfig);
						}
						
						T syncClient = (T)Proxy.newProxyInstance(iface.getClassLoader(),
							new Class[] {iface},new ThriftInvocationHandler(ThriftClassUtil.getClientClass(serviceClass), manager,  null));
						syncClientList.add(syncClient);
						if(serverInfo.getId() != null) {
							syncClientMap.put(serverInfo.getId(), syncClient);
						}
						
						T asyncClient = (T)Proxy.newProxyInstance(iface.getClassLoader(),
								new Class[] {iface},new ThriftInvocationHandler(ThriftClassUtil.getClientClass(serviceClass), manager,  executorService));
						asyncClientList.add(asyncClient);
						if(serverInfo.getId() != null) {
							asyncClientMap.put(serverInfo.getId(), asyncClient);
						}
					}
					ThriftClientFactory.this.syncClientList = syncClientList;
					ThriftClientFactory.this.syncClientMap = syncClientMap;
					ThriftClientFactory.this.asyncClientList = asyncClientList;
					ThriftClientFactory.this.asyncClientMap = asyncClientMap;
					logger.info("clients load finished,iface={},socketTimeout={},connectTimeout={},maxWorkerThreads={},poolConfig={}", 
						iface.getName(),socketTimeout,connectTimeout,maxWorkerThreads,poolConfig);
				}
				
			}
		});
	}
	
	private RecycleTSocketManeger getRecycleTSocketManeger(String host,int port, int socketTimeout, int connectTimeout, ThriftPoolConfig poolConfig) {
		String key = host + ":" + port;
		RecycleTSocketManeger recycleTSocketManeger = recycleTSocketManegerTemp.get(key);
		if(recycleTSocketManeger == null) {
			recycleTSocketManeger = new RecycleTSocketManeger(host, port, port, port, poolConfig);
			recycleTSocketManegerTemp.put(key, recycleTSocketManeger);
		}
		return recycleTSocketManeger;
	}
	
	/**
	 * 得到同步rpc调用客户端
	 * @param serverId 服务提供方id
	 * @return
	 */
	public T getSyncClient(String serverId) {
		T syncClient = syncClientMap.get(serverId);
		if(syncClient == null) {
			throw new RuntimeException("client with serverId " + serverId +" not found");
		}
		return syncClient;
	}
	
	/**
	 * 得到异步rpc调用客户端
	 * @param serverId 服务提供方id
	 * @return
	 */
	public T getAsyncClient(String serverId) {
		T asyncClient = asyncClientMap.get(serverId);
		if(asyncClient == null) {
			throw new RuntimeException("client with serverId " + serverId +" not found");
		}
		return asyncClient;
	}
	
	/**
	 * 得到同步rpc调用客户端
	 * @return
	 */
	public T getSyncClient() {
		if(syncClientList.size() <= 0) {
			throw new RuntimeException("no avaliable client");
		} else if(syncClientList.size() <= 1) {
			return syncClientList.get(0);
		} else {
			return syncClientList.get(new Random().nextInt(syncClientList.size()));
		}
	}
	
	/**
	 * 得到异步rpc调用客户端
	 * @return
	 */
	public T getAsyncClient() {
		if(asyncClientList.size() <= 0) {
			throw new RuntimeException("no avaliable client");
		} else if(asyncClientList.size() <= 1) {
			return asyncClientList.get(0);
		} else {
			return asyncClientList.get(new Random().nextInt(asyncClientList.size()));
		}
	}
	
	public void close() {
		for(RecycleTSocketManeger recycleTSocketManeger : recycleTSocketManegerTemp.values()) {
			recycleTSocketManeger.close();
		}
	}
	

}
