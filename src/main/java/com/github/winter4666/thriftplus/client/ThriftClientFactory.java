package com.github.winter4666.thriftplus.client;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.client.ttransport.SimpleTSocketManeger;
import com.github.winter4666.thriftplus.client.ttransport.TTransportManager;
import com.github.winter4666.thriftplus.client.ttransport.recycletsocket.RecycleTSocketManeger;
import com.github.winter4666.thriftplus.client.ttransport.recycletsocket.ThriftPoolConfig;
import com.github.winter4666.thriftplus.config.RpcConfig;
import com.github.winter4666.thriftplus.config.Service;
import com.github.winter4666.thriftplus.config.Service.Provider;

/**
 * thrift客户端工厂
 * @author wutian
 */
public class ThriftClientFactory<T> {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);
	
	private RpcConfig rpcConfig;
	
	private Service service;
	
	private Class<T> iface;
	
	private int socketTimeout = 3000;
	
	private int connectTimeout = 3000;
	
	private Integer maxWorkerThreads;
	
	private ThriftPoolConfig poolConfig;
	
	private Map<String, T> syncClients = new HashMap<String, T>();
	
	private Map<String, T> asyncClients = new HashMap<String, T>();
	
	public ThriftClientFactory() {
		
	}

	public void setRpcConfig(RpcConfig rpcConfig) {
		this.rpcConfig = rpcConfig;
	}
	
	public void setIface(Class<T> iface) {
		this.iface = iface;
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
		
		service = rpcConfig.getService(iface);
		List<Provider> providers = service.getProviders();
		for(Provider provider : providers) {
			
			//创建TTransportManager，判定是否需要使用连接池
			TTransportManager manager = null;
			if(poolConfig == null) {
				manager = new SimpleTSocketManeger(provider.getHost(), provider.getPort(), socketTimeout, connectTimeout);
			} else {
				manager = new RecycleTSocketManeger(provider.getHost(), provider.getPort(), socketTimeout, connectTimeout,poolConfig);
			}
			
			T syncClient = (T)Proxy.newProxyInstance(iface.getClassLoader(),
				new Class[] {iface},new ThriftInvocationHandler(service.getClientClass(), manager,  null));
			syncClients.put(provider.getId(), syncClient);
			
			T asyncClient = (T)Proxy.newProxyInstance(iface.getClassLoader(),
					new Class[] {iface},new ThriftInvocationHandler(service.getClientClass(), manager,  executorService));
			asyncClients.put(provider.getId(), asyncClient);
		}
		logger.info("rpc client factory load finished,iface={},socketTimeout={},connectTimeout={},maxWorkerThreads={},poolConfig={}", 
			iface.getName(),socketTimeout,connectTimeout,maxWorkerThreads,poolConfig);
	}
	
	/**
	 * 得到同步rpc调用客户端
	 * @param providerId 服务提供方id
	 * @return
	 */
	public T getSyncClient(String providerId) {
		return syncClients.get(providerId);
	}
	
	/**
	 * 得到异步rpc调用客户端
	 * @param providerId 服务提供方id
	 * @return
	 */
	public T getAsyncClient(String providerId) {
		return asyncClients.get(providerId);
	}
	
	/**
	 * 得到同步rpc调用客户端
	 * @return
	 */
	public T getSyncClient() {
		if(syncClients.size() == 1) {
			return syncClients.get(service.getProvider().getId());
		} else {
			return syncClients.get(service.getRandomProvider().getId());
		}
	}
	
	/**
	 * 得到异步rpc调用客户端
	 * @return
	 */
	public T getAsyncClient() {
		if(asyncClients.size() == 1) {
			return asyncClients.get(service.getProvider().getId());
		} else {
			return asyncClients.get(service.getRandomProvider().getId());
		}
	}
	

}
