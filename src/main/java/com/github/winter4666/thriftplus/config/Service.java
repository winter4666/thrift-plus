package com.github.winter4666.thriftplus.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TServiceClient;

/**
 * 对应xml配置文件中的service节点
 * @author wutian
 */
public class Service {
	
	private Class<?> serviceClass;
	
	private Class<? extends TServiceClient> clientClass;
	
	private Class<? extends TBaseProcessor<?>> processorClass;
	
	private List<Provider> providers = new ArrayList<>();
	
	private Map<String, Provider> providerMap = new HashMap<>();
	
	public Class<?> getServiceClass() {
		return serviceClass;
	}

	public void setServiceClass(Class<?> serviceClass) {
		this.serviceClass = serviceClass;
	}
	
	public Class<? extends TServiceClient> getClientClass() {
		return clientClass;
	}

	public void setClientClass(Class<? extends TServiceClient> clientClass) {
		this.clientClass = clientClass;
	}

	public Class<? extends TBaseProcessor<?>> getProcessorClass() {
		return processorClass;
	}

	public void setProcessorClass(Class<? extends TBaseProcessor<?>> processorClass) {
		this.processorClass = processorClass;
	}

	public List<Provider> getProviders() {
		return providers;
	}
	
	public Provider getProvider(String id) {
		return providerMap.get(id);
	}
	
	public Provider getProvider() {
		if(providers.size() == 1) {
			return providers.get(0);
		} else {
			throw new RuntimeException("get provider failed,providerSize=" + providers.size());
		}
	}
	
	public Provider getRandomProvider() {
		Random random = new Random();
		return providers.get(random.nextInt(providers.size()));
	}
	
	public void addProvider(String id,String host,int port) {
		Provider provider = new Provider();
		provider.setId(id);
		provider.setHost(host);
		provider.setPort(port);
		providers.add(provider);
		providerMap.put(id, provider);
	}

	public static class Provider {
		
		private String id;
		
		private String host;
		
		private int port;
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}
		
	}

}
