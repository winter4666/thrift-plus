package com.github.winter4666.thriftplus.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 读取rpc配置文件
 * @author wutian
 */
public class RpcConfig {
	
	private static Logger logger = LoggerFactory.getLogger(RpcConfig.class);
	
	private List<Service> services = new ArrayList<>();
	
	private Map<Class<?>,Service> serviceMap = new HashMap<>(); 
	
	public RpcConfig() {
		this("rpc.xml");
	}
	
	public RpcConfig(String configFile) {
		InputStream in = null;
		try {
			in = RpcConfig.class.getClassLoader().getResourceAsStream(configFile);
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = builderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(in);
			NodeList serviceNodes = document.getElementsByTagName("service");
			for(int i = 0;i < serviceNodes.getLength();i++) {
				Service service = new Service();
				Element serviceNode = (Element)serviceNodes.item(i);
				service.setServiceClass(Class.forName(serviceNode.getAttributes().getNamedItem("class").getNodeValue()));
				Element providersNode = (Element)serviceNode.getElementsByTagName("providers").item(0);
				NodeList providerNodes = providersNode.getElementsByTagName("provider");
				for(int j = 0;j < providerNodes.getLength();j++) {
					Node providerNode = providerNodes.item(j);
					NamedNodeMap attributes = providerNode.getAttributes();
					service.addProvider(attributes.getNamedItem("id") == null ? String.valueOf(j) : attributes.getNamedItem("id").getNodeValue(),
						attributes.getNamedItem("host").getNodeValue(), 
						Integer.valueOf(attributes.getNamedItem("port").getNodeValue()));
				}
				service.setClientClass(getClientClass(service.getServiceClass()));
				service.setProcessorClass(getProcessorClass(service.getServiceClass()));
				serviceMap.put(getIface(service.getServiceClass()), service);
				services.add(service);
			}
			logger.info("rpc config load successfully,configFile={},services={}",configFile, serviceMap.keySet());
		} catch (Throwable t) {
			throw new RuntimeException("rpc config load failed,configFile=" + configFile, t);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		}
	}
	
	public List<Service> getServices() {
		return services;
	}
	
	public Service getService(Class<?> iface) {
		return serviceMap.get(iface);
	}
	
    
	@SuppressWarnings("unchecked")
	private Class<? extends TServiceClient> getClientClass(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(TServiceClient.class.isAssignableFrom(childClass)) {
    			return (Class<? extends TServiceClient>)childClass;
    		}
    	}
    	throw new RuntimeException("client class not found,serviceClass=" + serviceClass.getName());
    }
	
	@SuppressWarnings("unchecked")
	private Class<? extends TBaseProcessor<?>> getProcessorClass(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(TBaseProcessor.class.isAssignableFrom(childClass)) {
    			return (Class<? extends TBaseProcessor<?>>)childClass;
    		}
    	}
    	throw new RuntimeException("processor class not found,serviceClass=" + serviceClass.getName());
	}
	
	private Class<?> getIface(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(childClass.getSimpleName().equals("Iface")) {
    			return childClass;
    		}
    	}
    	throw new RuntimeException("Iface not found,serviceClass=" + serviceClass.getName());
    }

}
