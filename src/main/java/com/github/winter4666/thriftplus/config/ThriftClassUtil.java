package com.github.winter4666.thriftplus.config;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TServiceClient;

public class ThriftClassUtil {
	
	@SuppressWarnings("unchecked")
	public static Class<? extends TServiceClient> getClientClass(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(TServiceClient.class.isAssignableFrom(childClass)) {
    			return (Class<? extends TServiceClient>)childClass;
    		}
    	}
    	throw new RuntimeException("client class not found,serviceClass=" + serviceClass.getName());
    }
	
	@SuppressWarnings("unchecked")
	public static Class<? extends TBaseProcessor<?>> getProcessorClass(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(TBaseProcessor.class.isAssignableFrom(childClass)) {
    			return (Class<? extends TBaseProcessor<?>>)childClass;
    		}
    	}
    	throw new RuntimeException("processor class not found,serviceClass=" + serviceClass.getName());
	}
	
	public static Class<?> getIface(Class<?> serviceClass) {
    	for(Class<?> childClass : serviceClass.getClasses()) {
    		if(childClass.getSimpleName().equals("Iface")) {
    			return childClass;
    		}
    	}
    	throw new RuntimeException("Iface not found,serviceClass=" + serviceClass.getName());
    }

}
