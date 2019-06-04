package com.github.winter4666.thriftplus.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.client.ttransport.TTransportManager;

/**
 * 实现rpc方法调用
 * @author wutian
 */
class ThriftInvocationHandler implements InvocationHandler {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftInvocationHandler.class);
	
	private Class<? extends TServiceClient> clientClass;
	
	private TTransportManager manager;
	
	private ExecutorService executorService;

	public ThriftInvocationHandler(Class<? extends TServiceClient> clientClass, TTransportManager manager, ExecutorService executorService) {
		super();
		this.clientClass = clientClass;
		this.manager = manager;
		this.executorService = executorService;
	}
	
	private Object executeRpc(Object proxy, Method method, Object[] args) {
		TTransport transport = null;
		try {
			transport = manager.getTTransport();
			TProtocol protocol = new TBinaryProtocol(transport);
			Constructor<? extends TServiceClient> constructor = clientClass.getConstructor(TProtocol.class);
			TServiceClient serviceClient = constructor.newInstance(protocol);
			return method.invoke(serviceClient, args);
		} catch (InvocationTargetException e) {
			logger.error(e.getTargetException().getMessage(), e.getTargetException());
			manager.destroyTTransport(transport);
			throw new RuntimeException("invoke method failed,method=" + method.getName(),e);
		} catch (Throwable t) {
			manager.destroyTTransport(transport);
			throw new RuntimeException("invoke method failed,method=" + method.getName(),t);
		} finally {
			if (transport != null) {
				manager.closeTTransport(transport);
			}
		}
	}

	@Override
	public Object invoke(final Object proxy,final Method method,final Object[] args) throws Throwable {
		if(executorService == null) {
			return executeRpc(proxy, method, args);
		} else {
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						executeRpc(proxy, method, args);
					} catch (Throwable t) {
						logger.error(t.getMessage(),t);
					}
				}
			});
			return null;
		}
	}

}
