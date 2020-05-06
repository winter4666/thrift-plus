package com.github.winter4666.thriftplus.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.client.ttransport.TTransportManager;
import com.github.winter4666.thriftplus.config.ThriftClassUtil;

/**
 * 实现rpc方法调用
 * @author wutian
 */
class ThriftInvocationHandler implements InvocationHandler {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftInvocationHandler.class);
	
	private ThriftClientFactory<?> thriftClientFactory;
	
	private Class<? extends TServiceClient> clientClass;
	
	private TTransportManager manager;
	
	private ExecutorService executorService;
	
	private int unsuccessfulAttempts = 0;

	public ThriftInvocationHandler(ThriftClientFactory<?> thriftClientFactory, TTransportManager manager, ExecutorService executorService) {
		super();
		this.thriftClientFactory = thriftClientFactory;
		this.clientClass = ThriftClassUtil.getClientClass(thriftClientFactory.serviceClass);
		this.manager = manager;
		this.executorService = executorService;
	}
	
	private Object executeRpc(Object proxy, Method method, Object[] args) throws Throwable {
		TTransport transport = null;
		try {
			transport = manager.getTTransport();
			TProtocol protocol = new TBinaryProtocol(transport);
			Constructor<? extends TServiceClient> constructor = clientClass.getConstructor(TProtocol.class);
			TServiceClient serviceClient = constructor.newInstance(protocol);
			Object ret = method.invoke(serviceClient, args);
			unsuccessfulAttempts = 0;
			return ret;
		} catch (InvocationTargetException e) {
			manager.destroyTTransport(transport);
			if(e.getTargetException().getCause() instanceof SocketTimeoutException) {
				unsuccessfulAttempts++;
				if(unsuccessfulAttempts > thriftClientFactory.maxFails) {
					logger.warn("unsuccessful attempts exceed maxFails {},try to remove server,serviceClass={},host={},port={}",
						thriftClientFactory.maxFails,thriftClientFactory.serviceClass.getSimpleName(),manager.getHost(), manager.getPort());
					thriftClientFactory.registry.removeServer(thriftClientFactory.serviceClass, manager.getHost(), manager.getPort());
				}
			}
			throw e.getCause();
		} catch (Throwable t) {
			manager.destroyTTransport(transport);
			throw t;
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
