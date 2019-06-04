package com.github.winter4666.thriftplus.server;

import java.lang.reflect.Constructor;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.config.RpcConfig;
import com.github.winter4666.thriftplus.config.Service;

/**
 * rpc server
 * @author wutian
 */
public class ThriftServer {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);
	
	private RpcConfig rpcConfig;
	
	private String providerId;
	
	private Class<?> ifaceClass;
	
	private Object serviceObject;
	
	private int minWorker;
	
	private int maxWorker;
	
	private TThreadPoolServer server;

	public void setRpcConfig(RpcConfig rpcConfig) {
		this.rpcConfig = rpcConfig;
	}

	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	public void setIfaceClass(Class<?> ifaceClass) {
		this.ifaceClass = ifaceClass;
	}

	public void setServiceObject(Object serviceObject) {
		this.serviceObject = serviceObject;
	}
	
	public void setMinWorker(int minWorker) {
		this.minWorker = minWorker;
	}

	public void setMaxWorker(int maxWorker) {
		this.maxWorker = maxWorker;
	}
	
	public void init() {
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					if(ifaceClass == null) {
						ifaceClass = serviceObject.getClass().getInterfaces()[0];
					}
					Service service = rpcConfig.getService(ifaceClass);
					int port;
					if(providerId == null) {
						port = service.getProvider().getPort();
					} else {
						port = service.getProvider(providerId).getPort();
					}
					
					logger.info("start thrift server,serviceClass={},port={},minWorker={},maxWorker={}",
						serviceObject.getClass().getSimpleName(),port,minWorker,maxWorker);
					TServerTransport transport = new TServerSocket(port);
					TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(transport);
					Constructor<? extends TBaseProcessor<?>> constructor = service.getProcessorClass().getConstructor(ifaceClass);
					TBaseProcessor<?> processor = constructor.newInstance(serviceObject);
		            serverArgs.processor(processor);
		            serverArgs.minWorkerThreads(minWorker);
		            serverArgs.maxWorkerThreads(maxWorker);
		            server = new TThreadPoolServer(serverArgs);
		            server.serve();
				} catch (Throwable t) {
					throw new RuntimeException("start server failed",t);
				}
			}
		});
		thread.setDaemon(true);
		thread.start();
	}
	
	public void close() {
		logger.info("stop thrift server,serviceClass={}",serviceObject.getClass().getSimpleName());
		server.stop();
	}
	
	

}
