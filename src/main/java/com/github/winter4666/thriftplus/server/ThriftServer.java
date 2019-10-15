package com.github.winter4666.thriftplus.server;

import java.lang.reflect.Constructor;
import java.net.InetAddress;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.winter4666.thriftplus.config.Registry;
import com.github.winter4666.thriftplus.config.ThriftClassUtil;

/**
 * rpc server
 * @author wutian
 */
public class ThriftServer {
	
	private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);
	
	private Registry registry;
	
	private String ip;
	
	private int port;
	
	private Class<?> serviceClass;
	
	private Object serviceObject;
	
	private int minWorker;
	
	private int maxWorker;
	
	private String id;
	
	private boolean backup = false;
	
	private TThreadPoolServer server;
	
	/**
	 * 设置注册中心
	 * @param registry
	 */
	public void setRegistry(Registry registry) {
		this.registry = registry;
	}
	
	/**
	 * 设置服务绑定的ip，默认通过{@link java.net.InetAddress#getLocalHost()}取本机ip
	 * @param ip
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	/**
	 * 设置服务端口号
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}
	
	/**
	 * 设置服务的唯一id
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * 设置是否为备用的服务，默认false
	 * @param backup
	 * @return
	 */
	public void setBackup(boolean backup) {
		this.backup = backup;
	}

	/**
	 * 设置thrift生成的service类
	 * @param serviceClass
	 */
	public void setServiceClass(Class<?> serviceClass) {
		this.serviceClass = serviceClass;
	}
	
	/**
	 * 设置服务实现类对象
	 * @param serviceObject
	 */
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
					if(ip == null) {
						ip = InetAddress.getLocalHost().getHostAddress();
					}
					logger.info("start thrift server,serviceClass={},ip={},port={},id={},minWorker={},maxWorker={},backup={}",
						serviceClass.getSimpleName(),ip, port, id, minWorker,maxWorker,backup);
					registry.registerServer(serviceClass, ip, port, id ,backup);
					Class<?> ifaceClass = ThriftClassUtil.getIface(serviceClass);
					TServerTransport transport = new TServerSocket(port);
					TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(transport);
					Constructor<? extends TBaseProcessor<?>> constructor = ThriftClassUtil.getProcessorClass(serviceClass).getConstructor(ifaceClass);
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
		logger.info("stop thrift server,serviceClass={}",serviceClass.getSimpleName());
		registry.close();
		server.stop();
	}

}
