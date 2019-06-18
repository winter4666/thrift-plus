package com.github.winter4666.thriftplus.client.ttransport.recycletsocket;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.github.winter4666.thriftplus.client.ttransport.TTransportManager;

/**
 * 每次调用getTTransport都从连接池里拿一条tcp连接，closeTTransport时归还该连接 
 * @author wutian
 */
public class RecycleTSocketManeger implements TTransportManager {
	
	private GenericObjectPool<TSocket> pool;
	
	private String host;
	
	private int port;
	
	public RecycleTSocketManeger(String host, int port, int socketTimeout, int connectTimeout, ThriftPoolConfig poolConfig) {
		this.host = host;
		this.port = port;
		pool = new GenericObjectPool<>(new TSocketPoolObjectFactory(host, port, socketTimeout, connectTimeout),poolConfig);
	}


	@Override
	public TTransport getTTransport() {
		try {
			return pool.borrowObject();
		} catch (Exception e) {
			throw new RuntimeException("Could not get a TTransport from the pool", e);
		}
	}

	@Override
	public void closeTTransport(TTransport transport) {
		if(transport != null) {
			pool.returnObject((TSocket)transport);
		}
	}

	@Override
	public void destroyTTransport(TTransport transport) {
		if(transport != null) {
			transport.close();
		}
	}
	
	/**
	 * 关闭连接池
	 */
	public void close() {
		pool.close();
	}


	@Override
	public String getHost() {
		return host;
	}


	@Override
	public int getPort() {
		return port;
	}

}
