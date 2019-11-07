package com.github.winter4666.thriftplus.client.ttransport;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 每次调用getTTransport都会新建一条tcp连接，closeTTransport时关闭该连接 
 * @author wutian
 */
public class SimpleTSocketManeger implements TTransportManager {
	
	private String host;
	
	private int port;
	
	private int socketTimeout;
	
	private int connectTimeout;

	public SimpleTSocketManeger(String host, int port, int socketTimeout, int connectTimeout) {
		super();
		this.host = host;
		this.port = port;
		this.socketTimeout = socketTimeout;
		this.connectTimeout = connectTimeout;
	}

	@Override
	public TTransport getTTransport() {
		try {
			TTransport transport = new TSocket(host, port, socketTimeout, connectTimeout);
			transport.open();
			return transport;
		} catch (TTransportException e) {
			throw new RuntimeException("getTTransport error,host="+host+",port="+port , e);
		}
	}

	@Override
	public void closeTTransport(TTransport transport) {
		if (transport != null) {
			transport.close();
		}		
	}

	@Override
	public void destroyTTransport(TTransport transport) {
		
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
