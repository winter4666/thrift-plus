package com.github.winter4666.thriftplus.client.ttransport.recycletsocket;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSocketPoolObjectFactory extends BasePooledObjectFactory<TSocket> {
	
	private static Logger logger = LoggerFactory.getLogger(TSocketPoolObjectFactory.class);
	
	private String host;
	
	private int port;
	
	private int socketTimeout;
	
	private int connectTimeout;

	public TSocketPoolObjectFactory(String host, int port, int socketTimeout, int connectTimeout) {
		super();
		this.host = host;
		this.port = port;
		this.socketTimeout = socketTimeout;
		this.connectTimeout = connectTimeout;
	}

	@Override
	public void destroyObject(PooledObject<TSocket> p) throws Exception {
		TSocket socket = p.getObject();
		if(socket.isOpen()) {
			socket.close();
		}
	}

	@Override
	public boolean validateObject(PooledObject<TSocket> p) {
		return p.getObject().isOpen();
	}

	@Override
	public TSocket create() throws Exception {
		try {
			logger.warn("create TSocket,host={},port={},socketTimeout={},connectTimeout={}",
				host, port, socketTimeout, connectTimeout);
			TSocket socket = new TSocket(host, port, socketTimeout, connectTimeout);
			socket.open();
			return socket;
		} catch(Throwable t) {
			throw new RuntimeException("create TSocket failed",t);
		}
	}

	@Override
	public PooledObject<TSocket> wrap(TSocket socket) {
		return new DefaultPooledObject<TSocket>(socket);
	}

}
