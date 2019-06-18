package com.github.winter4666.thriftplus.client.ttransport;

import org.apache.thrift.transport.TTransport;

public interface TTransportManager {
	
	String getHost();
	
	int getPort();
	
	TTransport getTTransport();
	
	void destroyTTransport(TTransport transport);
	
	void closeTTransport(TTransport transport);
	
}
