package com.github.winter4666.thriftplus.client.ttransport;

import org.apache.thrift.transport.TTransport;

public interface TTransportManager {
	
	TTransport getTTransport();
	
	void destroyTTransport(TTransport transport);
	
	void closeTTransport(TTransport transport);
	
}
