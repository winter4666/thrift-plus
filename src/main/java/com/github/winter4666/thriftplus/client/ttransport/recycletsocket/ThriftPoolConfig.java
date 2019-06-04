package com.github.winter4666.thriftplus.client.ttransport.recycletsocket;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TSocket;

/**
 * thrift客户端连接池配置
 * @author wutian
 */
public class ThriftPoolConfig extends GenericObjectPoolConfig<TSocket> {
	
	public ThriftPoolConfig() {
		//设置默认参数
		setTestOnCreate(true);
		setTestOnBorrow(true);
		setTestOnReturn(true);
		setMaxWaitMillis(6000);
	}

}
