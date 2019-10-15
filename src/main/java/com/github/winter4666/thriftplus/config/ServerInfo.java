package com.github.winter4666.thriftplus.config;

/**
 * 服务节点信息
 * @author wutian
 */
public class ServerInfo {
	
	private String host;
	
	private int port;
	
	private String id;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
