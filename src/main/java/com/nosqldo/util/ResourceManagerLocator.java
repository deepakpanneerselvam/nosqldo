package com.nosqldo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ResourceManagerLocator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static Configuration getHbaseConfig(){
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.default.name", "");
		conf.set("zookeeper.session.timeout", "");
		conf.set("hbase.zookeeper.peerport", "");
		conf.set("hbase.zookeeper.property.clientPort", "");
		return conf;
	}
	
}
