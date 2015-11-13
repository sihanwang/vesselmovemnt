package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.tablemgt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.PropertyConfigurator;

public class DropNamespace {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
	    Admin admin = connection.getAdmin();

	    	    // create namespace for cdb vessel solution
	    admin.deleteNamespace("cdb_vessel");
	}

}
