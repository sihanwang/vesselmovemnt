package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.tablemgt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.log4j.PropertyConfigurator;

public class CreateNamespace {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		PropertyConfigurator.configure("log4j.properties");
		
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
	    Admin admin = connection.getAdmin();

	    
	    // create namespace for cdb vessel solution
	    NamespaceDescriptor namespace = NamespaceDescriptor.create("cdb_vessel").build();
	    try {
			admin.createNamespace(namespace);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
