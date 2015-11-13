package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.tablemgt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;



public class CreateTable {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
	    Admin admin = connection.getAdmin();

	    TableName tableName = TableName.valueOf("cdb_vessel", "vessel_location");
	    HTableDescriptor desc = new HTableDescriptor(tableName);

	    HColumnDescriptor coldef = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    desc.addFamily(coldef);

	    admin.createTable(desc,Bytes.toBytes("00000000000000000000000000"),Bytes.toBytes("99999999999999999999999999"),8);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail = admin.isTableAvailable(tableName);
	    System.out.println("Table available: " + avail);
	    
	  }


}
