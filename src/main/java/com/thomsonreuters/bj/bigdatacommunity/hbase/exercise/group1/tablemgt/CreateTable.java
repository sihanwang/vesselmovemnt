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
	    
	    ////////////////////////////////////////////////////
	    //create vessel location table
	    //rowkey: shipid(10)+timestamp(19 desc)

	    TableName tableName_location = TableName.valueOf("cdb_vessel", "vessel_location");
	    HTableDescriptor desc_location = new HTableDescriptor(tableName_location);

	    HColumnDescriptor coldef_location = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    coldef_location.setMaxVersions(1);
	    desc_location.addFamily(coldef_location);

	    admin.createTable(desc_location,Bytes.toBytes("00000000000000000000000000000"),Bytes.toBytes("99999999999999999999999999999"),7);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_location = admin.isTableAvailable(tableName_location);
	    System.out.println("The availability of table vessel_location: " + avail_location);
	    
	    
	    ////////////////////////////////////////////////////	    
	    //create vessel event table
	    //rowkey: shipid(10)+timestamp(19 desc)+polygonid(10)
	    //qualifier:entertime,entercoordinates,exittime,exitcoordinates,destination

	    TableName tableName_event = TableName.valueOf("cdb_vessel", "vessel_event");
	    HTableDescriptor desc_event = new HTableDescriptor(tableName_event);

	    HColumnDescriptor coldef_event = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    coldef_event.setMaxVersions(1);
	    desc_event.addFamily(coldef_event);

	    admin.createTable(desc_event,Bytes.toBytes("000000000000000000000000000000000000000"),Bytes.toBytes("999999999999999999999999999999999999999"),7);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_event = admin.isTableAvailable(tableName_event);
	    System.out.println("The availability of table vessel_event: " + avail_event);
	    
	  }


}
