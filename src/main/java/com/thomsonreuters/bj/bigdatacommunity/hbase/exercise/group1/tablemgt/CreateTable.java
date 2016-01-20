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
import org.apache.hadoop.hbase.io.compress.Compression;
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
	    //rowkey: imo(7)+timestamp(19 desc)

	    TableName tableName_location = TableName.valueOf("cdb_vessel", "vessel_location");
	    HTableDescriptor desc_location = new HTableDescriptor(tableName_location);

	    HColumnDescriptor coldef_location = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    coldef_location.setMaxVersions(1);
	    coldef_location.setCompressionType(Compression.Algorithm.SNAPPY);
	    desc_location.addFamily(coldef_location);

	    admin.createTable(desc_location);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_location = admin.isTableAvailable(tableName_location);
	    System.out.println("The availability of table vessel_location: " + avail_location);
	    
	    
	    ////////////////////////////////////////////////////	    
	    //create vessel event table
	    //rowkey: imo(7)+timestamp(19 desc)+polygonid(10)
	    //qualifier:entertime,entercoordinates,exittime,exitcoordinates,destination

	    TableName tableName_event = TableName.valueOf("cdb_vessel", "vessel_event");
	    HTableDescriptor desc_event = new HTableDescriptor(tableName_event);

	    HColumnDescriptor coldef_event = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    coldef_event.setMaxVersions(1);
	    coldef_event.setCompressionType(Compression.Algorithm.SNAPPY);
	    desc_event.addFamily(coldef_event);

	    admin.createTable(desc_event);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_event = admin.isTableAvailable(tableName_event);
	    System.out.println("The availability of table vessel_event: " + avail_event);
	    
	    

	    ////////////////////////////////////////////////////	    
	    //create latest location table
	    //rowkey: global zone id (4)+ longlat22 ((long11(sign(1)+integer(3)+digit(7)))(lat10(sign(1)+integer(3)+(7))))+imo(7)+recordtime(19)
	    //qualifier:entertime,entercoordinates,exittime,exitcoordinates,destination

	    
	    TableName tableName_latest_location = TableName.valueOf("cdb_vessel", "latest_location");
	    HTableDescriptor desc_latest_location = new HTableDescriptor(tableName_latest_location);

	    HColumnDescriptor coldef_latest_location = new HColumnDescriptor(
	  	      Bytes.toBytes("details"));
	    coldef_latest_location.setMaxVersions(1);
	    coldef_latest_location.setCompressionType(Compression.Algorithm.SNAPPY);
	    desc_latest_location.addFamily(coldef_latest_location);
	  	    
	    admin.createTable(desc_latest_location);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_latest_location = admin.isTableAvailable(tableName_latest_location);
	    System.out.println("The availability of table vessel_event: " + avail_latest_location);	    
	    
	    
	    ////////////////////////////////////////////////////	    
	    //create vessel track range table
	    //rowkey: imo(7)
	    //qualifier:firstrecordtime,lastrecordtime

	    TableName tableName_trackinfo = TableName.valueOf("cdb_vessel", "vessel_track_info");
	    HTableDescriptor desc_trackinfo = new HTableDescriptor(tableName_trackinfo);

	    HColumnDescriptor coldef_trackinfo = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    coldef_trackinfo.setMaxVersions(1);
	    coldef_trackinfo.setCompressionType(Compression.Algorithm.SNAPPY);
	    desc_trackinfo.addFamily(coldef_trackinfo);

	    admin.createTable(desc_trackinfo);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail_trackinfo = admin.isTableAvailable(tableName_trackinfo);
	    System.out.println("The availability of table vessel_track_info: " + avail_trackinfo);
	    
	    
	    
	    
	  }


}
