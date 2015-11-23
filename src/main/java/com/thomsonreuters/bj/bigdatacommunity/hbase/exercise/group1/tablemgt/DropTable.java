package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.tablemgt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.PropertyConfigurator;

public class DropTable {

	public static void main(String[] args) throws IOException,
			InterruptedException {
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create(); // co
															// PutExample-1-CreateConf
															// Create the
															// required
															// configuration.
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin(); // ^^ PutExample

		TableName tablename_location = TableName.valueOf("cdb_vessel",
				"vessel_location");

		if (admin.tableExists(tablename_location)) {
			admin.disableTable(tablename_location);
			admin.deleteTable(tablename_location);
		}

		TableName tablename_event = TableName.valueOf("cdb_vessel",
				"vessel_event");

		if (admin.tableExists(tablename_event)) {
			admin.disableTable(tablename_event);
			admin.deleteTable(tablename_event);
		}

		connection.close();
	}

}
