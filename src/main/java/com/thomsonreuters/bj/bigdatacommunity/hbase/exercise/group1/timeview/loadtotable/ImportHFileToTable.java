package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.loadtotable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.NativeCodeLoader;

//java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar -Djava.library.path=/opt/cloudera/parcels/CDH-5.4.0-1.cdh5.4.0.p0.27/lib/hadoop/lib/native  com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.loadtotable.ImportHFileToTable /etc/hbase/conf/hbase-site.xml
public class ImportHFileToTable {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        
//        HTable table = new HTable(conf, "cdb_vessel:vessel_polygon");
//        String str_outPath = "8006234/output/";  
        HTable table = new HTable(conf,"cdb_vessel:time_view");
        String str_outPath = "8003662/timeview";
   
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);  
        
        loader.doBulkLoad(new Path(str_outPath),table);  
	}

}
