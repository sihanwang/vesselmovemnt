package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.loadtotable;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer; 
import org.apache.hadoop.hbase.util.Bytes;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.ConvertToTimeView;
import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.Key_region_hour_imo;
import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.VesselLocationList;
import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.VesselPoint;

//java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.loadtotable.LoadToTable -conf /etc/hbase/conf/hbase-site.xml 8003662/timeview1 /hbase/data/cdb_vessel/time_view

public class LoadToTable extends Configured implements Tool {
	private static byte[] details = Bytes.toBytes("details");
	private static byte[] locations = Bytes.toBytes("locations");
	
	static class TimeViewMapper extends Mapper<Key_region_hour_imo,VesselLocationList,ImmutableBytesWritable,KeyValue>
	{

		@Override
		protected void map(
				Key_region_hour_imo key,
				VesselLocationList value,
				Mapper<Key_region_hour_imo, VesselLocationList, ImmutableBytesWritable, KeyValue>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			//rowkey: region(6)+time(10)+imo(7)
			int RegionNumber=key.getRegion().get();
			String Hour=key.getHour().toString();
			String IMO=key.getIMO().toString();
			
			byte[] rowkey = Bytes.toBytes(LpadNum(RegionNumber,6)+Hour+IMO);		
			
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowkey);
			
			KeyValue kvCoordinates = new KeyValue(rowkey , details, locations, Bytes.toBytes(value.toString()));    

			context.write(rowKey, kvCoordinates);
			
			
		}
		
		public static String LpadNum(long num, int pad) {
			String res = Long.toString(num);
			if (pad > 0) {
				while (res.length() < pad) {
					res = "0" + res;
				}
			}
			return res;
		}		
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		String InputPutPath = arg0[0];
		String OutputPath = arg0[1];

		Job job = Job.getInstance(getConf(), "Load timeview file into table");
		job.setJarByClass(LoadToTable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(KeyValue.class);
        job.setMapperClass(TimeViewMapper.class);    
        job.setReducerClass(KeyValueSortReducer.class); 
		HTable table = new HTable(getConf(), "cdb_vessel:time_view");
		HFileOutputFormat.configureIncrementalLoad(job, table);

		FileInputFormat.addInputPath(job, new Path(InputPutPath));
		FileOutputFormat.setOutputPath(job, new Path(OutputPath));
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		try {

			int exitCode = ToolRunner.run(new LoadToTable(), args);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
