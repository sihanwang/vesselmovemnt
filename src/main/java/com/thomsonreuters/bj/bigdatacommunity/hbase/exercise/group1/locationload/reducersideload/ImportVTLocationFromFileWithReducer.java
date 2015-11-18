package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop jar /root/hadoop_lib/VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar hackathoninput

public class ImportVTLocationFromFileWithReducer extends Configured implements
		Tool {

	public enum Counters {
		ROWS, COLS, ERROR, VALID
	}

	static class ImportReducer
			extends
			Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable> {
		
		private Connection connection = null;
		private BufferedMutator VTLocation = null;
		private byte[] details = Bytes.toBytes("details");
		private byte[] speed = Bytes.toBytes("speed");
		private byte[] destination = Bytes.toBytes("destination");
		private byte[] timestamp = Bytes.toBytes("timestamp");
		private byte[] latitude = Bytes.toBytes("lat");
		private byte[] longitude = Bytes.toBytes("long");

		@Override
		protected void cleanup(
				Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			VTLocation.flush();
		}

		@Override
		protected void setup(
				Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			connection = ConnectionFactory.createConnection(context
					.getConfiguration());
			VTLocation = connection.getBufferedMutator(TableName
					.valueOf("cdb_vessel:vessel_location"));
		}

		@Override
		protected void reduce(Key_ShipIDAndRecordTime key,
				Iterable<TextArrayWritable> arg1, Context context)
				throws IOException, InterruptedException {
			
			VLongWritable shipid = key.getShipID();
			VLongWritable pos_time = key.getRecordTime();
			
			
			for (TextArrayWritable rowcontent : arg1 )
			{
				context.getCounter(Counters.ROWS).increment(1);
				try {


					byte[] rowkey = Bytes.toBytes(padNum(shipid.get(), 7)
							+ padNum(pos_time.get(), 19));
					Put put = new Put(rowkey);

					Writable[] content =  rowcontent.get();
					String Latitude = content[16].toString();
					String Longitude = content[15].toString();
					String Speed = content[18].toString();
					String Destination = content[9].toString();
					String Timestamp = content[21].toString();

					put.addColumn(details, speed, Bytes.toBytes(Speed));
					put.addColumn(details, destination, Bytes.toBytes(Destination));
					put.addColumn(details, latitude, Bytes.toBytes(Latitude));
					put.addColumn(details, longitude, Bytes.toBytes(Longitude));
					put.addColumn(details, timestamp, Bytes.toBytes(Timestamp));

					VTLocation.mutate(put);
					context.getCounter(Counters.VALID).increment(1);

				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ERROR).increment(1);
				}
			}
		}
	}

	
	
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.out.println("please input file location");
			return -1;
		}
		// TODO Auto-generated method stub

		Job job = Job.getInstance(getConf(),
				"Import vessel locations from files in " + args[0]
						+ " into table cdb_vessel:vessel_location"); // co

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setJarByClass(ImportVTLocationFromFileWithReducer.class);
		job.setJobName("Vessel_location_injection");
		job.setInputFormatClass(VTVesselLocationFileInputFormat.class);
		job.setMapOutputKeyClass(Key_ShipIDAndRecordTime.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		job.setPartitionerClass(Partitioner_ShipID.class);
		job.setGroupingComparatorClass(GroupComparator_ShipID.class);

		job.setReducerClass(ImportReducer.class);
		job.setNumReduceTasks(4);

		job.setOutputFormatClass(NullOutputFormat.class);
		


		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static String padNum(long num, int pad) {
		String res = Long.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}

	public static void main(String[] args) {

		try {
			System.out.println(System.getProperty("java.library.path"));

			int exitCode = ToolRunner.run(
					new ImportVTLocationFromFileWithReducer(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
