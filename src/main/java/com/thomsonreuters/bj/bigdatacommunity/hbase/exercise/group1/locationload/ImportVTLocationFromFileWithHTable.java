package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop jar /root/hadoop_lib/VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar -conf /etc/hadoop/conf/mapred-site.xml hackathoninput

public class ImportVTLocationFromFileWithHTable extends Configured implements Tool {

	public enum Counters { ROWS, COLS, ERROR, VALID }

	/**
	 * Implements the <code>Mapper</code> that reads the data and extracts the
	 * required information.
	 */
	// vv ParseJsonMulti
	static class ImportMapper
	extends Mapper<Key_ShipIDAndRecordTime, TextArrayWritable, ImmutableBytesWritable, Mutation> 
	{

		private Connection connection = null;
		private BufferedMutator VTLocation = null;
		private byte[] details = Bytes.toBytes("details");
		private byte[] speed = Bytes.toBytes("speed");
		private byte[] destination = Bytes.toBytes("destination");
		private byte[] timestamp = Bytes.toBytes("timestamp");
		private byte[] latitude = Bytes.toBytes("lat");
		private byte[] longitude = Bytes.toBytes("long");	    

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			connection = ConnectionFactory.createConnection(  context.getConfiguration());
			VTLocation = connection.getBufferedMutator(TableName.valueOf("cdb_vessel:vessel_location")); // co ParseJsonMulti-1-Setup Create and configure both target tables in the setup() method.

		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			VTLocation.flush();
		}

		// ^^ ParseJsonMulti
		/**
		 * Maps the input.
		 *
		 * @param row The row key.
		 * @param columns The columns of the row.
		 * @param context The task context.
		 * @throws java.io.IOException When mapping the input fails.
		 */
		// vv ParseJsonMulti
		@Override
		public void map(Key_ShipIDAndRecordTime key, TextArrayWritable rowcontent, Context context) // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
				throws IOException {

			context.getCounter(Counters.ROWS).increment(1);
			try {

				VLongWritable shipid=key.getShipID();
				VLongWritable pos_time=key.getRecordTime();
				byte[] rowkey =Bytes.toBytes(padNum(shipid.get(),7) + padNum(pos_time.get(),19));
				Put put = new Put(rowkey);

				Text[] content=(Text[])rowcontent.get();
				String Latitude=content[16].toString();
				String Longitude=content[15].toString();
				String Speed=content[18].toString();
				String Destination=content[9].toString();
				String Timestamp=content[21].toString();

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

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.out.println("please input file location");
			return -1;
		}
		// TODO Auto-generated method stub

		Job job = Job.getInstance(getConf(), "Import vessel locations from files in " + args[0] +
				" into table cdb_vessel:vessel_location" ); // co ImportFromFile-8-JobDef Define the job with the required classes.
		job.setJarByClass(ImportVTLocationFromFileWithHTable.class);
		job.setJobName("Vessel_location_injection");
		job.setInputFormatClass(VTVesselLocationFileInputFormat.class);
		job.setMapOutputKeyClass(Key_ShipIDAndRecordTime.class);
		job.setMapOutputValueClass(TextArrayWritable.class);	    	    
		job.setMapperClass(ImportMapper.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0); // co ImportFromFile-9-MapOnly This is a map only job, therefore tell the framework to bypass the reduce step.
		FileInputFormat.addInputPath(job, new Path(args[0]));

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

	public static void main(String[] args)
	{

		try {
			System.out.println(System.getProperty("java.library.path"));

			int exitCode = ToolRunner.run(new ImportVTLocationFromFileWithHTable(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
