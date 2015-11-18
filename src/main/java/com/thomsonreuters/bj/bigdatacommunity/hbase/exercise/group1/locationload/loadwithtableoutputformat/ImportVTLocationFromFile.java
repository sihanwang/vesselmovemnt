package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.loadwithtableoutputformat;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop com.thomsonreuters.ce.big.vessel.injection.data.ImportVTLocationFromFile hackathoninput

public class ImportVTLocationFromFile extends Configured implements Tool {

	public enum Counters { LINES }

	static class ImportMapper
	extends Mapper<Key_ShipIDAndRecordTime, TextArrayWritable, ImmutableBytesWritable, Mutation> 
	{ // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

		
	    private byte[] details = Bytes.toBytes("details");
	    private byte[] speed = Bytes.toBytes("speed");
	    private byte[] destination = Bytes.toBytes("destination");
	    private byte[] timestamp = Bytes.toBytes("timestamp");
	    private byte[] latitude = Bytes.toBytes("lat");
	    private byte[] longitude = Bytes.toBytes("long");
		
		
		// ^^ ImportFromFile
		/**
		 * Maps the input.
		 *
		 * @param offset The current offset into the input file.
		 * @param line The current line of the file.
		 * @param context The task context.
		 * @throws IOException When mapping the input fails.
		 */
		// vv ImportFromFile
		@Override
		public void map(Key_ShipIDAndRecordTime key, TextArrayWritable rowcontent, Context context) // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
				throws IOException {
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
				
				context.write(new ImmutableBytesWritable(rowkey), put);
				context.getCounter(Counters.LINES).increment(1);
				
				
			} catch (Exception e) {
				e.printStackTrace();
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
		job.setJarByClass(ImportVTLocationFromFile.class);
		job.setJobName("Vessel_location_injection");
		job.setInputFormatClass(VTVesselLocationFileInputFormat.class);
		job.setMapOutputKeyClass(Key_ShipIDAndRecordTime.class);
		job.setMapOutputValueClass(TextArrayWritable.class);	    	    
		job.setMapperClass(ImportMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "cdb_vessel:vessel_location");
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
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
			
			int exitCode = ToolRunner.run(new ImportVTLocationFromFile(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	


}
