package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type.*;

//hadoop jar VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar -conf /etc/hadoop/conf/mapred-site.xml 8003662/vessellocation

public class ImportVTLocationFromFileWithReducer extends Configured implements
		Tool {
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	
	public enum Counters {
		LOCATION_ROWS, LOCATION_ERROR, LOCATION_VALID, EVENT_ROWS, EVENT_ERROR, EVENT_VALID
	}

	static class ImportReducer
			extends
			Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable> {
		
		private Connection connection = null;
		private BufferedMutator VTLocation = null;
		private BufferedMutator VTEvent = null;
		private Table VTLocation_Table=null;
		private Table VTEvent_Table=null;
		private byte[] details = Bytes.toBytes("details");
		private byte[] speed = Bytes.toBytes("speed");
		private byte[] destination = Bytes.toBytes("destination");
		private byte[] timestamp = Bytes.toBytes("timestamp");
		private byte[] coordinates = Bytes.toBytes("coordinates");

		@Override
		protected void cleanup(
				Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			VTLocation.flush();
			VTEvent.flush();
			
			connection.close();
		}

		@Override
		protected void setup(
				Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			connection = ConnectionFactory.createConnection(context
					.getConfiguration());
			
			TableName VtLocation_Name=TableName.valueOf("cdb_vessel:vessel_location");
			VTLocation = connection.getBufferedMutator(VtLocation_Name);
			VTLocation_Table = connection.getTable(VtLocation_Name);
			
			TableName VtEvent_Name=TableName.valueOf("cdb_vessel:vessel_event");
			VTEvent = connection.getBufferedMutator(VtEvent_Name);
			VTEvent_Table = connection.getTable(VtEvent_Name);

		}

		@Override
		protected void reduce(Key_ShipIDAndRecordTime key,
				Iterable<TextArrayWritable> LocationList, Context context)
				throws IOException, InterruptedException {
			
			String shipid_str = padNum(key.getShipID().get(),10);
			VLongWritable pos_time = key.getRecordTime();
			
			
			
			/*
			
			////////////////////////////////////////////////////////////////////////////////
			//calculate events
			
			//Retrieve all the existing locations after the first new location.
			List<VesselLocation> AllAfterPoints = getLocationsAfter(VTLocation_Table,shipid_str, pos_time.get());
			
			
			
			//Find out the last location before the first new location in database
			
			
			if (AllAfterPoints.size()>0)
			{
				//remove all events start after the first new location.
				deleteEventsStartAfter(VTLocation_Table,shipid_str, pos_time.get());
				
				
				
			}
			
		    
		    
		    */
		    
		    
		    
			
			for (TextArrayWritable rowcontent : LocationList )
			{
				//population location
				context.getCounter(Counters.LOCATION_ROWS).increment(1);
				try {

					Writable[] content =  rowcontent.get();
					String Latitude = content[16].toString();
					String Longitude = content[15].toString();
					String Speed = content[18].toString();
					String Destination = content[9].toString();
					String Timestamp = content[21].toString();
					ParsePosition pos = new ParsePosition(0);
					long record_time=formatter.parse(Timestamp, pos).getTime();

					byte[] rowkey = Bytes.toBytes(shipid_str
							+ padNum(Long.MAX_VALUE-record_time, 19));
					Put put = new Put(rowkey);

					put.addColumn(details, speed, Bytes.toBytes(Speed));
					put.addColumn(details, destination, Bytes.toBytes(Destination));
					put.addColumn(details, coordinates, Bytes.toBytes(Latitude+","+Longitude));
					put.addColumn(details, timestamp, Bytes.toBytes(Timestamp));

					VTLocation.mutate(put);
					context.getCounter(Counters.LOCATION_VALID).increment(1);

				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.LOCATION_ERROR).increment(1);
				}
				
			}
		}
		
		public static List<VesselLocation> getLocationsAfter(Table VTLocation_Table,String shipid_str, long timestamp) throws IOException
		{
			
			//scan 'cdb_vessel:vessel_location',{FILTER=>"(PrefixFilter('0000003162')"}
			Scan GetExistingLocations = new Scan();
			GetExistingLocations.setStartRow(Bytes.toBytes(shipid_str + padNum(0, 19))).setStopRow(Bytes.toBytes(shipid_str + padNum(Long.MAX_VALUE-timestamp, 19)));
			
			ResultScanner Result_ExistingLocations = VTLocation_Table.getScanner(GetExistingLocations);
			List<VesselLocation> result=new ArrayList<VesselLocation>();
			
			for (Result res : Result_ExistingLocations) {
				VesselLocation VL = new VesselLocation();
				
			      for (Cell cell : res.rawCells()) {
			    	  String Qualifier=Bytes.toString(CellUtil.cloneQualifier(cell));
			    	  String Value=Bytes.toString(CellUtil.cloneValue(cell));
			    	  if (Qualifier.equals("lat"))
			    	  {
			    		  VL.latitude=Double.parseDouble(Value);
			    	  }
			    	  else if (Qualifier.equals("long"))
			    	  {
			    		  VL.longitude=Double.parseDouble(Value);
			    	  }
			    	  else if (Qualifier.equals("speed"))
			    	  {
			    		  VL.speed=Double.parseDouble(Value);
			    	  }
			    	  else if (Qualifier.equals("destination"))
			    	  {
			    		  VL.destination=Value;
			    	  }
			    	  else if (Qualifier.equals("timestamp"))
			    	  {
			    		  ParsePosition pos = new ParsePosition(0);
			    		  VL.recordtime=formatter.parse(Value, pos).getTime();
			    	  }
			        }
			      result.add(VL);
		    }
		    
		    Result_ExistingLocations.close();
		    
		    return result;
		    
		}
		
		public static void deleteEventsStartAfter(Table VTEvent_Table,String shipid_str, long timestamp) throws IOException
		{
			
			//scan 'cdb_vessel:vessel_event',{FILTER=>"(PrefixFilter('0000003162')"}			
			Scan GetEventsStartAfter = new Scan();
			GetEventsStartAfter.setStartRow(Bytes.toBytes(shipid_str + padNum(0, 19))).setStopRow(Bytes.toBytes(shipid_str + padNum(Long.MAX_VALUE-timestamp, 19)));
			GetEventsStartAfter.setFilter(new KeyOnlyFilter());
			
			ResultScanner Result_ExistingEvents = VTEvent_Table.getScanner(GetEventsStartAfter);
			List<Delete> deletes = new ArrayList<Delete>();
			
			for (Result res : Result_ExistingEvents) {
				deletes.add(new Delete(res.getRow()));
			}
			
			Result_ExistingEvents.close();
			
			VTEvent_Table.delete(deletes);
		}
		
		
		public static VesselLocation getLocationBefore(Table VTLocation_Table,String shipid_str, long timestamp) throws IOException
		{
			
			Scan getLocationBefore = new Scan();
			getLocationBefore.setStartRow(Bytes.toBytes(shipid_str + padNum(Long.MAX_VALUE-timestamp+1, 19)));
			return null;
			
			
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
	
	/*
	public static void main(String[] args) throws Exception {

		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));


	    Connection connection = ConnectionFactory.createConnection(conf);
	    Table table = connection.getTable(TableName.valueOf("cdb_vessel", "vessel_location"));

	    ImportReducer.deleteEventsStartAfter(table, padNum(3162,10), formatter.parse("2014-01-18T00:05:24Z", new ParsePosition(0)).getTime());
	    
	    connection.close();

	}	*/

	
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
