package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;




//java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview.ConvertToTimeView -conf /etc/hbase/conf/hbase-site.xml 2016-02-02T01:00:00 2016-02-02T02:00:00 8003662/timeview 
public class ConvertToTimeView extends Configured implements Tool {
	
	private enum Counters { ROWS, VESSEL_HOURS, ERROR, VESSEL_WITHOUTTYPE }
	
	private static DateTimeFormatter rawformatter =  DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC();
	private static DateTimeFormatter hourformatter=DateTimeFormat.forPattern("yyyyMMddHH").withZoneUTC();
	
	private static byte[] TYPE = Bytes.toBytes("TYPE");
	private static byte[] ves = Bytes.toBytes("ves");	
	private static byte[] details = Bytes.toBytes("details");
	private static byte[] timestamp = Bytes.toBytes("timestamp");
	private static byte[] locations = Bytes.toBytes("locations");
	
	
	static class VTLocationMapper extends TableMapper<Key_region_hour_imo,VesselPoint>
	{

		@Override
		protected void map(
				ImmutableBytesWritable row,
				Result columns,
				Mapper<ImmutableBytesWritable, Result, Key_region_hour_imo, VesselPoint>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.getCounter(Counters.ROWS).increment(1);

			try {
				
				VesselPoint VP=new VesselPoint();				
				
				for (Cell cell : columns.listCells()) {					
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VP.setCoordinates(Value);
					} else if (Qualifier.equals("speed")) {
						VP.setSpeed(Value);
					} else if (Qualifier.equals("destination")) {
						VP.setDestination(Value);
					} else if (Qualifier.equals("timestamp")) {
						VP.setTimestamp(Value);
					} else if (Qualifier.equals("previouslocation")) {
						VP.setPreviouslocation(Value);
					} else if (Qualifier.equals("nextlocation")) {
						VP.setNextlocation(Value);
					}
				}
				
				String IMO=Bytes.toString(row.get()).substring(0, 7);
				String strTimestamp=DateTime.parse(VP.getTimestamp(), rawformatter).toString(hourformatter);
				int regionnumber=CalculateRegionNumber(VP.getCoordinates());
				
				context.write(new Key_region_hour_imo(regionnumber,strTimestamp,IMO), VP);
							
			} catch (Exception e) {
				e.printStackTrace();

				context.getCounter(Counters.ERROR).increment(1);
			}			
		}
		
		private static int CalculateRegionNumber(String coordinates)
		{
			String[] longlat=coordinates.split(",");
			double Longitude=Double.parseDouble(longlat[1]);
			double Latitude=Double.parseDouble(longlat[0]);
			
			int xindex=(int)Math.floor((Longitude+180)*2);
			int yindex=(int)Math.floor((Latitude+90)*2);
			
			return yindex*720+xindex;
		}
		

		
	}
	
	
	static class VTLocationReducer extends Reducer<Key_region_hour_imo,VesselPoint,ImmutableBytesWritable,KeyValue>
	{

		private Connection connection = null;
		private Table Vessel_Table=null;

		@Override
		protected void setup(
				Reducer<Key_region_hour_imo, VesselPoint, ImmutableBytesWritable,KeyValue>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			connection = ConnectionFactory.createConnection(context
					.getConfiguration());
			TableName Vessel_Name = TableName
					.valueOf("cdb_vessel:vessel");
			Vessel_Table = connection.getTable(Vessel_Name);
		}
		
		@Override
		protected void cleanup(
				Reducer<Key_region_hour_imo, VesselPoint, ImmutableBytesWritable,KeyValue>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			connection.close();
		}		

		@Override
		protected void reduce(
				Key_region_hour_imo key,
				Iterable<VesselPoint> vesselpointlist,
				Reducer<Key_region_hour_imo, VesselPoint, ImmutableBytesWritable,KeyValue>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			context.getCounter(Counters.VESSEL_HOURS).increment(1);
			VesselPoint[] Coordinates=new VesselPoint[0];
			
			for (VesselPoint VP_Source : vesselpointlist)
			{
				
				VesselPoint VP=VP_Source.Clone();
				VesselPoint[] tempCoordinates=new VesselPoint[Coordinates.length];
				System.arraycopy(Coordinates, 0, tempCoordinates, 0, Coordinates.length);
				Coordinates=new VesselPoint[tempCoordinates.length+1];
				System.arraycopy(tempCoordinates,0,Coordinates,0,tempCoordinates.length);
				Coordinates[tempCoordinates.length]=VP;
				
			}
		
			VesselLocationList VLL= new VesselLocationList();
						
			String VesselType=getVesselType(Vessel_Table, key.getIMO().toString());
			
			if (VesselType ==null)
			{
				context.getCounter(Counters.VESSEL_WITHOUTTYPE).increment(1);
				return;
			}
			
			VLL.setType(VesselType);			
			VLL.setCoordinates(Coordinates);
			
			int RegionNumber=key.getRegion().get();
			String Hour=key.getHour().toString();
			String IMO=key.getIMO().toString();
			
			byte[] rowkey = Bytes.toBytes(LpadNum(RegionNumber,6)+Hour+IMO);		
			
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowkey);
			
			KeyValue kvCoordinates = new KeyValue(rowkey , details, locations, Bytes.toBytes(VLL.toString()));    

			context.write(rowKey, kvCoordinates);			
		
		}
		
		private static String getVesselType(Table Vessel_Table,String IMO_str) throws IOException
		{
			 Get get = new Get(Bytes.toBytes(IMO_str));
			 get.addColumn(ves, TYPE);	

			 
			 Result result = Vessel_Table.get(get);
			 byte[] type = result.getValue(ves,TYPE);

			return Bytes.toString(type);
		}		
		
		private static String LpadNum(long num, int pad) {
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
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String StartTime=args[0];
		String StopTime=args[1];

		String OutPutPath=args[2];

		// vv AnalyzeData
		Scan scan = new Scan(); 
		Filter StartTimeFilter=new SingleColumnValueFilter(details,timestamp,CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(StartTime));
		Filter StopTimeFilter=new SingleColumnValueFilter(details,timestamp,CompareFilter.CompareOp.LESS,Bytes.toBytes(StopTime));
		FilterList FL=new FilterList(FilterList.Operator.MUST_PASS_ALL);
		FL.addFilter(StartTimeFilter);
		FL.addFilter(StopTimeFilter);
		
		scan.setFilter(FL);
		//sscan.setCaching(5000);
		
	
		/*
		
		TableName VtLocation_Name = TableName
				.valueOf("cdb_vessel:vessel_location");

		PropertyConfigurator.configure("log4j.properties");
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));
		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);  
		Connection connection = ConnectionFactory.createConnection(conf);	
		Table VTLocation_Table = connection.getTable(VtLocation_Name);
		ResultScanner Result_ExistingLocations = VTLocation_Table.getScanner(scan);
		
		for (Result res : Result_ExistingLocations) {
			System.out.print(Bytes.toString(res.getRow())+" | ");
			
			for (Cell cell : res.rawCells()) {
				String Qualifier = Bytes.toString(CellUtil
						.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.print(Qualifier+" : "+ Value);
			}
			
			System.out.println();
			
		}

		Result_ExistingLocations.close();
		
		*/

		Job job = Job.getInstance(getConf(), "Convert data in vessel_location table to time view");
		job.setJarByClass(ConvertToTimeView.class);
		
		TableMapReduceUtil.initTableMapperJob("cdb_vessel:vessel_location", scan, VTLocationMapper.class,
				Key_region_hour_imo.class, VesselPoint.class, job); // co AnalyzeData-6-Util
														// Set up the table
														// mapper phase using
														// the supplied utility.
		job.setReducerClass(VTLocationReducer.class);
		
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(KeyValue.class);
		
		HTable table = new HTable(getConf(), "cdb_vessel:time_view");
		HFileOutputFormat.configureIncrementalLoad(job, table);
		
		FileOutputFormat.setOutputPath(job, new Path(OutPutPath));

		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	

	public static void main(String[] args) throws Exception {
		try {

			int exitCode = ToolRunner.run(
					new ConvertToTimeView(), args);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
