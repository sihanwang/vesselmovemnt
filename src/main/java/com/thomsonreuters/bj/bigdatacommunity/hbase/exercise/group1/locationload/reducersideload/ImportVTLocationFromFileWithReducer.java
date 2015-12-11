package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
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
import org.geotools.geometry.jts.JTSFactoryFinder;

import au.com.bytecode.opencsv.CSVParser;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type.VesselZone;

//java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer -conf /etc/hbase/conf/hbase-site.xml -files VesselZone 8003662/vessellocation_small 10
//hadoop jar VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar -files VesselZone 8003662/vessellocation_small
/*
com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer$C
EVENT_UPSERTS=51340
EVENT_VALID=51340
LOCATION_ROWS=10831869
LOCATION_VALID=10831869
VESSEL_WITHOUTTYPE=2
File Input Format Counters 
Bytes Read=0
File Output Format Counters 
Bytes Written=0
*/

public class ImportVTLocationFromFileWithReducer extends Configured implements
		Tool {
	private static DateFormat rawformatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");	

	public enum Counters {
		VESSEL_WITHOUTTYPE, LOCATION_ROWS, LOCATION_ERROR, LOCATION_VALID, EVENT_UPSERTS, EVENT_ERROR, EVENT_VALID
	}

	static class ImportReducer
			extends
			Reducer<Key_IMOAndRecordTime, TextArrayWritable, NullWritable, NullWritable> {

		private Connection connection = null;
		private BufferedMutator VTLocation = null;
		private BufferedMutator VTEvent = null;
		private BufferedMutator VTVessel = null;
		private BufferedMutator LastLocation_BM = null;
		
		private Table VTLocation_Table = null;
		private Table VTEvent_Table = null;
		private Table Vessel_Table=null;
		
		private HashMap<Integer, VesselZone> Zonemap;
		
		private static byte[] details = Bytes.toBytes("details");
		private static byte[] speed = Bytes.toBytes("speed");
		private static byte[] destination = Bytes.toBytes("destination");
		private static byte[] timestamp = Bytes.toBytes("timestamp");
		private static byte[] coordinates = Bytes.toBytes("coordinates");
		private static byte[] entrytime = Bytes.toBytes("entertime");
		private static byte[] exittime = Bytes.toBytes("exittime");
		private static byte[] entrycoordinates = Bytes.toBytes("entercoordinates");
		private static byte[] exitcoordinates = Bytes.toBytes("exitcoordinates");
		private static byte[] TYPE = Bytes.toBytes("TYPE");
		private static byte[] ves = Bytes.toBytes("ves");
		private static byte[] lastlocation=Bytes.toBytes("lastlocation");
		private static byte[] imo=Bytes.toBytes("imo");

		
		
		@Override
		protected void cleanup(
				Reducer<Key_IMOAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			VTLocation.flush();
			VTEvent.flush();
			VTVessel.flush();
			LastLocation_BM.flush();

			connection.close();
		}

		@Override
		protected void setup(
				Reducer<Key_IMOAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			// TODO Auto-generated method stub
			connection = ConnectionFactory.createConnection(context
					.getConfiguration());

			TableName VtLocation_Name = TableName
					.valueOf("cdb_vessel:vessel_location");
			VTLocation = connection.getBufferedMutator(VtLocation_Name);
			VTLocation_Table = connection.getTable(VtLocation_Name);

			TableName VtEvent_Name = TableName
					.valueOf("cdb_vessel:vessel_event");
			VTEvent = connection.getBufferedMutator(VtEvent_Name);
			VTEvent_Table = connection.getTable(VtEvent_Name);
			
			TableName Vessel_Name = TableName
					.valueOf("cdb_vessel:vessel");
			VTVessel = connection.getBufferedMutator(Vessel_Name);
			Vessel_Table = connection.getTable(Vessel_Name);
			
			TableName LastLocation_Name = TableName
					.valueOf("cdb_vessel:latest_location");
			LastLocation_BM=connection.getBufferedMutator(LastLocation_Name);
			
			
			try {
				File Zonemapfile=new File("VesselZone");
				ObjectInputStream OIS= new ObjectInputStream(new FileInputStream(Zonemapfile));
				Zonemap= (HashMap<Integer, VesselZone>)OIS.readObject();
				OIS.close();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		protected void reduce(Key_IMOAndRecordTime key,
				Iterable<TextArrayWritable> LocationList, Context context)
				throws IOException, InterruptedException {

			String IMO_str = LpadNum(key.getIMO().get(), 7);
			VLongWritable pos_time = key.getRecordTime();

			// //////////////////////////////////////////////////////////////////////////////
			// calculate events

			// Retrieve all the existing locations after the first new location.
			List<VesselLocation> AllAfterPoints = getLocationsAfter(
					VTLocation_Table, IMO_str, pos_time.get());

			// Find out the last location before the first new location in			
			VesselLocation LastLocation = getLocationBefore(VTLocation_Table,key.getIMO().get(), pos_time.get());

			if (AllAfterPoints.size() > 0) {
				// remove all events start after the first new location.
				deleteEventsStartAfter(VTEvent_Table, IMO_str,pos_time.get());
				
//				if (LastLocation!=null)
//				{
//					//update existing events that started BEFORE the first new location and end after the first to end as the last location
//					updateExistingEventsToEndAtLastLocation(VTEvent_Table,key.getIMO().get(),LastLocation);
//				}
			}
			
			Map<Integer, VesselEvent> PreviousZoneEvents;
			
			if (LastLocation!=null)
			{
			//Get all events with exit at last location
				PreviousZoneEvents = getAllEventsWithExistAtLastLocation(VTEvent_Table,key.getIMO().get(),LastLocation);
			}
			else
			{
				PreviousZoneEvents=new HashMap<Integer, VesselEvent>();
			}
			
			
			//populate new locations
			
			for (TextArrayWritable rowcontent : LocationList) {
				// population location
				context.getCounter(Counters.LOCATION_ROWS).increment(1);
				VesselLocation newlocation=new VesselLocation();
				
				try {

					Writable[] content = rowcontent.get();
					String Latitude = content[16].toString().trim();
					String Longitude = content[15].toString().trim();
					String Coordinates=Latitude + "," + Longitude;
					String Speed = content[18].toString().trim();
					String Destination = content[9].toString().trim();
					String Timestamp = content[21].toString().trim();
					ParsePosition pos = new ParsePosition(0);
					long record_time = rawformatter.parse(Timestamp, pos)
							.getTime();

					byte[] rowkey = Bytes.toBytes(IMO_str
							+ LpadNum(Long.MAX_VALUE - record_time, 19));
					Put put = new Put(rowkey);

					put.addColumn(details, speed, Bytes.toBytes(Speed));
					put.addColumn(details, destination,
							Bytes.toBytes(Destination));
					put.addColumn(details, coordinates,
							Bytes.toBytes(Coordinates));
					
					put.addColumn(details, timestamp, Bytes.toBytes(rawformatter.format(new Date(record_time))));

					VTLocation.mutate(put);
					context.getCounter(Counters.LOCATION_VALID).increment(1);
					newlocation.coordinates=Coordinates;
					newlocation.recordtime=record_time;
					newlocation.speed=Double.parseDouble(Speed);
					newlocation.destination=Destination;
					

				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.LOCATION_ERROR).increment(1);
					continue;
				}
				
				AllAfterPoints.add(newlocation);
			}
			
			//sort AllAfterPoints
			java.util.Collections.sort(AllAfterPoints);
			
			//dedup
			AllAfterPoints=dedup(AllAfterPoints);
			
			
			/////////////////////////////////////////////////////////////////////
			//Store latest location
			//rowkey: global zone id (4)+ longlat22 ((long11(sign(1)+integer(3)+digit(7)))(lat11(sign(1)+integer(3)+(7))))+imo(7)+recordtime(19)
			/////////////////////////////////////////////////////////////////////			
			VesselLocation lastLocation=AllAfterPoints.get(AllAfterPoints.size()-1);	
			Vessel thisVessel=getVesselType(Vessel_Table, key.getIMO().get());
			
			String[] longlat=lastLocation.coordinates.split(",");
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
			Coordinate coord = new Coordinate(Double.parseDouble(longlat[1]), Double.parseDouble(longlat[0]));
			Point point = geometryFactory.createPoint(coord);

			Integer BelongedGlobalZoneIndex=null;

			for (int i=0 ; i<VesselZone.GlobalZones.length ; i++)
			{
				if (VesselZone.GlobalZones[i].covers(point))
				{
					BelongedGlobalZoneIndex=i;
					break;
				}
			}
						
			if (thisVessel.LastLocation!=null)
			{
				LastLocation_BM.mutate(new Delete(thisVessel.LastLocation));
			}
			
			
			byte[] lastlocationrowkey = Bytes.toBytes(LpadNum(BelongedGlobalZoneIndex,4) + ConvertCoordinatesToStr(longlat[1])+ConvertCoordinatesToStr(longlat[0]));
			Put lastlocation_put=new Put(lastlocationrowkey);	
			lastlocation_put.addColumn(details, imo, Bytes.toBytes(IMO_str));
			lastlocation_put.addColumn(details, timestamp, Bytes.toBytes(rawformatter.format(new Date(lastLocation.recordtime))));
			LastLocation_BM.mutate(lastlocation_put);
			
			//update vessel lastlocation
			Put vessel_lastlocation=new Put(Bytes.toBytes(String.valueOf(key.getIMO().get())));
			vessel_lastlocation.addColumn(ves, lastlocation, lastlocationrowkey);
			VTVessel.mutate(vessel_lastlocation);
			
			////////////////////////////////////////////////////////////////////
			
			ArrayList<VesselEvent> DerivedEventList=new ArrayList<VesselEvent>();
			
	
			if (thisVessel.VesselType ==null)
			{
				context.getCounter(Counters.VESSEL_WITHOUTTYPE).increment(1);
				return;
			}
			
			//calculating event
			for (VesselLocation VL : AllAfterPoints)
			{
				ArrayList<Integer> CurrentZones = LocateCurrentZone(VL.coordinates ,thisVessel.VesselType, Zonemap);
				
				Iterator<Map.Entry<Integer, VesselEvent>> it = PreviousZoneEvents.entrySet().iterator();  
				
				while (it.hasNext())
				{
					Map.Entry<Integer, VesselEvent> thisEntry=it.next();
					int Zone_Axsmarine_id = thisEntry.getKey();
					if (!CurrentZones.contains(Zone_Axsmarine_id))
					{
						VesselEvent PreviousEvent=thisEntry.getValue();

						if (!DerivedEventList.contains(PreviousEvent))
						{
							DerivedEventList.add(PreviousEvent);
						}
						//remove close event from PreviousZoneEvents;
						it.remove();
					}
				}


				for (Integer thisZone_Axsmarine_id : CurrentZones) {

					if (PreviousZoneEvents.containsKey(thisZone_Axsmarine_id))
					{
						//////////////////////////////////////////////////
						//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
						//////////////////////////////////////////////////
						VesselEvent PreviousEvent=PreviousZoneEvents.get(thisZone_Axsmarine_id);
						PreviousEvent.exitcoordinates=VL.coordinates;
						PreviousEvent.exittime=VL.recordtime;
						PreviousEvent.destination=VL.destination;
						
						if (!DerivedEventList.contains(PreviousEvent))
						{
							DerivedEventList.add(PreviousEvent);
						}
					}
					else
					{
						//////////////////////////////////////////////////
						//For current zones which only current locations belong to, fire new open events
						//////////////////////////////////////////////////
						VesselEvent NewEvent=new VesselEvent();
						NewEvent.entrycoordinates=VL.coordinates;
						NewEvent.entrytime=VL.recordtime;
						NewEvent.exitcoordinates=VL.coordinates;
						NewEvent.exittime=VL.recordtime;
						NewEvent.destination=VL.destination;		
						NewEvent.polygonid=thisZone_Axsmarine_id;
						
						PreviousZoneEvents.put(thisZone_Axsmarine_id, NewEvent);

						DerivedEventList.add(NewEvent);		

					}
				}		
			}
			

			
			
			//pupulate Derived Events into Hbase
			
			for (VesselEvent newEvent : DerivedEventList)
			{
			    //rowkey: IMO(7)+timestamp(19 desc)+polygonid(8)
			    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
				
				context.getCounter(Counters.EVENT_UPSERTS ).increment(1);
				
				byte[] rowkey = Bytes.toBytes(IMO_str
						+ LpadNum(Long.MAX_VALUE - newEvent.entrytime, 19)+LpadNum(newEvent.polygonid,10));
				Put put = new Put(rowkey);

				put.addColumn(details, entrytime, Bytes.toBytes(rawformatter.format(new Date(newEvent.entrytime))));
				put.addColumn(details, entrycoordinates, Bytes.toBytes(newEvent.entrycoordinates));
				put.addColumn(details, exittime, Bytes.toBytes(rawformatter.format(new Date(newEvent.exittime))));
				put.addColumn(details, exitcoordinates, Bytes.toBytes(newEvent.exitcoordinates));
				put.addColumn(details, destination,	Bytes.toBytes(newEvent.destination));
	
				VTEvent.mutate(put);
				context.getCounter(Counters.EVENT_VALID).increment(1);				
			}
			
		}
		
		public static String ConvertCoordinatesToStr(String longorlat)
		{
			BigDecimal bigDec = new BigDecimal(Double.parseDouble(longorlat));  
			double f1 =  bigDec.setScale(7, RoundingMode.HALF_UP).doubleValue(); 
			
			DecimalFormat df = new DecimalFormat("####.#######");
			
			longorlat=df.format(f1);
					
			boolean minus=false;
			String IntegerPart="";
			String DigitPart="";
			
			if (longorlat.startsWith("-") )
			{
				minus=true;
			}
			
			int index=longorlat.indexOf(".");
			
			if (minus)
			{
				if (index!=-1)
				{
					IntegerPart="-"+LpadNum(Integer.parseInt(longorlat.substring(1,index)),3);
				}
				else
				{
					IntegerPart="-"+LpadNum(Integer.parseInt(longorlat.substring(1)),3);
				}
			}
			else
			{
				if (index!=-1)
				{
					IntegerPart=LpadNum(Integer.parseInt(longorlat.substring(0,index)),4);
				}
				else
				{
					IntegerPart=LpadNum(Integer.parseInt(longorlat.substring(0)),4);
				}
				
				
			}
			
			if (index!=-1)
			{
				DigitPart=longorlat.substring(index+1);
			}
			else
			{
				DigitPart="";
			}
				
			while(DigitPart.length()<7)
			{
				DigitPart=DigitPart+"0";
			}
			
			return IntegerPart+DigitPart;

		}
		
		public static List<VesselLocation> dedup(
				List<VesselLocation> AllLocations) {
			List<VesselLocation> tempList = new ArrayList<VesselLocation>();
			for (VesselLocation VL : AllLocations) {
				if (!tempList.contains(VL)) {
					tempList.add(VL);
				}
			}

			return tempList;
		}

		public static List<VesselLocation> getLocationsAfter(
				Table VTLocation_Table, String imo_str, long timestamp)
				throws IOException {

			// scan
			// 'cdb_vessel:vessel_location',{FILTER=>"(PrefixFilter('0000003162')"}
			Scan GetExistingLocations = new Scan();
			GetExistingLocations.setStartRow(
					Bytes.toBytes(imo_str + LpadNum(0, 19))).setStopRow(
					Bytes.toBytes(imo_str
							+ LpadNum(Long.MAX_VALUE - timestamp, 19)));

			ResultScanner Result_ExistingLocations = VTLocation_Table
					.getScanner(GetExistingLocations);
			List<VesselLocation> result = new ArrayList<VesselLocation>();

			for (Result res : Result_ExistingLocations) {
				VesselLocation VL = new VesselLocation();

				for (Cell cell : res.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VL.coordinates = Value;
					} else if (Qualifier.equals("speed")) {
						VL.speed = Double.parseDouble(Value);
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						ParsePosition pos = new ParsePosition(0);
						VL.recordtime = rawformatter.parse(Value, pos).getTime();
					}
				}
				result.add(VL);
			}

			Result_ExistingLocations.close();

			return result;

		}

		public static void deleteEventsStartAfter(Table VTEvent_Table,
				String imo_str, long timestamp) throws IOException {

			// scan
			// 'cdb_vessel:vessel_event',{FILTER=>"(PrefixFilter('0000003162')"}
			Scan GetEventsStartAfter = new Scan();
			GetEventsStartAfter.setStartRow(
					Bytes.toBytes(imo_str + LpadNum(0, 19)+"00000000")).setStopRow(
					Bytes.toBytes(imo_str
							+ LpadNum(Long.MAX_VALUE - timestamp+1, 19)+"99999999"));
			GetEventsStartAfter.setFilter(new KeyOnlyFilter());

			ResultScanner Result_ExistingEvents = VTEvent_Table
					.getScanner(GetEventsStartAfter);
			List<Delete> deletes = new ArrayList<Delete>();

			for (Result res : Result_ExistingEvents) {
				deletes.add(new Delete(res.getRow()));
			}

			Result_ExistingEvents.close();

			VTEvent_Table.delete(deletes);
		}

		public static VesselLocation getLocationBefore(Table VTLocation_Table,
				long imo, long timestamp) throws IOException {

			Scan getLocationBefore = new Scan();
			getLocationBefore.setStartRow(Bytes.toBytes(LpadNum(imo,7)
					+ LpadNum(Long.MAX_VALUE - timestamp + 1, 19))).setStopRow(Bytes.toBytes(LpadNum(imo+1,10)
							+ LpadNum(0, 19)));
			
			getLocationBefore.setMaxResultSize(1);
			
			
			ResultScanner Result_LocationBefore = VTLocation_Table
					.getScanner(getLocationBefore);
			
			for (Result res : Result_LocationBefore) {
				VesselLocation VL = new VesselLocation();

				for (Cell cell : res.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VL.coordinates = Value;
					} else if (Qualifier.equals("speed")) {
						VL.speed = Double.parseDouble(Value);
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						ParsePosition pos = new ParsePosition(0);
						VL.recordtime = rawformatter.parse(Value, pos).getTime();
					}
				}
				
				Result_LocationBefore.close();
				return VL;
			}

			Result_LocationBefore.close();
			return null;
		}
		
		public static void updateExistingEventsToEndAtLastLocation(Table VTEvent_Table, long imo, VesselLocation lastlocation) throws IOException
		{
			//update existing events that started BEFORE the first new location and end after the first to end as the last location
			
			//Find existing events that started BEFORE the first new location and end after the first
			Scan getEventStartedBeforeAndEndAfter=new Scan();;
			getEventStartedBeforeAndEndAfter
			.setStartRow(Bytes.toBytes(LpadNum(imo,7)+LpadNum(Long.MAX_VALUE - lastlocation.recordtime, 19)+"0000000000"))
			.setStopRow(Bytes.toBytes(LpadNum(imo,7)+LpadNum(Long.MAX_VALUE, 19)+"9999999999"))
			.addColumn(details, exittime);
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.GREATER,new BinaryComparator(Bytes.toBytes(rawformatter.format(new Date(lastlocation.recordtime)))));
			getEventStartedBeforeAndEndAfter.setFilter(ExistTimeValuefilter);
			
			ResultScanner Result_eventcross = VTEvent_Table.getScanner(getEventStartedBeforeAndEndAfter);
			
			List<Put> puts = new ArrayList<Put>();
			for (Result res : Result_eventcross) {
				
				//vessel event table
			    //rowkey: imo(7)+timestamp(19 desc)+polygonid(8)
			    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
				byte[] rowkey=res.getRow();
				Put updateevent=new Put(rowkey);
				updateevent.addColumn(details, exittime, Bytes.toBytes(rawformatter.format(new Date(lastlocation.recordtime))));
				updateevent.addColumn(details,coordinates, Bytes.toBytes(lastlocation.coordinates));
				updateevent.addColumn(details,destination, Bytes.toBytes(lastlocation.destination));
				puts.add(updateevent);
				
			}
			
			Result_eventcross.close();
			VTEvent_Table.put(puts);
		}
		
		
		//Get all events with exit at last location
		public static Map<Integer,VesselEvent> getAllEventsWithExistAtLastLocation(Table VTEvent_Table, long imo, VesselLocation lastlocation) throws IOException
		{
			Scan getAllEventsWithExistAtLastLocation=new Scan();
			getAllEventsWithExistAtLastLocation
			.setStartRow(Bytes.toBytes(LpadNum(imo,7)+LpadNum(Long.MAX_VALUE - lastlocation.recordtime, 19)+"0000000000"))
			.setStopRow(Bytes.toBytes(LpadNum(imo,7)+LpadNum(Long.MAX_VALUE, 19)+"9999999999"))
			.addColumn(details, exittime);
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL ,new BinaryComparator(Bytes.toBytes(rawformatter.format(new Date(lastlocation.recordtime)))));
			getAllEventsWithExistAtLastLocation.setFilter(ExistTimeValuefilter);
			
			ResultScanner Result_event = VTEvent_Table.getScanner(getAllEventsWithExistAtLastLocation);
			
			Map<Integer, VesselEvent> events=new HashMap<Integer,VesselEvent>();
			
			for (Result res : Result_event) {
				
				Get get = new Get(res.getRow());
				get.addColumn(details, entrytime);
				get.addColumn(details, entrycoordinates);
				
				Result result = VTEvent_Table.get(get);
				String rowkey = Bytes.toString(result.getRow());
				String polygonid = rowkey.substring(26);

				VesselEvent VE = new VesselEvent();
				VE.exittime = lastlocation.recordtime;
				VE.exitcoordinates = lastlocation.coordinates;	
				VE.destination = lastlocation.destination;
				VE.polygonid = Integer.parseInt(polygonid);

				for (Cell cell : result.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					ParsePosition pos = new ParsePosition(0);

					if (Qualifier.equals("entertime")) {
						VE.entrytime = rawformatter.parse(Value, pos).getTime();
					} else if (Qualifier.equals("entercoordinates")) {
						VE.entrycoordinates = Value;
					}
				}
				
				events.put(VE.polygonid, VE);
			}

			Result_event.close();
			return events;
		}
		
		private static Vessel getVesselType(Table Vessel_Table,long imo) throws IOException
		{
			 Get get = new Get(Bytes.toBytes(LpadNum(imo,7)));
			 get.addColumn(ves, TYPE);	
			 get.addColumn(ves,lastlocation);
			 
			 Result result = Vessel_Table.get(get);
			 byte[] val = result.getValue(ves,TYPE);
			 byte[] last_location=result.getValue(ves,lastlocation);
			 
			 Vessel vessel=new Vessel();
			 vessel.LastLocation=last_location;
			 vessel.VesselType=Bytes.toString(val);
			 
			 return vessel;
		}
		
		private static ArrayList<Integer> LocateCurrentZone(String Coordinates, String VesselProductType,HashMap<Integer, VesselZone> Zonemap)
		{
			String[] longlat=Coordinates.split(",");
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
			Coordinate coord = new Coordinate(Double.parseDouble(longlat[1]), Double.parseDouble(longlat[0]));
			Point point = geometryFactory.createPoint(coord);

			ArrayList<Integer> CurrentZones = new ArrayList<Integer>();

			Integer BelongedGlobalZoneIndex=null;

			for (int i=0 ; i<VesselZone.GlobalZones.length ; i++)
			{
				if (VesselZone.GlobalZones[i].covers(point))
				{
					BelongedGlobalZoneIndex=i;
					break;
				}
			}


			for (Map.Entry<Integer, VesselZone> thisEntry : Zonemap.entrySet()) {

				VesselZone thisZone=thisEntry.getValue();
				
				String ZoneType=thisZone.getZoneType();
				
				if (ZoneType.startsWith("ZONE"))
				{
					String Classfications=ZoneType.substring(5);
					
					if (VesselProductType.equals("Tankers"))
					{
						if (Classfications.indexOf("TANKER")==-1)
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Bulkers"))
					{
						if (Classfications.indexOf("DRY")==-1)
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Container / Roro"))
					{
						if (Classfications.indexOf("LINER")==-1)
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Miscellaneous") || VesselProductType.equals("Passenger"))
					{
						continue;
					}
					
				}					
				
				if (thisZone.IntersectedWithGlobalZone(BelongedGlobalZoneIndex))
				{
					if (thisZone.getPolygon().covers(point)) {
						CurrentZones.add(thisZone.getAxsmarine_ID());
					}	
				}					
			}

			return CurrentZones;
		}			
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("please input file location and reduce job number");
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
		job.setMapOutputKeyClass(Key_IMOAndRecordTime.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		job.setPartitionerClass(Partitioner_IMO.class);
		job.setGroupingComparatorClass(GroupComparator_IMO.class);

		job.setReducerClass(ImportReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[1]));

		job.setOutputFormatClass(NullOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;

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
	
	public static String RpadNum(long num, int pad) {
		String res = Long.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = res+"0";
			}
		}
		return res;
	}
	

	
	public static void unittest_getLocationBefore() throws Exception
	{
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("cdb_vessel",
				"vessel_location"));

		//ImportReducer.deleteEventsStartAfter(table, padNum(3162, 7), formatter.parse("2014-01-18T00:05:24Z", new ParsePosition(0)).getTime());

		ImportReducer.getLocationBefore(table, 3162, rawformatter.parse("2015-10-19T00:35:03.311Z", new ParsePosition(0)).getTime());
		connection.close();
	}

	public static void unittest_getForVesselType() throws Exception
	{
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("cdb_vessel:vessel"));

		//ImportReducer.deleteEventsStartAfter(table, padNum(3162, 7), formatter.parse("2014-01-18T00:05:24Z", new ParsePosition(0)).getTime());
		ImportReducer.getVesselType(table, 316255);
		
		connection.close();
	}
	
	
	public static void unittest_reducer() throws Exception
	{
		
		File WorkingFile=new File("D:\\test\\VTCurrentLocation\\51235.csv");
		VesselZone.ZoneMap=VesselZone.DownloadAllZonesHbase();

		ArrayList<TextArrayWritable> LocationList=new ArrayList<TextArrayWritable>();
		
		BufferedReader BR = new BufferedReader(	new InputStreamReader(new FileInputStream(WorkingFile)));
		String strRow = BR.readLine();
		CSVParser CSVP=new CSVParser(',','"','\\',true,false);
		
		Key_IMOAndRecordTime key= new Key_IMOAndRecordTime();
		boolean first=true;
		
		while (strRow!=null)
		{
			String[] nextrow=CSVP.parseLine(strRow);
						
			
			long IMO=Long.parseLong(nextrow[1].trim());
			String recordTime=nextrow[21].trim().substring(0, 19);
			
			ParsePosition pos = new ParsePosition(0);
			long record_time=rawformatter.parse(recordTime, pos).getTime();
			
			if (first)
			{
				key.set(new VLongWritable(IMO), new VLongWritable(record_time));
				first=false;
			}
			
			Text[] allfields=new Text[nextrow.length];

			for(int i=0;i<nextrow.length;i++)
			{
				allfields[i]=new Text(nextrow[i]);					
			}
			TextArrayWritable thisrow=new TextArrayWritable();
			thisrow.set(allfields);
			
			LocationList.add(thisrow);
			strRow = BR.readLine();
		}
		
		
		Connection connection = null;
		BufferedMutator VTLocation = null;
		BufferedMutator VTEvent = null;
		BufferedMutator VTVessel = null;
		BufferedMutator LastLocation_BM = null;
		
		Table VTLocation_Table = null;
		Table VTEvent_Table = null;
		Table Vessel_Table=null;
		Table LastLocation_Table=null;
		
		HashMap<Integer, VesselZone> Zonemap;		
		
		byte[] details = Bytes.toBytes("details");
		byte[] speed = Bytes.toBytes("speed");
		byte[] destination = Bytes.toBytes("destination");
		byte[] timestamp = Bytes.toBytes("timestamp");
		byte[] coordinates = Bytes.toBytes("coordinates");
		byte[] entrytime = Bytes.toBytes("entertime");
		byte[] exittime = Bytes.toBytes("exittime");
		byte[] entrycoordinates = Bytes.toBytes("entercoordinates");
		byte[] exitcoordinates = Bytes.toBytes("exitcoordinates");
		byte[] TYPE = Bytes.toBytes("TYPE");
		byte[] ves = Bytes.toBytes("ves");
		byte[] lastlocation=Bytes.toBytes("lastlocation");		
		byte[] imo=Bytes.toBytes("imo");
		
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));
		connection = ConnectionFactory.createConnection(conf);

		TableName VtLocation_Name = TableName
				.valueOf("cdb_vessel:vessel_location");
		VTLocation = connection.getBufferedMutator(VtLocation_Name);
		VTLocation_Table = connection.getTable(VtLocation_Name);

		TableName VtEvent_Name = TableName
				.valueOf("cdb_vessel:vessel_event");
		VTEvent = connection.getBufferedMutator(VtEvent_Name);
		VTEvent_Table = connection.getTable(VtEvent_Name);
		
		TableName Vessel_Name = TableName
				.valueOf("cdb_vessel:vessel");
		VTVessel = connection.getBufferedMutator(Vessel_Name);
		Vessel_Table = connection.getTable(Vessel_Name);
		
		TableName LastLocation_Name = TableName
				.valueOf("cdb_vessel:latest_location");
		LastLocation_BM=connection.getBufferedMutator(LastLocation_Name);
		LastLocation_Table = connection.getTable(LastLocation_Name);			
		
		try {
			File Zonemapfile=new File("VesselZone");
			ObjectInputStream OIS= new ObjectInputStream(new FileInputStream(Zonemapfile));
			Zonemap= (HashMap<Integer, VesselZone>)OIS.readObject();
			OIS.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		String IMO_str = LpadNum(key.getIMO().get(), 7);
		VLongWritable pos_time = key.getRecordTime();

		// //////////////////////////////////////////////////////////////////////////////
		// calculate events

		// Retrieve all the existing locations after the first new location.
		List<VesselLocation> AllAfterPoints = ImportReducer.getLocationsAfter(
				VTLocation_Table, IMO_str, pos_time.get());

		// Find out the last location before the first new location in			
		VesselLocation LastLocation = ImportReducer.getLocationBefore(VTLocation_Table,key.getIMO().get(), pos_time.get());

		if (AllAfterPoints.size() > 0) {
			// remove all events start after the first new location.
			ImportReducer.deleteEventsStartAfter(VTEvent_Table, IMO_str,pos_time.get());
			
			if (LastLocation!=null)
			{
				//update existing events that started BEFORE the first new location and end after the first to end as the last location
				ImportReducer.updateExistingEventsToEndAtLastLocation(VTEvent_Table,key.getIMO().get(),LastLocation);
			}
		}
		
		Map<Integer, VesselEvent> PreviousZoneEvents;
		
		if (LastLocation!=null)
		{
		//Get all events with exit at last location
			PreviousZoneEvents = ImportReducer.getAllEventsWithExistAtLastLocation(VTEvent_Table,key.getIMO().get(),LastLocation);
		}
		else
		{
			PreviousZoneEvents=new HashMap<Integer, VesselEvent>();
		}
		
		
		//populate new locations
		
		for (TextArrayWritable rowcontent : LocationList) {
			// population location
			//context.getCounter(Counters.LOCATION_ROWS).increment(1);
			VesselLocation newlocation=new VesselLocation();
			
			try {

				Writable[] content = rowcontent.get();
				String Latitude = content[16].toString();
				String Longitude = content[15].toString();
				String Coordinates=Latitude + "," + Longitude;
				String Speed = content[18].toString();
				String Destination = content[9].toString();
				String Timestamp = content[21].toString();
				ParsePosition pos = new ParsePosition(0);
				long record_time = rawformatter.parse(Timestamp, pos)
						.getTime();

				byte[] rowkey = Bytes.toBytes(IMO_str
						+ LpadNum(Long.MAX_VALUE - record_time, 19));
				Put put = new Put(rowkey);

				put.addColumn(details, speed, Bytes.toBytes(Speed));
				put.addColumn(details, destination,
						Bytes.toBytes(Destination));
				put.addColumn(details, coordinates,
						Bytes.toBytes(Coordinates));
				
				put.addColumn(details, timestamp, Bytes.toBytes(rawformatter.format(new Date(record_time))));

				VTLocation.mutate(put);
				//context.getCounter(Counters.LOCATION_VALID).increment(1);
				newlocation.coordinates=Coordinates;
				newlocation.recordtime=record_time;
				newlocation.speed=Double.parseDouble(Speed);
				newlocation.destination=Destination;
				

			} catch (Exception e) {
				e.printStackTrace();
				//context.getCounter(Counters.LOCATION_ERROR).increment(1);
				continue;
			}
			
			AllAfterPoints.add(newlocation);
		}
		
		//sort AllAfterPoints
		java.util.Collections.sort(AllAfterPoints);
		
		//dedup
		AllAfterPoints=ImportReducer.dedup(AllAfterPoints);
		
		
		/////////////////////////////////////////////////////////////////////
		//Store latest location
		//rowkey: global zone id (4)+ longlat22 ((long11(sign(1)+integer(3)+digit(7)))(lat11(sign(1)+integer(3)+(7))))+imo(7)+recordtime(19)
		/////////////////////////////////////////////////////////////////////			
		VesselLocation lastLocation=AllAfterPoints.get(AllAfterPoints.size()-1);	
		Vessel thisVessel=ImportReducer.getVesselType(Vessel_Table, key.getIMO().get());
		
		String[] longlat=lastLocation.coordinates.split(",");
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		Coordinate coord = new Coordinate(Double.parseDouble(longlat[0]), Double.parseDouble(longlat[1]));
		Point point = geometryFactory.createPoint(coord);

		Integer BelongedGlobalZoneIndex=null;

		for (int i=0 ; i<VesselZone.GlobalZones.length ; i++)
		{
			if (VesselZone.GlobalZones[i].covers(point))
			{
				BelongedGlobalZoneIndex=i;
				break;
			}
		}
		
		if (thisVessel.LastLocation!=null)
		{
			LastLocation_BM.mutate(new Delete(thisVessel.LastLocation));
		}
		
		byte[] lastlocationrowkey = Bytes.toBytes(LpadNum(BelongedGlobalZoneIndex,4) + ImportReducer.ConvertCoordinatesToStr(longlat[0])+ImportReducer.ConvertCoordinatesToStr(longlat[1]));
		Put lastlocation_put=new Put(lastlocationrowkey);	
		lastlocation_put.addColumn(details, imo, Bytes.toBytes(IMO_str));
		lastlocation_put.addColumn(details, timestamp, Bytes.toBytes(rawformatter.format(new Date(lastLocation.recordtime))));
		LastLocation_BM.mutate(lastlocation_put);
		LastLocation_BM.flush();
		
		//update vessel lastlocation
		Put vessel_lastlocation=new Put(Bytes.toBytes(String.valueOf(key.getIMO().get())));
		vessel_lastlocation.addColumn(ves, lastlocation, lastlocationrowkey);
		VTVessel.mutate(vessel_lastlocation);
		VTVessel.flush();
		
		////////////////////////////////////////////////////////////////////
		
		ArrayList<VesselEvent> DerivedEventList=new ArrayList<VesselEvent>();
		

		if (thisVessel.VesselType ==null)
		{
			//context.getCounter(Counters.VESSEL_WITHOUTTYPE).increment(1);
			return;
		}
		
		//calculating event
		for (VesselLocation VL : AllAfterPoints)
		{
			ArrayList<Integer> CurrentZones = ImportReducer.LocateCurrentZone(VL.coordinates ,thisVessel.VesselType, VesselZone.ZoneMap);
			
			Iterator<Map.Entry<Integer, VesselEvent>> it = PreviousZoneEvents.entrySet().iterator();  
			
			while (it.hasNext())
			{
				Map.Entry<Integer, VesselEvent> thisEntry=it.next();
				int Zone_Axsmarine_id = thisEntry.getKey();
				if (!CurrentZones.contains(Zone_Axsmarine_id))
				{
					VesselEvent PreviousEvent=thisEntry.getValue();

					if (!DerivedEventList.contains(PreviousEvent))
					{
						DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				}
			}


			for (Integer thisZone_Axsmarine_id : CurrentZones) {

				if (PreviousZoneEvents.containsKey(thisZone_Axsmarine_id))
				{
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					VesselEvent PreviousEvent=PreviousZoneEvents.get(thisZone_Axsmarine_id);
					PreviousEvent.exitcoordinates=VL.coordinates;
					PreviousEvent.exittime=VL.recordtime;
					PreviousEvent.destination=VL.destination;
					
					if (!DerivedEventList.contains(PreviousEvent))
					{
						DerivedEventList.add(PreviousEvent);
					}
				}
				else
				{
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					VesselEvent NewEvent=new VesselEvent();
					NewEvent.entrycoordinates=VL.coordinates;
					NewEvent.entrytime=VL.recordtime;
					NewEvent.exitcoordinates=VL.coordinates;
					NewEvent.exittime=VL.recordtime;
					NewEvent.destination=VL.destination;		
					NewEvent.polygonid=thisZone_Axsmarine_id;
					
					PreviousZoneEvents.put(thisZone_Axsmarine_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				}
			}		
		}
		

		
		
		//pupulate Derived Events into Hbase
		
		for (VesselEvent newEvent : DerivedEventList)
		{
		    //rowkey: IMO(7)+timestamp(19 desc)+polygonid(8)
		    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
			
			//context.getCounter(Counters.EVENT_UPSERTS ).increment(1);
			
			byte[] rowkey = Bytes.toBytes(IMO_str
					+ LpadNum(Long.MAX_VALUE - newEvent.entrytime, 19)+LpadNum(newEvent.polygonid,10));
			Put put = new Put(rowkey);

			put.addColumn(details, entrytime, Bytes.toBytes(rawformatter.format(new Date(newEvent.entrytime))));
			put.addColumn(details, entrycoordinates, Bytes.toBytes(newEvent.entrycoordinates));
			put.addColumn(details, exittime, Bytes.toBytes(rawformatter.format(new Date(newEvent.exittime))));
			put.addColumn(details, exitcoordinates, Bytes.toBytes(newEvent.exitcoordinates));
			put.addColumn(details, destination,	Bytes.toBytes(newEvent.destination));

			VTEvent.mutate(put);
			//context.getCounter(Counters.EVENT_VALID).increment(1);				
		}
		
	
	}

	
	public static void main(String[] args) throws Exception {
				
		try {

			int exitCode = ToolRunner.run(
					new ImportVTLocationFromFileWithReducer(), args);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
