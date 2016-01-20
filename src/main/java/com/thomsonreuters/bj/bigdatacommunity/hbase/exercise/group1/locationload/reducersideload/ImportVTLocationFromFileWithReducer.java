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
import java.text.DecimalFormat;
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
import org.geotools.factory.FactoryRegistryException;
import org.geotools.geometry.jts.JTSFactoryFinder;

import au.com.bytecode.opencsv.CSVParser;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat; 
import org.joda.time.format.DateTimeFormatter;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type.VesselZone;

//java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer -conf /etc/hbase/conf/hbase-site.xml -files VesselZone 8003662/vessellocation_small 10
//hadoop jar VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar -files VesselZone 8003662/vessellocation_small
//nohup java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer -conf /etc/hbase/conf/hbase-site.xml -files VesselZone 0133272/ves/work 10 &


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
	private static DateTimeFormatter rawformatter =  DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC();

	public enum Counters {
		VESSEL_PROCESSED, VESSEL_WITHOUTTYPE, LOCATION_ROWS, LOCATION_ERROR, LOCATION_VALID, EVENT_UPSERTS, EVENT_ERROR, EVENT_VALID
	}

	static class ImportReducer
			extends
			Reducer<Key_IMOAndRecordTime, TextArrayWritable, NullWritable, NullWritable> {

		private Connection connection = null;
		private BufferedMutator VTLocation = null;
		private BufferedMutator VTEvent = null;
		private BufferedMutator LastLocation_BM = null;
		private BufferedMutator TrackInfo_BM = null;
		
		private Table VTLocation_Table = null;
		private Table VTEvent_Table = null;
		private Table Vessel_Table=null;
		private Table TrackInfo_Table=null;
		
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
		private static byte[] previouslocation=Bytes.toBytes("previouslocation");
		private static byte[] nextlocation=Bytes.toBytes("nextlocation");
		private static byte[] firstrecordtime=Bytes.toBytes("firstrecordtime");
		private static byte[] lastrecordtime=Bytes.toBytes("lastrecordtime");

		
		
		@Override
		protected void cleanup(
				Reducer<Key_IMOAndRecordTime, TextArrayWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub


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
			Vessel_Table = connection.getTable(Vessel_Name);
			
			TableName LastLocation_Name = TableName
					.valueOf("cdb_vessel:latest_location");
			LastLocation_BM=connection.getBufferedMutator(LastLocation_Name);			
			
			TableName TrackInfo_Name = TableName
					.valueOf("cdb_vessel:vessel_track_info");
			TrackInfo_BM=connection.getBufferedMutator(TrackInfo_Name);
			TrackInfo_Table=connection.getTable(TrackInfo_Name);
			
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

			try {
				context.getCounter(Counters.VESSEL_PROCESSED).increment(1);
				
				String IMO_str = LpadNum(key.getIMO().get(), 7);
				long first_pos_time = key.getRecordTime().get();
							
				
				/////////////////////////////////////////////////////////////////////////////////
				//Populate newPoints with new locations
				List<VesselLocation> newPoints=new ArrayList<VesselLocation>();
				
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
						String Timestamp = content[21].toString().trim().substring(0, 19);
						
						long record_time = DateTime.parse(Timestamp, rawformatter).getMillis();
						newlocation.coordinates=Coordinates;
						newlocation.recordtime=record_time;
						newlocation.speed=Speed;
						newlocation.destination=Destination;
						context.getCounter(Counters.LOCATION_VALID).increment(1);

					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.LOCATION_ERROR).increment(1);
						continue;
					}
					
					newPoints.add(newlocation);
				}
				

				/////////////////////////////////////////////////////////////////////////////////
				//Get last new post time
				long last_pos_time=newPoints.get(newPoints.size()-1).recordtime;
				
				////////////////////////////////////////////////////////////////////////////////
				//Get Existing trackinfo
				VesselTrackInfo VTI=getTrackInfo(TrackInfo_Table,IMO_str);			
				
				List<VesselLocation> AllBetweenPoints=new ArrayList<VesselLocation>();			
				
				String BeforeRowKey=null;
				String AfterRowKey=null;
				
				// //////////////////////////////////////////////////////////////////////////////
				// Retrieve all the existing locations between the first new location and the last new location.
				if ((VTI.FirstRecordTime !=null) && (VTI.LastRecordTime!=null))
				{
					
					if (last_pos_time < VTI.FirstRecordTime )
					{
						AfterRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - VTI.FirstRecordTime, 19);
					}
					else if (first_pos_time > VTI.LastRecordTime)
					{
						BeforeRowKey=IMO_str + LpadNum(Long.MAX_VALUE - VTI.LastRecordTime, 19);
					}
					else
					{
						AllBetweenPoints = ImportReducer.getLocationsBetween(VTLocation_Table, IMO_str, first_pos_time,last_pos_time);
									
						
						if (AllBetweenPoints.size()==0)
						{
							//Search for the first DB point before the first new point
							VesselLocation BeforeLocation=getLocationBefore(VTLocation_Table, IMO_str,first_pos_time);
							BeforeRowKey=IMO_str +LpadNum(Long.MAX_VALUE - BeforeLocation.recordtime, 19);
							AfterRowKey=BeforeLocation.nextlocation;


						}
						else
						{
							java.util.Collections.sort(AllBetweenPoints);	
							BeforeRowKey=AllBetweenPoints.get(0).previouslocation;
							AfterRowKey=AllBetweenPoints.get(AllBetweenPoints.size()-1).nextlocation;
						}
						
						List<Delete> deletes=ImportReducer.GetDeleteEventsBetween(VTEvent_Table, IMO_str,first_pos_time,last_pos_time);
						ImportReducer.DeleteEvents(VTEvent,deletes);
						VTEvent.flush();
					}			
					
				}

				// Find out the location before the first new location in	
				VesselLocation BeforeLocation = getLocation(VTLocation_Table,BeforeRowKey);
				
				// Find out the location after the last new location in			
				VesselLocation AfterLocation = getLocation(VTLocation_Table,AfterRowKey);		
				
				
				Map<Integer, VesselEvent> PreviousZoneEvents=new HashMap<Integer, VesselEvent>();;
				Map<Integer, VesselEvent> AfterZoneEvents=new HashMap<Integer, VesselEvent>();
				
				if (BeforeLocation!=null)
				{
				//Get all events with exit at last location
					PreviousZoneEvents = getAllEventsStartBeforeEndAfterBeforeLocation(VTEvent_Table,IMO_str,BeforeLocation);
				}

				
				////////////////////////////////////////////////////
				//Analyze and calculate previous and next location
				for (VesselLocation newlocation : newPoints) {
					
					int index=AllBetweenPoints.indexOf(newlocation);
					if (index!=-1)
					{
						VesselLocation dblocation=AllBetweenPoints.get(index);
						dblocation.coordinates=newlocation.coordinates;
						dblocation.destination=newlocation.destination;
						dblocation.speed=newlocation.speed;
					}	
					else
					{
						AllBetweenPoints.add(newlocation);		
					}
				}
				
				java.util.Collections.sort(AllBetweenPoints);
				
				String previousRowKey=null;
				
				for (VesselLocation location: AllBetweenPoints)
				{
					location.previouslocation=previousRowKey;
					previousRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - location.recordtime, 19);
				}
				
				String  NextRowKey=null;
				
				for (int i=(AllBetweenPoints.size()-1); i>=0;i-- )
				{
					VesselLocation location=AllBetweenPoints.get(i);
					location.nextlocation=NextRowKey;
					NextRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - location.recordtime, 19);
				}
				
				
				AllBetweenPoints.get(0).previouslocation=BeforeRowKey;
				AllBetweenPoints.get(AllBetweenPoints.size()-1).nextlocation=AfterRowKey;
				

				////////////////////////////////////////////////////
				//Upsert all locations
				
				for (VesselLocation location : AllBetweenPoints) {
					// population location			
					try {

						byte[] rowkey = Bytes.toBytes(IMO_str
								+ LpadNum(Long.MAX_VALUE - location.recordtime, 19));
						Put put = new Put(rowkey);

						put.addColumn(details, speed, Bytes.toBytes(location.speed));
						put.addColumn(details, destination,
								Bytes.toBytes(location.destination));
						put.addColumn(details, coordinates,
								Bytes.toBytes(location.coordinates));
						
						put.addColumn(details, timestamp, Bytes.toBytes(new DateTime(location.recordtime).toString(rawformatter)));
						
						if(location.previouslocation!=null)
						{
							put.addColumn(details, previouslocation, Bytes.toBytes(location.previouslocation));
						}

						if (location.nextlocation!=null)
						{
							put.addColumn(details, nextlocation, Bytes.toBytes(location.nextlocation));
						}

						VTLocation.mutate(put);
						

					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.LOCATION_ERROR).increment(1);
						continue;
					}
					
				}
				
				//update before next location and after previous location
				
				if (BeforeRowKey!=null)
				{
					Put BeforeLocationPut = new Put(Bytes.toBytes(BeforeRowKey));
					BeforeLocationPut.addColumn(details, nextlocation, Bytes.toBytes(IMO_str+ LpadNum(Long.MAX_VALUE - AllBetweenPoints.get(0).recordtime, 19)));
					VTLocation.mutate(BeforeLocationPut);
				}

				if (AfterRowKey!=null)
				{

					Put AfterLocationPut = new Put(Bytes.toBytes(AfterRowKey));
					AfterLocationPut.addColumn(details, previouslocation, Bytes.toBytes(IMO_str+ LpadNum(Long.MAX_VALUE - AllBetweenPoints.get(AllBetweenPoints.size()-1).recordtime, 19)));
					VTLocation.mutate(AfterLocationPut);
				}
				
				VTLocation.flush();
							

				
				/////////////////////////////////////////////////////////////////////
				//Store latest location
				//rowkey: global zone id (4)+ longlat22 ((long11(sign(1)+integer(3)+digit(7)))(lat11(sign(1)+integer(3)+(7))))+imo(7)+recordtime(19)
				/////////////////////////////////////////////////////////////////////	
							
				Put vessel_track_info=new Put(Bytes.toBytes(IMO_str));
				
				if (AfterLocation==null)
				{
					//Get the last location
					VesselLocation lastLocation=AllBetweenPoints.get(AllBetweenPoints.size()-1);
					//update the last location
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
					
					if (VTI.LastLocation!=null)
					{
						LastLocation_BM.mutate(new Delete(VTI.LastLocation));					
					}
					
					byte[] lastlocationrowkey = Bytes.toBytes(LpadNum(BelongedGlobalZoneIndex,4) + ConvertCoordinatesToStr(longlat[1])+ConvertCoordinatesToStr(longlat[0]));
					Put lastlocation_put=new Put(lastlocationrowkey);	
					lastlocation_put.addColumn(details, imo, Bytes.toBytes(IMO_str));
					lastlocation_put.addColumn(details, timestamp, Bytes.toBytes(new DateTime(lastLocation.recordtime).toString(rawformatter)));
					LastLocation_BM.mutate(lastlocation_put);
					
					LastLocation_BM.flush();
					
					vessel_track_info.addColumn(details, lastlocation, lastlocationrowkey);
					vessel_track_info.addColumn(details, lastrecordtime, Bytes.toBytes(new DateTime(lastLocation.recordtime).toString(rawformatter)));

				}		
				else
				{
					//Get events that start before last new location and end after last new location				
					AfterZoneEvents = getAllEventsStartBeforeEndAfter(VTEvent_Table,IMO_str,AfterLocation.recordtime);
				}
				
				//update firstrecordtime and lastrecordtime
				if (BeforeLocation == null)
				{
					vessel_track_info.addColumn(details, firstrecordtime, Bytes.toBytes(new DateTime(AllBetweenPoints.get(0).recordtime).toString(rawformatter)));
				}
				
				if (!vessel_track_info.isEmpty())
				{
					TrackInfo_BM.mutate(vessel_track_info);	
					TrackInfo_BM.flush();
				}
							
				////////////////////////////////////////////////////////////////////
				
				ArrayList<VesselEvent> DerivedEventList=new ArrayList<VesselEvent>();
				
				///////////////////////////////////////////////////////////
				//Get Vessel
				String VesselType=getVesselType(Vessel_Table, IMO_str);
				
				if (VesselType ==null)
				{
					context.getCounter(Counters.VESSEL_WITHOUTTYPE).increment(1);
					return;
				}
				
				
				
				//calculating event
				for (VesselLocation VL : AllBetweenPoints)
				{
					ArrayList<Integer> CurrentZones = LocateCurrentZone(VL.coordinates ,VesselType, Zonemap);
					
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
				
				///////////////////////////////////////////////////////////////////////////////////////
				//Merge with PreviousZoneEvents with AfterZoneEvents
				
				Iterator<Map.Entry<Integer, VesselEvent>> it = AfterZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
					Map.Entry<Integer, VesselEvent> thisEntry=it.next();
					int Zone_Axsmarine_id = thisEntry.getKey();
					VesselEvent After_VE = thisEntry.getValue();
					
					VesselEvent Previous_VE=PreviousZoneEvents.get(Zone_Axsmarine_id);
					
					if (Previous_VE!=null)
					{
						Previous_VE.exitcoordinates=After_VE.exitcoordinates;
						Previous_VE.exittime=After_VE.exittime;
						Previous_VE.destination=After_VE.destination;
						if (!DerivedEventList.contains(Previous_VE))
						{
							DerivedEventList.add(Previous_VE);
						}
										
					}
					else
					{
						VesselEvent NewEvent=new VesselEvent();
						NewEvent.entrycoordinates=AfterLocation.coordinates;
						NewEvent.entrytime=AfterLocation.recordtime;
						NewEvent.exitcoordinates=After_VE.exitcoordinates;
						NewEvent.exittime=After_VE.exittime;
						NewEvent.destination=After_VE.destination;		
						NewEvent.polygonid=Zone_Axsmarine_id;
						DerivedEventList.add(NewEvent);		
						
					}
					//Delete This Event from HBase				
					DeleteEvent(VTEvent,IMO_str,After_VE);	
				}
				
				VTEvent.flush();
				
				//pupulate Derived Events into Hbase
				
				for (VesselEvent newEvent : DerivedEventList)
				{
				    //rowkey: IMO(7)+timestamp(19 desc)+polygonid(8)
				    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
					
					context.getCounter(Counters.EVENT_UPSERTS ).increment(1);
					
					byte[] rowkey = Bytes.toBytes(IMO_str
							+ LpadNum(Long.MAX_VALUE - newEvent.entrytime, 19)+LpadNum(newEvent.polygonid,10));
					Put put = new Put(rowkey);

					put.addColumn(details, entrytime, Bytes.toBytes(new DateTime(newEvent.entrytime).toString(rawformatter)));
					put.addColumn(details, entrycoordinates, Bytes.toBytes(newEvent.entrycoordinates));
					put.addColumn(details, exittime, Bytes.toBytes(new DateTime(newEvent.exittime).toString(rawformatter)));
					put.addColumn(details, exitcoordinates, Bytes.toBytes(newEvent.exitcoordinates));
					put.addColumn(details, destination,	Bytes.toBytes(newEvent.destination));

					VTEvent.mutate(put);
					context.getCounter(Counters.EVENT_VALID).increment(1);				
				}
				
				//VTLocation.flush(); Moved to the first step
				VTEvent.flush();
			} catch (RuntimeException e) {
				// TODO Auto-generated catch block
				System.out.println("Exception occured while loading data for:"+key.getIMO());
				throw e;
			
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

		public static List<VesselLocation> getLocationsBetween(
				Table VTLocation_Table, String imo_str, long first_timestamp,long last_timestamp)
				throws IOException {

			// scan
			// 'cdb_vessel:vessel_location',{FILTER=>"(PrefixFilter('0000003162')"}
			Scan GetExistingLocations = new Scan();
			GetExistingLocations.setStartRow(Bytes.toBytes(imo_str + LpadNum(Long.MAX_VALUE - last_timestamp , 19)))
								.setStopRow(Bytes.toBytes(imo_str + LpadNum(Long.MAX_VALUE - first_timestamp+1, 19)));
			GetExistingLocations.setCaching(1000);

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
						VL.speed = Value;
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						VL.recordtime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("previouslocation")) {
						VL.previouslocation = Value;
					} else if (Qualifier.equals("nextlocation")) {
						VL.nextlocation = Value;
					}
				}
				result.add(VL);
			}

			Result_ExistingLocations.close();

			return result;

		}

		public static List<Delete> GetDeleteEventsBetween(Table VTEvent_Table,
				String imo_str, long first_timestamp, long last_timestamp) throws IOException {
			// scan
			// 'cdb_vessel:vessel_event',{FILTER=>"(PrefixFilter('0000003162')"}
			Scan GetEventsBetween = new Scan();
			GetEventsBetween.setStartRow(
					Bytes.toBytes(imo_str + LpadNum(Long.MAX_VALUE - last_timestamp, 19)+"0000000000")).setStopRow(
					Bytes.toBytes(imo_str + LpadNum(Long.MAX_VALUE - first_timestamp+1, 19)+"9999999999"))
					.addColumn(details, exittime);
			GetEventsBetween.setCaching(100);
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL ,new BinaryComparator(Bytes.toBytes(new DateTime(last_timestamp).toString(rawformatter))));
			GetEventsBetween.setFilter(ExistTimeValuefilter);

			ResultScanner Result_ExistingEvents = VTEvent_Table
					.getScanner(GetEventsBetween);
			List<Delete> deletes = new ArrayList<Delete>();

			for (Result res : Result_ExistingEvents) {
				deletes.add(new Delete(res.getRow()));
				
			}

			Result_ExistingEvents.close();
			return deletes;
		}
		
		public static void DeleteEvents(BufferedMutator VTEvent,List<Delete> deletes) throws IOException
		{
			VTEvent.mutate(deletes);
		}
		
		
		private static void DeleteEvent(BufferedMutator VTEvent,String iMO_str, VesselEvent after_VE) throws IOException {
			// TODO Auto-generated method stub
			
			byte[] rowkey=Bytes.toBytes(iMO_str+LpadNum(Long.MAX_VALUE - after_VE.entrytime, 19)+LpadNum(after_VE.polygonid,10));
			VTEvent.mutate(new Delete(rowkey));
			
		}
		
		public static VesselLocation getLastLocation(Table VTLocation_Table, String IMO_str) throws IOException
		{
			Scan getLastLocation = new Scan();
			getLastLocation.setStartRow(Bytes.toBytes(IMO_str +LpadNum(0, 19)));
			getLastLocation.setMaxResultSize(1);
			
			ResultScanner Result_LastLocation = VTLocation_Table
					.getScanner(getLastLocation);
			
			for (Result res : Result_LastLocation) {
				VesselLocation VL = new VesselLocation();

				for (Cell cell : res.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VL.coordinates = Value;
					} else if (Qualifier.equals("speed")) {
						VL.speed = Value;
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						VL.recordtime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("previouslocation")) {
						VL.previouslocation = Value;
					} else if (Qualifier.equals("nextlocation")) {
						VL.nextlocation = Value;
					}
				}
				
				Result_LastLocation.close();
				return VL;
			}

			Result_LastLocation.close();
			return null;
		}
		
		public static VesselLocation getLocationBefore (Table VTLocation_Table, String IMO_str, long timestamp) throws IOException
		{		
			Scan getLastLocation = new Scan();
			getLastLocation.setStartRow(Bytes.toBytes(IMO_str +LpadNum(Long.MAX_VALUE - timestamp+1, 19)));
			getLastLocation.setMaxResultSize(1);
			
			ResultScanner Result_LastLocation = VTLocation_Table
					.getScanner(getLastLocation);
			
			for (Result res : Result_LastLocation) {
				VesselLocation VL = new VesselLocation();

				for (Cell cell : res.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VL.coordinates = Value;
					} else if (Qualifier.equals("speed")) {
						VL.speed = Value;
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						VL.recordtime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("previouslocation")) {
						VL.previouslocation = Value;
					} else if (Qualifier.equals("nextlocation")) {
						VL.nextlocation = Value;
					}
				}
				
				Result_LastLocation.close();
				return VL;
			}

			Result_LastLocation.close();
			return null;
		}		
		
		public static VesselLocation getLocation(Table VTLocation_Table,String RowKey) throws IOException
		{
			if (RowKey!=null)
			{
				Get get = new Get(Bytes.toBytes(RowKey));			
				Result result = VTLocation_Table.get(get);
				
				VesselLocation VL = new VesselLocation();

				for (Cell cell : result.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("coordinates")) {
						VL.coordinates = Value;
					} else if (Qualifier.equals("speed")) {
						VL.speed = Value;
					} else if (Qualifier.equals("destination")) {
						VL.destination = Value;
					} else if (Qualifier.equals("timestamp")) {
						VL.recordtime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("previouslocation")) {
						VL.previouslocation = Value;
					} else if (Qualifier.equals("nextlocation")) {
						VL.nextlocation = Value;
					}
				}
				return VL;
			}
			else
			{
				return  null;
			}
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
			getEventStartedBeforeAndEndAfter.setCaching(100);
			
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.GREATER,new BinaryComparator(Bytes.toBytes(new DateTime(lastlocation.recordtime).toString(rawformatter))));
			getEventStartedBeforeAndEndAfter.setFilter(ExistTimeValuefilter);
			
			ResultScanner Result_eventcross = VTEvent_Table.getScanner(getEventStartedBeforeAndEndAfter);
			
			List<Put> puts = new ArrayList<Put>();
			for (Result res : Result_eventcross) {
				
				//vessel event table
			    //rowkey: imo(7)+timestamp(19 desc)+polygonid(8)
			    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
				byte[] rowkey=res.getRow();
				Put updateevent=new Put(rowkey);
				updateevent.addColumn(details, exittime, Bytes.toBytes(new DateTime(lastlocation.recordtime).toString(rawformatter)));
				updateevent.addColumn(details,coordinates, Bytes.toBytes(lastlocation.coordinates));
				updateevent.addColumn(details,destination, Bytes.toBytes(lastlocation.destination));
				puts.add(updateevent);
				
			}
			
			Result_eventcross.close();
			VTEvent_Table.put(puts);
		}
		
		
		//Get all events with exit at last location
		public static Map<Integer,VesselEvent> getAllEventsStartBeforeEndAfterBeforeLocation(Table VTEvent_Table, String IMO_str, VesselLocation location) throws IOException
		{
			Scan getAllEventsWithExistAtLastLocation=new Scan();
			getAllEventsWithExistAtLastLocation
			.setStartRow(Bytes.toBytes(IMO_str+LpadNum(Long.MAX_VALUE - location.recordtime, 19)+"0000000000"))
			.setStopRow(Bytes.toBytes(IMO_str+LpadNum(Long.MAX_VALUE, 19)+"9999999999"))
			.addColumn(details, exittime);
			getAllEventsWithExistAtLastLocation.setCaching(100);
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL ,new BinaryComparator(Bytes.toBytes(new DateTime(location.recordtime).toString(rawformatter))));
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
				VE.exittime = location.recordtime;
				VE.exitcoordinates = location.coordinates;	
				VE.destination = location.destination;
				VE.polygonid = Integer.parseInt(polygonid);

				for (Cell cell : result.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("entertime")) {
						VE.entrytime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("entercoordinates")) {
						VE.entrycoordinates = Value;
					}
				}
				
				events.put(VE.polygonid, VE);
			}

			Result_event.close();
			return events;
		}
		
		
		public static Map<Integer,VesselEvent> getAllEventsStartBeforeEndAfter(Table VTEvent_Table, String IMO_str, long recordtime) throws IOException
		{
			Scan getAllEventsWithExistAtLastLocation=new Scan();
			getAllEventsWithExistAtLastLocation
			.setStartRow(Bytes.toBytes(IMO_str+LpadNum(Long.MAX_VALUE - recordtime, 19)+"0000000000"))
			.setStopRow(Bytes.toBytes(IMO_str+LpadNum(Long.MAX_VALUE, 19)+"9999999999"))
			.addColumn(details, exittime);
			getAllEventsWithExistAtLastLocation.setCaching(100);
			
			Filter ExistTimeValuefilter =new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(new DateTime(recordtime).toString(rawformatter))));
			getAllEventsWithExistAtLastLocation.setFilter(ExistTimeValuefilter);
			
			ResultScanner Result_event = VTEvent_Table.getScanner(getAllEventsWithExistAtLastLocation);
			
			Map<Integer, VesselEvent> events=new HashMap<Integer,VesselEvent>();
			
			for (Result res : Result_event) {
				
				Get get = new Get(res.getRow());
				
				Result result = VTEvent_Table.get(get);
				String rowkey = Bytes.toString(result.getRow());
				String polygonid = rowkey.substring(26);

				VesselEvent VE = new VesselEvent();
				VE.polygonid = Integer.parseInt(polygonid);

				for (Cell cell : result.rawCells()) {
					String Qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String Value = Bytes.toString(CellUtil.cloneValue(cell));

					if (Qualifier.equals("entertime")) {
						VE.entrytime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("entercoordinates")) {
						VE.entrycoordinates = Value;
					} else if (Qualifier.equals("exittime")) {
						VE.exittime = DateTime.parse(Value, rawformatter).getMillis();
					} else if (Qualifier.equals("exitcoordinates")) {
						VE.exitcoordinates = Value;
					} else if (Qualifier.equals("destination")) {
						VE.destination = Value;
					}
				}
				
				events.put(VE.polygonid, VE);
			}

			Result_event.close();
			return events;			
			
		}
		
		
		private static String getVesselType(Table Vessel_Table,String IMO_str) throws IOException
		{
			 Get get = new Get(Bytes.toBytes(IMO_str));
			 get.addColumn(ves, TYPE);	

			 
			 Result result = Vessel_Table.get(get);
			 byte[] type = result.getValue(ves,TYPE);

			return Bytes.toString(type);
		}
		
		
		private static VesselTrackInfo getTrackInfo(Table TrackInfo_Table,String IMO_str) throws IOException
		{
			 Get get = new Get(Bytes.toBytes(IMO_str));
			 get.addColumn(details,lastlocation);
			 get.addColumn(details, firstrecordtime);
			 get.addColumn(details, lastrecordtime);
			 
			 Result result = TrackInfo_Table.get(get);

			 byte[] last_location=result.getValue(details,lastlocation);
			 byte[] fist_recordtime=result.getValue(details,firstrecordtime);
			 byte[] last_recordtime=result.getValue(details,lastrecordtime);
			 
			 VesselTrackInfo trackinfo=new VesselTrackInfo();
			 trackinfo.LastLocation=last_location;
			 
			 if (fist_recordtime!=null)
			 {
				 trackinfo.FirstRecordTime=DateTime.parse(Bytes.toString(fist_recordtime), rawformatter).getMillis();
			 }
				 
			 if (last_recordtime!=null)
			 {
				 trackinfo.LastRecordTime=DateTime.parse(Bytes.toString(last_recordtime), rawformatter).getMillis();
			 }
			 
			 return trackinfo;
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
	

	

	
	public static void main(String[] args) throws Exception {
		
		
		try {

			int exitCode = ToolRunner.run(
					new ImportVTLocationFromFileWithReducer(), args);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//unittest_reducer();

	}
	
	
	public static void unittest_reducer() throws Exception
	{
		///////////////////////////////////////////////////////
		///////////////////////////////////////////////////////		
		File WorkingFile = new File("D:\\test\\VTCurrentLocation\\9604770\\VTCurrentLocation_2015-12-31T23_54_10_947Z~2016-01-01T00_00_08_078Z.csv");

		//VesselZone.ZoneMap = VesselZone.DownloadAllZonesHbase();

		ArrayList<TextArrayWritable> LocationList = new ArrayList<TextArrayWritable>();

		BufferedReader BR = new BufferedReader(new InputStreamReader(
				new FileInputStream(WorkingFile)));

		String strRow = BR.readLine();

		CSVParser CSVP = new CSVParser(',', '"', '\\', true, false);

		Key_IMOAndRecordTime key = new Key_IMOAndRecordTime();

		boolean first = true;

		while (strRow != null)

		{

			//System.out.println(strRow);
			String[] nextrow = CSVP.parseLine(strRow);

			long IMO = Long.parseLong(nextrow[1].trim());

			String recordTime = nextrow[21].trim().substring(0, 19);

			long record_time = DateTime.parse(recordTime, rawformatter).getMillis();

			if (first)

			{

				key.set(new VLongWritable(IMO), new VLongWritable(record_time));

				first = false;

			}

			Text[] allfields = new Text[nextrow.length];

			for (int i = 0; i < nextrow.length; i++)

			{

				allfields[i] = new Text(nextrow[i]);

			}

			TextArrayWritable thisrow = new TextArrayWritable();

			thisrow.set(allfields);

			LocationList.add(thisrow);

			strRow = BR.readLine();

		}

		Connection connection = null;

		BufferedMutator VTLocation = null;

		BufferedMutator VTEvent = null;

		BufferedMutator VTVessel = null;

		BufferedMutator LastLocation_BM = null;
		
		BufferedMutator TrackInfo_BM = null;

		Table VTLocation_Table = null;

		Table VTEvent_Table = null;

		Table Vessel_Table = null;

		Table LastLocation_Table = null;
		
		Table TrackInfo_Table=null;

		HashMap<Integer, VesselZone> Zonemap = null;

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
		byte[] previouslocation=Bytes.toBytes("previouslocation");
		byte[] nextlocation=Bytes.toBytes("nextlocation");
		byte[] firstrecordtime=Bytes.toBytes("firstrecordtime");
		byte[] lastrecordtime=Bytes.toBytes("lastrecordtime");

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

		LastLocation_BM = connection.getBufferedMutator(LastLocation_Name);

		LastLocation_Table = connection.getTable(LastLocation_Name);
		
		TableName TrackInfo_Name = TableName
				.valueOf("cdb_vessel:vessel_track_info");
		TrackInfo_BM=connection.getBufferedMutator(TrackInfo_Name);
		TrackInfo_Table=connection.getTable(TrackInfo_Name);		

		try {

			File Zonemapfile = new File("VesselZone");

			ObjectInputStream OIS = new ObjectInputStream(new FileInputStream(
					Zonemapfile));

			Zonemap = (HashMap<Integer, VesselZone>) OIS.readObject();

			OIS.close();

		} catch (ClassNotFoundException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}		


		//////////////////////////////////////////////////////
		//////////////////////////////////////////////////////
		//////////////////////////////////////////////////////


		//context.getCounter(Counters.VESSEL_PROCESSED).increment(1);
		
		String IMO_str = LpadNum(key.getIMO().get(), 7);
		long first_pos_time = key.getRecordTime().get();
					
		
		/////////////////////////////////////////////////////////////////////////////////
		//Populate newPoints with new locations
		List<VesselLocation> newPoints=new ArrayList<VesselLocation>();
		
		for (TextArrayWritable rowcontent : LocationList) {
			// population location
			//context.getCounter(Counters.LOCATION_ROWS).increment(1);
			VesselLocation newlocation=new VesselLocation();
			
			try {

				Writable[] content = rowcontent.get();
				String Latitude = content[16].toString().trim();
				String Longitude = content[15].toString().trim();
				String Coordinates=Latitude + "," + Longitude;
				String Speed = content[18].toString().trim();
				String Destination = content[9].toString().trim();
				String Timestamp = content[21].toString().trim().substring(0, 19);
				
				long record_time = DateTime.parse(Timestamp, rawformatter).getMillis();
				newlocation.coordinates=Coordinates;
				newlocation.recordtime=record_time;
				newlocation.speed=Speed;
				newlocation.destination=Destination;
				//context.getCounter(Counters.LOCATION_VALID).increment(1);

			} catch (Exception e) {
				e.printStackTrace();
				//context.getCounter(Counters.LOCATION_ERROR).increment(1);
				continue;
			}
			
			newPoints.add(newlocation);
		}
		

		/////////////////////////////////////////////////////////////////////////////////
		//Get last new post time
		long last_pos_time=newPoints.get(newPoints.size()-1).recordtime;
				
		////////////////////////////////////////////////////////////////////////////////
		//Get Existing trackinfo
		VesselTrackInfo VTI=ImportReducer.getTrackInfo(TrackInfo_Table,IMO_str);			
		
		List<VesselLocation> AllBetweenPoints=new ArrayList<VesselLocation>();			
		
		String BeforeRowKey=null;
		String AfterRowKey=null;
		
		// //////////////////////////////////////////////////////////////////////////////
		// Retrieve all the existing locations between the first new location and the last new location.
		if ((VTI.FirstRecordTime !=null) && (VTI.LastRecordTime!=null))
		{
			
			if (last_pos_time < VTI.FirstRecordTime )
			{
				AfterRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - VTI.FirstRecordTime, 19);
			}
			else if (first_pos_time > VTI.LastRecordTime)
			{
				BeforeRowKey=IMO_str + LpadNum(Long.MAX_VALUE - VTI.LastRecordTime, 19);
			}
			else
			{
				AllBetweenPoints = ImportReducer.getLocationsBetween(VTLocation_Table, IMO_str, first_pos_time,last_pos_time);
				java.util.Collections.sort(AllBetweenPoints);				
				
				BeforeRowKey=AllBetweenPoints.get(0).previouslocation;
				AfterRowKey=AllBetweenPoints.get(AllBetweenPoints.size()-1).nextlocation;
				
				List<Delete> deletes=ImportReducer.GetDeleteEventsBetween(VTEvent_Table, IMO_str,first_pos_time,last_pos_time);
				ImportReducer.DeleteEvents(VTEvent,deletes);
				VTEvent.flush();
			}			
			
		}

		// Find out the location before the first new location in	
		VesselLocation BeforeLocation = ImportReducer.getLocation(VTLocation_Table,BeforeRowKey);
		
		// Find out the location after the last new location in			
		VesselLocation AfterLocation = ImportReducer.getLocation(VTLocation_Table,AfterRowKey);		
		
		
		Map<Integer, VesselEvent> PreviousZoneEvents=new HashMap<Integer, VesselEvent>();;
		Map<Integer, VesselEvent> AfterZoneEvents=new HashMap<Integer, VesselEvent>();
		
		if (BeforeLocation!=null)
		{
		//Get all events with exit at last location
			PreviousZoneEvents = ImportReducer.getAllEventsStartBeforeEndAfterBeforeLocation(VTEvent_Table,IMO_str,BeforeLocation);
		}

		
		////////////////////////////////////////////////////
		//Analyze and calculate previous and next location
		for (VesselLocation newlocation : newPoints) {
			
			int index=AllBetweenPoints.indexOf(newlocation);
			if (index!=-1)
			{
				VesselLocation dblocation=AllBetweenPoints.get(index);
				dblocation.coordinates=newlocation.coordinates;
				dblocation.destination=newlocation.destination;
				dblocation.speed=newlocation.speed;
			}	
			else
			{
				AllBetweenPoints.add(newlocation);		
			}
		}
		
		java.util.Collections.sort(AllBetweenPoints);
		
		String previousRowKey=null;
		
		for (VesselLocation location: AllBetweenPoints)
		{
			location.previouslocation=previousRowKey;
			previousRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - location.recordtime, 19);
		}
		
		String  NextRowKey=null;
		
		for (int i=(AllBetweenPoints.size()-1); i>=0;i-- )
		{
			VesselLocation location=AllBetweenPoints.get(i);
			location.nextlocation=NextRowKey;
			NextRowKey=IMO_str	+ LpadNum(Long.MAX_VALUE - location.recordtime, 19);
		}
		
		
		AllBetweenPoints.get(0).previouslocation=BeforeRowKey;
		AllBetweenPoints.get(AllBetweenPoints.size()-1).nextlocation=AfterRowKey;
		

		////////////////////////////////////////////////////
		//Upsert all locations
		
		for (VesselLocation location : AllBetweenPoints) {
			// population location			
			try {

				byte[] rowkey = Bytes.toBytes(IMO_str
						+ LpadNum(Long.MAX_VALUE - location.recordtime, 19));
				Put put = new Put(rowkey);

				put.addColumn(details, speed, Bytes.toBytes(location.speed));
				put.addColumn(details, destination,
						Bytes.toBytes(location.destination));
				put.addColumn(details, coordinates,
						Bytes.toBytes(location.coordinates));
				
				put.addColumn(details, timestamp, Bytes.toBytes(new DateTime(location.recordtime).toString(rawformatter)));
				
				if(location.previouslocation!=null)
				{
					put.addColumn(details, previouslocation, Bytes.toBytes(location.previouslocation));
				}

				if (location.nextlocation!=null)
				{
					put.addColumn(details, nextlocation, Bytes.toBytes(location.nextlocation));
				}

				VTLocation.mutate(put);
				

			} catch (Exception e) {
				e.printStackTrace();
				//context.getCounter(Counters.LOCATION_ERROR).increment(1);
				continue;
			}
			
		}
		
		//update before next location and after previous location
		
		if (BeforeRowKey!=null)
		{
			Put BeforeLocationPut = new Put(Bytes.toBytes(BeforeRowKey));
			BeforeLocationPut.addColumn(details, nextlocation, Bytes.toBytes(IMO_str+ LpadNum(Long.MAX_VALUE - AllBetweenPoints.get(0).recordtime, 19)));
			VTLocation.mutate(BeforeLocationPut);
		}

		if (AfterRowKey!=null)
		{

			Put AfterLocationPut = new Put(Bytes.toBytes(AfterRowKey));
			AfterLocationPut.addColumn(details, previouslocation, Bytes.toBytes(IMO_str+ LpadNum(Long.MAX_VALUE - AllBetweenPoints.get(AllBetweenPoints.size()-1).recordtime, 19)));
			VTLocation.mutate(AfterLocationPut);
		}
		
		VTLocation.flush();
					

		
		/////////////////////////////////////////////////////////////////////
		//Store latest location
		//rowkey: global zone id (4)+ longlat22 ((long11(sign(1)+integer(3)+digit(7)))(lat11(sign(1)+integer(3)+(7))))+imo(7)+recordtime(19)
		/////////////////////////////////////////////////////////////////////	
					
		Put vessel_track_info=new Put(Bytes.toBytes(IMO_str));
		
		if (AfterLocation==null)
		{
			//Get the last location
			VesselLocation lastLocation=AllBetweenPoints.get(AllBetweenPoints.size()-1);
			//update the last location
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
			
			if (VTI.LastLocation!=null)
			{
				LastLocation_BM.mutate(new Delete(VTI.LastLocation));					
			}
			
			byte[] lastlocationrowkey = Bytes.toBytes(LpadNum(BelongedGlobalZoneIndex,4) + ImportReducer.ConvertCoordinatesToStr(longlat[1])+ImportReducer.ConvertCoordinatesToStr(longlat[0]));
			Put lastlocation_put=new Put(lastlocationrowkey);	
			lastlocation_put.addColumn(details, imo, Bytes.toBytes(IMO_str));
			lastlocation_put.addColumn(details, timestamp, Bytes.toBytes(new DateTime(lastLocation.recordtime).toString(rawformatter)));
			LastLocation_BM.mutate(lastlocation_put);
			
			LastLocation_BM.flush();
			
			vessel_track_info.addColumn(details, lastlocation, lastlocationrowkey);
			vessel_track_info.addColumn(details, lastrecordtime, Bytes.toBytes(new DateTime(lastLocation.recordtime).toString(rawformatter)));

		}		
		else
		{
			//Get events that start before last new location and end after last new location				
			AfterZoneEvents = ImportReducer.getAllEventsStartBeforeEndAfter(VTEvent_Table,IMO_str,AfterLocation.recordtime);
		}
		
		//update firstrecordtime and lastrecordtime
		if (BeforeLocation == null)
		{
			vessel_track_info.addColumn(details, firstrecordtime, Bytes.toBytes(new DateTime(AllBetweenPoints.get(0).recordtime).toString(rawformatter)));
		}
		
		if (!vessel_track_info.isEmpty())
		{
			TrackInfo_BM.mutate(vessel_track_info);	
			TrackInfo_BM.flush();
		}

					
		////////////////////////////////////////////////////////////////////
		
		ArrayList<VesselEvent> DerivedEventList=new ArrayList<VesselEvent>();
		
		///////////////////////////////////////////////////////////
		//Get Vessel
		String VesselType=ImportReducer.getVesselType(Vessel_Table, IMO_str);
		
		if (VesselType ==null)
		{
			//context.getCounter(Counters.VESSEL_WITHOUTTYPE).increment(1);
			VesselType="Bulkers";
		}
		
		
		
		//calculating event
		for (VesselLocation VL : AllBetweenPoints)
		{
			ArrayList<Integer> CurrentZones = ImportReducer.LocateCurrentZone(VL.coordinates ,VesselType, Zonemap);
			
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
		
		///////////////////////////////////////////////////////////////////////////////////////
		//Merge with PreviousZoneEvents with AfterZoneEvents
		
		Iterator<Map.Entry<Integer, VesselEvent>> it = AfterZoneEvents.entrySet().iterator();  
		while (it.hasNext())
		{
			Map.Entry<Integer, VesselEvent> thisEntry=it.next();
			int Zone_Axsmarine_id = thisEntry.getKey();
			VesselEvent After_VE = thisEntry.getValue();
			
			VesselEvent Previous_VE=PreviousZoneEvents.get(Zone_Axsmarine_id);
			
			if (Previous_VE!=null)
			{
				Previous_VE.exitcoordinates=After_VE.exitcoordinates;
				Previous_VE.exittime=After_VE.exittime;
				Previous_VE.destination=After_VE.destination;
				if (!DerivedEventList.contains(Previous_VE))
				{
					DerivedEventList.add(Previous_VE);
				}
								
			}
			else
			{
				VesselEvent NewEvent=new VesselEvent();
				NewEvent.entrycoordinates=AfterLocation.coordinates;
				NewEvent.entrytime=AfterLocation.recordtime;
				NewEvent.exitcoordinates=After_VE.exitcoordinates;
				NewEvent.exittime=After_VE.exittime;
				NewEvent.destination=After_VE.destination;		
				NewEvent.polygonid=Zone_Axsmarine_id;
				DerivedEventList.add(NewEvent);		
				
			}
			//Delete This Event from HBase				
			ImportReducer.DeleteEvent(VTEvent,IMO_str,After_VE);	
		}
		
		VTEvent.flush();
		
		//pupulate Derived Events into Hbase
		
		for (VesselEvent newEvent : DerivedEventList)
		{
		    //rowkey: IMO(7)+timestamp(19 desc)+polygonid(8)
		    //qualifier:entrytime,entrycoordinates,exittime,exitcoordinates,destination
			
			//context.getCounter(Counters.EVENT_UPSERTS ).increment(1);
			
			byte[] rowkey = Bytes.toBytes(IMO_str
					+ LpadNum(Long.MAX_VALUE - newEvent.entrytime, 19)+LpadNum(newEvent.polygonid,10));
			Put put = new Put(rowkey);

			put.addColumn(details, entrytime, Bytes.toBytes(new DateTime(newEvent.entrytime).toString(rawformatter)));
			put.addColumn(details, entrycoordinates, Bytes.toBytes(newEvent.entrycoordinates));
			put.addColumn(details, exittime, Bytes.toBytes(new DateTime(newEvent.exittime).toString(rawformatter)));
			put.addColumn(details, exitcoordinates, Bytes.toBytes(newEvent.exitcoordinates));
			put.addColumn(details, destination,	Bytes.toBytes(newEvent.destination));

			VTEvent.mutate(put);
			//context.getCounter(Counters.EVENT_VALID).increment(1);				
		}
		
		//VTLocation.flush(); Moved to the first step
		VTEvent.flush();
	
			
	}
	
	public static void unittest_debug() throws Exception
	{
				
		long record_time = DateTime.parse("2015-12-10T00:18:40", rawformatter).getMillis();
		long abc=Long.MAX_VALUE-record_time;
		System.out.println(new DateTime(Long.MAX_VALUE-9223370587148055807L).toString(rawformatter));
		
		
	}
	
}
