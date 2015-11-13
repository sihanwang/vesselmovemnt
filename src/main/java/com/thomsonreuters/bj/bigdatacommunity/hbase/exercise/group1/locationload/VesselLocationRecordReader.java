package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import au.com.bytecode.opencsv.CSVParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;



public class VesselLocationRecordReader extends RecordReader<Key_ShipIDAndRecordTime, TextArrayWritable> {

	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private Key_ShipIDAndRecordTime key= new Key_ShipIDAndRecordTime();
	private TextArrayWritable value = new TextArrayWritable();
	private boolean ReachEnd = false;
	private FSDataInputStream in = null;
	private long file_length;
	private long currentpos=0L;
	private CSVParser CSVP=new CSVParser(',','"','\\',true,false);
	private BufferedReader BR;
	
	
	public VesselLocationRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException
	{
		System.out.println("***************Constructor is called with index:"+index +"************************");
		Configuration conf = context.getConfiguration();

		Path file =split.getPath(index);;
		System.out.println("***************split.getPath("+index +"):"+file.toString() +"************************");
				
		long filelength=split.getLength(index);
		System.out.println("***************split.getLength("+index +"):"+filelength +"************************");
		
		long fileoffset=split.getOffset(index);
		System.out.println("***************split.getOffset("+index +"):"+fileoffset +"************************");
		
		FileSystem fs = file.getFileSystem(conf);

		in = fs.open(file);
		ZipInputStream zis=new ZipInputStream(in);
		ZipEntry ze=zis.getNextEntry();
		if (ze!=null)
		{
			String FileName = ze.getName();
			file_length=ze.getSize();
			if ((FileName.indexOf("VTCurrentLocation") >= 0 ))
			{
				BR=new BufferedReader(new InputStreamReader(zis));
			}
			else
			{
				ReachEnd=true;
			}
		}
		else
		{
			BR.close();
			IOUtils.closeStream(in);
			ReachEnd=true;
		}
		
		System.out.println("***************Constructor is finished with index:"+index +"************************");
		
		System.out.println();
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Key_ShipIDAndRecordTime getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentpos/(float)file_length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		  // Won't be called, use custom Constructor
		  // `CFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)`
		  // instead
		

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {


		// TODO Auto-generated method stub
		if (!ReachEnd) {

			String strRow=BR.readLine();
			
			if (strRow!=null)
			{
				String[] nextrow=CSVP.parseLine(strRow);

				currentpos=currentpos+strRow.getBytes().length;
				
				long shipID=Long.parseLong(nextrow[0].trim());
				String recordTime=nextrow[21].trim().substring(0, 19);
				
				ParsePosition pos = new ParsePosition(0);
				long record_time=formatter.parse(recordTime, pos).getTime();
				
				key.set(new VLongWritable(shipID), new VLongWritable(record_time));
				
				Text[] allfields=new Text[nextrow.length];

				for(int i=0;i<nextrow.length;i++)
				{
					allfields[i]=new Text(nextrow[i]);					
				}
				
				value.set(allfields);

				return true;
			}
			else
			{
				IOUtils.closeStream(in);
				BR.close();
				ReachEnd=true;
				return false;
			}
		}
		else
		{
			return false;
		}

	}


}
