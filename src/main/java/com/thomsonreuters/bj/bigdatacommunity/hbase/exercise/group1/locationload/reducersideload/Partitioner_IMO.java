package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;


import org.apache.hadoop.mapreduce.Partitioner;


public class Partitioner_IMO extends Partitioner<Key_IMOAndRecordTime, TextArrayWritable> {

	
	@Override
	public int getPartition(Key_IMOAndRecordTime key, TextArrayWritable value,
			int numReduceTasks) {
		// TODO Auto-generated method stub
		
		return (key.getIMO().hashCode() & Integer.MAX_VALUE) % numReduceTasks;

	}
}
