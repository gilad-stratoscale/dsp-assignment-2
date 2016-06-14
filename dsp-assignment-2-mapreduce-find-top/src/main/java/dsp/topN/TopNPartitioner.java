package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopNPartitioner extends Partitioner<Text,Text> {
	@Override
	public int getPartition(Text key, Text value, int numOfPartitions) {
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace(System.err));
		return Math.abs(key.toString().split("\t")[0].hashCode() % numOfPartitions);
	}
}
