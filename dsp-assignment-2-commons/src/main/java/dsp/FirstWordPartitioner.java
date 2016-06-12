package dsp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * partition according to the first word of the key
 */
public class FirstWordPartitioner extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text value, int numOfPartitions) {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace(System.err));
		return key.toString().split(" ")[0].hashCode() % numOfPartitions;
	}
}


