package dsp.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hagai_lvi on 31/05/2016.
 */
public class Stage2Partitioner extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text value, int numOfPartitions) {
		return key.toString().split(" ")[0].hashCode() % numOfPartitions;
	}
}

