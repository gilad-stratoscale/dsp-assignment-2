package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopNMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String decade = value.toString().split("\t")[0];
		String pair = value.toString().split("\t")[1];
		String pmi = value.toString().split("\t")[7];

		context.write(new Text(decade + "\t" + pmi + "\t" + pair), new Text(""));
		context.write(new Text(decade + "\t~\t~"), new Text(""));
	}
}
