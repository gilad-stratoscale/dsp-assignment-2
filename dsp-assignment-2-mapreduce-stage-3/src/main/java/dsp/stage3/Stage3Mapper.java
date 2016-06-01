package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Stage3Mapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().contains(" ")) {
			// This is a 2-gram
			context.write(
					// the key is the first word followed by the '+' sign
					new Text(value.toString().split(",")[0].split(" ")[0] + " +"),
					value);
		}
		else {
			// This is a single word
			context.write(
					new Text(value.toString().split(",")[0] + " *"), value);
		}
	}

}