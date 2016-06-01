package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Stage3Reducer extends Reducer<Text, Text, Text, Text>  {

	private String currentKey = null;
	private int currentKeyCount = -1;

	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			String words = value.toString().split(",")[0];
			int count = Integer.parseInt(value.toString().split(",")[1]);

			if (key.toString().endsWith("*")) {
				// This is the count of the first word
				this.currentKey = key.toString().split(" ")[0];
				this.currentKeyCount = count;
				// TODO assert that the iterator has no more values

				context.write(new Text(currentKey), new Text(currentKey + "," + currentKeyCount));
				return;
			}

			assert currentKeyCount != -1;

			String valueToEmit = words + "," + count + "," + currentKey + "," + currentKeyCount;
			context.write(
					new Text(words.split(" ")[1]), // Get the second word as a key
					new Text(
							valueToEmit
					));

		}
	}

	static String inverseTwoGram(String twoGram) {
		assert twoGram.split(" ").length == 2;

		return twoGram.split(" ")[1] +
				" " +
				twoGram.split(" ")[0];
	}
}
