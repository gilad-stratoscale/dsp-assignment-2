package dsp.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Stage2Reducer extends Reducer<Text, Text, Text, Text>  {

	private String currentWord = null;
	private String currentDecade = null;
	private long currentKeyCount = -1;

	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace(System.err));
		for (Text value : values) {
			String seperator = "\t";
			String decade = value.toString().split(seperator)[0];
			String words = value.toString().split(seperator)[1];
			Long count;
			try {
				count = Long.parseLong(value.toString().split(seperator)[2]);
			}catch (Exception e) {
				throw new RuntimeException(key.toString() + "\n" + value.toString(),e);
			}
			if (key.toString().endsWith("*")) {

				// This is the first word, the key is the decade + first word. remove the "*"
				this.currentWord = words;
				this.currentDecade = decade;
				this.currentKeyCount = count;

				context.write(new Text(currentDecade + seperator + currentWord), new Text(currentWord + seperator + currentKeyCount));
				return;
			}

			assert currentKeyCount != -1;

			String valueToEmit = words + seperator + count + seperator + currentWord + seperator + currentKeyCount;
			context.write(
					new Text(decade + seperator + words.split(" ")[1]), // Get the second word as a key
					new Text(
							valueToEmit
					));

		}

		// Finished with this key, make everything null to verify correctness

		currentKeyCount = -1;
		currentDecade = null;
		currentWord = null;

	}

	static String inverseTwoGram(String twoGram) {
		assert twoGram.split(" ").length == 2;

		return twoGram.split(" ")[1] +
				" " +
				twoGram.split(" ")[0];
	}
}
