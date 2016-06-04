package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hagai_lvi on 03/06/2016.
 */
public class Stage3Reducer extends Reducer<Text, Text, Text, Text> {

	private String currentWord = null;
	private String currentDecade = null;
	private int currentKeyCount = -1;



	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String seperator = "\t";
			String decade = value.toString().split(seperator)[0];
			String words = value.toString().split(seperator)[1];
			int count = Integer.parseInt(value.toString().split(seperator)[2]);

			if (key.toString().endsWith("*")) {
				assert currentWord == null;
				assert currentDecade == null;
				assert currentKeyCount == -1;

				// This is the first word, the key is the decade + first word. remove the "*"
				this.currentWord = words;
				this.currentDecade = decade;
				this.currentKeyCount = count;
				// TODO assert that the iterator has no more values

				return;
			}

			assert currentKeyCount != -1;

			String word2 = value.toString().split(seperator)[3];
			String count2 = value.toString().split(seperator)[4];

			String valueToEmit = String.join(
					Stage3Mapper.SEPERATOR, decade,
					words, Integer.toString(count),
					currentWord, Integer.toString(currentKeyCount),
					word2, count2);

			context.write(
					new Text(decade + seperator + words),
					new Text(
							valueToEmit
					));

		}

		// Finished with this key, make everything null to verify correctness

		currentKeyCount = -1;
		currentDecade = null;
		currentWord = null;

	}
}
