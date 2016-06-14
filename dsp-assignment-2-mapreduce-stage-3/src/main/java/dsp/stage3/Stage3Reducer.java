package dsp.stage3;

import dsp.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



/**
 * Created by hagai_lvi on 03/06/2016.
 */
public class Stage3Reducer extends Reducer<Text, Text, Text, Text> {

	private String currentWord = null;
	private String currentDecade = null;
	private long currentKeyCount = -1;



	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String seperator = "\t";
			String decade = key.toString().split(seperator)[0];
			String words = value.toString().split(seperator)[0];
			long count = Long.parseLong(value.toString().split(seperator)[1]);

			if (key.toString().endsWith("*")) {

				// This is the first word, the key is the decade + first word. remove the "*"
				this.currentWord = words;
				this.currentDecade = decade;
				this.currentKeyCount = count;
				// TODO assert that the iterator has no more values

				return;
			}

			assert currentKeyCount != -1;

			String word2 = value.toString().split(seperator)[2];
			String count2 = value.toString().split(seperator)[3];

            String counterValueStr = context.getConfiguration().get(Constants.COUNTER_NAME_PREFIX+decade);
            long totalWords= -1;
            try {
                if (counterValueStr !=null) {
                    totalWords = Long.parseLong(counterValueStr);
                }
            }
            catch(NumberFormatException e) {
                e.printStackTrace(System.err);
				throw e;
            }

			String valueToEmit = String.join(
					Stage3Mapper.SEPERATOR,
					Long.toString(count),
					currentWord,
					Long.toString(currentKeyCount),
					word2,
					count2,
					Double.toString(calcPmi(count, Long.parseLong(count2), currentKeyCount, totalWords)));

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

	private double calcPmi(long count, long count1, long count2, long totalWords) {
		return logBaseTwo(count) + logBaseTwo(totalWords) - logBaseTwo(count1) - logBaseTwo(count2);
	}

	private double logBaseTwo(double i) {
		return Math.log(i) / Math.log(2);
	}
}
