package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
		extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private StopWordsFilter filter = new StopWordsFilter();

	@Override
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String currentWord = itr.nextToken();
			if (filter.shouldFilter(currentWord)) {
				continue;
			}

			word.set(currentWord);
			context.write(word, one);
		}
	}
}