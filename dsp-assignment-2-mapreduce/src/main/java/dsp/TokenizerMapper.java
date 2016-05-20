package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

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

	/**
	 * Return a list of 2grams from the given ngram.
	 * If ngram is a 1-gram, then returns an empty list
	 * @throws AssertionError if the string is not of the expected format
	 */
	List<String> ngramTo2gram(String ngram) {
		String[] split = ngram.split(" ");

		assert split.length >= 1;

		if (split.length == 1) {
			// 1grams are ignored
			return new LinkedList<String>();
		}
		else if (split.length == 2) {
			String sorted = sort2gram(ngram);
			return Collections.singletonList(sorted);
		}
		else {
			int middle = split.length / 2;
			List<String> res = new LinkedList<String>();

			for (int i = 0 ; i < split.length ; i++ ){
				if (i != middle) {
					res.add(sort2gram(split[i] + " " + split[middle]));
				}
			}
			return res;
		}
	}

	/**
	 * sorts a 2 gram lexicographically
	 * @throws AssertionError if this is not a 2gram
	 */
	String sort2gram(String twoGram) {
		String[] split = twoGram.split(" ");
		assert split.length == 2;
		if (split[0].compareTo(split[1]) < 0 ){
			return split[0] + " " + split[1];
		}
		else {
			return split[1] + " " + split[0];
		}
	}
}