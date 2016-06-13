package dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class TokenizerMapper
		extends Mapper<Object, Text, Text, LongWritable> {

	public static final int MIN_DECADE = 1900;
	private StopWordsFilter filter = new StopWordsFilter();
	//final static Logger logger = Logger.getLogger(TokenizerMapper.class);


	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

        System.out.println("WORD COUNT SAYS: Got key=\n" + key + "\nValue=\n" + value.toString());

		String[] splits = value.toString().split("\t");
		String ngram = splits[0].toLowerCase();

		incrementWordCounter(context, ngram);

		int decade = getDecade(splits[1]);
		if (decade < MIN_DECADE) {
			return;
		}
		LongWritable count = new LongWritable(Long.parseLong(splits[2]));

		if (ngram.split(" ").length < 0) {
			return;
		}

        String withoutPunctuationAndNumbers = removePunctuationAndNumbers(ngram);
		String withoutStopWords = removeStopWords(withoutPunctuationAndNumbers);

		if (withoutStopWords.length() == 0) {
			return;
		}

		// Due to removing punctuation, numbers and stopwords there might be a situation in which
		// there are 2 spaces in a row
		String trimWhitspaces = withoutStopWords.replaceAll("\\s+"," ").trim();
		List<String> twoGrams = ngramTo2gram(trimWhitspaces);

		for (String s : trimWhitspaces.split(" ")) {
			context.write(new Text(decade + "\t" + s), count);

		}
		for (String twoGram : twoGrams) {
			context.write(new Text(decade + "\t" + twoGram), count);
		}

	}

	private void incrementWordCounter(Context context, String ngram) {
		context.getCounter(Constants.Counters.WORD).increment(ngram.split(" ").length);
	}

	/**
	 * Return the decade of a year
	 */
	int getDecade(String year) {
		int yearInt = Integer.parseInt(year);
		return yearInt - (yearInt % 10);
	}

	public String removePunctuationAndNumbers (String s) {
        return s.replaceAll("[']","").replaceAll("[,-]"," ").replaceAll("[^A-Za-z\\s]","").trim();
    }


    /**
	 * Remove all the stop words from the string
	 * Assumes that the string has exactly single space between each 2 words
	 */
	String removeStopWords(String s) {
		String[] split = s.split(" ");
		String res = Arrays.stream(split).filter((String x) -> !filter.shouldFilter(x)).
				collect(Collectors.joining(" "));

		return res;
	}

	/**
	 * Return a list of 2grams from the given ngram.
	 * If ngram is a 1-gram, then returns an empty list
	 *
	 * @throws AssertionError if the string is not of the expected format
	 */
	List<String> ngramTo2gram(String ngram) {
		String[] split = ngram.split(" ");

		assert split.length >= 1;

		if (split.length == 1) {
			// 1grams are ignored
			return new LinkedList<String>();
		} else if (split.length == 2) {
			String sorted = sort2gram(ngram);
			return Collections.singletonList(sorted);
		} else {
			int middle = split.length / 2;
			List<String> res = new LinkedList<String>();

			for (int i = 0; i < split.length; i++) {
				if (i != middle) {
					res.add(sort2gram(split[i] + " " + split[middle]));
				}
			}
			return res;
		}
	}

	/**
	 * sorts a 2 gram lexicographically
	 *
	 * @throws AssertionError if this is not a 2gram
	 */
	String sort2gram(String twoGram) {
		String[] split = twoGram.split(" ");
		assert split.length == 2 : "split.length is " + split.length;
		if (split[0].compareTo(split[1]) < 0) {
			return split[0] + " " + split[1];
		} else {
			return split[1] + " " + split[0];
		}
	}
}