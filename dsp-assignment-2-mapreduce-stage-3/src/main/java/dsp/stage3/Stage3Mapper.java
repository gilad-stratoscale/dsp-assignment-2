package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hagai_lvi on 03/06/2016.
 */
public class Stage3Mapper extends Mapper<Object, Text, Text, Text> {

	public static final String SEPERATOR = "\t";

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String keyPostfix = getKeyPostfix(value.toString());
		context.write(
				new Text(getKey(value.toString()) + " " + keyPostfix),
				new Text(getValue(value.toString()))
		);
	}

	/**
	 * Return "+" for 2grams and "*" for 1grams
	 */
	static String getKeyPostfix(String s) {
		return s.split(SEPERATOR)[3].contains(" ") ? "+" : "*";
	}

	/**
	 * extract the key from the line
	 */
	String getKey(String line) {
		int i = line.indexOf(
				SEPERATOR,
				line.indexOf(SEPERATOR)+1
		);
		return line.substring(0, i);
	}

	/**
	 * Extract the value from the line
	 */
	String getValue(String line) {
		int i = line.indexOf(
				SEPERATOR,
				line.indexOf(SEPERATOR)+1
		);
		return line.substring(i + 1);
	}
}
