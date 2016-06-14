package dsp.stage3;

import dsp.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hagai_lvi on 13/06/2016.
 */
public class Stage3MapReduceTest {

	private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		Stage3Mapper mapper = new Stage3Mapper();

		Reducer<Text, Text, Text, Text> reducer = new Stage3Reducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void test() throws IOException {

		String year = "1990", word="aaaa", count="10";

		mapReduceDriver.getConfiguration().set(Constants.COUNTER_NAME_PREFIX+year,"1000");

		mapReduceDriver.addInput(new Text(),
				new Text(
						String.join(Stage3Mapper.SEPERATOR, year, word, word, count)
				));


		String word1 = "aaaa";
		String word2 = "aaaa";
		String count1 = "2";
		String count2 = "10";
		mapReduceDriver.addInput(new Text(),
				new Text(
						String.join(Stage3Mapper.SEPERATOR, year, word2, word1 + " " + word2, count1, word1, count2)
				));
		mapReduceDriver.withOutput(
				new Text(String.join(Stage3Mapper.SEPERATOR, year, "aaaa" + " " + "aaaa")),
				new Text(String.join(Stage3Mapper.SEPERATOR, "2", "aaaa", "10", "aaaa", "10", "4.321928094887362")));
		mapReduceDriver.runTest();

	}

}
