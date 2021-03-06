package dsp.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hagai_lvi on 31/05/2016.
 */
public class Stage2ReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, Text, Text, Text> reducer = new Stage2Reducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void reduce() throws IOException {

		List<Text> firstList = new LinkedList<>();
		String seperator = "\t";
		Text firstKey = new Text("1990" + seperator + "good" + seperator + "2");
		firstList.add(firstKey);
		reduceDriver.withInput(new Text("1990" + seperator + "good *"), firstList);

		List<Text> values = new LinkedList<>();
		values.add(new Text("1990" + seperator + "good boy" + seperator + "3"));
		values.add(new Text("1990" + seperator + "good girl" + seperator + "4"));
		reduceDriver.withInput(new Text("good"),values);

		reduceDriver.withOutput(
				new Text("1990" + seperator + "good"),
				new Text("good" + seperator + "2"));
		reduceDriver.withOutput(
				new Text("1990" + seperator + "boy"),
				new Text("good boy" + seperator + "3" + seperator + "good" + seperator + "2"));
		reduceDriver.withOutput(
				new Text("1990" + seperator + "girl"),
				new Text("good girl" + seperator + "4" + seperator + "good" + seperator + "2"));
		reduceDriver.runTest();
	}

	@Test
	public void inverseTwoGram() throws Exception {
		Assert.assertEquals("def abc", Stage2Reducer.inverseTwoGram("abc def"));
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException() throws Exception {
		Stage2Reducer.inverseTwoGram("");
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException2() throws Exception {
		Stage2Reducer.inverseTwoGram("abc");
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException3() throws Exception {
		Stage2Reducer.inverseTwoGram("abc def ghi");
	}

}