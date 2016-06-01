package dsp.stage3;

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
public class Stage3ReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, Text, Text, Text> reducer = new Stage3Reducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void reduce() throws IOException {

		List<Text> firstList = new LinkedList<>();
		Text firstKey = new Text("good,2");
		firstList.add(firstKey);
		reduceDriver.withInput(new Text("good *"), firstList);

		List<Text> values = new LinkedList<>();
		values.add(new Text("good boy,3"));
		values.add(new Text("good girl,4"));
		reduceDriver.withInput(new Text("good"),values);

		reduceDriver.withOutput(new Text("good"), new Text("good,2"));
		reduceDriver.withOutput(new Text("boy"), new Text("good boy,3,good,2"));
		reduceDriver.withOutput(new Text("girl"), new Text("good girl,4,good,2"));
		reduceDriver.runTest();
	}

	@Test
	public void inverseTwoGram() throws Exception {
		Assert.assertEquals("def abc", Stage3Reducer.inverseTwoGram("abc def"));
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException() throws Exception {
		Stage3Reducer.inverseTwoGram("");
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException2() throws Exception {
		Stage3Reducer.inverseTwoGram("abc");
	}

	@Test(expected = AssertionError.class)
	public void inverseTwoGramThrowException3() throws Exception {
		Stage3Reducer.inverseTwoGram("abc def ghi");
	}

}