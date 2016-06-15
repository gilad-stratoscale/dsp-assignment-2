package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopNReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, Text, Text, Text> reducer = new TopNReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void reduce() throws Exception {
		reduceDriver.getConfiguration().setInt("N",2);
		List<Text> lst = Collections.singletonList(new Text(""));
		reduceDriver.withInput(new Text("1980\t125.1231\tddd bbb"),lst);
		reduceDriver.withInput(new Text("1980\t124.1231\tccc bbb"),lst);
		reduceDriver.withInput(new Text("1980\t123.1231\taaa bbb"),lst);
		reduceDriver.withInput(new Text("1980\t~\t~"),lst);
		reduceDriver.withInput(new Text("1980\t~\t~"),lst);
		reduceDriver.withInput(new Text("1980\t~\t~"),lst);


		reduceDriver.withOutput(new Text("1980\t125.1231\tddd bbb"), new Text(""));
		reduceDriver.withOutput(new Text("1980\t124.1231\tccc bbb"), new Text(""));
		reduceDriver.runTest();
	}

	@Test
	public void reduce2() throws Exception {
		reduceDriver.getConfiguration().setInt("N",2);
		List<Text> lst = Collections.singletonList(new Text(""));
		reduceDriver.withInput(new Text("1980\t125.1231\tddd bbb"),lst);
		reduceDriver.withInput(new Text("1980\t~\t~"),lst);


		reduceDriver.withOutput(new Text("1980\t125.1231\tddd bbb"), new Text(""));
		reduceDriver.runTest();
	}

	@Test
	public void getDecade() throws Exception {

	}

	@Test
	public void getPmi() throws Exception {

	}

	@Test
	public void cleanSet() throws Exception {
		TopNReducer t = new TopNReducer();

		int n = 2;
		TreeSet<String> set = getSet();
		t.cleanSet(set, n);
		Assert.assertEquals(2, set.size());
		Assert.assertTrue(set.contains("1980\t124.1231\tccc bbb"));
		Assert.assertTrue(set.contains("1980\t125.1231\tddd bbb"));

	}

	private TreeSet<String> getSet() {
		TreeSet<String> res = new TreeSet<>();

		res.add("1980\t123.1231\taaa bbb");
		res.add("1980\t124.1231\tccc bbb");
		res.add("1980\t125.1231\tddd bbb");
		return res;
	}

}