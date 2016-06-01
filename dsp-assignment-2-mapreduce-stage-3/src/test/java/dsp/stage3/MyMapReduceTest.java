package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MyMapReduceTest {
	private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		Stage3Mapper mapper = new Stage3Mapper();

		Reducer<Text, Text, Text, Text> reducer = new Stage3Reducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.withInput(new Text(), new Text("abc def,1"));
		mapReduceDriver.withInput(new Text(), new Text("abc,2"));
		mapReduceDriver.withInput(new Text(), new Text("abc xyz,1"));

		mapReduceDriver.withInput(new Text(), new Text("aaa bbb,3"));
		mapReduceDriver.withInput(new Text(), new Text("aaa ccc,2"));
		mapReduceDriver.withInput(new Text(), new Text("aaa,5"));

		mapReduceDriver.withOutput(new Text("aaa"), new Text("aaa,5"));

		mapReduceDriver.withOutput(new Text("bbb"), new Text("aaa bbb,3,aaa,5"));
		mapReduceDriver.withOutput(new Text("ccc"), new Text("aaa ccc,2,aaa,5"));

		mapReduceDriver.withOutput(new Text("abc"), new Text("abc,2"));

		mapReduceDriver.withOutput(new Text("def"), new Text("abc def,1,abc,2"));
		mapReduceDriver.withOutput(new Text("xyz"), new Text("abc xyz,1,abc,2"));

		mapReduceDriver.runTest();
	}
}
