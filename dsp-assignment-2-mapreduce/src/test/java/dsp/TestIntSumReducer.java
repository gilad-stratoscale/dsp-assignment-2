package dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIntSumReducer {
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, LongWritable, Text, LongWritable> reducer = new IntSumReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		List<LongWritable> values = new ArrayList();
		values.add(new LongWritable(1));
		values.add(new LongWritable(2));
		values.add(new LongWritable(3));
		reduceDriver.withInput(new Text("abc"), values);
		reduceDriver.withOutput(new Text("abc"), new LongWritable(6));
		reduceDriver.runTest();
	}
}
