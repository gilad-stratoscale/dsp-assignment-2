package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIntSumReducer {
	private ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text,IntWritable,Text,IntWritable> reducer = new IntSumReducer();
		reduceDriver = ReduceDriver.newReduceDriver();
		reduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));
		reduceDriver.withInput(new Text("abc"), values);
		reduceDriver.withOutput(new Text("abc"), new IntWritable(6));
		reduceDriver.runTest();
	}
}
