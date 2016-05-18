package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMapReduce {

	private MapDriver<Object, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
	private MapReduceDriver<Object, Text, Text, IntWritable,Text,IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();
		mapDriver = MapDriver.newMapDriver(mapper);

		Reducer<Text,IntWritable,Text,IntWritable> reducer = new IntSumReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(
				new Text(),// Just for serialization...
				new Text(
						"abc abc ggg xxx\n" +
								"xxx ccc"
				));

		mapReduceDriver.withOutput(new Text("abc"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("ccc"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("ggg"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("xxx"), new IntWritable(2));

		mapReduceDriver.runTest();
	}
}
