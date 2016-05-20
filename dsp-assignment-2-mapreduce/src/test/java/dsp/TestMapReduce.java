package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMapReduce {

	private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();

		Reducer<Text, IntWritable, Text, IntWritable> reducer = new IntSumReducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduce() throws IOException {
		addInput("abc abc ggg xxx xxx");
		addInput("abc abc ggg xxx xxx");

		mapReduceDriver.withOutput(new Text("abc ggg"), new IntWritable(4));
		mapReduceDriver.withOutput(new Text("ggg xxx"), new IntWritable(4));

		mapReduceDriver.runTest();
	}

	private void addInput(String text) {
		mapReduceDriver.withInput(
				new Text(),// Just for serialization...
				new Text(
						text
				));
	}
}
