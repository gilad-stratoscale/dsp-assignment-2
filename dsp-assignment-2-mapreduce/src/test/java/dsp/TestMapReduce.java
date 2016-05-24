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
		addInput("377500\t' leaves open the possibility\t1981\t4\t4\t3");
		addInput("75500\t\"\"\" During the final stages\"\t1994\t3\t3\t3\n");
		addInput("500\t\"! \"\" \"\" It ought\"\t1893\t1\t1\t1");
		mapReduceDriver.withOutput(new Text("final stages"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("leaves open"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("open possibility"), new IntWritable(1));

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
