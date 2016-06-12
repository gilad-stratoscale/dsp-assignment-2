package dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMapReduce {

	private MapReduceDriver<Object, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();

		Reducer<Text, LongWritable, Text, LongWritable> reducer = new IntSumReducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduce() throws IOException {
		addInput("' leaves open the possibility\t1981\t4\t4\t3");
		addInput("\"\"\" During the final stages\"\t1994\t3\t3\t3\n");
		addInput("\"! \"\" \"\" It ought\"\t1893\t1\t1\t1");
		mapReduceDriver.withOutput(new Text("1980\tleaves"), new LongWritable(4));
		mapReduceDriver.withOutput(new Text("1980\tleaves open"), new LongWritable(4));
		mapReduceDriver.withOutput(new Text("1980\topen"), new LongWritable(4));
		mapReduceDriver.withOutput(new Text("1980\topen possibility"), new LongWritable(4));
		mapReduceDriver.withOutput(new Text("1980\tpossibility"), new LongWritable(4));
		mapReduceDriver.withOutput(new Text("1990\tfinal"), new LongWritable(3));
		mapReduceDriver.withOutput(new Text("1990\tfinal stages"), new LongWritable(3));
		mapReduceDriver.withOutput(new Text("1990\tstages"), new LongWritable(3));

		mapReduceDriver.runTest();
	}

	/**
	 * regression test for a dot in the input
	 */
	@Test
	public void testMapReduceRegression() throws IOException {
		addInput("\"\"\" 2 Cor . iv\"\t1900\t2\t2\t2");
		mapReduceDriver.withOutput(new Text("1900\tcor"), new LongWritable(2));
		mapReduceDriver.withOutput(new Text("1900\tcor iv"), new LongWritable(2));
		mapReduceDriver.withOutput(new Text("1900\tiv"), new LongWritable(2));
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
