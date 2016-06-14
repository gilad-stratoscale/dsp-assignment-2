package dsp.stage2;

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
		Stage2Mapper mapper = new Stage2Mapper();

		Reducer<Text, Text, Text, Text> reducer = new Stage2Reducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.withInput(new Text(), new Text("2000\tabc def\t1"));
		mapReduceDriver.withInput(new Text(), new Text("2000\tdef\t1"));
		mapReduceDriver.withInput(new Text(), new Text("2000\tabc\t2"));
		mapReduceDriver.withInput(new Text(), new Text("2000\tabc xyz\t1"));
		mapReduceDriver.withInput(new Text(), new Text("2000\txyz\t1"));

		mapReduceDriver.withInput(new Text(), new Text("1990\taaa bbb\t3"));
		mapReduceDriver.withInput(new Text(), new Text("1990\tbbb\t3"));
		mapReduceDriver.withInput(new Text(), new Text("1990\taaa ccc\t2"));
		mapReduceDriver.withInput(new Text(), new Text("1990\tccc\t2"));
		mapReduceDriver.withInput(new Text(), new Text("1990\taaa\t5"));

		mapReduceDriver.withOutput(new Text("1990\taaa"), new Text("aaa\t5"));

		mapReduceDriver.withOutput(new Text("1990\tbbb"), new Text("aaa bbb\t3\taaa\t5"));
		mapReduceDriver.withOutput(new Text("1990\tccc"), new Text("aaa ccc\t2\taaa\t5"));

		mapReduceDriver.withOutput(new Text("1990\tbbb"), new Text("bbb\t3"));
		mapReduceDriver.withOutput(new Text("1990\tccc"), new Text("ccc\t2"));

		mapReduceDriver.withOutput(new Text("2000\tabc"), new Text("abc\t2"));

		mapReduceDriver.withOutput(new Text("2000\tdef"), new Text("abc def\t1\tabc\t2"));
		mapReduceDriver.withOutput(new Text("2000\txyz"), new Text("abc xyz\t1\tabc\t2"));
		mapReduceDriver.withOutput(new Text("2000\tdef"), new Text("def\t1"));
		mapReduceDriver.withOutput(new Text("2000\txyz"), new Text("xyz\t1"));

		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceSameWord() throws IOException {
		mapReduceDriver.withInput(new Text(), new Text("1990\taaa\t8"));
		mapReduceDriver.withInput(new Text(), new Text("1990\taaa aaa\t6"));
		mapReduceDriver.withOutput(new Text("1990\taaa"), new Text("aaa\t8"));
		mapReduceDriver.withOutput(new Text("1990\taaa"), new Text("aaa aaa\t6\taaa\t8"));
		mapReduceDriver.runTest();
	}
}
