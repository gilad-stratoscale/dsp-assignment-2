package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestTokenizerMapper {
	private MapDriver<Object, Text, Text, IntWritable> mapDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(1), new Text("abc"));
		mapDriver.withOutput(new Text("abc"), new IntWritable(1));
		mapDriver.runTest();
	}

}
