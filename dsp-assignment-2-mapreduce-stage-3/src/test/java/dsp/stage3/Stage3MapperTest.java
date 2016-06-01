package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Stage3MapperTest {

	private MapDriver<Object, Text, Text, Text>  mapDriver;
	private Stage3Mapper mapper;

	@Before
	public void setUp() {
		mapper = new Stage3Mapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testMapperWithSingleWord() throws IOException {
		mapDriver.withInput(
				new Text(),
				new Text("2000\tabc\t5"));
		mapDriver.withOutput(new Text("2000\tabc *"), new Text("2000\tabc\t5"));

		mapDriver.runTest();
	}

	@Test
	public void testMapperWithTwoGram() throws IOException {
		mapDriver.withInput(
				new Text(),
				new Text("2000\tabc def\t5"));
		mapDriver.withOutput(new Text("2000\tabc +"), new Text("2000\tabc def\t5"));

		mapDriver.runTest();
	}

}
