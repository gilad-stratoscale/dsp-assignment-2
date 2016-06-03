package dsp.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Stage2MapperTest {

	private MapDriver<Object, Text, Text, Text>  mapDriver;
	private Stage2Mapper mapper;

	@Before
	public void setUp() {
		mapper = new Stage2Mapper();
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
