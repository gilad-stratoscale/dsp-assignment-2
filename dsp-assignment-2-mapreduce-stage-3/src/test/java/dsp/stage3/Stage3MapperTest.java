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
				new Text("abc,5"));
		mapDriver.withOutput(new Text("abc *"), new Text("abc,5"));

		mapDriver.runTest();
	}

	@Test
	public void testMapperWithTwoGram() throws IOException {
		mapDriver.withInput(
				new Text(),
				new Text("abc def,5"));
		mapDriver.withOutput(new Text("abc +"), new Text("abc def,5"));

		mapDriver.runTest();
	}

}
