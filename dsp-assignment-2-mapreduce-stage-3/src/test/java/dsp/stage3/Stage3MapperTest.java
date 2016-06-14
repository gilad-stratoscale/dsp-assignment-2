package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by hagai_lvi on 03/06/2016.
 */
public class Stage3MapperTest {

	private MapDriver<Object, Text, Text, Text> mapDriver;
	private Stage3Mapper mapper;

	@Before
	public void setUp() {
		mapper = new Stage3Mapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}


	@Test
	public void map1() throws Exception {
		String word = "abc";
		String year = "1980";
		String count = "1";
		mapDriver.addInput(new Text(),
				new Text(
						String.join(Stage3Mapper.SEPERATOR, year, word, word, count)
				));
		mapDriver.addOutput(
				new Text(String.join(Stage3Mapper.SEPERATOR, year, word) + " *"),
				new Text(String.join(Stage3Mapper.SEPERATOR, word, count))
		);

		mapDriver.runTest();
	}

	@Test
	public void map2() throws Exception {
		String word1 = "abc";
		String word2 = "def";
		String year = "1980";
		String count1 = "1";
		String count2 = "2";
		mapDriver.addInput(new Text(),
				new Text(
						String.join(Stage3Mapper.SEPERATOR, year, word2, word1 + " " + word2, count1, word1, count2)
				));
		mapDriver.addOutput(
				new Text(String.join(Stage3Mapper.SEPERATOR, year, word2) + " +"),
				new Text(String.join(Stage3Mapper.SEPERATOR, word1 + " " + word2, count1, word1, count2))
		);
		mapDriver.runTest();
	}

	@Test
	public void getKeyPostfix(){
		String keyPostfix = Stage3Mapper.getKeyPostfix(String.join(
				Stage3Mapper.SEPERATOR,
				"1980",
				"abc",
				"abc",
				"1"
		));

		Assert.assertEquals(keyPostfix, "*");

		String key2Postfix = Stage3Mapper.getKeyPostfix(
				"1580\twhate\ter whate\t1\ter\t1\n"
		);

		Assert.assertEquals(key2Postfix, "+");

	}

}