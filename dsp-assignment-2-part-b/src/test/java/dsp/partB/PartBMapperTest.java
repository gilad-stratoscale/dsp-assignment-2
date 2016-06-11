package dsp.partB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class PartBMapperTest {
	private MapDriver<Object, Text, Text, Text> mapDriver;
	private PartBMapper mapper;

	@Before
	public void setUp() {
		mapper = new PartBMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}


	@Test
	public void map() throws Exception {
		mapDriver.
				withInput(new Text(),new Text(
						// related words
						"1790\ttiger feline\t1790\ttiger feline\t1\tfeline\t1\ttiger\t1\t7.0812413")).
				withInput(new Text(),new Text(
						// unrelated words
						"1810\tstock live\t1810\tstock live\t1\tlive\t1\tstock\t1\t2.86791248")).
				withInput(new Text(""), new Text(
						// words that were not tagged
						"1810\ta b\t1810\ta b\t1\tb\t1\ta\t1\t3.97814814")).
				withOutput(new Text(""), new Text("tiger\tfeline\t7.0812413")).
				withOutput(new Text(""), new Text("stock\tlive\t2.86791248"));
		mapDriver.runTest();
	}

}