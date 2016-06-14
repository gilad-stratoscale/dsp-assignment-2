package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by hagai_lvi on 14/06/2016.
 */
public class TopNMapperTest {


	private MapDriver<Object, Text, Text, Text> mapDriver;
	private TopNMapper mapper;

	@Before
	public void setUp() {
		mapper = new TopNMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void map() throws Exception {

		mapDriver.withInput(new Text(""), new Text("1900\tabdomen abdomen\t10\tabdomen\t12787\tabdomen\t12787\t3.9316017295349717\n"));
		System.out.println(mapDriver.run());
	}

}