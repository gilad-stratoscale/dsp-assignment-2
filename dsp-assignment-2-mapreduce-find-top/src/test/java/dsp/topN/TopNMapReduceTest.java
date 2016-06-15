package dsp.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hagai_lvi on 15/06/2016.
 */
public class TopNMapReduceTest {

	private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		TopNMapper mapper = new TopNMapper();

		Reducer<Text, Text, Text, Text> reducer = new TopNReducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.setKeyOrderComparator(new TopNComparator());

	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.getConfiguration().setInt("N",2);
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa bbb\t10\taaa\t12787\tbbb\t12787\t3.9316017295349717\n"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ccc\t10\taaa\t12787\tccc\t12787\t4.9316017295349717\n"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ddd\t10\taaa\t12787\tddd\t12787\t5.9316017295349717\n"));
		System.out.println(mapReduceDriver.run());
	}

}