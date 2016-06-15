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
	public void testMapReduceSameDecade() throws IOException {
		mapReduceDriver.getConfiguration().setInt("N",2);
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa bbb\t10\taaa\t12787\tbbb\t12787\t3.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ccc\t10\taaa\t12787\tccc\t12787\t4.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ddd\t10\taaa\t12787\tddd\t12787\t5.9316017295349717"));
		mapReduceDriver.withOutput(new Text("1900\t5.9316017295349717\taaa ddd"),new Text(""));
		mapReduceDriver.withOutput(new Text("1900\t4.9316017295349717\taaa ccc"),new Text(""));
		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceMultipleDecades() throws IOException {
		mapReduceDriver.getConfiguration().setInt("N",2);
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa bbb\t10\taaa\t12787\tbbb\t12787\t2.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ccc\t10\taaa\t12787\tccc\t12787\t1.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ddd\t10\taaa\t12787\tddd\t12787\t0.9316017295349717"));

		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa bbb\t10\taaa\t12787\tbbb\t12787\t100.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa ccc\t10\taaa\t12787\tccc\t12787\t200.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa ddd\t10\taaa\t12787\tddd\t12787\t300.9316017295349717"));

		mapReduceDriver.withOutput(new Text("1900\t2.9316017295349717\taaa bbb"),new Text(""));
		mapReduceDriver.withOutput(new Text("1900\t1.9316017295349717\taaa ccc"),new Text(""));

		mapReduceDriver.withOutput(new Text("1910\t300.9316017295349717\taaa ddd"),new Text(""));
		mapReduceDriver.withOutput(new Text("1910\t200.9316017295349717\taaa ccc"),new Text(""));

		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceMultipleDecadesNegativePmi() throws IOException {
		mapReduceDriver.getConfiguration().setInt("N",2);
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa bbb\t10\taaa\t12787\tbbb\t12787\t-2.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ccc\t10\taaa\t12787\tccc\t12787\t-1.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1900\taaa ddd\t10\taaa\t12787\tddd\t12787\t-0.9316017295349717"));

		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa bbb\t10\taaa\t12787\tbbb\t12787\t100.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa ccc\t10\taaa\t12787\tccc\t12787\t200.9316017295349717"));
		mapReduceDriver.withInput(new Text(""), new Text("1910\taaa ddd\t10\taaa\t12787\tddd\t12787\t300.9316017295349717"));

		mapReduceDriver.withOutput(new Text("1900\t-0.9316017295349717\taaa ddd"),new Text(""));
		mapReduceDriver.withOutput(new Text("1900\t-1.9316017295349717\taaa ccc"),new Text(""));

		mapReduceDriver.withOutput(new Text("1910\t300.9316017295349717\taaa ddd"),new Text(""));
		mapReduceDriver.withOutput(new Text("1910\t200.9316017295349717\taaa ccc"),new Text(""));

		mapReduceDriver.runTest();

	}

}