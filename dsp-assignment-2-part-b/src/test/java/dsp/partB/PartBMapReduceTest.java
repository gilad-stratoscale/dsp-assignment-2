package dsp.partB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hagai_lvi on 11/06/2016.
 */
public class PartBMapReduceTest {
	private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		PartBMapper mapper = new PartBMapper();

		Reducer<Text, Text, Text, Text> reducer = new PartBReducer();

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void test() throws IOException {
		mapReduceDriver.
				withInput(new Text(),new Text(
						// related words
						"1790\ttiger feline\t1\tfeline\t1\ttiger\t1\t7.0812413")).
				withInput(new Text(),new Text(
						// unrelated words
						"1810\tstock live\t1\tlive\t1\tstock\t1\t2.86791248")).
				withInput(new Text(""), new Text(
						// words that were not tagged
						"1810\ta b\t1\tb\t1\ta\t1\t3.97814814")).
				withOutput(new Text(""), new Text("fscore:1.0 pmi threshold:7.0812413"));
		mapReduceDriver.runTest();
	}

}
