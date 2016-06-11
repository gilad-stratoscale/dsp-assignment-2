package dsp.partB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class PartBReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, Text, Text, Text> reducer = new PartBReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void reduce() throws Exception {
		List<Text> values = Collections.singletonList(new Text(String.join("\t", "tiger", "feline", "2")));
		reduceDriver.withInput(new Text(""), values);
		System.out.println(reduceDriver.run());
	}

}