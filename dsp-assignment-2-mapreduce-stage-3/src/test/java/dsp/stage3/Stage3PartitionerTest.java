package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 31/05/2016.
 */
public class Stage3PartitionerTest {

	@Test
	public void getPartition(){
		Stage3Partitioner p = new Stage3Partitioner();
		Assert.assertEquals(
				p.getPartition(new Text("abc"), new Text("dontcare"), 123),
				p.getPartition(new Text("abc "), new Text("dontcare"), 123)
		);

		Assert.assertEquals(
				p.getPartition(new Text("abc"), new Text("dontcare"), 123),
				p.getPartition(new Text("abc def"), new Text("dontcare"), 123)
		);

		Assert.assertEquals(
				p.getPartition(new Text("abc def"), new Text("dontcare"), 123),
				p.getPartition(new Text("abc xyz"), new Text("dontcare"), 123)
		);
	}

}