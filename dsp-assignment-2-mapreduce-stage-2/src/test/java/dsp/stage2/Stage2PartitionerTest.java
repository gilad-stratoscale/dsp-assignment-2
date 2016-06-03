package dsp.stage2;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 31/05/2016.
 */
public class Stage2PartitionerTest {

	@Test
	public void getPartition(){
		Stage2Partitioner p = new Stage2Partitioner();
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