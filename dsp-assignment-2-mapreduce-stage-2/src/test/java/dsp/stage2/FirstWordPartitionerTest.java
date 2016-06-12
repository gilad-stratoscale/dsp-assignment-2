package dsp.stage2;

import dsp.FirstWordPartitioner;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 31/05/2016.
 */
public class FirstWordPartitionerTest {

	@Test
	public void getPartition(){
		FirstWordPartitioner p = new FirstWordPartitioner();
		Assert.assertEquals(
				p.getPartition(new Text("1580 abc"), new Text("dontcare"), 123),
				p.getPartition(new Text("1580 abc "), new Text("dontcare"), 123)
		);

		Assert.assertEquals(
				p.getPartition(new Text("abc"), new Text("dontcare"), 123),
				p.getPartition(new Text("abc def"), new Text("dontcare"), 123)
		);

		Assert.assertEquals(
				p.getPartition(new Text("abc def"), new Text("dontcare"), 123),
				p.getPartition(new Text("abc xyz"), new Text("dontcare"), 123)
		);

		Assert.assertEquals(
				p.getPartition(new Text("1580\twhate +"), new Text("dontcare"), 123),
				p.getPartition(new Text("1580\twhate *"), new Text("dontcare"), 123)
		);

		Assert.assertTrue(
				p.getPartition(new Text("1580\tabc def"), new Text("dontcare"), 123) !=
						p.getPartition(new Text("1590\tabc def"), new Text("dontcare"), 123)
		);

		Assert.assertTrue(
				p.getPartition(new Text("1580\tabc +"), new Text("dontcare"), 123) !=
						p.getPartition(new Text("1590\tabc +"), new Text("dontcare"), 123)
		);
	}

}