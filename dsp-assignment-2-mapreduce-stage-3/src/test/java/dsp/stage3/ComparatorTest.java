package dsp.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.Test;


/**
 * The default hadoop comparator seem to be just fine for our cause, as long as we use * and + as demonstrated
 * in the test.
 */
public class ComparatorTest {
	@Test
	public void compare() throws Exception {
		WritableComparator c = WritableComparator.get(Text.class);

		Assert.assertEquals(
				0, c.compare(new Text("a"), new Text("a"))
		);

		Assert.assertTrue(
				c.compare(new Text("a *"), new Text("a +")) < 0
		);

		Assert.assertTrue(
				c.compare(new Text("a +"), new Text("a *")) > 0
		);

	}

}