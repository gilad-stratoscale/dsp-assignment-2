package dsp.topN;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 15/06/2016.
 */
public class TopNComparatorTest {

	private TopNComparator c = new TopNComparator();;

	@Test
	public void testDifferentDecadeWithPmi() {
		int compare = c.compare(new Text("1990\t1.23\tabc edf"), new Text("1980\t1.23\tabc edf"));
		Assert.assertTrue("expected bigger than 0, got " + compare,compare > 0);
	}

	@Test
	public void testDifferentDecadeWithTilde() {
		int compare = c.compare(new Text("1990\t~\t~"), new Text("1980\t~\t~"));
		Assert.assertTrue("expected bigger than 0, got " + compare,compare > 0);
	}

	@Test
	public void testDifferentDecadeWithPmiAndTilde() {
		int compare = c.compare(new Text("1990\t1.23\tabc edf"), new Text("1980\t~\t~"));
		Assert.assertTrue("expected bigger than 0, got " + compare,compare > 0);
	}


	@Test
	public void testSameDecadeOneWithPMIAndOtherWithTilde() throws Exception {
		int compare = c.compare(new Text("1990\t1.23\tabc edf"),new Text("1990\t~\t~"));
		Assert.assertTrue("expected smaller than 0, got " + compare,compare < 0);
	}

	@Test
	public void testSameDecadeWithTilde() throws Exception {
		int compare = c.compare(new Text("1990\t~\t~"),new Text("1990\t~\t~"));
		Assert.assertEquals("expected 0, got " + compare,compare, 0);
	}


	@Test
	public void testSameDecadeBiggerPmi() throws Exception {
		int compare = c.compare(new Text("1990\t1.23\tabc edf"),new Text("1990\t1.2300001\taaa bbb"));
		Assert.assertTrue("expected smaller than 0, got " + compare,compare > 0);

		compare = c.compare(new Text("1990\t2.23\tabc edf"),new Text("1990\t10.2300001\taaa bbb"));
		Assert.assertTrue("expected smaller than 0, got " + compare,compare > 0);
	}

	public void testEquals() {
		Text o1 = new Text("1990\t1.23\tabc edf");
		Text o2 = new Text("1990\t1.23\tabc edf");
		int compare = c.compare(o1,o2);
		Assert.assertEquals(0, compare);
	}
}