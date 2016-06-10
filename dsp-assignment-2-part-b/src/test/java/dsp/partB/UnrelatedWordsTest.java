package dsp.partB;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class UnrelatedWordsTest {
	@Test
	public void checkIfUnrelated() throws Exception {
		Assert.assertFalse(UnrelatedWords.checkIfUnrelated("a", "b"));
		Assert.assertTrue(UnrelatedWords.checkIfUnrelated("experience","music"));
		Assert.assertTrue(UnrelatedWords.checkIfUnrelated("music", "experience"));
	}

}