package dsp.partB;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class RelatedWordsTest {
	@Test
	public void checkIfRelated() throws Exception {
		Assert.assertFalse(RelatedWords.checkIfRelated("a", "b"));
		Assert.assertTrue(RelatedWords.checkIfRelated("sun", "planet"));
		Assert.assertTrue(RelatedWords.checkIfRelated("planet", "sun"));
	}

}