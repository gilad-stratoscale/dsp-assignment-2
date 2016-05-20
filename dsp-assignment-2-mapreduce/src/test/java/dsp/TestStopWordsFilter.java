package dsp;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStopWordsFilter {

	@Test
	public void shouldFilter() throws Exception {
		StopWordsFilter stopWordsFilter = new StopWordsFilter();
		assertFalse(stopWordsFilter.shouldFilter("abc"));

		assertTrue(stopWordsFilter.shouldFilter("did"));
	}

}