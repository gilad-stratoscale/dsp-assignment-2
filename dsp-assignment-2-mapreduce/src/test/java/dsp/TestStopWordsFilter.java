package dsp;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestStopWordsFilter {
	@Test
	public void shouldFilter() throws Exception {
		StopWordsFilter stopWordsFilter = new StopWordsFilter();
		 assertFalse(stopWordsFilter.shouldFilter("abc"));

		assertTrue(stopWordsFilter.shouldFilter("did"));
	}

}