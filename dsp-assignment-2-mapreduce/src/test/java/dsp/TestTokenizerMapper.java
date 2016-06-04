package dsp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestTokenizerMapper {
	private MapDriver<Object, Text, Text, IntWritable> mapDriver;
	private TokenizerMapper mapper;

	@Before
	public void setUp() {
		mapper = new TokenizerMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(1), new Text("\"! \"\" \"\" It ought\"\t1893\t1\t1\t1"));
		mapDriver.runTest();
	}

	@Test
	public void testMapper2() throws IOException {
		mapDriver.withInput(new IntWritable(1), new Text("\"\"\" During the final stages\"\t1994\t3\t3\t3\n"));
		mapDriver.withOutput(new Text("1990\tfinal stages"),new IntWritable(1));
		mapDriver.withOutput(new Text("1990\tfinal"),new IntWritable(1));
		mapDriver.withOutput(new Text("1990\tstages"),new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testMapper3() throws IOException {
		mapDriver.withInput(new IntWritable(1), new Text("' leaves open the possibility\t1981\t4\t4\t3"));
		mapDriver.withOutput(new Text("1980\tleaves open"),new IntWritable(1));
		mapDriver.withOutput(new Text("1980\tleaves"),new IntWritable(1));
		mapDriver.withOutput(new Text("1980\topen"),new IntWritable(1));
		mapDriver.withOutput(new Text("1980\topen possibility"),new IntWritable(1));
		mapDriver.withOutput(new Text("1980\topen"),new IntWritable(1));
		mapDriver.withOutput(new Text("1980\tpossibility"),new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testSort2Gram() {

		Assert.assertEquals("aaa aaa", mapper.sort2gram("aaa aaa"));

		Assert.assertEquals("aaa bbb", mapper.sort2gram("aaa bbb"));

		Assert.assertEquals("aaa bbb", mapper.sort2gram("bbb aaa"));
	}


	@Test
	public void testOneGram() {
		Assert.assertEquals(0, mapper.ngramTo2gram("aaa").size());
	}

	@Test
	public void testTwoGram() {
		List<String> twoGrams = mapper.ngramTo2gram("aaa bbb");
		Assert.assertEquals(1, twoGrams.size());
		Assert.assertTrue(twoGrams.contains("aaa bbb"));
	}

	@Test
	public void testThreeGram() {
		List<String> twoGrams = mapper.ngramTo2gram("aaa bbb ccc");
		Assert.assertEquals(2, twoGrams.size());
		Assert.assertTrue(twoGrams.contains("aaa bbb"));
		Assert.assertTrue(twoGrams.contains("bbb ccc"));
	}

	@Test
	public void testFourGram() {
		List<String> twoGrams = mapper.ngramTo2gram("aaa bbb ccc ddd");
		Assert.assertEquals(3, twoGrams.size());
		Assert.assertTrue(twoGrams.contains("aaa ccc"));
		Assert.assertTrue(twoGrams.contains("bbb ccc"));
		Assert.assertTrue(twoGrams.contains("ccc ddd"));
	}

	@Test
	public void testFiveGram() {
		List<String> twoGrams = mapper.ngramTo2gram("aaa bbb ccc ddd eee");

		Assert.assertEquals(4, twoGrams.size());

		Assert.assertTrue(twoGrams.contains("aaa ccc"));
		Assert.assertTrue(twoGrams.contains("bbb ccc"));
		Assert.assertTrue(twoGrams.contains("ccc ddd"));
		Assert.assertTrue(twoGrams.contains("ccc eee"));
	}

	@Test
	public void testRemoveStopWords() {
		Assert.assertEquals("extract pairs words", mapper.removeStopWords("extract pairs of words"));
		Assert.assertEquals("", mapper.removeStopWords(""));
		Assert.assertEquals(
				"extract pairs extract pairs extract pairs",
				mapper.removeStopWords("extract pairs extract pairs extract pairs"));

	}

    @Test
    public void testPunctuationAndNumbers() {
        Assert.assertEquals("passwrd", mapper.removePunctuationAndNumbers("passw0rd"));
        Assert.assertEquals("a b c", mapper.removePunctuationAndNumbers("a,b,c"));
        Assert.assertEquals("number cant", mapper.removePunctuationAndNumbers("number1,can't"));
    }

    @Test
    public void testFiveGramWithPunctuationNumbersAndStopWords() {
        String withoutPunctuationAndNumbers = mapper.removePunctuationAndNumbers("and a1a2a bbb,cc3c of d'd'd,eee");
        String withoutStopWords = mapper.removeStopWords(withoutPunctuationAndNumbers);
        List<String> twoGrams = mapper.ngramTo2gram(withoutStopWords);

        Assert.assertEquals(4, twoGrams.size());

        Assert.assertTrue(twoGrams.contains("aaa ccc"));
        Assert.assertTrue(twoGrams.contains("bbb ccc"));
        Assert.assertTrue(twoGrams.contains("ccc ddd"));
        Assert.assertTrue(twoGrams.contains("ccc eee"));
    }

	@Test
	public void testGetDecade() {
		Assert.assertEquals(1970, mapper.getDecade("1979"));
		Assert.assertEquals(1970, mapper.getDecade("1975"));
		Assert.assertEquals(1970, mapper.getDecade("1970"));
	}

	@Test
	public void regressionTestSpaces() throws IOException {
		/* Here, the only non stopword non punctuation is united, so expect empty output
		 * Before, there was a bug that left spaces of punctuation, so " united" was splitted to
		 * 2 words - "" and "united" and thus was considered a 2gram
		 */

		String value = "! then - united to\t1950\t3\t3\t3";
		String key = "13000";

		mapDriver.
				withInput(
						new Text(key),
						new Text(value));
		mapDriver.runTest();

	}


}
