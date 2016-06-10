package dsp.partB;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class FScoreCalculatorTest {

	@Test
	public void calculateFScore() throws Exception {

		double threshold = 0.5;
		Assert.assertEquals(0.5, FScoreCalculator.calculateFScore(getTrios1(), threshold), 0);

		Assert.assertEquals(2.0/3, FScoreCalculator.calculateFScore(getTrios2(), threshold), 0);

	}

	private List<Trio> getTrios1() {

		List<Trio> res = new LinkedList<>();
		res.add(new Trio("tiger", "jaguar", 0.8));// related
		res.add(new Trio("closet", "clothes", 0.4));// related

		res.add(new Trio("stock","live", 0.6));// unrelated
		res.add(new Trio("population","development", 0.2));// unrelated
		return res;
	}

	private List<Trio> getTrios2() {

		List<Trio> res = new LinkedList<>();
		res.add(new Trio("tiger", "jaguar", 0.8));// related
		res.add(new Trio("closet", "clothes", 0.4));// related
		res.add(new Trio("tiger", "feline", 0.6)); //related

		res.add(new Trio("stock","live", 0.6));// unrelated
		res.add(new Trio("population","development", 0.2));// unrelated
		return res;
	}
}