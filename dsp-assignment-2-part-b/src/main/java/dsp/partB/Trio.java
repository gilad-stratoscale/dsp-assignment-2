package dsp.partB;

/**
 * Created by hagai_lvi on 10/06/2016.
 */
public class Trio {
	private String w1;
	private String w2;

	public String getW1() {
		return w1;
	}

	public String getW2() {
		return w2;
	}

	public double getPmi() {
		return pmi;
	}

	public Trio(String w1, String w2, double pmi) {
		this.w1 = w1;
		this.w2 = w2;
		this.pmi = pmi;
	}

	private double pmi;
}
