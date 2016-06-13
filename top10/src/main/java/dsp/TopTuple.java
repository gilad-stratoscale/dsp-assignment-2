package dsp;

/**
 * Created by thinkPAD on 6/13/2016.
 */
public class TopTuple implements Comparable<TopTuple> {


    String firstWord;
    String secondWord;
    Double pmi;

    public TopTuple(String firstWord, String secondWord, Double pmi) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.pmi = pmi;
    }


    @Override
    public int compareTo(TopTuple o) {
        return this.pmi > o.pmi ? 1 : (this.pmi < o.pmi ? -1 : 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopTuple topTuple = (TopTuple) o;

        return pmi != null ? pmi.equals(topTuple.pmi) : topTuple.pmi == null;
    }

    @Override
    public int hashCode() {
        return pmi.hashCode();
    }

    @Override
    public String toString() {
        return "firstWord='" + firstWord + '\'' +
                ", secondWord='" + secondWord + '\'' +
                ", pmi=" + pmi;
    }
}
