package dsp;

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
        return this.pmi > o.pmi ? 1 : (this.pmi < o.pmi ? -1 :
                (this.firstWord.compareTo(o.firstWord) > 0 ? 1 :
                        ((this.secondWord.compareTo(o.secondWord) > 0 ? 1 : -1))));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopTuple topTuple = (TopTuple) o;

        if (firstWord != null ? !firstWord.equals(topTuple.firstWord) : topTuple.firstWord != null) return false;
        if (secondWord != null ? !secondWord.equals(topTuple.secondWord) : topTuple.secondWord != null) return false;
        return pmi != null ? pmi.equals(topTuple.pmi) : topTuple.pmi == null;

    }

    @Override
    public int hashCode() {
        int result = firstWord != null ? firstWord.hashCode() : 0;
        result = 31 * result + (secondWord != null ? secondWord.hashCode() : 0);
        result = 31 * result + (pmi != null ? pmi.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "firstWord='" + firstWord + '\'' +
                ", secondWord='" + secondWord + '\'' +
                ", pmi=" + pmi;
    }
}
