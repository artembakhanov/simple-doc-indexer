package lib;

public class Word {
    private String word;
    private int id;
    private double idf;

    public Word(String word, int id, int idf) {
        this.word = word;
        this.id = id;
        this.idf = idf;
    }

    public Word(String[] params) {
        this.word = params[0];
        this.idf = Double.parseDouble(params[1]);
        this.id = Integer.parseInt(params[2]);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }
}
