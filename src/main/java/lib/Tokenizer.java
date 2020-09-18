package lib;

public class Tokenizer {
    public static String[] tokenize(String text) {
        return text.split("[^\\p{L}]+");
    }
}
