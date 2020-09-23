package query;

import com.google.common.collect.Collections2;
import lib.Tokenizer;
import lib.Word;

import java.util.Collection;
import java.util.HashMap;

public class Preprocessor {
    private final HashMap<Integer, Double> queryWords = new HashMap<>();

    public void preprocess(String query, HashMap<String, Word> words) {
        for (String word: Tokenizer.tokenize(query)) {
            if (words.containsKey(word)) {
                Word wordInfo = words.get(word);
                if (queryWords.containsKey(wordInfo.getId())) {
                    queryWords.put(wordInfo.getId(), queryWords.get(wordInfo.getId()) + 1/wordInfo.getIdf());
                } else {
                    queryWords.put(wordInfo.getId(), 1/wordInfo.getIdf());
                }
            }
        }
    }

    public String getString() {
        final Collection<String> elements = Collections2.transform(
                this.queryWords.entrySet(),
                integerDoubleEntry -> {
                    assert integerDoubleEntry != null;
                    return integerDoubleEntry.getKey() + ":" + integerDoubleEntry.getValue();
                }
        );
        System.out.println(String.join(";", elements));
        return String.join(";", elements);
    }

    public static HashMap<Integer, Double> fromString(String record) {
        if (record == null) record = "";
        String[] elements = record.split(";"); // todo: fix unknown words
        HashMap<Integer, Double> query = new HashMap<>();
        for (String element: elements) {
            String[] vector = element.split(":");
            query.put(Integer.parseInt(vector[0]), Double.parseDouble(vector[1]));
        }

        return query;
    }

    public void preprocessBM25(String query, HashMap<String, Word> words) {
        for (String word: Tokenizer.tokenize(query)) {
            if (words.containsKey(word)) {
                Word wordInfo = words.get(word);
                if (!queryWords.containsKey(wordInfo.getId())) {
                    // We need to retrieve only IDF of the word in the query for the formula.
                    queryWords.put(wordInfo.getId(), wordInfo.getIdf());
                }
            }
        }
    }
}
