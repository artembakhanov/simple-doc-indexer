package lib;

import org.json.JSONObject;


/**
 * This is a class for static methods that work with vector representation of the documents.
 */
public class DocumentVector {
    public static JSONObject parseFromLine(String line) {
        return new JSONObject(line);
    }

    public static String toLine(String id, String title, String url, int docLength) {
        JSONObject object = new JSONObject();
        object.put("id", id);
        object.put("title", title);
        object.put("url", url);
        object.put("docLength", docLength);
        return object.toString(0);
    }

    public static String toLine(String key, String vectorized) {
        JSONObject object = new JSONObject(key);
        object.put("vectorized", vectorized);

        return object.toString(0);
    }
}
