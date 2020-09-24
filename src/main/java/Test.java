import org.apache.commons.cli.*;
import org.json.JSONObject;

public class Test {
    public static void main(String[] args) throws ParseException {
        JSONObject object = new JSONObject();

        object.put("hui", "kill");
        object.put("hui1", "kill");
        object.put("hui2", "kill");

        System.out.println(object.toString(0));
    }
}
