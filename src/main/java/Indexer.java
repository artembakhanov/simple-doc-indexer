import indexer.IDFEvaluator;
import indexer.WordCounter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Indexer {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // args[0] - wikipedia input folder
        // args[1] - output folder folder
        Configuration conf = new Configuration();

        WordCounter.run(conf, args, true);
        IDFEvaluator.run(conf, args, true);
    }
}