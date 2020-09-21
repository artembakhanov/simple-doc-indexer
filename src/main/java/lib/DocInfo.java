package lib;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DocInfo {

    public static Pair<Integer, Double> getInfo(Configuration conf, String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(args[1], "doc_info"))));
        String[] line = reader.readLine().split("\t");

        Pair<Integer, Double> docInfo = new Pair<>(Integer.parseInt(line[0]), Double.parseDouble(line[1]));
        conf.set("docNumber", String.valueOf(docInfo.fst));
        conf.set("docAverage", String.valueOf(docInfo.snd));

        return docInfo;
    }
}
