package lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Vocabulary {

    public static HashMap<String, Word> loadVocabulary(Configuration conf, String path) throws IOException {
        HashMap<String, Word> words = new HashMap<>();
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] files = fileSystem.listStatus(new Path(path, Const.WORDS),
                path1 -> path1.getName().startsWith("part"));

        for (FileStatus file : files) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(file.getPath())));

            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] params = line.split("\t");
                words.put(params[0], new Word(params));
            }
        }

        return words;
    }
}
