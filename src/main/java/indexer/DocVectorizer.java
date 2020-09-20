package indexer;

import lib.Word;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class DocVectorizer {
    public static class DocVectorizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject object = new JSONObject(line);
            String text = object.getString("text");
            String id = object.getString("id");
            String title = object.getString("title");
            String url = object.getString("url");

            String[] tokens = text.split("[^\\p{L}]+");
            for (String token : tokens) {
                context.write(new Text(id + ":" + title + ":" + url), new Text(token.toLowerCase()));
            }
        }
    }

    public static class DocVectorizerCombiner extends Reducer<Text, Text, Text, Text> {
        private static final HashMap<String, Word> words = new HashMap();

        public void setup(Context context) throws IOException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            URI[] cacheFiles = context.getCacheFiles();

            for (URI cacheFile : cacheFiles) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(cacheFile.getPath()))));


                String line = "";
                while ((line = reader.readLine()) != null) {
                    String[] params = line.split("\t");
                    words.put(params[0], new Word(params));
                }
            }

            System.out.println(words.toString());
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, Double> word_counts = new TreeMap<>();

            for (Text value : values) {
                Word word = words.get(value.toString());
                if (!word_counts.containsKey(word.getId())) {
                    word_counts.put(word.getId(), 1 / word.getIdf());
                } else {
                    word_counts.put(word.getId(), word_counts.get(word.getId()) + 1 / word.getIdf());
                }
            }

            List<String> textList = new ArrayList<String>();
            for (Map.Entry<Integer, Double> entry : word_counts.entrySet()) {
                textList.add(entry.getKey() + ":" + entry.getValue());
            }

            String[] arrayTemp = new String[textList.size()];
            textList.toArray(arrayTemp);
            context.write(key, new Text(String.join(";", arrayTemp)));
        }
    }

    public static class DocVectorizerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static boolean run(Configuration conf, String[] args, boolean verbose) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "Document Vectorizer");
        job.setJarByClass(DocVectorizer.class);
        job.setMapperClass(DocVectorizerMapper.class);
        job.setCombinerClass(DocVectorizerCombiner.class);
        job.setReducerClass(DocVectorizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/idf"));


        FileStatus[] files = fileSystem.listStatus(new Path(args[1] + "/final"), new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("part");
            }
        });
        for (FileStatus file : files) {
            job.addCacheFile(file.getPath().toUri());
        }

        return job.waitForCompletion(verbose);
    }
}
