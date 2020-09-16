package indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;

public class WordCounter {
    public static class WordCounterMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject object = new JSONObject(line);
            String text = object.getString("text");
            String id = object.getString("id");

            String[] tokens = text.split("[^\\p{L}]+");
            for (String token : tokens) {
                context.write(new Text(token.toLowerCase() + ":" + id), new Text("1"));
            }
        }
    }

    public static class WordCounterCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int wordCounter = 0;

            String[] params = key.toString().split(":");
            String word = params[0];

            for (Text ignored : values) {
                wordCounter += 1;
            }

            context.write(new Text(word), new Text(String.valueOf(wordCounter)));
        }
    }

    public static class WordCounterReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static boolean run(Configuration conf, String[] args, boolean verbose) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "Word Counter");
        job.setJarByClass(WordCounter.class);
        job.setMapperClass(WordCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(WordCounterCombiner.class);
        job.setReducerClass(WordCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

        return job.waitForCompletion(verbose);
    }
}
