package indexer;

import lib.Const;
import lib.DocInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IDFEvaluator {
    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            // just pass it to reducer, since the number of words of WordCounter output is number of appearances
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        private static double docNumber;
        private static String idfType;
        private static int counter = 0;

        public void setup(Context context) {
            docNumber = Double.parseDouble(context.getConfiguration().get("docNumber"));
            idfType = context.getConfiguration().get("idfType");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int docCounter = 0;
            for (Text ignored : values) {
                docCounter += 1;
            }

            double idf = idfType.equals("log") ? Math.log(docNumber / docCounter) : 1.0 / docCounter;
            context.write(key, new Text(idf + "\t" + counter)); // word - idf - word_id
            counter += 1;
        }
    }

    public static boolean run(Configuration conf, String[] args, boolean verbose) throws IOException, ClassNotFoundException, InterruptedException {
        DocInfo.getInfo(conf, args[1]);

        Job job = Job.getInstance(conf, "IDF Evaluator");
        job.setJarByClass(IDFEvaluator.class);
        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1], Const.TEMP));
        FileOutputFormat.setOutputPath(job, new Path(args[1], Const.WORDS));

        return job.waitForCompletion(verbose);
    }
}
