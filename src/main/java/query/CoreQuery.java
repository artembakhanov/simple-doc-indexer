package query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import lib.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class CoreQuery {

    public static class QueryMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private static HashMap<Integer, Double> query;
        private final TreeMap<Double, String> tmapMap = new TreeMap<>();
        private String solver;

        public void setup(Context context) {
            query = Preprocessor.fromString(context.getConfiguration().get("query"));
            solver = context.getConfiguration().get("solver");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            double relevance = 0.0;
            // I do not create hashmap and search for words in query because it would take the same amount of time
            // as to create a HashMap for each of the word in the document
            JSONObject object = DocumentVector.parseFromLine(line[1]);
            String vectorized = object.getString("vectorized");
            if (context.getConfiguration().get("solver").equals("BM25")) {
                // Formula for BM25 and arguments for it
                int docLength = object.getInt("docLength");
                for (String element : vectorized.split(";")) {
                    Integer idx = Integer.parseInt(element.split(":")[0]);
                    if (query.containsKey(idx)) {
                        double tfidf = Double.parseDouble(element.split(":")[1]);
                        Double idf = query.get(idx);
                        double avSize = Double.parseDouble(context.getConfiguration().get("avgdl"));
                        // Multiplying by itself is always faster than Math.pow()
                        int k1 = 2;
                        double b = 0.75;
                        relevance +=  tfidf * (k1 + 1) / (tfidf / idf + k1 * (1 + b * (docLength / avSize - 1)));
                    }
                }
            } else {
                // Basic solver
                for (String element : vectorized.split(";")) {
                    Integer idx = Integer.parseInt(element.split(":")[0]);
                    if (query.containsKey(idx)) {
                        // Adding relevance for each word that is in the text.
                        // Words in the text are unique, which is guaranteed from the previous steps.
                        relevance += query.get(idx) * Double.parseDouble(element.split(":")[1]);
                    }
                }
            }
            if (relevance != 0.0) {
                tmapMap.put(relevance, line[1]);
            }

            // Getting top N pages on each step. Following this tutorial:
            // https://www.geeksforgeeks.org/how-to-find-top-n-records-using-mapreduce/
            if (tmapMap.size() > Integer.parseInt(context.getConfiguration().get("n"))) {
                tmapMap.remove(tmapMap.firstKey());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, String> entry : tmapMap.entrySet()) {
                Double relevance = entry.getKey();
                JSONObject object = DocumentVector.parseFromLine(entry.getValue());
                JSONObject result = new JSONObject()
                        .put("title", object.getString("title"))
                        .put("url", object.getString("url"))
                        .put("relevance", relevance);
                context.write(
                        new Text(result.toString(0).replaceAll("\n", "")),
                        new DoubleWritable(relevance));
            }
        }
    }


    public static class QueryReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static final TreeMap<Double, String> tmapReduce = new TreeMap<>();

        public void reduce(Text nameID, Iterable<DoubleWritable> relevances,
                           Context context
        ) throws IOException, InterruptedException {
            double relevance = 0.0;
            for (DoubleWritable value: relevances) {
                relevance = value.get();
            }

            tmapReduce.put(relevance, nameID.toString());

            if (tmapReduce.size() > Integer.parseInt(context.getConfiguration().get("n"))) {
                tmapReduce.remove(tmapReduce.firstKey());
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<Double, String> entry : tmapReduce.descendingMap().entrySet()) {

                Double count = entry.getKey();
                String name = entry.getValue();
                context.write(new Text(name), new DoubleWritable(count));
            }
        }
    }

    public static boolean run(Configuration conf, String[] args, boolean verbose) throws IOException, ClassNotFoundException, InterruptedException {
        // 0 - indexer output folder
        // 1 - query output folder
        // 2 - query results number
        // 3 - query text

        HashMap<String, Word> words = Vocabulary.loadVocabulary(conf, args[0]);
        Preprocessor preprocessor = new Preprocessor();
        // Preprocessing query for different solver type
        if (conf.get("solver").equals("BM25")) {
            preprocessor.preprocessBM25(args[3], words);
        } else {
            preprocessor.preprocess(args[3], words);
        }

        conf.set("query", preprocessor.getString());
        conf.set("n", args[2]);
        conf.set("avgdl", String.valueOf(DocInfo.getInfo(conf, args[0]).snd));


        //Sending job
        Job job = Job.getInstance(conf, "Query Documents");
        job.setSortComparatorClass(DoubleReversedComparator.class);
        job.setJarByClass(CoreQuery.class);
        job.setMapperClass(QueryMapper.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0], Const.VECTORIZED));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(verbose);


        if (result) {
            FileSystem fileSystem = FileSystem.get(conf);
            FileStatus[] docs = fileSystem.listStatus(new Path(args[1]), new PartPathFilter());

            System.out.format("RESULTS%n");
            System.out.format("+-----+--------------------------------+------------+%n");
            System.out.format("| NUM | Title                          | Relevance  |%n");
            System.out.format("+-----+--------------------------------+------------+%n");
            String lineFormat = "| %-3d | %-30s | %-10s |%n";

            int resultCounter = 0;
            for (FileStatus doc: docs) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(doc.getPath())));
                String line = "";
                while ((line = reader.readLine()) != null) {
                    JSONObject queryResult = new JSONObject(line.split("\t")[0]);
                    String title = queryResult.getString("title");
                    System.out.format(lineFormat,
                            resultCounter,
                            (title.length() > 28 ) ? title.substring(0, 28) + ".." : title,
                            String.format("%.3f", queryResult.getDouble("relevance")));

                    resultCounter += 1;
                }
            }
            System.out.format("+-----+--------------------------------+------------+%n");
        } else {
            System.out.println("The program did not finish successfully.");
            System.exit(1);
        }

        return result;
    }
}
