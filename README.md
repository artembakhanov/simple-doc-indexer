# Introduction to Big Data. 

# Assignment №1. MapReduce. Simple Text Indexer

Team INNOPIDORbI

Artem Bahanov DS18-01 email: a.bahanov@innopolis.university

Pavel Tishkin      DS18-01 email: p.tishkin@innopolis.university

## Introduction

Nowadays, Big Data surrounds us everywhere, be it the market research done by companies to enhance the opportunities of logistics in company [(studies from DHL)](https://www.dhl.com/content/dam/downloads/g0/about_us/innovation/CSI_Studie_BIG_DATA.pdf) or it may be used to improve the quality of education [(paper by Christos Vaitsis, Vasilis Hervatis and Nabil Zary)](https://www.intechopen.com/books/big-data-on-real-world-applications/introduction-to-big-data-in-education-and-its-contribution-to-the-quality-improvement-processes) and, at last, it may be used in social networking [(an article by Roberta Nicora for medium.com)](https://medium.com/dative-io/how-is-big-data-impacting-social-media-df31aa3f66f6).  

As we have observed, Big Data has a significant influence on a variety of modern industries, roughly speaking, it is beneficial to apply Big Data analysis to get an impact on the results of companies work. But what are the methods to analyze and make use of the data so big, that it would not be possible to do this by means of an extremely powerful computer?

## Assignment Discussion

Now that we have discussed some matters about Big Data analysis, let us proceed with the assignment. This Assignment requires us to create a Simple Text Indexer using MapReduce. A part of the Wikipedia pages were given as a Big Dataset which is going to be indexed to find documents that fit the search. To index it and perform queries across multiple machines, Hadoop MapReduce in Java was used. It allows to separate the computation across multiple Hadoop clusters. So, it solves the problem of how to operate with the Big Data. The program was written on Java. We are going to discuss it now.

## Code Discussion

Let us briefly consider the overall structure of the program. Here is the diagram that explains it:

![image-20200923162621585](images\image-20200923162621585.png)



As you can see, the task was split into two parts, Indexing and Querying. Querying required three separate MapReduce in order to: 

1. Create a dictionary of the all of the words coming from the document
2. Compute IDF for each word in the dictionary
3. Vectorize each document in the Dataset

Querying process was pretty straightforward. It fetched top-n number of documents for the given query. Both of the parameters were specified in Command Line.

Now Let us proceed to discuss each Indexing and Querying in more details.

### Indexer Part

Indexer Part is implemented with switching between Basic IDF (in how many documents the word occurs, was implemented firstly) and Log. --idf-type consumes "normal" or "log" parameter for switching between IDF.

#### Word Counter

For each job in MapReduce a single document is sent and all the words are parsed from it. Form the combination of the words from the documents, for each document there is a list of their unique words. It is also worth mentioning, that for preprocessing, the number of documents is counted.

#### Map

First of all, it is necessary to discard repeating words from the documents. To achieve this, WordCounter was implemented. Documents are represented as lines, serialized, and sent to map parses the document into words. They are then sent to the Reducer. It was chosen to not make extraction of words from the document into a new MapReduce, because it would take the same amount of time, if not more, thus it is obsolete. Words are passed in form <{Word, Doc_ID}, Obsolete> - to retrieve all the occurrences of a word in different documents.

```python
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
```



#### Reduce

In the Reduce class, unique words are combined into dictionary. Records are of form <Word, Obsolete> - to make a dictionary. In this dictionary there are documents and list of their unique words.

```python
public static class WordCounterReducer extends Reducer<Text, Text, Text, Text> {
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
```

#### Summary

![image-20200924210735548](images\image-20200924210735548.png)



#### IDF Counter

Secondly, we need to calculate Inverse Document Frequencies, based on the results of the previous step. This MapReduce works in two modes for calculating IDF. It can evaluate IDF as number of occurrences in different documents and, also, evaluate it log(N/n), where N - overall number of documents,   n - number of occurrences of a unique word in a single document (it is done for each document), also, each word is assigned with a unique ID. 

#### Map

In Map class, unique word are extracted from the file that was accumulated on the WordCounter step. These words are forwarded to Reduce in form <Word, Obsolete>.

```python
public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }
```

#### Reduce

In Reduce, the words are aggregated, their IDF is calculated and the result is written to the file in form <Word, IDF>. Unique words are stored into constant WORDS, where they are assigned with IDs.

```python
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int docCounter = 0;
            for (Text ignored : values) {
                docCounter += 1;
            }

            double idf = idfType.equals("log") ? Math.log(docNumber / docCounter) : 1.0 / docCounter;
            context.write(key, new Text(idf + "\t" + counter)); // word - idf - word_id
            counter += 1;
        }
```

#### Summary

![image-20200924210559550](images\image-20200924210559550.png)

#### Documents vectorization

Finally, to preform querying, vector form of documents is required. This MapReduce goes once again through the documents and uses file from the previous step for IDF.

#### Map

Each document is passed to Map. Then, its metadata is written using delimiter ":" as follows: id:title:url for the better form of output for query. Next, each word is passed to Reduce in the formar <metadata, word>

```python
public static class DocumentVectorizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject object = new JSONObject(line);
            String text = object.getString("text");
            String id = object.getString("id");
            String title = object.getString("title");
            String url = object.getString("url");

            String[] tokens = text.split("[^\\p{L}]+");
            for (String token : tokens) {
                context.write(new Text(DocumentVector.toLine(id, title, url, tokens.length)), new Text(token.toLowerCase()));
            }
        }
    }
```

#### Reduce

For each word passing a Word object is created, which contains metadata and IDF. Their TFIDF are calculated and stored to a different file in a format <delimeter, metadata:vector>. With this, the indexing part ends.

```python
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, Double> word_counts = new TreeMap<>();

            for (Text value : values) {
                Word word = words.get(value.toString());
                if (!word_counts.containsKey(word.getId())) {
                    word_counts.put(word.getId(), word.getIdf());
                } else {
                    word_counts.put(word.getId(), word_counts.get(word.getId()) + word.getIdf());
                }
            }

            List<String> textList = new ArrayList<String>();
            for (Map.Entry<Integer, Double> entry : word_counts.entrySet()) {
                textList.add(entry.getKey() + ":" + entry.getValue());
            }

            String[] arrayTemp = new String[textList.size()];
            textList.toArray(arrayTemp);
            context.write(new Text("d"), new Text(DocumentVector.toLine(key.toString(), String.join(";", arrayTemp))));
        }
```



#### Summary

![image-20200924220256995](images\image-20200924220256995.png)

### Query Part

Query class contains switching between two different relevance solvers: Basic solver (q_i*d_i - multiplications of docs TF/IDF, was implemented firstly) and [BM25](https://ru.wikipedia.org/wiki/Okapi_BM25). To switch between them, the fourth argument should be passed to the command line.

#### Preprocess

First of all, the query was preprocessed. For each word TF/IDF value was computed (for basic solver) or IDF was associated (as IDF is used in Okapi BM25 formula). All the words that are in the query sentence but not in the document dictionary were dropped out for the sake of optimization (Preprocessing can be found the class with the respective name). Result is of form <WordID, TF/IDF> (Basic) or <WordID, IDF> (BM25). These results are then used to vectorize the query. 

```python
public void preprocess(String query, HashMap<String, Word> words) {
        for (String word: Tokenizer.tokenize(query)) {
            if (words.containsKey(word)) {
                Word wordInfo = words.get(word);
                if (queryWords.containsKey(wordInfo.getId())) {
                    queryWords.put(wordInfo.getId(), queryWords.get(wordInfo.getId()) + wordInfo.getIdf());
                } else {
                    queryWords.put(wordInfo.getId(), wordInfo.getIdf());
                }
            }
        }
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
```

#### Map

The resulting document vectors are serialized and sent to the map function as well as the vectorized document. During deserialization, only words from the query message are deserialized into HashMap. Then the program iterates over all the word vectors for the document and adds relevance for the words that are present in the query. Document words vector are not deserialized  into the HashMap, because it would take the same amount of execution time, as to iterate for them and compute relevance inline. Program It computes the relevance between one document and search. As we need only n documents, on each machine only n <DocName, Relevance> records are sent to the reducer. It is implemented using TreeMap in Java. If after the insertion of the new element number of tree records becomes more than n, tree drops a record with the minimal relevance.

```python
public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            double relevance = 0.0;
            int k1 = 2;
            double b = 0.75;
            JSONObject object = DocumentVector.parseFromLine(line[1]);
            String vectorized = object.getString("vectorized");
            int docLength = object.getInt("docLength");
            if (context.getConfiguration().get("solver").equals("BM25")) {
                for (String element : vectorized.split(";")) {
                    Integer idx = Integer.parseInt(element.split(":")[0]);
                    if (query.containsKey(idx)) {
                        Double tfidf = Double.parseDouble(element.split(":")[1]);
                        Double idf = query.get(idx);
                        double avSize = Double.parseDouble(context.getConfiguration().get("avgdl"));
                        // Multiplying by itself is always faster than Math.pow()
                        relevance += idf * tfidf*(k1 + 1) / (tfidf + k1* (1 + b * (docLength / avSize - 1)));
                    }
                }
            } else {
                for (String element : vectorized.split(";")) {
                    Integer idx = Integer.parseInt(element.split(":")[0]);
                    if (query.containsKey(idx)) {
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

                context.write(
                        new Text("Title: " + object.getString("title") + "\tURL: " + object.getString("url")),
                        new DoubleWritable(relevance));
            }
        }
    }
```

#### Reduce

Reduce function is very simple one. As it is not necessary to combine any records, it chooses top n Documents by relevance. 

```python
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
```

#### Summary

![image-20200923092250287](images\image-20200923092250287.png)

## Results

In this section, you can see this program in work via screenshots.



## Okapi BM25 vs Basic implementation

Now that we have discussed the code, let us compare, which of the computations for relevance performs better, as both are in the working state.

We have decided to access the performance of them using the formula presented in the instructions. There is is:
$$
MAP=1|Q|∑_{q∈Q} AP(q)
$$
, where
$$
AP=\frac{1}{N_{rel}}∑_{k=1}^{Nl} P(k)⋅rel(k)
$$
We used five requests with 10 pages in the response. As a result, we got: Basic: 0.655, BM25: 0.76. [There](https://docs.google.com/spreadsheets/d/1I4i4_erzN4W76ZCvv1RleNCMhNO6s1yqLQk-fRSNZok/edit?usp=sharing) you can see the queries and their associated relevance to the query. We understand that some of our decisions are purely subjective

While Naïve implementation performs relatively well,  Okapi BM25 preforms for roughly 15.5% better.

## Results

As a result of this Assignment our team has gotten a working program which manages to find documents relevant for the search words, specified in the command line. Here you can see the examples of the work of the program:

#TODO Add examples

## Conclusion

So, during this homework we have successfully applied MapReduce to the Text Indexing and Querying from these indices. We have enhanced our Java and Hadoop MapReduce skills and were able to create a Simple Text Indexer. Some of the improvements to the algorithms were implemented to the original code, preserving the ability to use inefficient versions. On the Innopolis Hadoop cluster you will be able to test everything yourself. Also, there is a link to the [GitHub](https://github.com/artembakhanov/simple-doc-indexer/tree/master/src/main/java/indexer), where you can familiarize with the code of the project.

## Responsibility for the tasks

Artem Bakhanov - Creating a Git Repository, writing Document Indexer, Testing the Code on his Machine.

Pavel Tishkin - Writing Document Query, writing a report for the submission.