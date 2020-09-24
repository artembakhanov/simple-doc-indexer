import indexer.DocumentVectorizer;
import indexer.IDFEvaluator;
import indexer.WordCounter;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Indexer {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // args[0] - wikipedia input folder
        // args[1] - output folder folder
        Configuration conf = new Configuration();

        Options options = new Options();

        Option optNoVerbose = new Option("nv", "no-verbose", false, "Show MapReduce progress output.");
        Option optIdfType = new Option("idf", "idf-type", true, "Choose IDF type: 'normal' or 'log'.");
        Option optHelp = new Option("h", "help", false, "Help message.");

        options.addOption(optNoVerbose);
        options.addOption(optIdfType);
        options.addOption(optHelp);

        boolean verbose;
        String idfType = "log";

        CommandLineParser cmdLinePosixParser = new PosixParser();
        CommandLine commandLine = null;
        try {
            commandLine = cmdLinePosixParser.parse(options, args);
        } catch (UnrecognizedOptionException e) {
            System.err.println("Unknown option: " + e.getOption());
            System.exit(1);
        } catch (MissingArgumentException e) {
            System.err.println("No argument for the option " + e.getOption());
            System.exit(1);
        } catch (ParseException e) {
            System.err.println("There was some error while parsing arguments.");
            System.exit(1);
        }
        verbose = !commandLine.hasOption(optNoVerbose.getOpt());

        if (commandLine.hasOption(optHelp.getOpt())) {
            new HelpFormatter().printHelp(
                    "hadoop jar IBDProject.jar Indexer [OPTIONS] <INPUT DIRECTORY> <OUTPUT DIRECTORY>",
                    "", options, "Please note that the output directory must not exist.");
            System.exit(0);
        }

        if (commandLine.hasOption(optIdfType.getOpt())) {
            idfType = commandLine.getOptionValue(optIdfType.getOpt());
        }

        String[] posArgs = commandLine.getArgs();
        if (posArgs.length < 2) {
            System.err.println("Most probably you did not specify input or output path. Please see --help.");
            System.exit(1);
        }

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(posArgs[1]))) {
            System.err.println("Output directory already exists. Please choose non existent one. Please see --help.");
            System.exit(1);
        }

        conf.set("idfType", idfType);

        WordCounter.run(conf, posArgs, verbose);
        IDFEvaluator.run(conf, posArgs, verbose);
        DocumentVectorizer.run(conf, posArgs, verbose);
    }
}