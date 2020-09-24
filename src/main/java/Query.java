import lib.Const;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import query.CoreQuery;

import java.io.IOException;

public class Query {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 0 - indexer output folder
        // 1 - query output folder
        // 2 - query results number
        // 3 - query text
        // 4 (optional) - relevance solver
        Configuration conf = new Configuration();

        Options options = new Options();
        // Optional arguement solver BM25 or Basic
        Option optSolverType = new Option("s", "solver", true, "Solver for query, can be BM25 or Basic");
        Option optHelp = new Option("h", "help", false, "Help message.");
        Option optNoVerbose = new Option("nv", "no-verbose", false, "Show MapReduce progress output.");


        options.addOption(optSolverType);
        options.addOption(optHelp);
        options.addOption(optNoVerbose);

        String solverType = "BM25";

        CommandLineParser cmdLinePosixParser = new PosixParser();
        CommandLine cmd = null;
        // Mistakes from cmd
        try {
            cmd = cmdLinePosixParser.parse(options, args);
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
        // Wrong solver name
        if (cmd.hasOption(optHelp.getOpt())) {
            new HelpFormatter().printHelp(
                    "hadoop jar IBDProject.jar Query [OPTIONS] <INDEXER OUTPUT> <QUERY OUTPUT> <RESULT NUMBER> <QUERY TEXT>",
                    "", options, "Please note that the solver can be only be 'BM25' or 'Basic'.");
            System.exit(0);
        }

        boolean verbose = !cmd.hasOption(optNoVerbose.getOpt());

        if (cmd.hasOption(optSolverType.getOpt())) {
            solverType = cmd.getOptionValue(optSolverType.getOpt());
        }

        if (! solverType.equals("BM25") & (! solverType.equals("Basic"))) {
            System.err.println("Wrong solver type was specified. please refer to help to see the available types.");
        }

        String[] posArgs = cmd.getArgs();
        // Not enough arguments
        if (posArgs.length < 3) {
            System.err.println("You did not specified the appropriate amount of arguments. Required arguments are: <indexer output folder> <query output folder> <query results number> <query text>");
            System.exit(1);
        }

        FileSystem fileSystem = FileSystem.get(conf);
        // Wrong Index Folder
        if (! (fileSystem.exists(new Path(posArgs[0], Const.VECTORIZED)) & fileSystem.exists(new Path(posArgs[0],Const.WORDS)))) {
            System.err.println("Indexer file does not contain all necessary files.");
            System.exit(1);
        }
        // Output already exists
        if (fileSystem.exists(new Path(posArgs[1]))) {
            System.err.println("Output directory already exists. Please choose non existent one. Please see --help.");
            System.exit(1);
        }
        // Number of pages to fetch is not a number
        if (!posArgs[2].matches("[0-9]+")) {
            System.err.println("Number of results contains characters other than numbers");
        }

        conf.set("solver", solverType);
        CoreQuery.run(conf, posArgs, verbose);
    }
}
