package syndeticlogic.catena.text;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.postings.IdTable;
import syndeticlogic.catena.text.postings.IdTable.TableType;

public class QueryConfig {
    private static final Log log = LogFactory.getLog(QueryConfig.class);
    private final Options options = new Options();
    private final CommandLineParser parser;
    private final String usage;
    private final String header;
    private final String[] args;
    private String indexDirectory;
    private IdTable.TableType tableType;
    
    public TableType getTableType() {
        return tableType;
    }
    
    public QueryConfig(String[] args) throws Exception {
        this.args = args;
        usage = "query.sh -index <directory>";
        header = "Reads a query from standard input, excutes it and prints the results to stdin" + System.getProperty("line.separator") + System.getProperty("line.separator") + "Options:";
        Option help = new Option("help", "Print this message and exit.");

        @SuppressWarnings("static-access")
        Option indexDir = OptionBuilder.withArgName("directory").hasArg().withDescription("Index directory").create("index");
//        @SuppressWarnings("static-access")
  //      Option postingsStrategy = OptionBuilder.withArgName("strategy").hasArg().withDescription("Posting compression strategy").create("postings");
        @SuppressWarnings("static-access")
        Option postingCompression = OptionBuilder.withArgName("postings-strategy").hasArg().withDescription("Postings List compression stragtegy.  Options are uncoded and variable").create("posting");
        
        options.addOption(postingCompression);
        
        options.addOption(help);
        options.addOption(indexDir);
        parser = new GnuParser();
    }

    public boolean parse() {
        boolean ret = false;
        if (args == null || args.length <= 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printUsage(new PrintWriter(System.err), 80, usage);
            formatter = new HelpFormatter();
            formatter.printHelp(80, usage, header, options, "");
            return ret;
        }

        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            assert line != null;
            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(80, usage, header, options, "");
                return false;
            }
            
            tableType = TableType.Uncoded;
            if(line.hasOption("posting")) {
                String strategy = line.getOptionValue("posting");
                if("uncoded".equals(strategy)) {
                    /* default */
                } else if ("variable".equals(strategy)) {
                    System.err.println("VariableByteCoded set ");
                    tableType = TableType.VariableByteCoded;
                } else {
                    System.err.println(strategy+ " is not a valid postings list encoding");
                }
            }
            
            if (line.hasOption("index")) {
                indexDirectory = line.getOptionValue("index");
                if(indexDirectory == null || "".equals(indexDirectory)){
                    return false;
                } 
                ret = true;
            } else {
                log.warn("Invalid parameters");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(80, usage, header, options, "");
            }
        } catch (org.apache.commons.cli.ParseException e) {
            log.warn("Exception parsing arguments", e);
        }
        return ret;
    }

    public String getIndexPrefix() {
        return indexDirectory;
    }
}
