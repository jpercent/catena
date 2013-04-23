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

import syndeticlogic.catena.text.IdTable.TableType;

public class CorpusManagerConfig {
    private static final Log log = LogFactory.getLog(CorpusManagerConfig.class);
    private final Options options = new Options();
    private final CommandLineParser parser;
    private final String usage;
    private final String header;
    private final String[] args;
    private String outputPrefix;
    private String inputPrefix;
    private IdTable.TableType tableType;
    private boolean removeOutputDir;

    public CorpusManagerConfig(String[] args) throws Exception {
        this.args = args;
        usage = "index.sh -corpus <corpus-directory> -output <output-perfix> [-posting <strategy> -rm...";
        header = "Generate an index for the corpus" + System.getProperty("line.separator") + System.getProperty("line.separator") + "Options:";
        Option help = new Option("help", "Print this message and exit.");

        @SuppressWarnings("static-access")
        Option corpusDir = OptionBuilder.withArgName("corpus-directory").hasArg().withDescription("Path corpus.").create("corpus");

        @SuppressWarnings("static-access")
        Option outputDir = OptionBuilder.withArgName("output-directory").hasArg().withDescription("Output directory").create("output");
        
        @SuppressWarnings("static-access")
        Option remove = OptionBuilder.withDescription("Removes the output directory").create("remove");
        
        @SuppressWarnings("static-access")
        Option postingCompression = OptionBuilder.withArgName("postings-strategy").hasArg().withDescription("Postings List compression stragtegy.  Options are uncoded and variable").create("posting");

        options.addOption(help);
        options.addOption(corpusDir);
        options.addOption(outputDir);
        options.addOption(postingCompression);
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
            
            if(line.hasOption("remove")) {
                removeOutputDir = true;
            }
            
            tableType = TableType.Uncoded;
            if(line.hasOption("posting")) {
                String strategy = line.getOptionValue("posting");
                if("uncoded".equals(strategy)) {
                    /* default */
                } else if ("variable".equals(strategy)) {
                    tableType = TableType.VariableByteCoded;
                } else {
                    System.err.println(strategy+ " is not a valid postings list encoding");
                }
            }
            
            if (line.hasOption("corpus") && line.hasOption("output")) {
                inputPrefix = line.getOptionValue("corpus");
                outputPrefix = line.getOptionValue("output");
                if(inputPrefix == null || "".equals(inputPrefix)){
                    return false;
                } else if(outputPrefix == null || "".equals(outputPrefix)){
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
    
    public String getOutputPrefix() {
        return outputPrefix;
    }

    public String getInputPrefix() {
        return inputPrefix;
    }

    public TableType getTableType() {
        return tableType;
    } 
    
    public boolean removeOutputPrefix() {
        return removeOutputDir;
    }
}
