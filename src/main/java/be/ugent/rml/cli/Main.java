package be.ugent.rml.cli;

import be.ugent.rml.Executor;
import be.ugent.rml.NAMESPACES;
import be.ugent.rml.Utils;
import be.ugent.rml.conformer.MappingConformer;
import be.ugent.rml.functions.FunctionLoader;
import be.ugent.rml.functions.lib.IDLabFunctions;
import be.ugent.rml.metadata.MetadataGenerator;
import be.ugent.rml.records.JSONOptRecordFactory;
import be.ugent.rml.records.RecordsFactory;
import be.ugent.rml.records.ReferenceFormulationRecordFactory;
import be.ugent.rml.records.XMLSAXRecordFactory;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.store.RDF4JRepository;
import be.ugent.rml.store.QuadStoreFactory;
import be.ugent.rml.store.RDF4JStore;
import be.ugent.rml.store.SimpleQuadStore;
import be.ugent.rml.term.NamedNode;
import be.ugent.rml.term.Term;
import ch.qos.logback.classic.Level;
import org.apache.commons.cli.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    public static void main(String[] args) {
        main(args, System.getProperty("user.dir"));
    }

    /**
     * Main method use for the CLI. Allows to also set the current working directory via the argument basePath.
     * @param args     the CLI arguments
     * @param basePath the basePath used during the execution.
     */
    public static void main(String[] args, String basePath) {
        Options options = new Options();
        Option mappingdocOption = Option.builder("m")
                .longOpt("mappingfile")
                .hasArg()
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .desc("one or more mapping file paths and/or strings (multiple values are concatenated). " +
                        "r2rml is converted to rml if needed using the r2rml arguments.")
                .build();
        Option outputfileOption = Option.builder("o")
                .longOpt("outputfile")
                .hasArg()
                .desc("path to output file (-o stdout can be used for debugging)")
                .build();
        Option functionfileOption = Option.builder("f")
                .longOpt("functionfile")
                .hasArg()
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .desc("path to functions.ttl file (dynamic functions with relative paths are found relative to the cwd)")
                .build();
        Option triplesmapsOption = Option.builder("t")
                .longOpt("triplesmaps")
                .hasArg()
                .desc("IRIs of the triplesmaps that should be executed in order, split by ',' (default is all triplesmaps)")
                .build();
        Option removeduplicatesOption = Option.builder("d")
                .longOpt("duplicates")
                .desc("remove duplicates in the output")
                .build();
        Option configfileOption = Option.builder("c")
                .longOpt("configfile")
                .hasArg()
                .desc("path to configuration file")
                .build();
        Option helpOption = Option.builder("h")
                .longOpt("help")
                .desc("show help info")
                .build();
        Option verboseOption = Option.builder("v")
                .longOpt("verbose")
                .desc("show more details in debugging output")
                .build();
        Option metadataOption = Option.builder("e")
                .longOpt("metadatafile")
                .hasArg()
                .desc("path to output metadata file")
                .build();
        Option metadataDetailLevelOption = Option.builder("l")
                .longOpt("metadataDetailLevel")
                .hasArg()
                .desc("generate metadata on given detail level (dataset - triple - term)")
                .build();
        Option serializationFormatOption = Option.builder("s")
                .longOpt("serialization")
                .desc("serialization format (nquads (default), turtle, trig, trix, jsonld, hdt)")
                .hasArg()
                .build();
        Option jdbcDSNOption = Option.builder("dsn")
                .longOpt("r2rml-jdbcDSN")
                .desc("DSN of the database when using R2RML rules")
                .hasArg()
                .build();
        Option passwordOption = Option.builder("p")
                .longOpt("r2rml-password")
                .desc("password of the database when using R2RML rules")
                .hasArg()
                .build();
        Option usernameOption = Option.builder("u")
                .longOpt("r2rml-username")
                .desc("username of the database when using R2RML rules")
                .hasArg()
                .build();
        Option triplesStoreOption = Option.builder("ts")
                .longOpt("triplesStore")
                .desc("Address to reach the triples store. If specified produced triples are also written at this address." +
                        "Also repositoryId option -r should be provided.")
                .hasArg()
                .build();
        Option repositoryIdOption = Option.builder("r")
                .longOpt("repositoryId")
                .desc("Repository Id related to the triples store." +
                        "Also triplesStore option -ts should be provided.")
                .hasArg()
                .build();
        Option contextOption = Option.builder("ctx")
                .longOpt("contextIRI")
                .desc("Specify a context for querying the triple store.")
                .hasArg()
                .build();
        Option batchSizeOption = Option.builder("b")
                .longOpt("batchSize")
                .desc("If -inc is set it is used as batch size for incremental updates, " +
                        "i.e., number of statements for each write, otherwise it is ignored.")
                .hasArg()
                .build();
        Option incrementalUpdateOption = Option.builder("inc")
                .longOpt("incrementalUpdate")
                .desc("Incremental update option to incrementally load triples in the repository while performing                                       \n" +
                        "the mapping procedure. If -b is not set each triple generated is directly written " +
                        "to the repository.")
                .build();
        Option noCacheOption = Option.builder("n")
                .longOpt("noCache")
                .desc("Do not use records and subject cache in the executor.")
                .build();
        Option orderedOption = Option.builder("ord")
                .longOpt("ordered")
                .desc("Mapping execution is ordered by logical source and caches are cleaned after each logical source." +
                        "This option improves memory consumption and it is advisable if no join condition exist among mappings.")
                .build();
        Option baseIRIOption = Option.builder("iri")
                .longOpt("baseIRI")
                .desc("Specify a base IRI for relative IRIs.")
                .hasArg()
                .build();
        Option baseIRIPrefixOption = Option.builder("pb")
                .longOpt("prefixBaseIRI")
                .desc("Specify a prefix for the base IRI used for relative IRIs.")
                .hasArg()
                .build();
        Option emptyStringsOption = Option.builder("es")
                .longOpt("emptyStrings")
                .desc("Set option if empty strings should be considered as values.")
                .build();
        Option saxOption = Option.builder("sax")
                .longOpt("saxRecordFactory")
                .desc("[beta] Enable Saxon parser for XPath reference formulation.")
                .build();
        Option jsonOptOption = Option.builder("jopt")
                .longOpt("jsonOptRecordFactory")
                .desc("[beta] Enable optimized parser for JSONPath reference formulation.")
                .build();
        options.addOption(mappingdocOption);
        options.addOption(outputfileOption);
        options.addOption(functionfileOption);
        options.addOption(removeduplicatesOption);
        options.addOption(triplesmapsOption);
        options.addOption(configfileOption);
        options.addOption(helpOption);
        options.addOption(verboseOption);
        options.addOption(serializationFormatOption);
        options.addOption(metadataOption);
        options.addOption(metadataDetailLevelOption);
        options.addOption(jdbcDSNOption);
        options.addOption(passwordOption);
        options.addOption(usernameOption);
        options.addOption(triplesStoreOption);
        options.addOption(repositoryIdOption);
        options.addOption(contextOption);
        options.addOption(batchSizeOption);
        options.addOption(incrementalUpdateOption);
        options.addOption(noCacheOption);
        options.addOption(orderedOption);
        options.addOption(baseIRIOption);
        options.addOption(baseIRIPrefixOption);
        options.addOption(emptyStringsOption);
        options.addOption(saxOption);
        options.addOption(jsonOptOption);

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine lineArgs = parser.parse(options, args);

            // Check if config file is given
            Properties configFile = null;
            if (lineArgs.hasOption("c")) {
                configFile = new Properties();
                configFile.load(Utils.getReaderFromLocation(lineArgs.getOptionValue("c")));
            }

            if (checkOptionPresence(helpOption, lineArgs, configFile)) {
                printHelp(options);
                return;
            }

            if (checkOptionPresence(verboseOption, lineArgs, configFile)) {
                setLoggerLevel(Level.DEBUG);
            } else {
                setLoggerLevel(Level.ERROR);
                Logger databaseLog = LoggerFactory.getLogger(RDF4JRepository.class);
                ((ch.qos.logback.classic.Logger) databaseLog).setLevel(Level.DEBUG);
                Logger infoLog = LoggerFactory.getLogger(Executor.class);
                ((ch.qos.logback.classic.Logger) infoLog).setLevel(Level.INFO);
            }

            String[] mOptionValue = getOptionValues(mappingdocOption, lineArgs, configFile);
            if (mOptionValue == null) {
                printHelp(options);
            } else {
                // Concatenate all mapping files
                List<InputStream> lis = Arrays.stream(mOptionValue)
                        .map(Utils::getInputStreamFromFileOrContentString)
                        .collect(Collectors.toList());
                InputStream is = new SequenceInputStream(Collections.enumeration(lis));

                Map<String, String> mappingOptions = new HashMap<>();
                for (Option option : new Option[]{jdbcDSNOption, passwordOption, usernameOption}) {
                    if (checkOptionPresence(option, lineArgs, configFile)) {
                        mappingOptions.put(option.getLongOpt().replace("r2rml-", ""), getOptionValues(option, lineArgs, configFile)[0]);
                    }
                }

                // Read mapping file.
                RDF4JStore rmlStore = new RDF4JStore();
                rmlStore.read(is, null, RDFFormat.TURTLE);

                // Convert mapping file to RML if needed.
                MappingConformer conformer = new MappingConformer(rmlStore, mappingOptions);

                try {
                    boolean conversionNeeded = conformer.conform();

                    if (conversionNeeded) {
                        logger.info("Conversion to RML was needed.");
                    }
                } catch (Exception e) {
                    logger.error(fatal, "Failed to make mapping file conformant to RML spec.", e);
                }

                Map<String, ReferenceFormulationRecordFactory> map = new HashMap<>();
                if (checkOptionPresence(saxOption, lineArgs, configFile))
                    map.put(NAMESPACES.QL + "XPath", new XMLSAXRecordFactory());
                if (checkOptionPresence(jsonOptOption, lineArgs, configFile))
                    map.put(NAMESPACES.QL + "JSONPath", new JSONOptRecordFactory());
                RecordsFactory factory = new RecordsFactory(basePath, map);
                if (checkOptionPresence(emptyStringsOption, lineArgs, configFile))
                    factory.setEmptyStrings(true);

                String outputFormat = getPriorityOptionValue(serializationFormatOption, lineArgs, configFile);
                QuadStore outputStore;
                boolean tripleStore = checkOptionPresence(triplesStoreOption, lineArgs, configFile)
                        && checkOptionPresence(repositoryIdOption, lineArgs, configFile);

                if (outputFormat == null || outputFormat.equals("nquads") || outputFormat.equals("hdt")) {
                    outputStore = new SimpleQuadStore();
                } else {
                    outputStore = new RDF4JStore();
                }

                IRI context = null;
                if (checkOptionPresence(contextOption, lineArgs, configFile)) {
                    ValueFactory vf = SimpleValueFactory.getInstance();
                    context = vf.createIRI(getPriorityOptionValue(contextOption, lineArgs, configFile));
                }
                if (tripleStore) {
                    String ts = getPriorityOptionValue(triplesStoreOption, lineArgs, configFile);
                    String repoID = getPriorityOptionValue(repositoryIdOption, lineArgs, configFile);
                    if (checkOptionPresence(batchSizeOption, lineArgs, configFile))
                        outputStore = new RDF4JRepository(ts, repoID, context,
                                Integer.parseInt(getPriorityOptionValue(batchSizeOption, lineArgs, configFile)),
                                checkOptionPresence(incrementalUpdateOption, lineArgs, configFile));
                    else
                        outputStore = new RDF4JRepository(ts, repoID, context, 0, false);
                }

                Executor executor;

                // Extract required information and create the MetadataGenerator
                MetadataGenerator metadataGenerator = null;
                String metadataFile = getPriorityOptionValue(metadataOption, lineArgs, configFile);
                String requestedDetailLevel = getPriorityOptionValue(metadataDetailLevelOption, lineArgs, configFile);

                if (checkOptionPresence(metadataOption, lineArgs, configFile)) {
                    if (requestedDetailLevel != null) {
                        MetadataGenerator.DETAIL_LEVEL detailLevel;
                        switch (requestedDetailLevel) {
                            case "dataset":
                                detailLevel = MetadataGenerator.DETAIL_LEVEL.DATASET;
                                break;
                            case "triple":
                                detailLevel = MetadataGenerator.DETAIL_LEVEL.TRIPLE;
                                break;
                            case "term":
                                detailLevel = MetadataGenerator.DETAIL_LEVEL.TERM;
                                break;
                            default:
                                logger.error("Unknown metadata detail level option. Use the -h flag for more info.");
                                return;
                        }
                        metadataGenerator = new MetadataGenerator(
                                detailLevel,
                                getPriorityOptionValue(metadataOption, lineArgs, configFile),
                                mOptionValue,
                                rmlStore
                        );
                    } else {
                        logger.error("Please specify the detail level when requesting metadata generation. Use the -h flag for more info.");
                    }
                }

                String[] fOptionValue = getOptionValues(functionfileOption, lineArgs, configFile);
                FunctionLoader functionLoader;

                // Read function description files.
                if (fOptionValue == null) {
                    functionLoader = new FunctionLoader();
                } else {
                    logger.debug("Using custom path to functions.ttl file: " + Arrays.toString(fOptionValue));
                    RDF4JStore functionDescriptionTriples = new RDF4JStore();
                    functionDescriptionTriples.read(Utils.getInputStreamFromFile(Utils.getFile("functions_idlab.ttl")), null, RDFFormat.TURTLE);
                    Map<String, Class> libraryMap = new HashMap<>();
                    libraryMap.put("IDLabFunctions", IDLabFunctions.class);
                    List<InputStream> lisF = Arrays.stream(fOptionValue)
                            .map(Utils::getInputStreamFromFileOrContentString)
                            .collect(Collectors.toList());
                    for (int i = 0; i < lisF.size(); i++) {
                        functionDescriptionTriples.read(lisF.get(i), null, RDFFormat.TURTLE);
                    }
                    functionLoader = new FunctionLoader(functionDescriptionTriples, libraryMap);
                }

                // We have to get the InputStreams of the RML documents again,
                // because we can only use an InputStream once
                lis = Arrays.stream(mOptionValue)
                        .map(Utils::getInputStreamFromFileOrContentString)
                        .collect(Collectors.toList());
                is = new SequenceInputStream(Collections.enumeration(lis));

                String baseIRI;
                if (checkOptionPresence(baseIRIOption, lineArgs, configFile)) {
                    baseIRI = getPriorityOptionValue(baseIRIOption, lineArgs, configFile);
                    logger.debug("Base IRI set to value: " + lineArgs.getOptionValue("iri"));
                }
                else
                    baseIRI = Utils.getBaseDirectiveTurtle(is);

                executor = new Executor(rmlStore, factory, functionLoader, outputStore, baseIRI);
                if (checkOptionPresence(noCacheOption, lineArgs, configFile))
                    executor.setNoCache(true);
                if (checkOptionPresence(orderedOption, lineArgs, configFile))
                    executor.setOrdered(true);

                List<Term> triplesMaps = new ArrayList<>();

                String tOptionValue = getPriorityOptionValue(triplesmapsOption, lineArgs, configFile);
                if (tOptionValue != null) {
                    List<String> triplesMapsIRI = Arrays.asList(tOptionValue.split(","));
                    triplesMapsIRI.forEach(iri -> {
                        triplesMaps.add(new NamedNode(iri));
                    });
                }

                if (metadataGenerator != null) {
                    metadataGenerator.preMappingGeneration(triplesMaps.isEmpty() ?
                            executor.getTriplesMaps() : triplesMaps, rmlStore);
                }

                // Get start timestamp for post mapping metadata
                String startTimestamp = Instant.now().toString();

                try {
                    QuadStore result = executor.execute(triplesMaps, checkOptionPresence(removeduplicatesOption, lineArgs, configFile),
                            metadataGenerator);

                    // Get stop timestamp for post mapping metadata
                    String stopTimestamp = Instant.now().toString();

                    // Generate post mapping metadata and output all metadata
                    if (metadataGenerator != null) {
                        metadataGenerator.postMappingGeneration(startTimestamp, stopTimestamp,
                                result);

                        writeOutput(metadataGenerator.getResult(), metadataFile, outputFormat);
                    }

                    String outputFile = getPriorityOptionValue(outputfileOption, lineArgs, configFile);

                    if (result.isEmpty()) {
                        logger.info("No results!");
                        // Write even if no results
                    }
                    result.copyNameSpaces(rmlStore);

                    if (checkOptionPresence(baseIRIOption, lineArgs, configFile))
                        if (checkOptionPresence(baseIRIPrefixOption, lineArgs, configFile))
                            result.addNamespace(getPriorityOptionValue(baseIRIPrefixOption, lineArgs, configFile), baseIRI);
                        else
                            result.addNamespace("base", baseIRI);

                    //If --inc option is set, triples are discarded once written to the db
                    if (checkOptionPresence(outputfileOption, lineArgs, configFile) &&
                            !checkOptionPresence(incrementalUpdateOption, lineArgs, configFile))
                        //Write quads
                        writeOutput(result, outputFile, outputFormat);

                    // Graceful shutDown of QuadStore
                    result.shutDown();

                } catch (Exception e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            logger.error("Parsing failed. Reason: " + exp.getMessage());
            printHelp(options);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    private static boolean checkOptionPresence(Option option, CommandLine lineArgs, Properties properties) {
        return lineArgs.hasOption(option.getOpt()) || (properties != null
                && properties.getProperty(option.getLongOpt()) != null
                && !properties.getProperty(option.getLongOpt()).equals("false"));  // ex: 'help = false' in the config file shouldn't return the help text
    }

    private static String getPriorityOptionValue(Option option, CommandLine lineArgs, Properties properties) {
        if (lineArgs.hasOption(option.getOpt())) {
            return lineArgs.getOptionValue(option.getOpt());
        } else if (properties != null && properties.getProperty(option.getLongOpt()) != null) {
            return properties.getProperty(option.getLongOpt());
        } else {
            return null;
        }
    }

    private static String[] getOptionValues(Option option, CommandLine lineArgs, Properties properties) {
        if (lineArgs.hasOption(option.getOpt())) {
            return lineArgs.getOptionValues(option.getOpt());
        } else if (properties != null && properties.getProperty(option.getLongOpt()) != null) {
            return properties.getProperty(option.getLongOpt()).split(" ");
        } else {
            return null;
        }
    }


    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar mapper.jar <options>\noptions:", options);
    }

    private static void setLoggerLevel(Level level) {
        Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ((ch.qos.logback.classic.Logger) root).setLevel(level);
    }

    private static void writeOutput(QuadStore store, String outputFile, String format) {
        boolean hdt = format != null && format.equals("hdt");

        if (hdt) {
            try {
                format = "nquads";
                File tmpFile = File.createTempFile("file", ".nt");
                tmpFile.deleteOnExit();
                String uncompressedOutputFile = tmpFile.getAbsolutePath();

                File nquadsFile = writeOutputUncompressed(store, uncompressedOutputFile, format);
                Utils.ntriples2hdt(uncompressedOutputFile, outputFile);
                nquadsFile.deleteOnExit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            if (format != null) {
                format = format.toLowerCase();
            } else {
                format = "nquads";
            }

            writeOutputUncompressed(store, outputFile, format);
        }
    }

    private static File writeOutputUncompressed(QuadStore store, String outputFile, String format) {
        File targetFile = null;

        if (store.size() > 1) {
            logger.info(store.size() + " quads were generated");
        } else {
            logger.info(store.size() + " quad was generated");
        }

        try {
            Writer out;
            String doneMessage = null;

            //if output file provided, write to triples output file
            if (outputFile == null || outputFile.equals("stdout")) {
                out = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
            } else {
                targetFile = new File(outputFile);
                logger.info("Writing quads to " + targetFile.getPath() + "...");

                if (!targetFile.isAbsolute()) {
                    targetFile = new File(System.getProperty("user.dir") + "/" + outputFile);
                }

                doneMessage = "Writing to " + targetFile.getPath() + " is done.";

                out = Files.newBufferedWriter(targetFile.toPath(), StandardCharsets.UTF_8);
            }

            store.write(out, format);
            out.close();

            if (doneMessage != null) {
                logger.info(doneMessage);
            }
        } catch (Exception e) {
            System.err.println("Writing output failed. Reason: " + e.getMessage());
        }

        return targetFile;
    }
}
