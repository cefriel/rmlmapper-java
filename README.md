# RML Mapper - Cefriel fork

[![Maven Central](https://img.shields.io/maven-central/v/com.cefriel/rmlmapper.svg?label=Maven%20Central)](https://search.maven.org/artifact/com.cefriel/rmlmapper)


This repository contains a fork of the [RMLio/rmlmapper-java](https://github.com/RMLio/rmlmapper-java) repository maintained by Cefriel as a building block of the [Chimera](https://github.com/cefriel/chimera) solution for semantic data conversion.

The fork is updated to version [4.7.0](https://github.com/RMLio/rmlmapper-java/releases/tag/v4.7.0) of the [RMLio/rmlmapper-java](https://github.com/RMLio/rmlmapper-java).

### Changelog ###

#### Add support for RDF4J Repositories ####
- A new QuadStore subclass is defined, `RDF4JRepository`, targeting a remote repository (identified by the address and the repository ID) or an already initialized `Repository` for RDF4J
- New options:
    - `--triplesStore`: Address to reach the triples store. If specified produced triples are written at this address.
    - `--repositoryId`: Repository Id related to the triples store. Also triplesStore option -ts should be provided.

#### Base IRI and Prefix Base IRI ####
- Base IRI can be provided also as an argument in cli.Main
- Relative IRIs are allowed also for objects in generated triples. The base IRI is added, as for subjects, to build a valid IRI.
- The namespace related to the base IRI is added to the output model/to the triple store. If a prefix for the base IRI is provided it is used to identify the base IRI, otherwise, the `base:` prefix is used.

New options:
- `--baseIRI` and `--prefixBaseIRI`: To set base IRI and a related prefix (otherwise @base is parsed)
    
#### Concurrency ####
- Concurrent Executor
- Modification to make multi-thread safe different procedures such as functions loading, record access and storing of generated triples.

New options:
 - _TODO_ Enable concurrent execution with CLI options
    
#### Memory Performance optimizations #### 
When the conversion is applied to huge datasets the bottleneck is often mainly related to memory consumption since the conversion is executed once and without particular timing constraints. Analysing memory dumps we retrieved the set of objects retaining the greater amount of memory: 
- `RDF4JStore`: storing the triples generated by the mapping procedure. 
- Caches in `Executor` class: subject and records cache in the executor to improve performances when join conditions are used in the RML specification. 
- Cache in `RecordsFactory` class: records cache to improve performances when the same logical source is referred by multiple TriplesMap.

Given this information, we implemented a set of options to optimize performances of the lifting procedure in specific cases.

#### Optimized JSON and XML files access ####
We offer alternative implementations of parsing procedures for JSON and XML files. For JSON, we added caches to optimize the compilation of paths and the retrieval. For XML, we changed the implementation using Saxon-HE that guarantees huge improvements in memory consumption and time required to process XPath expressions.  

New options:
 - `-sax` Enable Saxon parser for XPath reference formulation.
 - `-jopt`Enable optimized parser for JSONPath reference formulation.

##### Incremental Writes #####
If a Triple Store is used as output store some additional options can help improve performances. In case of huge materialized knowledge graphs, to reduce memory consumption and to avoid flooding the triple store with a unique insert query, we created options to manage batch-size updates to the repository each time the number of triples generated reaches the batch-size. Activating this option, triples written to the triple store are discarded once completed the query removing data from memory. Duplicates elimination is guaranteed and demanded to the triple store. Requests to the Triple Store can be done in multithreading, to avoid stopping the mapping procedure, with the `ConcurrentRDF4JRepository` class (currently this approach is not available through CLI options).

New options:
- `--incrementalUpdate`: Incremental update option to incrementally load triples in the database while performing mapping procedure. Note that if a triples store is used duplicated triples are automatically removed, therefore, even if incremental updates are constant in size the triples store size may not grow linearly.
- `--batchSize`: Batch size, i.e., number of statements for each update writing generated triples to the triples store. If `-inc` is set the default value is 0, i.e. every triple generated is directly written to the triplestore. If `-inc` is not set its value is ignored. 
- _TODO_ Enable concurrent writes with CLI options
    
##### Mappings without join conditions #####
If mappings have no or few join conditions some additional options can help improve performances. We add an option to avoid using subjects and record caches in the executor and, we tested it noticing that memory consumption lowers while no changes in execution time are observed. Moreover, to reduce even more the memory used during the execution we add an option to order the execution of TriplesMap by logical source, cleaning the records cache in RecordsFactory each time all TriplesMap related to a specific logical source are completed.

New options:
- `--noCache`: Do not use subjects and records caches in the executor.
- `--ordered`: Mapping execution is ordered by logical source and records caches are cleaned after each logical source.

#### Other changes ####
- Empty strings in a csv/json/xml records are not considered in mappings (to avoid having ?s ?p "" kind of triples). To consider also empty strings as records `-es` option should be set. 
- If a logical source is not found, the procedure continues skipping the mapping and logging the event
- Changed `-o` option behaviour. If `-o` option is not set, the default behaviour is do nothing. To print to stdout it is required to use `-o stdout`. To save to file the `-o` option should be set, it can be combined with `-ts` and `-r` options if `-inc` is not set.
- Add `-ctx` option to specify a context (named graph) for triples generated.

### `rmlmapper-cefriel.jar` ###
This is the intended usage of the `rmlmapper-cefriel.jar`.
```
usage: java -jar rmlmapper-cefriel.jar <options>
options:
 -b,--batchSize <arg>             If -inc is set it is used as batch size for incremental updates, i.e., 
                                  number of statements for each write, otherwise it is ignored.
 -ctx,--context <arg>             IRI identifying named graph for triples generated.
 -es,--emptyStrings               Set option if empty strings should be considered as values.
 -f,--functionfile <arg>          Path to functions.ttl file (dynamic functions are found relative to functions.ttl).
 -jopt,--jsonOptRecordFactory     Enable optimized parser for JSONPath reference formulation.
 -inc,--incrementalUpdate         Incremental update option to incrementally load triples in the repository 
                                  while performing the mapping procedure. If -b is not set each triple 
                                  generated is directly written to the repository.
 -iri,--baseIRI <arg>             Specify a base IRI for relative IRIs. Otherwise @base is parsed.
 -m,--mappingfile <arg>           One or more mapping file paths and/or strings (multiple values are concatenated).
 -n,--noCache                     Do not use subjects and records caches in the executor. 
 -ord,--ordered                   Mapping execution is ordered by logical source and caches are cleaned 
                                  after each logical source.
 -o,--outputfile <arg>            Path to output file (-o stdout can be used for debugging).
 -pb,--prefixBaseIRI <arg>        Specify a prefix for the base IRI used for relative IRIs.
 -r,--repositoryId <arg>          Repository Id related to the triples store. Also option -ts
                                  should be provided.
 -sax,--saxRecordFactory          Enable Saxon parser for XPath reference formulation.
 -s,--serialization <arg>         Serialization format (nquads (default), turtle, trig, trix, jsonld, hdt).
 -ts,--triplesStore <arg>         Address to reach the triples store. If specified produced triples are also
                                  written at this address. Also option -r should be provided.
 -v,--verbose                     Show more details in debugging output.
 ```
