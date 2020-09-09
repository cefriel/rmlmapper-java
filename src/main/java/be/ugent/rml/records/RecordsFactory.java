package be.ugent.rml.records;

import be.ugent.rml.Executor;
import be.ugent.rml.NAMESPACES;
import be.ugent.rml.Utils;
import be.ugent.rml.access.Access;
import be.ugent.rml.access.AccessFactory;
import be.ugent.rml.store.Quad;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.term.NamedNode;
import be.ugent.rml.term.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class creates records based on RML rules.
 */
public class RecordsFactory {

    private static final Logger logger = LoggerFactory.getLogger(RecordsFactory.class);

    private Map<Access, Map<String, Map<String, List<Record>>>> recordCache;
    private AccessFactory accessFactory;
    private Map<String, ReferenceFormulationRecordFactory> referenceFormulationRecordFactoryMap;

    public RecordsFactory(String basePath) {
        this(basePath, null);
    }

    public RecordsFactory(AccessFactory accessFactory) {
        this(accessFactory, null);
    }

    public RecordsFactory(String basePath,
                          Map<String, ReferenceFormulationRecordFactory> map) {
        accessFactory = new AccessFactory(basePath);
        init(map);
    }

    public RecordsFactory(AccessFactory accessFactory,
                          Map<String, ReferenceFormulationRecordFactory> map) {
        this.accessFactory = accessFactory;
        init(map);
    }

    private void init(Map<String, ReferenceFormulationRecordFactory> map) {
        recordCache = new ConcurrentHashMap<>();
        if (map == null)
            referenceFormulationRecordFactoryMap = new ConcurrentHashMap<>();
        else
            referenceFormulationRecordFactoryMap = new ConcurrentHashMap<>(map);
        if(referenceFormulationRecordFactoryMap.get(NAMESPACES.QL + "XPath") == null)
            referenceFormulationRecordFactoryMap.put(NAMESPACES.QL + "XPath", new XMLRecordFactory());
        if(referenceFormulationRecordFactoryMap.get(NAMESPACES.QL + "JSONPath") == null)
            referenceFormulationRecordFactoryMap.put(NAMESPACES.QL + "JSONPath", new JSONRecordFactory());
        if(referenceFormulationRecordFactoryMap.get(NAMESPACES.QL + "CSV") == null)
            referenceFormulationRecordFactoryMap.put(NAMESPACES.QL + "CSV", new CSVRecordFactory());

        for(String key : referenceFormulationRecordFactoryMap.keySet())
            logger.info("Reference Formulation implementation [key: " + key.replaceAll(NAMESPACES.QL, "") +
                    ", " + referenceFormulationRecordFactoryMap.get(key).getClass().getCanonicalName() + "]");
    }

    public void setEmptyStrings(boolean emptyStrings) {
        for(String key : referenceFormulationRecordFactoryMap.keySet())
            referenceFormulationRecordFactoryMap.get(key).setEmptyStrings(emptyStrings);
    }

    /**
     * This method creates and returns records for a given Triples Map and set of RML rules.
     * @param triplesMap the Triples Map for which the record need to be created.
     * @param rmlStore the QuadStore with the RML rules.
     * @return a list of records.
     * @throws IOException
     */
     public List<Record> createRecords(Term triplesMap, QuadStore rmlStore) throws IOException {
        // Get Logical Sources.
        List<Term> logicalSources = Utils.getObjectsFromQuads(rmlStore.getQuads(triplesMap, new NamedNode(NAMESPACES.RML + "logicalSource"), null));

        // Check if there is at least one Logical Source.
        if (!logicalSources.isEmpty()) {
            Term logicalSource = logicalSources.get(0);

            Access access = accessFactory.getAccess(logicalSource, rmlStore);

            // Get Logical Source information
            List<Term> referenceFormulations = Utils.getObjectsFromQuads(rmlStore.getQuads(logicalSource, new NamedNode(NAMESPACES.RML + "referenceFormulation"), null));
            List<Term> tables = Utils.getObjectsFromQuads(rmlStore.getQuads(logicalSource, new NamedNode(NAMESPACES.RR + "tableName"), null));

            // If no rml:referenceFormulation is given, but a table is given --> CSV
            if (referenceFormulations.isEmpty() && !tables.isEmpty()) {
                referenceFormulations = new ArrayList<>();
                referenceFormulations.add(0, new NamedNode(NAMESPACES.QL + "CSV"));
            }

            if (referenceFormulations.isEmpty()) {
                throw new Error("The Logical Source of " + triplesMap + " does not have a reference formulation.");
            } else {
                String referenceFormulation = referenceFormulations.get(0).getValue();

                return getRecords(access, logicalSource, referenceFormulation, rmlStore);
            }
        } else {
            throw new Error("No Logical Source is found for " + triplesMap + ". Exactly one Logical Source is required per Triples Map.");
        }
    }

    /**
     * This method returns records if they can be found in the cache of the factory.
     * @param access the access from which records need to come.
     * @param referenceFormulation the used reference formulation.
     * @param hash the hash used for the cache. Currently, this hash is based on the Logical Source (see hashLogicalSource()).
     * @return
     */
    private List<Record> getRecordsFromCache(Access access, String referenceFormulation, String hash) {
        synchronized (recordCache) {
            if (recordCache.containsKey(access)
                    && recordCache.get(access).containsKey(referenceFormulation)
                    && recordCache.get(access).get(referenceFormulation).containsKey(hash)
            ) {
                return recordCache.get(access).get(referenceFormulation).get(hash);
            } else {
                return null;
            }
        }
    }

    /**
     * This method puts a list of records in the cache.
     * @param access the access from which the records where fetched.
     * @param referenceFormulation the used reference formulation.
     * @param hash the used hash for the cache. Currently, this hash is based on the Logical Source (see hashLogicalSource()).
     * @param records the records that needs to be put into the cache.
     */
    private void putRecordsIntoCache(Access access, String referenceFormulation, String hash, List<Record> records) {
        synchronized (recordCache) {
            if (!recordCache.containsKey(access)) {
                recordCache.put(access, new ConcurrentHashMap<>());
            }

            if (!recordCache.get(access).containsKey(referenceFormulation)) {
                recordCache.get(access).put(referenceFormulation, new ConcurrentHashMap<>());
            }

            recordCache.get(access).get(referenceFormulation).put(hash, records);
        }

    }

    /**
     * This method returns the records either from the cache or by fetching them for the data sources.
     * @param access the access from which the records needs to be fetched.
     * @param logicalSource the used Logical Source.
     * @param referenceFormulation the used reference formulation.
     * @param rmlStore the QuadStore with the RML rules.
     * @return a list of records.
     * @throws IOException
     */
    synchronized private List<Record> getRecords(Access access, Term logicalSource, String referenceFormulation, QuadStore rmlStore) throws IOException {
        String logicalSourceHash = hashLogicalSource(logicalSource, rmlStore);

        // Try to get the records from the cache.
        List<Record> records = getRecordsFromCache(access, referenceFormulation, logicalSourceHash);

        // If there are no records in the cache.
        // fetch from the data source.
        if (records == null) {
            try {
                // Select the Record Factory based on the reference formulation.
                ReferenceFormulationRecordFactory factory = referenceFormulationRecordFactoryMap.get(referenceFormulation);
                records = factory.getRecords(access, logicalSource, rmlStore);

                // Store the records in the cache for later.
                putRecordsIntoCache(access, referenceFormulation, logicalSourceHash, records);

                return records;
            } catch (IOException e) {
                throw e;
            }
        }

        return records;
    }

    /**
     * This method returns a hash for a Logical Source.
     * @param logicalSource the Logical Source for which a hash is wanted.
     * @param rmlStore the QuadStore of the RML rules.
     * @return a hash for the Logical Source.
     */
    private String hashLogicalSource(Term logicalSource, QuadStore rmlStore) {
        List<Quad> quads = rmlStore.getQuads(logicalSource, null, null);
        final String[] hash = {""};

        quads.forEach(quad -> {
            if (!quad.getPredicate().getValue().equals(NAMESPACES.RML + "source")
                    && !quad.getPredicate().getValue().equals(NAMESPACES.RML + "referenceFormulation") ) {
                hash[0] += quad.getObject().getValue();
            }
        });

        return hash[0];
    }

    public void cleanRecordCache() {
        recordCache = new ConcurrentHashMap<>();
    }

}
