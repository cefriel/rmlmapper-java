package be.ugent.rml.store;

import be.ugent.rml.term.BlankNode;
import be.ugent.rml.term.Literal;
import be.ugent.rml.term.NamedNode;
import be.ugent.rml.term.Term;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.contextaware.ContextAwareRepository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Writer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConcurrentRDF4JRepository extends QuadStore {

    public static int CORE_POOL_SIZE = 4;
    public static int MAXIMUM_POOL_SIZE = 5;
    public static int KEEP_ALIVE_MINUTES = 10;
    private ThreadPoolExecutor executor;

    private Repository repo;
    private boolean shutdownRepository;

    private int batchSize;
    private boolean incremental;
    private AtomicInteger numBatches;
    private AtomicInteger numWrites;

    private Model model;
    private int triplesWithGraphCounter;

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRDF4JRepository.class);

    public ConcurrentRDF4JRepository(String dbAddress, String repositoryID, IRI context, int batchSize, boolean incremental) {
        repo = new HTTPRepository(dbAddress, repositoryID);
        shutdownRepository = true;
        repo.init();
        init(context, batchSize, incremental);
    }

    public ConcurrentRDF4JRepository(Repository r, IRI context, int batchSize, boolean incremental) {
        this.repo = r;
        init(context, batchSize, incremental);
    }

    private void init (IRI context, int batchSize, boolean incremental) {
        if (context != null) {
            ContextAwareRepository cRepo = new ContextAwareRepository(this.repo);
            cRepo.setInsertContext(context);
            this.repo = cRepo;
        }
        model = new TreeModel();
        this.batchSize = batchSize;
        this.incremental = incremental;
        logger.info("Options [Batch Size: " + batchSize + ", Incremental: " + incremental + "]");
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
        executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE,
                KEEP_ALIVE_MINUTES, TimeUnit.MINUTES, workQueue);
        logger.info("Executor initialized [core_pool_size: " + CORE_POOL_SIZE
                + ", maximum_pool_size: " + MAXIMUM_POOL_SIZE
                + ", keep_alive_minutes: " + KEEP_ALIVE_MINUTES + "]");
        triplesWithGraphCounter = 0;
        numBatches = new AtomicInteger(0);
        numWrites = new AtomicInteger(0);
    }

    @Override
    public void removeDuplicates() {
        // Triple Store already simplifies duplicated quads
    }

    @Override
    public void addQuad(Term subject, Term predicate, Term object, Term graph) {
        Resource s = getFilterSubject(subject);
        IRI p = getFilterPredicate(predicate);
        Value o = getFilterObject(object);
        Resource g = getFilterGraph(graph);

        synchronized (model) {
            model.add(s, p, o); // Discarded now ,g);

            if (incremental && model.size() >= batchSize)
                writeToRepository();
        }

    }

    @Override
    public List<Quad> getQuads(Term subject, Term predicate, Term object, Term graph) {
        throw new Error("Method getQuads() not implemented.");
    }

    @Override
    public void addNamespace(String prefix, String name) {
        try (RepositoryConnection con = repo.getConnection()) {
            con.setNamespace(prefix, name);
        }
    }

    @Override
    public void copyNameSpaces(QuadStore store) {
        if (store instanceof RDF4JStore) {
            RDF4JStore rdf4JStore = (RDF4JStore) store;

            rdf4JStore.getModel()
                    .getNamespaces()
                    .forEach(namespace -> addNamespace(namespace.getPrefix(), namespace.getName()));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void writeToRepository() {
        if (triplesWithGraphCounter > 0)
            logger.warn("There are graphs generated. They are not supported yet.");

        if (batchSize == 0)
            batchSize = model.size();

        Set<Statement> batch = new HashSet<>();
        Iterator<Statement> i = model.iterator();
        int c = 0;
        while (i.hasNext()) {
            batch.add(i.next());
            i.remove();
            if (c >= batchSize - 1 || !i.hasNext()) {
                final Model b = new TreeModel(batch);
                executor.execute(() -> {
                    try (RepositoryConnection con = repo.getConnection()) {
                        con.add(b);
                        logger.info("Query completed! [query_num: " + numWrites.incrementAndGet() + ", size: " + b.size() + "]");
                    }
                });
                logger.info("Concurrent write to database queued [batch_num: " + numBatches.incrementAndGet() + "]");
                batch = new HashSet<>();
                c = -1;
            }
            c += 1;
        }
    }

    /**
     * Statements in the buffer are flushed to the repository. Internal components are gracefully stopped.
     * If the Repository object is passed as argument in the constructor, the shutDown() method is not called on the repository.
     */
    @Override
    public void shutDown() {
        synchronized (model) {
            writeToRepository();
        }

        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(60, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("Execution stopped: TIMEOUT");
            }
        }

        if (shutdownRepository)
            repo.shutDown();
    }

    @Override
    public void read(InputStream is, String base, RDFFormat format) throws Exception {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    @Override
    public void write(Writer out, String format) {
            switch (format) {
                case "repo":
                    writeToRepository();
                    break;
                case "turtle":
                    Rio.write(model, out, RDFFormat.TURTLE);

                    if (triplesWithGraphCounter > 0) {
                        logger.warn("There are graphs generated. However, Turtle does not support graphs. Use Trig instead.");
                    }

                    break;
                case "trig":
                    Rio.write(model, out, RDFFormat.TRIG);
                    break;
                case "trix":
                    Rio.write(model, out, RDFFormat.TRIX);
                    break;
                case "jsonld":
                    Rio.write(model, out, RDFFormat.JSONLD);
                    break;
                case "nquads":
                    Rio.write(model, out, RDFFormat.NQUADS);
                    break;
                default:
                    throw new Error("Serialization " + format + " not supported");
            }
    }

    @Override
    public boolean isEmpty() {
        synchronized (model) {
            return model.isEmpty();
        }
    }

    @Override
    public int size() {
        synchronized (model) {
            return model.size();
        }
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    @Override
    public void removeQuads(Term subject, Term predicate, Term object, Term graph) {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    @Override
    public boolean contains(Term subject, Term predicate, Term object, Term graph) {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    @Override
    public boolean isIsomorphic(QuadStore store) {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    @Override
    public boolean isSubset(QuadStore store) {
        throw new UnsupportedOperationException("Method not implemented.");
    }

    private Resource getFilterSubject(Term subject) {
        if (subject != null) {
            ValueFactory vf = SimpleValueFactory.getInstance();

            if (subject instanceof BlankNode) {
                return vf.createBNode(subject.getValue());
            } else {
                return vf.createIRI(subject.getValue());
            }
        } else {
            return null;
        }
    }

    private IRI getFilterPredicate(Term predicate) {
        if (predicate != null) {
            return SimpleValueFactory.getInstance().createIRI(predicate.getValue());
        } else {
            return null;
        }
    }

    private Value getFilterObject(Term object) {
        if (object != null) {
            ValueFactory vf = SimpleValueFactory.getInstance();

            if (object instanceof BlankNode) {
                return vf.createBNode(object.getValue());
            } else if (object instanceof Literal) {
                Literal literal = (Literal) object;

                if (literal.getDatatype() != null) {
                    return vf.createLiteral(object.getValue(), vf.createIRI(literal.getDatatype().getValue()));
                } else if (literal.getLanguage() != null) {
                    return vf.createLiteral(object.getValue(), literal.getLanguage());
                } else {
                    return vf.createLiteral(object.getValue());
                }
            } else {
                return vf.createIRI(object.getValue());
            }
        } else {
            return null;
        }
    }

    private Resource getFilterGraph(Term graph) {
        return getFilterSubject(graph);
    }

    /**
     * Convert given string to Term
     *
     * @param str
     * @return
     */
    // TODO refactor Term class to use library (Jena/RDF4J) and have this built-in
    private Term convertStringToTerm(String str) {
        if (str.startsWith("_:")) {
            return new BlankNode(str.replace("_:", ""));
        } else if (str.startsWith("\"\"\"")) {
            // Triple quoted literal
            return new Literal(str.substring(4, str.length() - 3));
        } else if (str.startsWith("\"")) {
            Pattern pattern;
            boolean hasLanguage = str.contains("@") && str.lastIndexOf("@") > str.lastIndexOf("\"");
            boolean hasDatatype = str.contains("^^");
            if (hasLanguage) {
                pattern = Pattern.compile("^\"([^\"]*)\"@([^@]*)");
            } else if (hasDatatype) {
                pattern = Pattern.compile("^\"([^\"]*)\"\\^\\^<([^>]*)>");
            } else {
                pattern = Pattern.compile("^\"([^\"]*)\"");
            }

            Matcher matcher = pattern.matcher(str);

            if (matcher.find()) {
                if (hasLanguage) {
                    return new Literal(matcher.group(1), matcher.group(2));
                } else if (hasDatatype) {
                    return new Literal(matcher.group(1), new NamedNode(matcher.group(2)));
                } else {
                    return new Literal(matcher.group(1));
                }
            } else {
                throw new Error("Invalid Literal: " + str);
            }
        } else {
            return new NamedNode(str);
        }
    }
}
