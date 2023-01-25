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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RDF4JRepository extends QuadStore {

    Repository repo;
    private RepositoryConnection connection;
    boolean shutdownRepository;

    int batchSize;
    boolean incremental;
    AtomicInteger numWrites;

    Model model;

    private static final Logger logger = LoggerFactory.getLogger(RDF4JRepository.class);

    public RDF4JRepository(String dbAddress, String repositoryID, IRI context, int batchSize, boolean incremental) {
        repo = new HTTPRepository(dbAddress, repositoryID);
        shutdownRepository = true;
        repo.init();
        init(context, batchSize, incremental);
    }

    public RDF4JRepository(Repository r, IRI context, int batchSize, boolean incremental) {
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
        logger.debug("Options [Batch Size: " + batchSize + ", Incremental: " + incremental + "]");
        this.batchSize = batchSize;
        this.incremental = incremental;
        if(incremental && batchSize == 0)
            connection = repo.getConnection();
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

        if (incremental && batchSize == 0) {
            synchronized (connection) {
                connection.add(s, p, o, g);
            }
        } else {
            synchronized (model) {
                model.add(s, p, o, g);
                if (incremental && model.size() >= batchSize)
                    writeToRepository();
            }
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
        try (RepositoryConnection con = repo.getConnection()) {
            // todo rdf4j 4.2.2 bug, usage of ContextAwareRepository and this particular add method causes stackoverflow
            // con.add(model);

            for (Statement st : model.getStatements(null,null,null))
            {
                con.add(st);
            }

            for (Namespace ns : model.getNamespaces()) {
                con.setNamespace(ns.getPrefix(), ns.getName());
            }
        }
        logger.debug("Query completed! [query_num: " + numWrites.incrementAndGet() + ", size: " + model.size() + "]");
        model = new TreeModel();
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

        if (connection != null)
            connection.close();

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

    Resource getFilterSubject(Term subject) {
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

    IRI getFilterPredicate(Term predicate) {
        if (predicate != null) {
            return SimpleValueFactory.getInstance().createIRI(predicate.getValue());
        } else {
            return null;
        }
    }

    Value getFilterObject(Term object) {
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

    Resource getFilterGraph(Term graph) {
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
