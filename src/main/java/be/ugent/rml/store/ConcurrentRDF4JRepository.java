package be.ugent.rml.store;

import be.ugent.rml.term.Term;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentRDF4JRepository extends RDF4JRepository {

    public static ExecutorService executorService;
    public static int NUM_THREADS = 4;

    private AtomicInteger numBatches;
    private ExecutorCompletionService<String> completionService;
    private List<Future<String>> jobs;

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRDF4JRepository.class);

    public ConcurrentRDF4JRepository(String dbAddress, String repositoryID, IRI context, int batchSize, boolean incremental) {
        super(dbAddress, repositoryID, context, batchSize, incremental);
        init();
    }

    public ConcurrentRDF4JRepository(Repository r, IRI context, int batchSize, boolean incremental) {
        super(r, context, batchSize, incremental);
        init();
    }

    private void init() {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(NUM_THREADS);
            logger.info("ExecutorService for ConcurrentRDF4JRepository initialized [num_threads = " + NUM_THREADS + "]");
        }
        completionService = new ExecutorCompletionService<>(executorService);
        jobs = new ArrayList<>();
        numBatches = new AtomicInteger(0);
    }

    @Override
    public void addQuad(Term subject, Term predicate, Term object, Term graph) {
        Resource s = getFilterSubject(subject);
        IRI p = getFilterPredicate(predicate);
        Value o = getFilterObject(object);
        Resource g = getFilterGraph(graph);

        synchronized (model) {
            model.add(s, p, o, g);
            if (incremental && model.size() >= batchSize)
                writeToRepository();
        }
    }

    private void writeToRepository() {
        final Model b = new TreeModel(model);
        jobs.add(completionService.submit(() -> {
            try (RepositoryConnection con = repo.getConnection()) {
                con.add(b);
            }
            return "Concurrent write completed! [query_num: " + numWrites.incrementAndGet() + ", size: " + b.size() + "]";
        }));
        logger.debug("Concurrent write to database queued [batch_num: " + numBatches.incrementAndGet() + "]");
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

        /* wait for all tasks to complete */
        for (Future<String> task : jobs)
            try {
                String result = task.get();
                logger.info(result);
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }

        if (shutdownRepository)
            repo.shutDown();
    }
}