package be.ugent.rml.store;

import be.ugent.rml.term.Term;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentRDF4JRepository extends RDF4JRepository {

    public static int CORE_POOL_SIZE = 4;
    public static int MAXIMUM_POOL_SIZE = 5;
    public static int KEEP_ALIVE_MINUTES = 10;
    private ThreadPoolExecutor executor;

    private AtomicInteger numBatches;

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRDF4JRepository.class);

    public ConcurrentRDF4JRepository(String dbAddress, String repositoryID, IRI context, int batchSize, boolean incremental) {
        super(dbAddress, repositoryID, context, batchSize, incremental);
        init(context, batchSize, incremental);
    }

    public ConcurrentRDF4JRepository(Repository r, IRI context, int batchSize, boolean incremental) {
        super(r, context, batchSize, incremental);
        init(context, batchSize, incremental);
    }

    private void init(IRI context, int batchSize, boolean incremental) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
        executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE,
                KEEP_ALIVE_MINUTES, TimeUnit.MINUTES, workQueue);
        logger.info("Executor initialized [core_pool_size: " + CORE_POOL_SIZE
                + ", maximum_pool_size: " + MAXIMUM_POOL_SIZE
                + ", keep_alive_minutes: " + KEEP_ALIVE_MINUTES + "]");
        numBatches = new AtomicInteger(0);
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

    private void writeToRepository() {
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
}