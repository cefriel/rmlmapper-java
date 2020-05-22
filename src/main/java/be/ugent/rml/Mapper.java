package be.ugent.rml;

import be.ugent.rml.metadata.MetadataGenerator;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.term.ProvenancedTerm;
import be.ugent.rml.term.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiConsumer;

public interface Mapper {

    Logger logger = LoggerFactory.getLogger(Mapper.class);

    default void setNoCache(boolean flag) {
        logger.debug("Option no-cache not implemented for this typology of Mapper");
    };

    default void setOrdered(boolean flag) {
        logger.debug("Option ordered not implemented for this typology of Mapper");
    };

    QuadStore execute(List<Term> triplesMaps, boolean removeDuplicates, MetadataGenerator metadataGenerator) throws Exception;
    QuadStore executeWithFunction(List<Term> triplesMaps, boolean removeDuplicates, BiConsumer<ProvenancedTerm, PredicateObjectGraph> pogFunction) throws Exception;
    QuadStore execute(List<Term> triplesMaps) throws Exception;

    List<Term> getTriplesMaps();

}
