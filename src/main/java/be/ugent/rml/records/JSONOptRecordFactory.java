package be.ugent.rml.records;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.internal.JsonContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is a record factory that creates JSON records.
 */
public class JSONOptRecordFactory extends IteratorFormat<Object> implements ReferenceFormulationRecordFactory {

    private boolean emptyStrings;

    public JSONOptRecordFactory(boolean emptyStrings) {
        this.emptyStrings = emptyStrings;
    }

    /**
     * This method returns the records from a JSON document based on an iterator.
     * @param document the document from which records need to get.
     * @param iterator the used iterator.
     * @return a list of records.
     * @throws IOException
     */
    @Override
    List<Record> getRecordsFromDocument(Object document, String iterator) throws IOException {
        List<Record> records = new ArrayList<>();

        Configuration conf = Configuration.builder()
                .options(Option.ALWAYS_RETURN_LIST).build();

        try {
            List<Object> list = JsonPath.using(conf).parse(document).read(iterator);

            for(Object l : list) {
                String json = Configuration.defaultConfiguration().jsonProvider().toJson(l);
                Object record = Configuration.defaultConfiguration().jsonProvider().parse(json);
                records.add(new JSONOptRecord(record, emptyStrings));
            }
        } catch(PathNotFoundException e) {
            logger.warn(e.getMessage(), e);
        }

        return records;
    }

    /**
     * This method returns a JSON document from an InputStream.
     * @param stream the used InputStream.
     * @return a JSON document.
     * @throws IOException
     */
    @Override
    Object getDocumentFromStream(InputStream stream) throws IOException {
        return Configuration.defaultConfiguration().jsonProvider().parse(stream, "utf-8");
    }
}
