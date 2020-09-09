package be.ugent.rml.records;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a record factory that creates XML records.
 */
public class XMLRecordFactory extends IteratorFormat<Document> implements ReferenceFormulationRecordFactory {

    private XPath xPath;
    private ConcurrentHashMap<String, XPathExpression> iterators_map;
    
    public XMLRecordFactory() {
        xPath = XPathFactory.newInstance().newXPath();
        iterators_map = new ConcurrentHashMap<String, XPathExpression>();
    }

    /**
     * This method returns the records from an XML document based on an iterator.
     * @param document the document from which records need to get.
     * @param iterator the used iterator.
     * @return a list of records.
     * @throws IOException
     */
    @Override
    List<Record> getRecordsFromDocument(Document document, String iterator) throws IOException {
        List<Record> records = new ArrayList<>();
        XPathExpression expr = null;
        try {
            if (iterators_map.containsKey(iterator))
                expr = iterators_map.get(iterator);
            else {
                expr = xPath.compile(iterator);
                iterators_map.put(iterator, expr);
            }
           
            NodeList result = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

            for (int i = 0; i < result.getLength(); i ++) {
                records.add(new XMLRecord(result.item(i), emptyStrings, xPath, iterators_map));
            }
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        return records;
    }

    /**
     * This method returns an XML document from an InputStream.
     * @param stream the used InputStream.
     * @return an XML document.
     * @throws IOException
     */
    @Override
    Document getDocumentFromStream(InputStream stream) throws IOException {
        try {
            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = builderFactory.newDocumentBuilder();

            return builder.parse(stream);
        } catch (SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void setEmptyStrings(boolean emptyStrings) {
        this.emptyStrings = emptyStrings;
    }
}
