package be.ugent.rml.records;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class is a specific implementation of a record for XML.
 * Every record corresponds with an XML element in a data source.
 */
public class XMLRecord extends Record {

    private Node node;
    private boolean emptyStrings;
    private XPath xPath;
    private ConcurrentHashMap<String, XPathExpression> iterators_map;
    
    public XMLRecord(Node node, boolean emptyStrings, XPath xpath, ConcurrentHashMap<String, XPathExpression> iterators_map) {
        this.node = node;
        this.emptyStrings = emptyStrings;
        this.xPath = xpath;
        this.iterators_map = iterators_map;
    }

    /**
     * This method returns the objects for a reference (XPath) in the record.
     * @param value the reference for which objects need to be returned.
     * @return a list of objects for the reference.
     */
    @Override
    public List<Object> get(String value) {
        List<Object> results = new ArrayList<>();
        XPathExpression expr = null;
        
        try {
            if (iterators_map.containsKey(value))
                expr = iterators_map.get(value);
            else {
                expr = xPath.compile(value);
                iterators_map.put(value, expr);
            }
            
            NodeList result = (NodeList) expr.evaluate(node, XPathConstants.NODESET);

            for (int i = 0; i < result.getLength(); i ++) {
                String os = result.item(i).getTextContent();
                if (!os.equals("") || emptyStrings)
                    results.add(os);
            }
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        return results;
    }
}
