package be.ugent.rml.records;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import net.sf.saxon.om.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a specific implementation of a record for XML.
 * Every record corresponds with an XML element in a data source.
 */
public class XMLSAXRecord extends Record {

    private NodeInfo node;
    private boolean emptyStrings;
    private XPath xPath;
    private ConcurrentHashMap<String, XPathExpression> iterators_map;
    
    public XMLSAXRecord(NodeInfo node, boolean emptyStrings, XPath xpath, ConcurrentHashMap<String, XPathExpression> iterators_map) {
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
            
            List result = (List) expr.evaluate(node, XPathConstants.NODESET);
            if (result != null) {
                int count = result.size();
                // Go through each node in the list and display the serial number.
                for (int i = 0; i < count; i++) {
                    NodeInfo cNode = (NodeInfo) result.get(i);
                    String os = cNode.getStringValue();
                    if (!os.equals("") || emptyStrings)
                        results.add(os);
                }
            }

        } catch (XPathExpressionException e) {
            logger.warn(e.getMessage(), e);
        }
        logger.debug("Value: " + value + " Results: " + results.size());
        return results;
    }
}
