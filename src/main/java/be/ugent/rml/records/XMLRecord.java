package be.ugent.rml.records;

import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * This class is a specific implementation of a record for XML.
 * Every record corresponds with an XML element in a data source.
 */
public class XMLRecord extends Record {

    private Node node;
    private boolean emptyStrings;

    public XMLRecord(Node node, boolean emptyStrings) {
        this.node = node;
        this.emptyStrings = emptyStrings;
    }

    /**
     * This method returns the objects for a reference (XPath) in the record.
     * @param value the reference for which objects need to be returned.
     * @return a list of objects for the reference.
     */
    @Override
    public List<Object> get(String value) {
        List<Object> results = new ArrayList<>();
        XPath xPath = XPathFactory.newInstance().newXPath();

        try {
            NodeList result = (NodeList) xPath.compile(value).evaluate(node, XPathConstants.NODESET);

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
