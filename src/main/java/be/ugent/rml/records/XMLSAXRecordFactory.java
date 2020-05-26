package be.ugent.rml.records;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.transform.sax.SAXSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.TreeInfo;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.xpath.XPathFactoryImpl;

/**
 * This class is a record factory that creates XML records.
 * NOTE: This implementation is sensible to NAMESPACES.
 */
public class XMLSAXRecordFactory extends IteratorFormat<TreeInfo> implements ReferenceFormulationRecordFactory {

    private boolean emptyStrings;
    private XPath xpExpression;
    private ConcurrentHashMap<String, XPathExpression> iterators_map;
    private Configuration config;
    private XPathFactory xpFactory; 
    
    public XMLSAXRecordFactory(boolean emptyStrings) {
        this.emptyStrings = emptyStrings;
        iterators_map = new ConcurrentHashMap<String, XPathExpression>();

        // The following initialization code is specific to Saxon
        // Please refer to SaxonHE documentation for details
        System.setProperty("javax.xml.xpath.XPathFactory:"+ NamespaceConstant.OBJECT_MODEL_SAXON, "net.sf.saxon.xpath.XPathFactoryImpl");
        try {
            xpFactory = XPathFactory. newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
            xpExpression = xpFactory.newXPath();
            System.err.println("Loaded XPath Provider " + xpExpression.getClass().getName());
            config = ((XPathFactoryImpl) xpFactory).getConfiguration();
        } catch (XPathFactoryConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // End Saxon specific code
    }

    /**
     * This method returns the records from an XML document based on an iterator.
     * @param document the document from which records need to get.
     * @param iterator the used iterator.
     * @return a list of records.
     * @throws IOException
     */
    @Override
    List<Record> getRecordsFromDocument(TreeInfo document, String iterator) throws IOException {
        List<Record> records = new ArrayList<>();
        XPathExpression expr = null;
        try {
            if (iterators_map.containsKey(iterator))
                expr = iterators_map.get(iterator);
            else {
                expr = xpExpression.compile(iterator);
                iterators_map.put(iterator, expr);
            }
           
            ArrayList<Node> result = (ArrayList<Node>) expr.evaluate(document, XPathConstants.NODESET);

            ListIterator<Node> it = result.listIterator();
            while (it.hasNext()) {
                NodeInfo obj = (NodeInfo) it.next();
                records.add(new XMLSAXRecord(obj, emptyStrings, xpExpression, iterators_map));
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
    TreeInfo getDocumentFromStream(InputStream stream) throws IOException {
        try {
            // Build the source document.
            InputSource inputSrc = new InputSource(stream);
            SAXSource saxSrc = new SAXSource(inputSrc);
            
            return config.buildDocumentTree(saxSrc);
        } catch (XPathException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }
}
