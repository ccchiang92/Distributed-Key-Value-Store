package kvstore;

import static kvstore.KVConstants.*;

import java.util.concurrent.ConcurrentHashMap;

import java.util.Map;

// Imports for XML
import java.io.*;
import org.xml.sax.*;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

/**
 * This is a basic key-value store. Ideally this would go to disk, or some other
 * backing store.
 */
public class KVStore implements KeyValueInterface {

    private ConcurrentHashMap<String, String> store;

    /**
     * Construct a new KVStore.
     */
    public KVStore() {
        resetStore();
    }

    private void resetStore() {
        this.store = new ConcurrentHashMap<String, String>();
    }

    /**
     * Insert key, value pair into the store.
     *
     * @param  key String key
     * @param  value String value
     */
    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    /**
     * Retrieve the value corresponding to the provided key
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public String get(String key) throws KVException {
        String retVal = this.store.get(key);
        if (retVal == null) {
            KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
            throw new KVException(msg);
        }
        return retVal;
    }

    /**
     * Delete the value corresponding to the provided key.
     *
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public void del(String key) throws KVException {
        if(key != null) {
            if (!this.store.containsKey(key)) {
                KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
                throw new KVException(msg);
            }
            this.store.remove(key);
        }
    }

    /**
     * Serialize this store to XML. See the spec for specific output format.
     * This method is best effort. Any exceptions that arise can be dropped.
     */
    public String toXML() {
        try {
            // Use factory and builder in order to create document.
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document document = builder.newDocument();
            document.setXmlStandalone(true);


            // Put stuff in the document.
            Element root = document.createElement("KVStore");
            document.appendChild(root);

            for (Map.Entry<String, String> entry : store.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                Element kvPair = document.createElement("KVPair");
                Element keyElement = document.createElement("Key");
                keyElement.setTextContent(key);
                Element valueElement = document.createElement("Value");
                valueElement.setTextContent(value);

                kvPair.appendChild(keyElement);
                kvPair.appendChild(valueElement);
                root.appendChild(kvPair);
            }

            return documentToXML(document);

        }
        catch (FactoryConfigurationError e) { return null; }
        catch (ParserConfigurationException e) { return null; }
    }

    private String documentToXML(Document document) {
        try {
            // Use Transformer to turn the document into a String of XML.
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            DOMSource source = new DOMSource(document);
            Writer out = new StringWriter();
            transformer.transform(source, new StreamResult(out));
            return out.toString();
        }
        catch (TransformerConfigurationException e) { return null; }
        catch (TransformerException e) { return null; }
    }

    @Override
    public String toString() {
        return this.toXML();
    }

    /**
     * Serialize to XML and write to a file.
     * This method is best effort. Any exceptions that      *
     * @param fileName the file to write the serialized store
     */
    public void dumpToFile(String fileName) {
        try {
            PrintWriter file = new PrintWriter(fileName);
            file.write(this.toXML());
            file.close();
        } catch (IOException e) { }
    }

    /**
     * Replaces the contents of the store with the contents of a file
     * written by dumpToFile; the previous contents of the store are lost.
     * The store is cleared even if the file does not exist.
     * This method is best effort. Any exceptions that arise can be dropped.
     *
     * @param fileName the file containing the serialized store data
     */
    public void restoreFromFile(String fileName) {
        resetStore();

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document document = builder.parse(new File(fileName));

            NodeList kvPairs = document.getDocumentElement().getElementsByTagName("KVPair");
            for (int i = 0; i < kvPairs.getLength(); i++) {
                Element pair = (Element) kvPairs.item(i);
                String key =
                    ((Element) pair.getElementsByTagName("Key").item(0))
                        .getTextContent();
                String value =
                    ((Element) pair.getElementsByTagName("Value").item(0))
                        .getTextContent();
                store.put(key, value);
            }
        }
        catch (IOException e) { }
        catch (FactoryConfigurationError e) { }
        catch (ParserConfigurationException e) { }
        catch (SAXException e) { }
    }
}
