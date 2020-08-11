package kvstore;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {

	// Constants
	protected final int NUM_SETS;
	protected final int MAX_ELEMS_PER_SET;
	// Variables
	protected ArrayList<LinkedList<CacheEntry>> set;
	protected ArrayList<ReentrantLock> lockSet;

    /**
     * Creates a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */
    public KVCache(int numSets, int maxElemsPerSet) {
        // implement me
    	NUM_SETS = numSets;
    	MAX_ELEMS_PER_SET = maxElemsPerSet;

    	set = new ArrayList<LinkedList<CacheEntry>>(numSets);
    	lockSet = new ArrayList<ReentrantLock>(numSets);
    	for (int i = 0; i < numSets; i++) {
    		set.add(new LinkedList<CacheEntry>());
    		lockSet.add(new ReentrantLock());
    	}
    }

    /**
     * Retrieves an entry from the cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method.
     *
     * @param  key the key whose associated value is to be returned.
     * @return the value associated to this key or null if no value is
     *         associated with this key in the cache
     */
    @Override
    public String get(String key) {
        // implement me
    	int index = getSetId(key);
    	LinkedList<CacheEntry> ll = set.get(index);
    	String value = null;
    	for (int i = 0; i < ll.size(); i++) {
    		CacheEntry ce = ll.get(i);
    		if (ce.key.equals(key)) {
    			value = ce.value;
    			// Set reference bit to true
    			ce.isReferenced = true;
    		}
    	}
        return value;
    }

    /**
     * Adds an entry to this cache.
     * If an entry with the specified key already exists in the cache, it is
     * replaced by the new entry. When an entry is replaced, its reference bit
     * will be set to True. If the set is full, an entry is removed from
     * the cache based on the eviction policy. If the set isn't full, the entry
     * will be inserted behind all existing entries. For this policy, we suggest
     * using a LinkedList over an array to keep track of entries in a set since
     * deleting an entry in an array will leave a gap in the array, likely not
     * at the end. More details and explanations in the spec. Assumes access to
     * the corresponding set has already been locked by the caller of this
     * method.
     *
     * @param key the key with which the specified value is to be associated
     * @param value a value to be associated with the specified key
     */
    @Override
    public void put(String key, String value) {
        // implement me
    	int index = getSetId(key);
    	LinkedList<CacheEntry> ll = set.get(index);

    	// Check if key already exists
    	for (int i = 0; i < ll.size(); i++) {
    		CacheEntry ce = ll.get(i);
    		if (ce.key.equals(key)) {
    			ce.value = value;
    			ce.isReferenced = true;
    			return;
    		}
    	}    	
    	
    	// copy comments for design doc
    	while (ll.size() == MAX_ELEMS_PER_SET) {
            CacheEntry ce = ll.pop();
            if (ce.isReferenced == true) {
            	ce.isReferenced = false;
            	ll.add(ce);
            } 
    	}

    	// Add to set
    	CacheEntry ce = new CacheEntry(key, value, false);
        ll.add(ce);
    }

    /**
     * Removes an entry from this cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method. Does nothing if called on a key not in the cache.
     *
     * @param key key with which the specified value is to be associated
     */
    @Override
    public void del(String key) {
        // implement me
    	int index = getSetId(key);
    	LinkedList<CacheEntry> ll = set.get(index);
    	for (int i = 0; i < ll.size(); i++) {
    		CacheEntry ce = ll.get(i);
    		if (ce.key.equals(key)) {
    			ll.remove(i);
    			return;
    		}
    	}
    }

    /**
     * Get a lock for the set corresponding to a given key.
     * The lock should be used by the caller of the get/put/del methods 
     * so that different sets can be changed in parallel.
     *
     * @param  key key to determine the lock to return
     * @return lock for the set that contains the key
     */
    public Lock getLock(String key) {
        // implement me
    	int index = getSetId(key);
    	return lockSet.get(index);
    }

    /**
     * Get the id of the set for a specific key.
     *
     * @param  key key of interest
     * @return set of the key
     */
    private int getSetId(String key) {
        // implement me
        return Math.abs(key.hashCode() % NUM_SETS);
    }

    /**
     * Serialize this store to XML. See spec for details on output format.
     */
    public String toXML() {
        // implement me
    	try {

    		// Use factory and builder to create java.
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document document = builder.newDocument();
            document.setXmlStandalone(true);
            
            Element kvc = document.createElement("KVCache");
            for (int i = 0; i < NUM_SETS; i++) {
                    Element st =  document.createElement("Set");
                    st.setAttribute("Id", String.valueOf(i));
                    LinkedList<CacheEntry> ll = set.get(i);

                    for (int j = 0; j < ll.size(); j++) {
                            CacheEntry ce = ll.get(j);

                            Element cacheEntry = document.createElement("CacheEntry");
                            cacheEntry.setAttribute("isReferenced", ce.isReferenced.toString());
                            
                            Element key = document.createElement("Key");
                            key.setTextContent(ce.key);

                            Element value = document.createElement("Value");
                            value.setTextContent(ce.value);
                            
                            cacheEntry.appendChild(key);
                            cacheEntry.appendChild(value);

                            st.appendChild(cacheEntry);
                    }
                    kvc.appendChild(st);
            }
            document.appendChild(kvc);
            
            // Use Transformer to turn the document into a String of XML.
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            DOMSource source = new DOMSource(document);
            Writer out = new StringWriter();
            transformer.transform(source, new StreamResult(out));
            return out.toString();

    	} catch (TransformerException | ParserConfigurationException e) {
    		return null;
    	}
    }

    @Override
    public String toString() {
        return this.toXML();
    }
    
    /**
     * The Cache Entry inside a set
     */
    class CacheEntry {
    	
    	// Variables
    	public String key;
    	public String value;
    	public Boolean isReferenced;

    	public CacheEntry(String key, String value, Boolean isReferenced) {
    		this.key = key;
    		this.value = value;
    		this.isReferenced = isReferenced;
    	}
    	
    	public boolean equals(CacheEntry ce) {
    		return key.equals(ce.key) && value.equals(ce.value) && isReferenced.equals(ce.isReferenced);
    	}
    } 
    
 
}
