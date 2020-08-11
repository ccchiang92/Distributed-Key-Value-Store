package kvstore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class KVCacheTest {

	private static KVCache globalCache = new KVCache(3, 4); 
	
    /**
     * Verify the cache can put and get a KV pair successfully.
     */
    @Test
    public void singlePutAndGet() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
    }
    
    /**
     * A Bunch of tests
     */
    @Test
    public void runTests() {
    	KVCache kvc = new KVCache(5,6);
    	assertEquals(null, kvc.get(""));
    	assertEquals(null, kvc.get("stuf"));
    	
    	kvc.put("dragon", "ball");
    	assertEquals("ball", kvc.get("dragon"));
    	kvc.put("dragon", "goku");
    	assertEquals("goku", kvc.get("dragon"));

    	kvc.put("hey", null);
    	assertEquals(null, kvc.get("hey"));
    	
    	kvc.put("random55", "t1hings");
    	kvc.put("random4", "things");
    	kvc.put("random3", "things");
    	kvc.put("random2k", "things");
    	
    	kvc.del("dragon");
    	assertEquals(null, kvc.get("dragon"));

    	kvc.put("dead", "lock");
    	kvc.put("dead", "l0ck");
    	kvc.del("dead");
    	assertEquals(null, kvc.get("dead"));
    	
        CacheTest cache = new CacheTest(1, 4);
        
        // Test 1 as specified in design doc
        cache = new CacheTest(3, 4);
        assertEquals(null, cache.get("hello"));
        
        // Test 2 as specified in design doc
        cache.put("key", "value");
        assertEquals(false, cache.getIsReferenced("key"));
        assertEquals("value", cache.get("key"));
        assertEquals(true, cache.getIsReferenced("key"));
        
        cache.put("key", "lock");
        assertEquals(true, cache.getIsReferenced("key"));
        assertEquals("lock", cache.get("key"));
        assertEquals(true, cache.getIsReferenced("key"));

        cache.put("foo", "bar");
        cache.put("foo", "bar");
        assertEquals(true, cache.getIsReferenced("foo"));
        
        cache.fillCache();
        assertEquals(cache.getSetSize("fun"), 4);
        cache.put("fun", "done");
        assertEquals(cache.getSetSize("fun"), 4);

        // Test 3 as specified in design doc
        cache = new CacheTest(10, 3);
        assertEquals(null, cache.get("water"));
        cache.put("water", "melon");
        cache.del("water");
        assertEquals(null, cache.get("water"));
        cache.put("water", "melon");
        cache.put("water", "melon");
        cache.del("water");
        assertEquals(null, cache.get("water"));

        // Test 4 as specified in design doc
        Thread t1 = (new Thread(new GrabsLock()));
        t1.start();
        Thread t2 = (new Thread(new GrabsLock2()));
        t2.start();
        try {
			t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        assertEquals(globalCache.get("pudding"), "bad");
        
        // Test 5 as specified in design doc
        cache = new CacheTest(3, 6);
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 6)), true);
        cache.put("&*(@)Q)(\\\n*#**\"\"@(()!)", "&&DF*S*&F\";;\"\ndfakfj");
        cache.put("", "");
        cache.put("rain", "bow");
        cache.put("yellow", "beans");
        cache.put("big", "hat");
        cache.put("heart", "bleed");
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 6)), true);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 6)), true);
        cache = new CacheTest(100, 100);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 100)), true);
        cache = new CacheTest(56, 67);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 67)), true);      
        cache = new CacheTest(1, 67);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 67)), true);      
        cache = new CacheTest(19, 7);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 7)), true);      
        cache = new CacheTest(17, 1);
        cache.fillCache();
        assertEquals(cache.equals(xmlToKVCache(cache.toXML(), 1)), true);      

        assertEquals(cache.equals(globalCache), false);

        // Test 6 as specified in design doc
        cache = new CacheTest(1, 6);
        cache.fillCache();
        cache.setAllIsReferenced("pineapples");
        for (int i = 0; i < 6; i++) {
        	assertEquals(cache.getRefrenceAt("pineapples", i), true);
        }        
        cache.put("pineapples", "oranges");
        assertEquals(cache.indexInSet("pineapples"), 5);
        for (int i = 0; i < 6; i++) {
        	assertEquals(cache.getRefrenceAt("pineapples", i), false);
        }
        cache.setAllIsReferenced("pineapples");
        cache.put("tow", "truck");
        assertEquals(cache.indexInSet("pineapples"), 4);
        cache.setRefrenceAt("pineapples", 0);
        cache.setRefrenceAt("pineapples", 1);
        cache.setRefrenceAt("pineapples", 2);
        cache.setRefrenceAt("pineapples", 3);
        cache.put("ever", "note");
        assertEquals(cache.indexInSet("pineapples"), -1);
        
    }
    
    /**
     * This function parses and checks if valid xml doc along the way
     * @param xml
     * @param maxElemsPerSet
     */
    public KVCache xmlToKVCache(String xml, int maxElemsPerSet) {
    	// Set up the doc for parsing
    	Document doc = loadXMLFromString(xml);
    	// Grab all the elements named sets
    	NodeList set = doc.getElementsByTagName("KVCache");
    	assertEquals(set.getLength(), 1);
    	set = doc.getElementsByTagName("Set");
    	// Get Number of sets in KVCache and create new KVCache
    	int numSets = set.getLength();
    	KVCache cache = new KVCache(numSets, maxElemsPerSet);
    	
    	// Loop through and attempt to recreate the cache from the xml String
    	for (int i = 0; i < numSets; i++) {
    		// The Set
    		Element s = (Element) set.item(i);
    		assertEquals(s.getAttributes().getLength(), 1);
    		assertEquals(s.getAttribute("Id"), Integer.toString(i));
    		// Cache Entries
            NodeList nl = s.getElementsByTagName("CacheEntry");
    		for (int j = 0; j < nl.getLength(); j++) {
    			// The cache Entry
    			Element cacheEntry = (Element) nl.item(j);
    			assertEquals(cacheEntry.getAttributes().getLength(), 1);

    			// Extract key and value and put it in cache	
    			String key = ((Element) cacheEntry.getElementsByTagName("Key").item(0))
    							.getTextContent();
    			String value = ((Element) cacheEntry.getElementsByTagName("Value").item(0))
    							.getTextContent();
                cache.put(key, value);

                // Set reference to true if true
    			String attr = cacheEntry.getAttribute("isReferenced");
    			if (Boolean.getBoolean(attr)) {
    				cache.get(key);
    			}
    		}
    		
    	}
    	return cache;
    }
    
    public Document loadXMLFromString(String xml) {
    	try {
    		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xml));
            return builder.parse(is);
        } catch (SAXException | IOException | ParserConfigurationException e) {
        	return null;
        }
    }
    
    public static class GrabsLock implements Runnable {

		@Override
		public void run() {
			System.out.println("First Thread Running");
			ReentrantLock rl = (ReentrantLock) globalCache.getLock("pudding");
			rl.lock();
			System.out.println("First Thread Locking");
			assertEquals(rl.getQueueLength(), 0);
			globalCache.put("pudding", "tastey");
			try {
				System.out.println("First Thread Sleeping");
				Thread.sleep(1000);
				System.out.println("First Thread Waking");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			assertEquals(rl.getQueueLength(), 1);
			rl.unlock();
			System.out.println("First Thread Released Lock");
		}
    	
    }
    
    public static class GrabsLock2 implements Runnable {

		@Override
		public void run() {
			System.out.println("Second Thread Running");
			ReentrantLock rl = (ReentrantLock) globalCache.getLock("pudding");
			rl.lock();
			System.out.println("Second Thread Locking");
			globalCache.put("pudding", "bad");
			rl.unlock();
			System.out.println("Second Thread Released Lock");
		}
    	
    }
    
    private class CacheTest extends KVCache {

		public CacheTest(int numSets, int maxElemsPerSet) {
			super(numSets, maxElemsPerSet);
		}
    	
		public Boolean getIsReferenced(String key) {
		 	int index = getSetId(key);
			LinkedList<CacheEntry> ll = set.get(index);
			for (int i = 0; i < ll.size(); i++) {
				CacheEntry ce = ll.get(i);
				if (ce.key == key) {
					return ce.isReferenced;
				}
			}
		    return null;   	
		}
    
	    public boolean equals(KVCache cache) {
	    	if (NUM_SETS != cache.NUM_SETS ||
	    		MAX_ELEMS_PER_SET != cache.MAX_ELEMS_PER_SET) {
	    		return false;
	    	}
	
	    	for (int i = 0; i < NUM_SETS; i++) {
	    		int localCESize = set.get(i).size();
	    		int extCESize = cache.set.get(i).size();
	    		if (localCESize == extCESize) {
	    			for (int j = 0; j < set.get(i).size(); j++) {
	    				CacheEntry localCE = set.get(i).get(j);
	    				CacheEntry extCE = cache.set.get(i).get(j);
	    				if (!localCE.equals(extCE)) {
	    					return false;
	    				}
	    			}
	    		} else {
	                System.out.println(3);
	    			return false;
	    		}
	    	}
	    	return true;
	    }
	    
	    /**
	     * returns the index in the set where the key resides
	     * @param key
	     */
	    public int indexInSet(String key) {
	    	int setid = getSetId(key);
	        LinkedList<CacheEntry> ll = set.get(setid);
	    	for (int i = 0; i < ll.size(); i++) {
	            CacheEntry ce = ll.get(i);
	    		if (ce.key == key) {
	    			return i;
	    		}
	    	}
	    	return -1;
	    }
	   
	    /**
	     * sets all reference bits to true in the set in which the key belongs in
	     * @param key
	     */
	    public void setAllIsReferenced(String key) {
	    	int setid = getSetId(key);
	        LinkedList<CacheEntry> ll = set.get(setid);
	    	for (int i = 0; i < ll.size(); i++) {
	            CacheEntry ce = ll.get(i);
	            ce.isReferenced = true;
	    	}
	    }
	    
	    public Boolean getRefrenceAt(String key, int index) {
	    	int setid = getSetId(key);
	        LinkedList<CacheEntry> ll = set.get(setid);
	        CacheEntry ce = ll.get(index);
	        return ce.isReferenced;
	    }
	    
	    public void setRefrenceAt(String key, int index) {
	     	int setid = getSetId(key);
	        LinkedList<CacheEntry> ll = set.get(setid);
	        CacheEntry ce = ll.get(index);
	        ce.isReferenced = true;
	    }
	    
	    public void fillCache() {
	    	for (int i = 0; i < NUM_SETS; i++) {
	    		LinkedList<CacheEntry> ll = set.get(i);
	    		while(ll.size() < MAX_ELEMS_PER_SET) {
	    			put(randomString(6), randomString(6));
	    		}
	    	}
	    }
	    
	    public int getSetSize(String key) {
	        int setid = getSetId(key);
	        LinkedList<CacheEntry> ll = set.get(setid);
	        return ll.size();
	    }
	    
	    public String randomString(int size) {
	    	String alphabet = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	    	String s = "";
	    	for (int i = 0; i < size; i++) {
	    		int index = (int)(Math.random() * alphabet.length());
	    		s += alphabet.charAt(index);
	    	}
	    	return s;
	    }
	   
	    public int getSetId(String key) {
	    	return Math.abs(key.hashCode() % NUM_SETS);
	    }
    }
}
