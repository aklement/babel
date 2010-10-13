package babel.util.misc;

import java.util.HashMap;
import java.util.Set;

/**
 * Assumes one-to-one mapping between keys and values.  Will throw an 
 * exception if a key or value being added already exists in the map.
 */
public class InvertibleHashMap<K, V> {

  public InvertibleHashMap() {
    kvmap = new HashMap<K, V>(); 
    vkmap = new HashMap<V, K>();
  }

  public InvertibleHashMap(int initialCapacity) {
    kvmap = new HashMap<K, V>(initialCapacity); 
    vkmap = new HashMap<V, K>(initialCapacity);
  }

  public InvertibleHashMap(int initialCapacity, float loadFactor) {
    kvmap = new HashMap<K, V>(initialCapacity, loadFactor); 
    vkmap = new HashMap<V, K>(initialCapacity, loadFactor);
  }
  
  public int size() {
    return kvmap.size();
  }
  
  public boolean isEmpty() {
    return kvmap.isEmpty();
  }
  
  public boolean containsKey(Object key) {
    return kvmap.containsKey(key);
  }
  
  public boolean containsValue(Object value) {
    return vkmap.containsKey(value);
  }
  
  public V getValue(Object key) {
    return kvmap.get(key);
  }
  
  public K getKey(Object val) {
    return vkmap.get(val);
  }
  
  public void put(K key, V value) {
    if (kvmap.containsKey(key) || vkmap.containsKey(value)) {
      throw new IllegalArgumentException("Non unique key or value.");
    } else if (key == null || value == null) {
      throw new IllegalArgumentException("Key or value is null.");      
    }
    kvmap.put(key, value);
    vkmap.put(value, key);
  }
  
  public void removeKey(Object key) {
    if (kvmap.containsKey(key)) {
      vkmap.remove(vkmap.get(key));
      kvmap.remove(key);
    }
  }
  
  public void removeValue(Object value) {
    if (vkmap.containsKey(value)) {
      kvmap.remove(vkmap.get(value));
      vkmap.remove(value);
    }
  }
  
  public void clear() {
    kvmap.clear();
    vkmap.clear();
  }
  
  public Set<K> keySet() {
    return kvmap.keySet();
  }
  
  public Set<V> valueSet() {
    return vkmap.keySet();
  }
  
  private transient HashMap<V, K> vkmap;
  private transient HashMap<K, V> kvmap;
}
