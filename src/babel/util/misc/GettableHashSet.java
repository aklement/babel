package babel.util.misc;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class GettableHashSet<E> extends AbstractSet<E> implements Set<E> {

  public GettableHashSet() {
    map = new HashMap<E, E>();
  }

  public GettableHashSet(Collection<? extends E> c) {
    map = new HashMap<E, E>(Math.max((int) (c.size()/.75f) + 1, 16));
    addAll(c);
  }

  public GettableHashSet(int initialCapacity, float loadFactor) {
    map = new HashMap<E, E>(initialCapacity, loadFactor);
  }

  public GettableHashSet(int initialCapacity) {
    map = new HashMap<E, E>(initialCapacity);
  }

  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  public E get(E key) {
    return map.get(key);
  }
  
  public boolean add(E e) {
    return map.put(e, e) == null;
  }

  public boolean remove(Object o) {
    return map.remove(o) != null;
  }
  
  public void clear() {
    map.clear();
  }

  @SuppressWarnings("unchecked")
  public Object clone() {
    try {
      GettableHashSet<E> newSet = (GettableHashSet<E>)super.clone();
      newSet.map = (HashMap<E, E>)map.clone();
      return newSet;
    } catch (CloneNotSupportedException e) {
      throw new InternalError();
    }
  }
  
  private transient HashMap<E, E> map;
}
