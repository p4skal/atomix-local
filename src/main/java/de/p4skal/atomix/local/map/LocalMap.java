package de.p4skal.atomix.local.map;

import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.set.DistributedSet;
import io.atomix.primitive.SyncPrimitive;
import java.util.Map.Entry;

public interface LocalMap<K, V> extends SyncPrimitive {
  @Override
  default void delete() {
    clear();
  }

  int size();

  boolean isEmpty();

  boolean containsKey(K key);

  boolean containsValue(V value);

  V get(K key);

  V getOrDefault(K key, V defaultValue);

  V put(K key, V value);

  V remove(K key);

  void clear();

  DistributedSet<K> keySet();

  DistributedCollection<V> values();

  DistributedSet<Entry<K, V>> entrySet();

  boolean remove(K key, V value);

  @Override
  AsyncLocalMap<K, V> async();
}
