package de.p4skal.atomix.local.map;

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface LocalMapService<K> {
  @Query
  int size();

  @Query
  boolean containsKey(K key);

  @Query
  boolean containsValue(byte[] value);

  @Query
  byte[] get(K key);

  @Command
  byte[] put(K key, byte[] value);

  @Command
  byte[] remove(K key);

  @Command("removeValue")
  boolean remove(K key, byte[] value);

  @Command
  void clear();

  @Query
  Set<K> keySet();

  @Query
  Collection<byte[]> values();

  @Query
  Set<Map.Entry<K, byte[]>> entrySet();

  @Command
  IteratorBatch<K> iterateKeys();

  @Query
  IteratorBatch<K> nextKeys(long iteratorId, int position);

  @Command
  void closeKeys(long iteratorId);

  @Command
  IteratorBatch<byte[]> iterateValues();

  @Query
  IteratorBatch<byte[]> nextValues(long iteratorId, int position);

  @Command
  void closeValues(long iteratorId);

  @Command
  IteratorBatch<Map.Entry<K, byte[]>> iterateEntries();

  @Query
  IteratorBatch<Map.Entry<K, byte[]>> nextEntries(long iteratorId, int position);

  @Command
  void closeEntries(long iteratorId);
}
