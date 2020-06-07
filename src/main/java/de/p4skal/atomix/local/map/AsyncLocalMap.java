package de.p4skal.atomix.local.map;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

public interface AsyncLocalMap<K, V> extends AsyncPrimitive {
  @Override
  default CompletableFuture<Void> delete() {
    return clear();
  }

  CompletableFuture<Integer> size();

  default CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(s -> s == 0);
  }

  CompletableFuture<Boolean> containsKey(K key);

  CompletableFuture<Boolean> containsValue(V value);

  CompletableFuture<V> get(K key);

  CompletableFuture<V> getOrDefault(K key, V defaultValue);

  CompletableFuture<V> put(K key, V value);

  CompletableFuture<V> remove(K key);

  CompletableFuture<Boolean> remove(K key, V value);

  CompletableFuture<Void> clear();

  AsyncDistributedSet<K> keySet();

  AsyncDistributedCollection<V> values();

  AsyncDistributedSet<Entry<K, V>> entrySet();

  @Override
  default LocalMap<K, V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  LocalMap<K, V> sync(Duration operationTimeout);
}
