package de.p4skal.atomix.local.map;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

public class DelegatingAsyncLocalMap<K, V>
    extends DelegatingAsyncPrimitive<AsyncLocalMap<K, V>> implements AsyncLocalMap<K, V> {
  public DelegatingAsyncLocalMap(AsyncLocalMap<K, V> delegate) {
    super(delegate);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.delegate().size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return this.delegate().containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return this.delegate().containsValue(value);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return this.delegate().get(key);
  }

  @Override
  public CompletableFuture<V> getOrDefault(K key, V defaultValue) {
    return this.delegate().getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return this.delegate().put(key, value);
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return this.delegate().remove(key);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return this.delegate().remove(key, value);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return this.delegate().clear();
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return this.delegate().keySet();
  }

  @Override
  public AsyncDistributedCollection<V> values() {
    return this.delegate().values();
  }

  @Override
  public AsyncDistributedSet<Entry<K, V>> entrySet() {
    return this.delegate().entrySet();
  }

  @Override
  public LocalMap<K, V> sync(Duration operationTimeout) {
    return new BlockingLocalMap<>(this, operationTimeout.toMillis());
  }
}
