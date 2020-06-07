package de.p4skal.atomix.local.map;

import com.google.common.base.Throwables;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockingLocalMap<K, V> extends Synchronous<AsyncLocalMap<K, V>> implements
    LocalMap<K, V> {
  private final AsyncLocalMap<K, V> asyncMap;
  private final long operationTimeoutMillis;

  public BlockingLocalMap(AsyncLocalMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int size() {
    return this.complete(this.asyncMap.size());
  }

  @Override
  public boolean isEmpty() {
    return this.complete(this.asyncMap.isEmpty());
  }

  @Override
  public boolean containsKey(K key) {
    return this.complete(this.asyncMap.containsKey(key));
  }

  @Override
  public boolean containsValue(V value) {
    return this.complete(this.asyncMap.containsValue(value));
  }

  @Override
  public V get(K key) {
    return this.complete(this.asyncMap.get(key));
  }

  @Override
  public V getOrDefault(K key, V defaultValue) {
    return this.complete(this.asyncMap.getOrDefault(key, defaultValue));
  }

  @Override
  public V put(K key, V value) {
    return this.complete(this.asyncMap.put(key, value));
  }

  @Override
  public V remove(K key) {
    return this.complete(this.asyncMap.remove(key));
  }

  @Override
  public boolean remove(K key, V value) {
    return this.complete(this.asyncMap.remove(key, value));
  }

  @Override
  public void clear() {
    this.complete(this.asyncMap.clear());
  }

  @Override
  public DistributedSet<K> keySet() {
    return new BlockingDistributedSet<K>(this.asyncMap.keySet(), this.operationTimeoutMillis);
  }

  @Override
  public DistributedCollection<V> values() {
    return new BlockingDistributedCollection<>(this.asyncMap.values(), this.operationTimeoutMillis);
  }

  @Override
  public DistributedSet<Entry<K, V>> entrySet() {
    return new BlockingDistributedSet<>(this.asyncMap.entrySet(), this.operationTimeoutMillis);
  }

  @Override
  public AsyncLocalMap<K, V> async() {
    return this.asyncMap;
  }

  protected <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(this.operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
