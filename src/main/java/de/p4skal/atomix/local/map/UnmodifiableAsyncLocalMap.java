package de.p4skal.atomix.local.map;

import io.atomix.utils.concurrent.Futures;
import java.util.concurrent.CompletableFuture;

public class UnmodifiableAsyncLocalMap<K, V> extends DelegatingAsyncLocalMap<K, V> {
  private static final String ERROR_MSG = "map updates are not allowed";

  public UnmodifiableAsyncLocalMap(AsyncLocalMap<K, V> backingMap) {
    super(backingMap);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }
}
