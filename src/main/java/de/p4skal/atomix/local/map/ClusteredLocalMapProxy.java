package de.p4skal.atomix.local.map;

import io.atomix.cluster.MemberId;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import de.p4skal.atomix.local.ClusteredProxyIterator;
import de.p4skal.atomix.local.LocalCommunicator;

@RequiredArgsConstructor
public class ClusteredLocalMapProxy implements AsyncLocalMap<String, byte[]> {
  private final LocalCommunicator<LocalMapService<String>> communicator;

  @Override
  public String name() {
    return this.communicator.getName();
  }

  @Override
  public PrimitiveType type() {
    return this.communicator.getType();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return this.communicator.getProtocol();
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public LocalMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingLocalMap<>(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.communicator
        .applyAll(LocalMapService::size)
        .resultsFuture(0, 500L)
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return this.communicator
        .applyAll(service -> service.containsKey(key))
        .resultsFuture(false, 500L)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return this.communicator
        .applyAll(service -> service.containsValue(value))
        .resultsFuture(false, 500L)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public CompletableFuture<byte[]> get(String key) {
    return this.communicator
        .applyAll(service -> service.get(key))
        .resultsFuture(null, 500L)
        .thenApply(results -> results.filter(Objects::nonNull).findAny().orElse(null));
  }

  @Override
  public CompletableFuture<byte[]> getOrDefault(String key, byte[] defaultValue) {
    return this.get(key).thenApply(value -> value == null ? defaultValue : value);
  }

  @Override
  public CompletableFuture<byte[]> put(String key, byte[] value) {
    final MemberId putMember = this.communicator.getLocalOrMemberId(key);
    return this.communicator
        .applyAll(
            (communication, service) ->
                communication.getMemberId().equals(putMember)
                    ? service.put(key, value)
                    : service.remove(key))
        .resultsFuture(null, 500L)
        .thenApply(results -> results.filter(Objects::nonNull).findAny().orElse(null));
  }

  @Override
  public CompletableFuture<byte[]> remove(String key) {
    return this.communicator
        .applyAll(service -> service.remove(key))
        .resultsFuture(null, 500L)
        .thenApply(results -> results.filter(Objects::nonNull).findAny().orElse(null));
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return this.communicator
        .applyAll(service -> service.remove(key, value))
        .resultsFuture(null, 500L)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public AsyncDistributedSet<String> keySet() {
    return new LocalMapKeySet();
  }

  @Override
  public AsyncDistributedCollection<byte[]> values() {
    return new LocalMapValuesCollection();
  }

  @Override
  public AsyncDistributedSet<Entry<String, byte[]>> entrySet() {
    return new LocalMapEntrySet();
  }

  @Override
  public CompletableFuture<Void> clear() {
    return this.communicator.acceptAll(LocalMapService::clear).awaitFuture(500L);
  }

  private class LocalMapEntrySet implements AsyncDistributedSet<Map.Entry<String, byte[]>> {
    @Override
    public String name() {
      return ClusteredLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ClusteredLocalMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(
        CollectionEventListener<Entry<String, byte[]>> listener, Executor executor) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(
        CollectionEventListener<Map.Entry<String, byte[]>> listener) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> add(Entry<String, byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Entry<String, byte[]> element) {
      return ClusteredLocalMapProxy.this.remove(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return ClusteredLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ClusteredLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ClusteredLocalMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Entry<String, byte[]> element) {
      return get(element.getKey())
          .thenApply(value -> value != null && Arrays.equals(value, element.getValue()));
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<Entry<String, byte[]>> iterator() {
      return new ClusteredProxyIterator<>(
          ClusteredLocalMapProxy.this.communicator,
          LocalMapService::iterateEntries,
          LocalMapService::nextEntries,
          LocalMapService::closeEntries);
    }

    @Override
    public DistributedSet<Entry<String, byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return ClusteredLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return ClusteredLocalMapProxy.this.delete();
    }

    @Override
    public CompletableFuture<Boolean> prepare(
        TransactionLog<SetUpdate<Entry<String, byte[]>>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }
  }

  private class LocalMapKeySet implements AsyncDistributedSet<String> {
    @Override
    public String name() {
      return ClusteredLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ClusteredLocalMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(
        CollectionEventListener<String> listener, Executor executor) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(
        CollectionEventListener<String> listener) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return ClusteredLocalMapProxy.this.remove(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return ClusteredLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ClusteredLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ClusteredLocalMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> keys) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<String> iterator() {
      return new ClusteredProxyIterator<>(
          ClusteredLocalMapProxy.this.communicator,
          LocalMapService::iterateKeys,
          LocalMapService::nextKeys,
          LocalMapService::closeKeys);
    }

    @Override
    public DistributedSet<String> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return ClusteredLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return ClusteredLocalMapProxy.this.delete();
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }
  }

  private class LocalMapValuesCollection implements AsyncDistributedCollection<byte[]> {
    @Override
    public String name() {
      return ClusteredLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ClusteredLocalMapProxy.this.protocol();
    }

    @Override
    public PrimitiveType type() {
      return DistributedCollectionType.instance();
    }

    @Override
    public CompletableFuture<Boolean> add(byte[] element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return ClusteredLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ClusteredLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ClusteredLocalMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(byte[] element) {
      return containsValue(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<byte[]> iterator() {
      return new ClusteredProxyIterator<>(
          ClusteredLocalMapProxy.this.communicator,
          LocalMapService::iterateValues,
          LocalMapService::nextValues,
          LocalMapService::closeValues);
    }

    @Override
    public DistributedCollection<byte[]> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return ClusteredLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return ClusteredLocalMapProxy.this.delete();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(
        CollectionEventListener<byte[]> listener, Executor executor) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(
        CollectionEventListener<byte[]> listener) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }
}
