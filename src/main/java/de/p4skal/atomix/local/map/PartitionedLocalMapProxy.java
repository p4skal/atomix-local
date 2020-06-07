package de.p4skal.atomix.local.map;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedProxyIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

@Deprecated
public class PartitionedLocalMapProxy
    extends AbstractAsyncPrimitive<AsyncLocalMap<String, byte[]>, LocalMapService<String>>
    implements AsyncLocalMap<String, byte[]> {
  private final ClusterMembershipService membershipService;
  private final PartitionService partitionService;

  public PartitionedLocalMapProxy(
      ProxyClient<LocalMapService<String>> proxy,
      PrimitiveRegistry registry,
      ClusterMembershipService membershipService,
      PartitionService partitionService) {
    super(proxy, registry);
    this.membershipService = membershipService;
    this.partitionService = partitionService;
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
    return this.getProxyClient()
        .applyAll(LocalMapService::size)
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return this.getProxyClient()
        .applyAll(service -> service.containsKey(key))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return this.getProxyClient()
        .applyAll(service -> service.containsValue(value))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public CompletableFuture<byte[]> get(String key) {
    return this.getProxyClient()
        .applyAll(service -> service.get(key))
        .thenApply(results -> results.filter(Objects::nonNull).findAny().orElse(null));
  }

  @Override
  public CompletableFuture<byte[]> getOrDefault(String key, byte[] defaultValue) {
    return this.get(key).thenApply(value -> value == null ? defaultValue : value);
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
  @SuppressWarnings("unchecked")
  public CompletableFuture<byte[]> put(String key, byte[] value) {
    // TODO: fix race condition
    return this.getFull(key)
        .thenCompose(
            result -> {
              if (result == null) return this.applyLocal(key, service -> service.put(key, value));
              return result.getKey().apply(service -> service.put(key, value));
            });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<byte[]> remove(String key) {
    return this.getProxyClient()
        .applyAll(service -> service.remove(key))
        .thenApply(results -> results.filter(Objects::nonNull).findAny().orElse(null));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return this.getProxyClient()
        .applyAll(service -> service.remove(key, value))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findAny().orElse(false));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return this.getProxyClient().acceptAll(LocalMapService::clear);
  }

  @Override
  public CompletableFuture<AsyncLocalMap<String, byte[]>> connect() {
    return super.connect()
        .thenCompose(
            map ->
                Futures.allOf(getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenApply(map -> this);
  }

  private Optional<ProxySession<LocalMapService<String>>> getLocalPartition() {
    final MemberId localId = this.membershipService.getLocalMember().id();

    return this.getProxyClient().getPartitionIds().stream()
        .map(
            partitionId ->
                this.partitionService
                    .getPartitionGroup(partitionId.group())
                    .getPartition(partitionId))
        .filter(partition -> localId.equals(partition.primary()))
        .findAny()
        .map(Partition::id)
        .map(this.getProxyClient()::getPartition);
  }

  private ProxySession<LocalMapService<String>> getKeyPartition(String key) {
    return this.getLocalPartition().orElse(this.getProxyClient().getPartition(key));
  }

  <R> CompletableFuture<R> applyLocal(String key, Function<LocalMapService<String>, R> operation) {
    return this.getKeyPartition(key).apply(operation);
  }

  private CompletableFuture<Entry<ProxySession<LocalMapService<String>>, byte[]>> getFull(
      String key) {
    return this.applyAll(service -> service.get(key))
        .thenApply(
            results ->
                results
                    .filter(
                        result ->
                            result != null && result.getKey() != null && result.getValue() != null)
                    .findAny()
                    .orElse(null));
  }

  <R> CompletableFuture<Stream<Entry<ProxySession<LocalMapService<String>>, R>>> applyAll(
      Function<LocalMapService<String>, R> operation) {
    return Futures.allOf(
        this.getProxyClient().getPartitions().stream()
            .map(proxy -> proxy.apply(operation).thenApply(r -> Maps.immutableEntry(proxy, r))));
  }

  private class LocalMapEntrySet implements AsyncDistributedSet<Map.Entry<String, byte[]>> {
    @Override
    public String name() {
      return PartitionedLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedLocalMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(
        CollectionEventListener<Map.Entry<String, byte[]>> listener, Executor executor) {
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
      return PartitionedLocalMapProxy.this.remove(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedLocalMapProxy.this.clear();
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
      return new PartitionedProxyIterator<>(
          getProxyClient(),
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
      return PartitionedLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedLocalMapProxy.this.delete();
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
      return PartitionedLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedLocalMapProxy.this.protocol();
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
      return PartitionedLocalMapProxy.this.remove(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedLocalMapProxy.this.clear();
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
      return new PartitionedProxyIterator<>(
          getProxyClient(),
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
      return PartitionedLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedLocalMapProxy.this.delete();
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
      return PartitionedLocalMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedLocalMapProxy.this.protocol();
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
      return PartitionedLocalMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedLocalMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedLocalMapProxy.this.clear();
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
      return new PartitionedProxyIterator<>(
          getProxyClient(),
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
      return PartitionedLocalMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedLocalMapProxy.this.delete();
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
