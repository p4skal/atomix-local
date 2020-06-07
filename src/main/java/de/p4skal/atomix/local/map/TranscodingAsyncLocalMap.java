package de.p4skal.atomix.local.map;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.TranscodingAsyncDistributedSet;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.Getter;

public class TranscodingAsyncLocalMap<K1, V1, K2, V2> extends DelegatingAsyncPrimitive
    implements AsyncLocalMap<K1, V1> {

  @Getter
  private final AsyncLocalMap<K2, V2> backingMap;
  protected final Function<K1, K2> keyEncoder;
  protected final Function<K2, K1> keyDecoder;
  protected final Function<V2, V1> valueDecoder;
  protected final Function<V1, V2> valueEncoder;
  protected final Function<Entry<K2, V2>, Entry<K1, V1>> entryDecoder;
  protected final Function<Entry<K1, V1>, Entry<K2, V2>> entryEncoder;

  public TranscodingAsyncLocalMap(
      AsyncLocalMap<K2, V2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    super(backingMap);
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
    this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
    this.entryDecoder =
        e ->
            e == null
                ? null
                : Maps.immutableEntry(
                    keyDecoder.apply(e.getKey()), valueDecoder.apply(e.getValue()));
    this.entryEncoder =
        e ->
            e == null
                ? null
                : Maps.immutableEntry(
                    keyEncoder.apply(e.getKey()), valueEncoder.apply(e.getValue()));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.backingMap.size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K1 key) {
    try {
      return this.backingMap.containsKey(this.keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V1 value) {
    try {
      return this.backingMap.containsValue(this.valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> get(K1 key) {
    try {
      return this.backingMap.get(this.keyEncoder.apply(key)).thenApply(this.valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> getOrDefault(K1 key, V1 defaultValue) {
    try {
      return this.backingMap
          .getOrDefault(this.keyEncoder.apply(key), this.valueEncoder.apply(defaultValue))
          .thenApply(this.valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> put(K1 key, V1 value) {
    try {
      return this.backingMap
          .put(this.keyEncoder.apply(key), this.valueEncoder.apply(value))
          .thenApply(this.valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> remove(K1 key) {
    try {
      return this.backingMap.remove(this.keyEncoder.apply(key)).thenApply(this.valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, V1 value) {
    try {
      return this.backingMap.remove(this.keyEncoder.apply(key), this.valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> clear() {
    return this.backingMap.clear();
  }

  @Override
  public AsyncDistributedSet<K1> keySet() {
    return new TranscodingAsyncDistributedSet<>(
        this.backingMap.keySet(), this.keyEncoder, this.keyDecoder);
  }

  @Override
  public AsyncDistributedCollection<V1> values() {
    return new TranscodingAsyncDistributedCollection<>(
        this.backingMap.values(), this.valueEncoder, this.valueDecoder);
  }

  @Override
  public AsyncDistributedSet<Entry<K1, V1>> entrySet() {
    return new TranscodingAsyncDistributedSet<>(
        this.backingMap.entrySet(), this.entryEncoder, this.entryDecoder);
  }

  @Override
  public LocalMap<K1, V1> sync(Duration operationTimeout) {
    return new BlockingLocalMap<>(this, operationTimeout.toMillis());
  }
}
