package de.p4skal.atomix.local.map;

import com.google.common.collect.Maps;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.SneakyThrows;

public class DefaultLocalMapService<K> extends AbstractPrimitiveService<LocalMapClient>
    implements LocalMapService<K> {
  private static final int MAX_ITERATOR_BATCH_SIZE = 1024 * 32;

  private final Serializer serializer;
  private Map<K, byte[]> map = Maps.newConcurrentMap();
  private Map<Long, IteratorContext> entryIterators = Maps.newHashMap();

  public DefaultLocalMapService(PrimitiveType primitiveType) {
    super(primitiveType, LocalMapClient.class);
    this.serializer =
        Serializer.using(
            Namespace.builder()
                .register(primitiveType.namespace())
                .register(SessionId.class)
                .register(new HashMap<>().keySet().getClass())
                .register(DefaultIterator.class)
                .build());
  }

  @Override
  public Serializer serializer() {
    return this.serializer;
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeObject(Maps.newHashMap(this.map));
    output.writeObject(this.entryIterators);
  }

  @Override
  public void restore(BackupInput input) {
    this.map = Maps.newConcurrentMap();
    this.map.putAll(input.readObject());
    this.entryIterators = input.readObject();
  }

  @Override
  public int size() {
    return this.map.size();
  }

  @Override
  @SneakyThrows
  public boolean containsKey(K key) {
    return this.map.containsKey(key);
  }

  @Override
  public boolean containsValue(byte[] value) {
    return this.map.containsValue(value);
  }

  @Override
  public byte[] get(K key) {
    return this.map.get(key);
  }

  @Override
  public byte[] put(K key, byte[] value) {
    return this.map.put(key, value);
  }

  @Override
  public byte[] remove(K key) {
    return this.map.remove(key);
  }

  @Override
  public boolean remove(K key, byte[] value) {
    return this.map.remove(key, value);
  }

  @Override
  public void clear() {
    this.map.clear();
  }

  @Override
  public Set<K> keySet() {
    return this.map.keySet();
  }

  @Override
  public Collection<byte[]> values() {
    return this.map.values();
  }

  @Override
  public Set<Entry<K, byte[]>> entrySet() {
    return this.map.entrySet();
  }

  @Override
  public IteratorBatch<K> iterateKeys() {
    return this.iterate(DefaultIterator::new, (k, v) -> k);
  }

  @Override
  public IteratorBatch<K> nextKeys(long iteratorId, int position) {
    return this.next(iteratorId, position, (k, v) -> k);
  }

  @Override
  public void closeKeys(long iteratorId) {
    this.close(iteratorId);
  }

  protected void close(long iteratorId) {
    this.entryIterators.remove(iteratorId);
  }

  @Override
  public IteratorBatch<byte[]> iterateValues() {
    return this.iterate(DefaultIterator::new, (k, v) -> v);
  }

  @Override
  public IteratorBatch<byte[]> nextValues(long iteratorId, int position) {
    return this.next(iteratorId, position, (k, v) -> v);
  }

  @Override
  public void closeValues(long iteratorId) {
    this.close(iteratorId);
  }

  @Override
  public IteratorBatch<Entry<K, byte[]>> iterateEntries() {
    return this.iterate(DefaultIterator::new, Maps::immutableEntry);
  }

  @Override
  public IteratorBatch<Entry<K, byte[]>> nextEntries(long iteratorId, int position) {
    return this.next(iteratorId, position, Maps::immutableEntry);
  }

  @Override
  public void closeEntries(long iteratorId) {
    this.close(iteratorId);
  }

  protected <T> IteratorBatch<T> iterate(
      Function<String, IteratorContext> contextFactory, BiFunction<K, byte[], T> function) {
    IteratorContext iterator = contextFactory.apply(getLocalMemberId().id());
    if (!iterator.iterator().hasNext()) {
      return null;
    }

    long iteratorId = getCurrentIndex();
    entryIterators.put(iteratorId, iterator);
    IteratorBatch<T> batch = next(iteratorId, 0, function);
    if (batch.complete()) {
      entryIterators.remove(iteratorId);
    }
    return batch;
  }

  protected <T> IteratorBatch<T> next(
      long iteratorId, int position, BiFunction<K, byte[], T> function) {
    IteratorContext context = entryIterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<T> entries = new ArrayList<>();
    int size = 0;
    while (context.iterator().hasNext()) {
      context.incrementPosition();
      if (context.position() > position) {
        Map.Entry<K, byte[]> entry = context.iterator().next();
        entries.add(function.apply(entry.getKey(), entry.getValue()));
        size += entry.getValue().length;

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (entries.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(
        iteratorId, context.position, entries, !context.iterator().hasNext());
  }

  protected abstract class IteratorContext {
    private final String memberId;
    private int position = 0;
    private transient Iterator<Entry<K, byte[]>> iterator;

    public IteratorContext(String memberId) {
      this.memberId = memberId;
    }

    protected abstract Iterator<Map.Entry<K, byte[]>> create();

    public String memberId() {
      return memberId;
    }

    public int position() {
      return position;
    }

    public void incrementPosition() {
      position++;
    }

    public Iterator<Map.Entry<K, byte[]>> iterator() {
      if (iterator == null) {
        iterator = create();
      }
      return iterator;
    }
  }

  protected class DefaultIterator extends IteratorContext {
    public DefaultIterator(String memberId) {
      super(memberId);
    }

    @Override
    protected Iterator<Map.Entry<K, byte[]>> create() {
      return map.entrySet().iterator();
    }
  }
}
