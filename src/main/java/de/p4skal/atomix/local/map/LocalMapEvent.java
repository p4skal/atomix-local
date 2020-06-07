package de.p4skal.atomix.local.map;

import io.atomix.utils.event.AbstractEvent;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@ToString
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
public class LocalMapEvent<K, V> extends AbstractEvent<LocalMapEvent.Type, K> {
  private final V newValue;
  private final V oldValue;

  public LocalMapEvent(LocalMapEvent.Type type, K key, V newValue, V oldValue) {
    super(type, key);
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  public enum Type {
    INSERT, UPDATE, REMOVE
  }
}
