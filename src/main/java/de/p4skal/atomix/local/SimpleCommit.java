package de.p4skal.atomix.local;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.session.Session;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClockTimestamp;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class SimpleCommit<T> implements Commit<T> {
  private final OperationId operation;
  private final T value;

  @Override
  public long index() {
    return 0;
  }

  @Override
  public Session session() {
    return null;
  }

  @Override
  public LogicalTimestamp logicalTime() {
    return new LogicalTimestamp(0L);
  }

  @Override
  public WallClockTimestamp wallClockTime() {
    return new WallClockTimestamp(0L);
  }

  @Override
  public <U> Commit<U> map(Function<T, U> transcoder) {
    return new SimpleCommit<>(operation, transcoder.apply(value));
  }

  @Override
  public Commit<Void> mapToNull() {
    return new SimpleCommit<>(operation, null);
  }
}
