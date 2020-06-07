package de.p4skal.atomix.local;

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.CloseFunction;
import io.atomix.core.iterator.impl.NextFunction;
import io.atomix.core.iterator.impl.OpenFunction;
import io.atomix.utils.concurrent.Futures;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ClusteredProxyIterator<S, T> implements AsyncIterator<T> {
  private final Iterator<AsyncIterator<T>> members;
  private volatile AsyncIterator<T> iterator;
  private AtomicBoolean closed = new AtomicBoolean();

  public ClusteredProxyIterator(
      LocalCommunicator<S> communicator,
      OpenFunction<S, T> openFunction,
      NextFunction<S, T> nextFunction,
      CloseFunction<S> closeFunction) {
    this.members = communicator.getMemberIds().stream()
        .<AsyncIterator<T>>map(memberId -> new LocalProxyIterator<>(communicator, memberId, openFunction, nextFunction, closeFunction))
        .collect(Collectors.toList())
        .iterator();
    iterator = members.next();
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return iterator.hasNext()
        .thenCompose(hasNext -> {
          if (!hasNext) {
            if (members.hasNext()) {
              if (closed.get()) {
                return Futures.exceptionalFuture(new IllegalStateException("Iterator closed"));
              }
              iterator = members.next();
              return hasNext();
            }
            return CompletableFuture.completedFuture(false);
          }
          return CompletableFuture.completedFuture(true);
        });
  }

  @Override
  public CompletableFuture<T> next() {
    return iterator.next();
  }

  @Override
  public CompletableFuture<Void> close() {
    closed.set(true);
    return iterator.close();
  }
}
