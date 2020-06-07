package de.p4skal.atomix.local;

import io.atomix.cluster.MemberId;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.CloseFunction;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.iterator.impl.NextFunction;
import io.atomix.core.iterator.impl.OpenFunction;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.OrderedFuture;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class LocalProxyIterator<S, T> implements AsyncIterator<T> {
  private final LocalCommunicator<S> communicator;
  private final MemberId memberId;
  private final NextFunction<S, T> nextFunction;
  private final CloseFunction<S> closeFunction;
  private final CompletableFuture<IteratorBatch<T>> openFuture;
  private volatile CompletableFuture<IteratorBatch<T>> batch;
  private volatile CompletableFuture<Void> closeFuture;

  public LocalProxyIterator(
      LocalCommunicator<S> communicator,
      MemberId memberId,
      OpenFunction<S, T> openFunction,
      NextFunction<S, T> nextFunction,
      CloseFunction<S> closeFunction) {
    this.communicator = communicator;
    this.memberId = memberId;
    this.nextFunction = nextFunction;
    this.closeFunction = closeFunction;
    this.openFuture = OrderedFuture.wrap(communicator.applyOn(memberId, openFunction::open).resultFuture());
    this.batch = openFuture;
  }

  private CompletableFuture<Iterator<T>> batch() {
    return batch.thenCompose(iterator -> {
      if (iterator != null && !iterator.hasNext()) {
        batch = fetch(iterator.position());
        return batch.thenApply(Function.identity());
      }
      return CompletableFuture.completedFuture(iterator);
    });
  }

  private CompletableFuture<IteratorBatch<T>> fetch(int position) {
    return openFuture.thenCompose(initialBatch -> {
      if (!initialBatch.complete()) {
        return communicator.applyOn(memberId, service -> nextFunction.next(service, initialBatch.id(), position))
            .resultFuture()
            .thenCompose(nextBatch -> {
              if (nextBatch == null) {
                return close().thenApply(v -> null);
              }
              return CompletableFuture.completedFuture(nextBatch);
            });
      }
      return CompletableFuture.completedFuture(null);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = openFuture.thenCompose(initialBatch -> {
            if (initialBatch != null && !initialBatch.complete()) {
              return communicator.acceptOn(memberId, service -> closeFunction.close(service, initialBatch.id())).resultFuture();
            }
            return CompletableFuture.completedFuture(null);
          });
        }
      }
    }
    return closeFuture;
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return batch().thenApply(iterator -> iterator != null && iterator.hasNext());
  }

  @Override
  public CompletableFuture<T> next() {
    return batch().thenCompose(iterator -> {
      if (iterator == null) {
        return Futures.exceptionalFuture(new NoSuchElementException());
      }
      return CompletableFuture.completedFuture(iterator.next());
    });
  }
}
