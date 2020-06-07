package de.p4skal.atomix.local;

import com.google.common.collect.Maps;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter(AccessLevel.PACKAGE)
@RequiredArgsConstructor
public class CommunicationResult<S, R> {
  private final LocalCommunication<S> communication;
  private final CompletableFuture<R> resultFuture;

  public CompletableFuture<R> resultFuture() {
    return this.resultFuture;
  }

  public CompletableFuture<Entry<LocalCommunication<S>, R>> communicationFuture() {
    return this.resultFuture.thenApply(result -> Maps.immutableEntry(this.communication, result));
  }

  public static <T> T solve(CompletableFuture<T> future, T defaultValue, long timeout) {
    try {
      return future.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Throwable throwable) {
      return defaultValue;
    }
  }
}
