package de.p4skal.atomix.local;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommunicationResults<S, R> {
  private final Collection<CommunicationResult<S, R>> results;
  private CompletableFuture<Map<LocalCommunication<S>, R>> future;

  public CommunicationResults(Collection<CommunicationResult<S, R>> results) {
    this.results = results;
  }

  public CommunicationResults(Stream<CommunicationResult<S, R>> results) {
    this(results.collect(Collectors.toList()));
  }

  public CompletableFuture<Map<LocalCommunication<S>, R>> future(R defaultValue, long timeout) {
    if (this.future == null) {
      synchronized (this) {
        if (this.future == null) {
          this.future =
              CompletableFuture.supplyAsync(
                  () -> {
                    final CompletableFuture<Void> allFuture =
                        CompletableFuture.allOf(
                            this.results.stream()
                                .map(CommunicationResult::getResultFuture)
                                .toArray(CompletableFuture[]::new));
                    try {
                      allFuture.get(timeout, TimeUnit.MILLISECONDS);
                    } catch (Throwable ignored) {
                    }

                    return this.results.stream()
                        .collect(
                            HashMap::new,
                            (map, result) ->
                                map.put(
                                    result.getCommunication(),
                                    result.getResultFuture().getNow(defaultValue)),
                            HashMap::putAll);
                  });
        }
      }
    }
    return this.future;
  }

  public CompletableFuture<Stream<R>> resultsFuture(R defaultValue, long timeout) {
    return this.future(defaultValue, timeout).thenApply(map -> map.values().stream());
  }

  public CompletableFuture<Void> awaitFuture(long timeout) {
    return this.future(null, timeout).thenApply(ignored -> null);
  }

  public Map<LocalCommunication<S>, R> get(R defaultValue, long timeout) {
    this.future(defaultValue, timeout);
    try {
      return this.future.get(timeout + 1000L, TimeUnit.MILLISECONDS);
    } catch (Throwable throwable) {
      return this.results.stream()
          .collect(
              HashMap::new,
              (map, result) -> map.put(result.getCommunication(), defaultValue),
              HashMap::putAll);
    }
  }

  public Stream<R> getStream(R defaultValue, long timeout) {
    this.future(defaultValue, timeout);
    try {
      return this.future.get(timeout + 1000L, TimeUnit.MILLISECONDS).values().stream();
    } catch (Throwable throwable) {
      return this.results.stream().map(ignored -> defaultValue);
    }
  }
}
