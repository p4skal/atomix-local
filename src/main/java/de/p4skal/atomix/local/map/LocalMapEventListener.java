package de.p4skal.atomix.local.map;

import io.atomix.utils.event.EventListener;

public interface LocalMapEventListener<K, V> extends EventListener<LocalMapEvent<K, V>> {}
