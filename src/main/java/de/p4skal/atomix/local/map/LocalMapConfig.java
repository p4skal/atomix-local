package de.p4skal.atomix.local.map;

import io.atomix.core.map.MapConfig;
import io.atomix.primitive.PrimitiveType;

public class LocalMapConfig extends MapConfig<LocalMapConfig> {

  @Override
  public PrimitiveType getType() {
    return LocalMapType.instance();
  }
}

