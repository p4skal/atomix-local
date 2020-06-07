package de.p4skal.atomix.local;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocalResponse {
  public static LocalResponse ok(byte[] result) {
    return new LocalResponse(0, result);
  }

  public static LocalResponse error() {
    return new LocalResponse(1, null);
  }

  private int status;
  private byte[] result;
}
