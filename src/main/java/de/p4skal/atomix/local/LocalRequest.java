package de.p4skal.atomix.local;

import io.atomix.primitive.operation.PrimitiveOperation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocalRequest {
  private String name;
  private String type;
  private PrimitiveOperation operation;
}
