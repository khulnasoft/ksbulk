/*
 * Copyright KhulnaSoft, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.khulnasoft.oss.ksbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class DefaultMappedField implements MappedField {

  private final String name;

  public DefaultMappedField(@NonNull String name) {
    this.name = name;
  }

  @Override
  @NonNull
  public String getFieldDescription() {
    return name;
  }

  @Override
  @NonNull
  public String getFieldName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    // implementation note: it is important to consider other implementations of MappedField
    if (!(o instanceof MappedField)) {
      return false;
    }
    MappedField that = (MappedField) o;
    return name.equals(that.getFieldName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  @NonNull
  public String toString() {
    return getFieldDescription();
  }
}
