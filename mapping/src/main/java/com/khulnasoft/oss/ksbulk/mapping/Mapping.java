/*
 * Copyright KhulnaSoft, Inc.
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
package com.khulnasoft.oss.ksbulk.mapping;

import com.khulnasoft.oss.driver.api.core.type.DataType;
import com.khulnasoft.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.driver.api.core.type.reflect.GenericType;
import com.khulnasoft.oss.ksbulk.connectors.api.Field;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/**
 * Defines a bidirectional, many-to-many relationship between record fields and CQL columns.
 *
 * <p>In write workflows, CQL words correspond to bound variables in the write statement. In read
 * workflows, CQL words correspond to row variables in a read result.
 */
public interface Mapping {

  /**
   * Maps the given field to one or more variables in a write bound statement.
   *
   * @param field the field to find the variable for.
   * @return the bound statement variables the given field maps to, or an empty collection if the
   *     field does not map to any known bound statement variable.
   */
  @NonNull
  Set<CQLWord> fieldToVariables(@NonNull Field field);

  /**
   * Maps the given row variable to one or more fields in a record.
   *
   * @param variable the row variable; never {@code null}.
   * @return the fields the given variable maps to, or an empty collection if the variable does not
   *     map to any known field.
   */
  @NonNull
  Set<Field> variableToFields(@NonNull CQLWord variable);

  /**
   * Returns the codec to use for the given bound statement or row variable.
   *
   * <p>Note that the given variable name must be supplied unquoted, even if it requires quoting to
   * comply with the syntax of CQL identifiers.
   *
   * @param <T> the codec's Java type.
   * @param variable the bound statement or row variable; never {@code null}.
   * @param cqlType the CQL type; never {@code null}.
   * @param javaType the Java type; never {@code null}.
   * @return the codec to use; never {@code null}.
   * @throws CodecNotFoundException if a suitable codec cannot be found.
   */
  @NonNull
  <T> TypeCodec<T> codec(
      @NonNull CQLWord variable,
      @NonNull DataType cqlType,
      @NonNull GenericType<? extends T> javaType)
      throws CodecNotFoundException;

  /**
   * Returns all the fields in this mapping.
   *
   * @return the fields in this mapping.
   */
  @NonNull
  Set<Field> fields();

  /**
   * Returns all the variables in this mapping.
   *
   * @return the variables in this mapping.
   */
  @NonNull
  Set<CQLWord> variables();
}
