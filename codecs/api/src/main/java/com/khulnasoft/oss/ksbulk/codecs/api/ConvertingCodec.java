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
package com.khulnasoft.oss.ksbulk.codecs.api;

import com.khulnasoft.oss.driver.api.core.ProtocolVersion;
import com.khulnasoft.oss.driver.api.core.type.DataType;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

/**
 * A codec that converts to and from an external representation of a value (i.e., the value, as
 * produced by a connector) and its internal representation (i.e., the Java type that matches the
 * target CQL type).
 *
 * @param <EXTERNAL> The external type (as produced and consumed by the connector)
 * @param <INTERNAL> The internal type (the one that matches the target CQL type)
 */
public abstract class ConvertingCodec<EXTERNAL, INTERNAL> implements TypeCodec<EXTERNAL> {

  protected final TypeCodec<INTERNAL> internalCodec;
  protected final GenericType<EXTERNAL> javaType;

  protected ConvertingCodec(TypeCodec<INTERNAL> internalCodec, Class<EXTERNAL> javaType) {
    this.internalCodec = internalCodec;
    this.javaType = GenericType.of(javaType);
  }

  protected ConvertingCodec(TypeCodec<INTERNAL> internalCodec, GenericType<EXTERNAL> javaType) {
    this.internalCodec = internalCodec;
    this.javaType = javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return internalCodec.getCqlType();
  }

  @NonNull
  @Override
  public GenericType<EXTERNAL> getJavaType() {
    return javaType;
  }

  public TypeCodec<INTERNAL> getInternalCodec() {
    return internalCodec;
  }

  public GenericType<INTERNAL> getInternalJavaType() {
    return internalCodec.getJavaType();
  }

  @Override
  public ByteBuffer encode(EXTERNAL external, @NonNull ProtocolVersion protocolVersion) {
    INTERNAL value = externalToInternal(external);
    return internalCodec.encode(value, protocolVersion);
  }

  @Override
  public EXTERNAL decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    INTERNAL internal = internalCodec.decode(bytes, protocolVersion);
    return internalToExternal(internal);
  }

  // parse and format are implemented out of completeness, but are never used

  @Override
  public EXTERNAL parse(String s) {
    INTERNAL value = internalCodec.parse(s);
    return internalToExternal(value);
  }

  @NonNull
  @Override
  public String format(EXTERNAL s) {
    INTERNAL value = externalToInternal(s);
    return internalCodec.format(value);
  }

  /**
   * Converts the external representation of a value (i.e., the value, as produced by a connector)
   * to its internal representation (i.e., the Java type that matches the target CQL type).
   *
   * @param external the value's external form.
   * @return the value's internal form.
   */
  public abstract INTERNAL externalToInternal(EXTERNAL external);

  /**
   * Converts the internal representation of a value (i.e., the Java type that matches the target
   * CQL type) to its external representation (i.e., the value, as consumed by a connector).
   *
   * @param internal the value's internal form.
   * @return the value's external form.
   */
  public abstract EXTERNAL internalToExternal(INTERNAL internal);
}
