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
package com.khulnasoft.oss.ksbulk.codecs.jdk.bool;

import static com.khulnasoft.oss.ksbulk.codecs.api.CommonConversionContext.BOOLEAN_NUMBERS;
import static com.khulnasoft.oss.ksbulk.codecs.jdk.JdkCodecUtils.isNumeric;

import com.khulnasoft.oss.driver.api.core.type.DataType;
import com.khulnasoft.oss.driver.api.core.type.DataTypes;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.driver.api.core.type.reflect.GenericType;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodecFactory;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodecProvider;
import com.khulnasoft.oss.ksbulk.codecs.api.IdempotentConvertingCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

public class BooleanConvertingCodecsProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (externalJavaType.equals(GenericType.BOOLEAN)) {
      if (cqlType == DataTypes.TEXT) {
        return Optional.of(new BooleanToStringCodec());
      }
      if (cqlType == DataTypes.BOOLEAN) {
        return Optional.of(new IdempotentConvertingCodec<>(TypeCodecs.BOOLEAN));
      }
      if (isNumeric(cqlType)) {
        TypeCodec<Number> typeCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
        return Optional.of(
            new BooleanToNumberCodec<>(
                typeCodec, codecFactory.getContext().getAttribute(BOOLEAN_NUMBERS)));
      }
    }
    return Optional.empty();
  }
}
