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
package com.khulnasoft.oss.ksbulk.codecs.jdk.number;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;

public class NumberToStringCodec<EXTERNAL extends Number>
    extends ConvertingCodec<EXTERNAL, String> {
  private final FastThreadLocal<NumberFormat> numberFormat;

  public NumberToStringCodec(Class<EXTERNAL> javaType, FastThreadLocal<NumberFormat> numberFormat) {
    super(TypeCodecs.TEXT, javaType);
    this.numberFormat = numberFormat;
  }

  @Override
  public EXTERNAL internalToExternal(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from string to number");
  }

  @Override
  public String externalToInternal(EXTERNAL value) {
    if (value == null) {
      return null;
    }
    return CodecUtils.formatNumber(value, numberFormat.get());
  }
}
