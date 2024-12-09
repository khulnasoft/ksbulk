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

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;

public class NumberToNumberCodec<EXTERNAL extends Number, INTERNAL extends Number>
    extends ConvertingCodec<EXTERNAL, INTERNAL> {

  public NumberToNumberCodec(Class<EXTERNAL> javaType, TypeCodec<INTERNAL> targetCodec) {
    super(targetCodec, javaType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public EXTERNAL internalToExternal(INTERNAL value) {
    return CodecUtils.convertNumber(value, (Class<EXTERNAL>) getJavaType().getRawType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public INTERNAL externalToInternal(EXTERNAL value) {
    return CodecUtils.convertNumber(
        value, (Class<INTERNAL>) internalCodec.getJavaType().getRawType());
  }
}
