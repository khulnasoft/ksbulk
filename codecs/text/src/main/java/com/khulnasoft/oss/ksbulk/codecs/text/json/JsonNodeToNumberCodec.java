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
package com.khulnasoft.oss.ksbulk.codecs.text.json;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.format.temporal.TemporalFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import com.khulnasoft.oss.ksbulk.codecs.api.util.OverflowStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

abstract class JsonNodeToNumberCodec<N extends Number> extends JsonNodeConvertingCodec<N> {

  private final FastThreadLocal<NumberFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final TemporalFormat temporalFormat;
  private final ZoneId timeZone;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final Map<String, Boolean> booleanStrings;
  private final List<N> booleanNumbers;

  JsonNodeToNumberCodec(
      TypeCodec<N> targetCodec,
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanStrings,
      List<N> booleanNumbers,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.temporalFormat = temporalFormat;
    this.timeZone = timeZone;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.booleanStrings = booleanStrings;
    this.booleanNumbers = booleanNumbers;
  }

  Number parseNumber(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    return CodecUtils.parseNumber(
        node.asText(),
        numberFormat.get(),
        temporalFormat,
        timeZone,
        timeUnit,
        epoch,
        booleanStrings,
        booleanNumbers);
  }

  N narrowNumber(Number number, Class<? extends N> targetClass) {
    return CodecUtils.narrowNumber(number, targetClass, overflowStrategy, roundingMode);
  }
}
