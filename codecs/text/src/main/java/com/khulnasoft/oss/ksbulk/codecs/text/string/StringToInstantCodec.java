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
package com.khulnasoft.oss.ksbulk.codecs.text.string;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.format.temporal.TemporalFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  private final ZoneId timeZone;
  private final ZonedDateTime epoch;

  public StringToInstantCodec(
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      ZonedDateTime epoch,
      List<String> nullStrings) {
    super(TypeCodecs.TIMESTAMP, temporalFormat, nullStrings);
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toInstant(temporal, timeZone, epoch.toLocalDate());
  }
}
