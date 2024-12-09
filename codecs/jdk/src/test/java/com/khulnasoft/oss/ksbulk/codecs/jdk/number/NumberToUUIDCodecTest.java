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

import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.FIXED;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.MAX;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.MIN;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.RANDOM;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.driver.api.core.uuid.Uuids;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class NumberToUUIDCodecTest {

  private NumberToInstantCodec<Long> instantCodec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    assertThat(new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());
  }
}