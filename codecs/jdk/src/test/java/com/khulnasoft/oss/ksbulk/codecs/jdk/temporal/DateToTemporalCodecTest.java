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
package com.khulnasoft.oss.ksbulk.codecs.jdk.temporal;

import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.junit.jupiter.api.Test;

class DateToTemporalCodecTest {

  @Test
  void should_convert_from_java_util_date() {

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(LocalDate.parse("2010-06-29"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(1)))
        .convertsFromExternal(Date.from(Instant.parse("2010-06-30T23:59:59Z")))
        .toInternal(LocalDate.parse("2010-07-01"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromExternal(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromExternal(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("00:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("22:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_to_java_util_date() {

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromInternal(LocalDate.parse("2010-06-30"))
        .toExternal(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalDate.parse("2010-06-29"))
        .toExternal(Date.from(Instant.parse("2010-06-29T01:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(1)))
        .convertsFromInternal(LocalDate.parse("2010-07-01"))
        .toExternal(Date.from(Instant.parse("2010-06-30T23:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromInternal(LocalTime.parse("23:59:59"))
        .toExternal(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromInternal(LocalTime.parse("00:59:59"))
        .toExternal(Date.from(Instant.parse("1969-12-31T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalTime.parse("22:59:59"))
        .toExternal(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_from_java_sql_timestamp() {

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                Timestamp.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .toInternal(LocalDate.parse("2010-06-29"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.ofHours(1)))
        .convertsFromExternal(Timestamp.from(Instant.parse("2010-06-30T23:59:59Z")))
        .toInternal(LocalDate.parse("2010-07-01"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromExternal(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromExternal(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("00:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .toInternal(LocalTime.parse("22:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_to_java_sql_timestamp() {

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                Timestamp.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromInternal(LocalDate.parse("2010-06-30"))
        .toExternal(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalDate.parse("2010-06-29"))
        .toExternal(Timestamp.from(Instant.parse("2010-06-29T01:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.DATE, ZoneOffset.ofHours(1)))
        .convertsFromInternal(LocalDate.parse("2010-07-01"))
        .toExternal(Timestamp.from(Instant.parse("2010-06-30T23:00:00Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromInternal(LocalTime.parse("23:59:59"))
        .toExternal(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromInternal(LocalTime.parse("00:59:59"))
        .toExternal(Timestamp.from(Instant.parse("1969-12-31T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(Timestamp.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalTime.parse("22:59:59"))
        .toExternal(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_from_java_sql_date() {

    assertThat(new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .toInternal(Instant.parse("2010-06-30T01:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_to_java_sql_date() {

    assertThat(new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(Instant.parse("2010-06-30T01:00:00Z"))
        .toExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .convertsFromInternal(LocalDate.parse("2010-06-30"))
        .toExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalDate.parse("2010-06-30"))
        .toExternal(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_from_java_sql_time() {

    assertThat(new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .toInternal(Instant.parse("1970-01-01T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .toInternal(Instant.parse("1970-01-01T01:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_to_java_sql_time() {

    assertThat(new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC))
        .convertsFromInternal(Instant.parse("1970-01-01T00:00:00Z"))
        .toExternal(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, TypeCodecs.TIMESTAMP, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(Instant.parse("1970-01-01T01:00:00Z"))
        .toExternal(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .convertsFromInternal(LocalTime.parse("23:59:59"))
        .toExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .convertsFromInternal(LocalTime.parse("23:59:59"))
        .toExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.TIME, ZoneOffset.ofHours(-1)))
        .convertsFromInternal(LocalTime.parse("23:59:59"))
        .toExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_between_incompatible_types() {

    assertThat(new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.TIME, ZoneOffset.UTC))
        .cannotConvertFromExternal(
            new java.sql.Date(Instant.parse("1970-01-01T23:59:59Z").toEpochMilli()))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, TypeCodecs.TIME, ZoneOffset.ofHours(1)))
        .cannotConvertFromExternal(
            new java.sql.Date(Instant.parse("1970-01-01T23:59:59Z").toEpochMilli()))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.DATE, ZoneOffset.UTC))
        .cannotConvertFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.DATE, ZoneOffset.ofHours(-1)))
        .cannotConvertFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, TypeCodecs.DATE, ZoneOffset.ofHours(1)))
        .cannotConvertFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
