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
package com.khulnasoft.oss.ksbulk.codecs.api.util;

import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.convertNumber;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.convertTemporal;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.formatNumber;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.instantToNumber;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.numberToInstant;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.parseNumber;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toBigDecimal;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toBigIntegerExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toByteValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toDoubleValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toFloatValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toInstant;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toIntValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toLocalDate;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toLocalDateTime;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toLocalTime;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toLongValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toShortValueExact;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils.toZonedDateTime;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.FIXED;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.MAX;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.MIN;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.TimeUUIDGenerator.RANDOM;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.Instant.EPOCH;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.khulnasoft.dse.driver.api.core.data.geometry.LineString;
import com.khulnasoft.dse.driver.api.core.data.geometry.Polygon;
import com.khulnasoft.dse.driver.api.core.data.time.DateRange;
import com.khulnasoft.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.khulnasoft.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.khulnasoft.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.khulnasoft.oss.driver.api.core.data.ByteUtils;
import com.khulnasoft.oss.driver.api.core.uuid.Uuids;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.Lists;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.khulnasoft.oss.ksbulk.codecs.api.format.temporal.TemporalFormat;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CodecUtilsTest {

  private final Instant i1 = Instant.parse("2017-11-23T12:24:59Z");
  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");

  private final NumberFormat numberFormat1 =
      CodecUtils.getNumberFormat("#,###.##", US, HALF_EVEN, true);

  private final NumberFormat numberFormat2 =
      CodecUtils.getNumberFormat("0.###E0", US, UNNECESSARY, true);

  private final NumberFormat numberFormat3 =
      CodecUtils.getNumberFormat("#.##", US, UNNECESSARY, true);

  private final NumberFormat numberFormat4 =
      CodecUtils.getNumberFormat("#,###.##", US, UNNECESSARY, false);

  private final TemporalFormat timestampFormat1 =
      CodecUtils.getTemporalFormat(
          "CQL_TIMESTAMP",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          new FastThreadLocal<NumberFormat>() {
            @Override
            protected NumberFormat initialValue() {
              return numberFormat1;
            }
          },
          true);

  private final Map<String, Boolean> booleanInputWords =
      ImmutableMap.of("true", true, "false", false);

  private final List<BigDecimal> booleanNumbers =
      Lists.newArrayList(BigDecimal.ONE, BigDecimal.ZERO);

  @Test
  void should_parse_number_complex() {
    assertThat(
            parseNumber(
                null,
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isNull();
    assertThat(
            parseNumber(
                "",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isNull();
    // user patterns
    assertThat(
            parseNumber(
                "-123456.78",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    assertThat(
            parseNumber(
                "-123,456.78",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    // exponent
    assertThat(
            parseNumber(
                "1,234.123E78",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("1234.123E78"));
    // java notation
    assertThat(
            parseNumber(
                "0x1.fffP+1023",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(0x1.fffP+1023); // parsed as double
    assertThat(
            parseNumber(
                "+Infinity",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.POSITIVE_INFINITY); // parsed as double
    assertThat(
            parseNumber(
                "-Infinity",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.NEGATIVE_INFINITY); // parsed as double
    assertThat(
            parseNumber(
                "NaN",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.NaN); // parsed as double
    // timestamps -> unit since epoch
    assertThat(
            parseNumber(
                "2017-12-05T12:44:36+01:00",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli());
    // booleans
    assertThat(
            parseNumber(
                "TRUE",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(BigDecimal.ONE);
    assertThat(
            parseNumber(
                "FALSE",
                numberFormat1,
                timestampFormat1,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(BigDecimal.ZERO);
  }

  @Test
  void should_parse_number() throws ParseException {
    assertThat(parseNumber("1,234.567", numberFormat1)).isEqualTo(new BigDecimal("1234.567"));
    assertThat(parseNumber("0.1234E10", numberFormat2)).isEqualTo(new BigDecimal("0.1234E10"));
    assertThatThrownBy(() -> parseNumber("1,234.567", numberFormat2))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Invalid number format: 1,234.567");
    assertThatThrownBy(() -> parseNumber("0.1234 ABC", numberFormat1))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Invalid number format: 0.1234 ABC");
    // check that parsing is still possible even if formatting is "turned off"
    assertThat(parseNumber("1,234.567", numberFormat4)).isEqualTo(new BigDecimal("1234.567"));
    assertThatThrownBy(() -> parseNumber("0.1234 ABC", numberFormat4))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Invalid number format: 0.1234 ABC");
  }

  @Test
  void should_format_number() {
    assertThat(formatNumber(null, numberFormat1)).isNull();
    // rounded up because of HALF_EVEN
    assertThat(formatNumber(123_456.789, numberFormat1)).isEqualTo("123,456.79");
    assertThat(formatNumber(Float.MAX_VALUE, numberFormat1))
        .isEqualTo("340,282,350,000,000,000,000,000,000,000,000,000,000");
    // rounded to 0.00, then -> 0 because fraction digits are optional
    assertThat(formatNumber(Float.MIN_VALUE, numberFormat1)).isEqualTo("0");
    assertThat(formatNumber(Float.MIN_VALUE, numberFormat2)).isEqualTo("1.4E-45");
    // special Double values
    assertThat(formatNumber(Double.NaN, numberFormat1)).isEqualTo("NaN");
    assertThat(formatNumber(Double.POSITIVE_INFINITY, numberFormat1)).isEqualTo("Infinity");
    assertThat(formatNumber(Double.NEGATIVE_INFINITY, numberFormat1)).isEqualTo("-Infinity");
    // with rounding mode UNNECESSARY, check that all fraction digits are printed
    assertThat(formatNumber(Math.PI, numberFormat3)).isEqualTo("3.141592653589793");
    assertThat(formatNumber(Float.MIN_VALUE, numberFormat3))
        .isEqualTo("0.0000000000000000000000000000000000000000000014");
    // without formatting
    assertThat(formatNumber(1234, numberFormat4)).isEqualTo("1234");
    assertThat(formatNumber(1234.5678, numberFormat4)).isEqualTo("1234.5678");
    assertThat(formatNumber(new BigDecimal("1.2E+10"), numberFormat4))
        .isEqualTo(new BigDecimal("1.2E+10").toString());
    assertThat(formatNumber(Math.PI, numberFormat4)).isEqualTo("3.141592653589793");
    assertThat(formatNumber(Long.MIN_VALUE, numberFormat4))
        .isEqualTo(Long.toString(Long.MIN_VALUE));
    assertThat(formatNumber(Long.MAX_VALUE, numberFormat4))
        .isEqualTo(Long.toString(Long.MAX_VALUE));
    assertThat(formatNumber(Double.MIN_VALUE, numberFormat4))
        .isEqualTo(Double.toString(Double.MIN_VALUE));
    assertThat(formatNumber(Double.MAX_VALUE, numberFormat4))
        .isEqualTo(Double.toString(Double.MAX_VALUE));
  }

  @Test
  void should_convert_temporal() {
    assertThat(convertTemporal(null, LocalDate.class, UTC, LocalDate.ofEpochDay(0))).isNull();
    // to LocalDate
    assertThat(
            convertTemporal(
                Instant.parse("2010-06-30T00:00:00Z"),
                LocalDate.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // to LocalTime
    assertThat(
            convertTemporal(
                Instant.parse("1970-01-01T23:59:59Z"),
                LocalTime.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // to LocalDateTime
    assertThat(
            convertTemporal(
                Instant.parse("1970-01-01T23:59:59Z"),
                LocalDateTime.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // to Instant
    assertThat(
            convertTemporal(
                LocalDate.parse("2010-06-30"), Instant.class, UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T00:00:00Z"));
    // to ZonedDateTime
    assertThat(
            convertTemporal(
                LocalDate.parse("2010-06-30"), ZonedDateTime.class, UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00Z"));
    // to unsupported temporal
    assertThatThrownBy(
            () ->
                convertTemporal(
                    ZonedDateTime.parse("2010-06-30T00:00:00Z"),
                    YearMonth.class,
                    UTC,
                    LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toZonedDateTime() {
    // from LocalDate
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00Z"));
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T01:00:00Z"));
    // from LocalTime
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-02T00:59:59Z"));
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T22:59:59Z"));
    // from LocalDateTime
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T23:59:59Z"));
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-07-01T00:59:59Z"));
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T22:59:59Z"));
    // from Instant
    assertThat(toZonedDateTime(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    assertThat(
            toZonedDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T22:59:59-01:00"));
    assertThat(
            toZonedDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-02T00:59:59+01:00"));
    // from parsed temporals
    assertThat(
            toZonedDateTime(
                timestampFormat1.parse("2010-06-30T00:00:00+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toZonedDateTime(
                timestampFormat1.parse("2010-06-30T00:00:00+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    // from ZonedDateTime
    assertThat(
            toZonedDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    // from unsupported temporal
    assertThatThrownBy(() -> toZonedDateTime(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toInstant() {
    // from LocalDate
    assertThat(toInstant(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T00:00:00Z"));
    assertThat(toInstant(LocalDate.parse("2010-06-30"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(toInstant(LocalDate.parse("2010-06-30"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T01:00:00Z"));
    // from LocalTime
    assertThat(toInstant(LocalTime.parse("23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T23:59:59Z"));
    assertThat(toInstant(LocalTime.parse("23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-02T00:59:59Z"));
    assertThat(toInstant(LocalTime.parse("23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T22:59:59Z"));
    // from LocalDateTime
    assertThat(toInstant(LocalDateTime.parse("2010-06-30T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T23:59:59Z"));
    assertThat(
            toInstant(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-07-01T00:59:59Z"));
    assertThat(
            toInstant(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T22:59:59Z"));
    // from Instant
    assertThat(toInstant(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T23:59:59Z"));
    // from ZonedDateTime
    assertThat(
            toInstant(
                ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toInstant(
                ZonedDateTime.parse("2010-06-30T00:00:00+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    // from parsed temporals
    assertThat(
            toInstant(
                timestampFormat1.parse("2010-06-30T00:00:00+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toInstant(
                timestampFormat1.parse("2010-06-30T00:00:00+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    // from unsupported temporal
    assertThatThrownBy(() -> toInstant(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalDateTime() {
    // from LocalDate
    assertThat(toLocalDateTime(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("2010-06-30T00:00:00"));
    // from LocalTime
    assertThat(toLocalDateTime(LocalTime.parse("00:00:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T00:00:00"));
    // from LocalDateTime
    assertThat(
            toLocalDateTime(
                LocalDateTime.parse("1970-01-01T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from Instant
    assertThat(toLocalDateTime(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T22:59:59"));
    assertThat(
            toLocalDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-02T00:59:59"));
    // from ZonedDateTime
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"),
                ofHours(-1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from parsed temporals
    assertThat(
            toLocalDateTime(
                timestampFormat1.parse("1970-01-01T23:59:59+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                timestampFormat1.parse("1970-01-01T23:59:59+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalDateTime(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalDate() {
    // from LocalTime (not supported)
    assertThatThrownBy(() -> toLocalDate(LocalTime.parse("23:59:59"), UTC))
        .isInstanceOf(DateTimeException.class);
    // from LocalDate
    assertThat(toLocalDate(LocalDate.parse("2010-06-30"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from LocalDateTime
    assertThat(toLocalDate(LocalDateTime.parse("2010-06-30T00:00:00"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from Instant
    assertThat(toLocalDate(Instant.parse("2010-06-30T00:00:00Z"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(Instant.parse("2010-06-30T00:00:00Z"), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-29"));
    assertThat(toLocalDate(Instant.parse("2010-06-30T23:59:59Z"), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-07-01"));
    // from ZonedDateTime
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from parsed temporals
    assertThat(toLocalDate(timestampFormat1.parse("2010-06-30T00:00:00+01:00"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(timestampFormat1.parse("2010-06-30T00:00:00+01:00"), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(timestampFormat1.parse("2010-06-30T00:00:00+01:00"), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalDate(YearMonth.of(2018, 2), UTC))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalTime() {
    // from LocalDate (not supported)
    assertThatThrownBy(() -> toLocalTime(LocalDate.parse("2018-02-03"), UTC))
        .isInstanceOf(DateTimeException.class);
    // from LocalTime
    assertThat(toLocalTime(LocalTime.parse("23:59:59"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from LocalDateTime
    assertThat(toLocalTime(LocalDateTime.parse("1970-01-01T23:59:59"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from Instant
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1)))
        .isEqualTo(LocalTime.parse("22:59:59"));
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), ofHours(1)))
        .isEqualTo(LocalTime.parse("00:59:59"));
    // from ZonedDateTime
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), ofHours(1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), ofHours(-1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from parsed temporals
    assertThat(toLocalTime(timestampFormat1.parse("1970-01-01T23:59:59+01:00"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(timestampFormat1.parse("1970-01-01T23:59:59+01:00"), ofHours(1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(timestampFormat1.parse("1970-01-01T23:59:59+01:00"), ofHours(-1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalTime(YearMonth.of(2018, 2), UTC))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void should_convert_number() {
    assertThat(convertNumber(123, Byte.class)).isEqualTo((byte) 123);
    assertThat(convertNumber(123, Short.class)).isEqualTo((short) 123);
    assertThat(convertNumber(123, Integer.class)).isEqualTo(123);
    assertThat(convertNumber(123, Long.class)).isEqualTo(123L);
    assertThat(convertNumber(123, BigInteger.class)).isEqualTo(new BigInteger("123"));
    assertThat(convertNumber(123, Float.class)).isEqualTo(123f);
    assertThat(convertNumber(123, Double.class)).isEqualTo(123d);
    assertThat(convertNumber(123, BigDecimal.class)).isEqualTo(new BigDecimal("123"));
    assertThatThrownBy(() -> convertNumber(123, AtomicLong.class))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123 from Integer to AtomicLong");
  }

  @Test
  void test_toByteValueExact() {
    assertThat(toByteValueExact((byte) 123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact((short) 123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123L)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123d)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123f)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(BigInteger.valueOf(123L))).isEqualTo((byte) 123);
    assertThat(toByteValueExact(BigDecimal.valueOf(123d))).isEqualTo((byte) 123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toByteValueExact(123.45f))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Float to Byte");
    assertThatThrownBy(() -> toByteValueExact(123.45d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Double to Byte");
    // too big for byte
    assertThatThrownBy(() -> toByteValueExact(Short.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Short to Byte", Short.MAX_VALUE));
    assertThatThrownBy(() -> toByteValueExact(Integer.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Integer to Byte", Integer.MAX_VALUE));
    assertThatThrownBy(() -> toByteValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(String.format("Cannot convert %s from Long to Byte", Long.MAX_VALUE));
    BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toByteValueExact(bigInteger))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigInteger to Byte", bigInteger));
    BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toByteValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Byte", bigDecimal));
  }

  @Test
  void test_toShortValueExact() {
    assertThat(toShortValueExact((byte) 123)).isEqualTo((short) 123);
    assertThat(toShortValueExact((short) 123)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123L)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123d)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123f)).isEqualTo((short) 123);
    assertThat(toShortValueExact(BigInteger.valueOf(123L))).isEqualTo((short) 123);
    assertThat(toShortValueExact(BigDecimal.valueOf(123d))).isEqualTo((short) 123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toShortValueExact(123.45f))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Float to Short");
    assertThatThrownBy(() -> toShortValueExact(123.45d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Double to Short");
    // too big for short
    assertThatThrownBy(() -> toShortValueExact(Integer.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Integer to Short", Integer.MAX_VALUE));
    assertThatThrownBy(() -> toShortValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Long to Short", Long.MAX_VALUE));
    BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toShortValueExact(bigInteger))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigInteger to Short", bigInteger));
    BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toShortValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Short", bigDecimal));
  }

  @Test
  void test_toIntValueExact() {
    assertThat(toIntValueExact((byte) 123)).isEqualTo(123);
    assertThat(toIntValueExact((short) 123)).isEqualTo(123);
    assertThat(toIntValueExact(123)).isEqualTo(123);
    assertThat(toIntValueExact(123L)).isEqualTo(123);
    assertThat(toIntValueExact(123d)).isEqualTo(123);
    assertThat(toIntValueExact(123f)).isEqualTo(123);
    assertThat(toIntValueExact(BigInteger.valueOf(123L))).isEqualTo(123);
    assertThat(toIntValueExact(BigDecimal.valueOf(123d))).isEqualTo(123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toIntValueExact(123.45f))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Float to Integer");
    assertThatThrownBy(() -> toIntValueExact(123.45d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Double to Integer");
    // too big for int
    assertThatThrownBy(() -> toIntValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Long to Integer", Long.MAX_VALUE));
    BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toIntValueExact(bigInteger))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigInteger to Integer", bigInteger));
    BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE);
    assertThatThrownBy(() -> toIntValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Integer", bigDecimal));
  }

  @Test
  void test_toLongValueExact() {
    assertThat(toLongValueExact((byte) 123)).isEqualTo(123L);
    assertThat(toLongValueExact((short) 123)).isEqualTo(123L);
    assertThat(toLongValueExact(123)).isEqualTo(123L);
    assertThat(toLongValueExact(123L)).isEqualTo(123L);
    assertThat(toLongValueExact(123d)).isEqualTo(123L);
    assertThat(toLongValueExact(123f)).isEqualTo(123L);
    assertThat(toLongValueExact(BigInteger.valueOf(123L))).isEqualTo(123L);
    assertThat(toLongValueExact(BigDecimal.valueOf(123d))).isEqualTo(123L);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toLongValueExact(123.45f))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Float to Long");
    assertThatThrownBy(() -> toLongValueExact(123.45d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Double to Long");
    // too big for long
    BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    assertThatThrownBy(() -> toLongValueExact(bigInteger))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigInteger to Long", bigInteger));
    BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    assertThatThrownBy(() -> toLongValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Long", bigDecimal));
  }

  @Test
  void test_toBigIntegerExact() {
    assertThat(toBigIntegerExact((byte) 123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact((short) 123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123L)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123d)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123f)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(BigInteger.valueOf(123L))).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(BigDecimal.valueOf(123d))).isEqualTo(BigInteger.valueOf(123L));
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toBigIntegerExact(123.45f))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Float to BigInteger");
    assertThatThrownBy(() -> toBigIntegerExact(123.45d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from Double to BigInteger");
    assertThatThrownBy(() -> toBigIntegerExact(BigDecimal.valueOf(123.45d)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 123.45 from BigDecimal to BigInteger");
  }

  @Test
  void test_toFloatValueExact() {
    assertThat(toFloatValueExact((byte) 123)).isEqualTo(123f);
    assertThat(toFloatValueExact((short) 123)).isEqualTo(123f);
    assertThat(toFloatValueExact(123)).isEqualTo(123f);
    assertThat(toFloatValueExact(123L)).isEqualTo(123f);
    assertThat(toFloatValueExact(123d)).isEqualTo(123f);
    assertThat(toFloatValueExact(123f)).isEqualTo(123f);
    assertThat(toFloatValueExact(BigInteger.valueOf(123L))).isEqualTo(123f);
    assertThat(toFloatValueExact(BigDecimal.valueOf(123d))).isEqualTo(123f);
    assertThat(toFloatValueExact(Double.NEGATIVE_INFINITY)).isEqualTo(Float.NEGATIVE_INFINITY);
    assertThat(toFloatValueExact(Double.POSITIVE_INFINITY)).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(toFloatValueExact(Double.NaN)).isNaN();
    // float -> double type widening may alter the original
    assertThatThrownBy(() -> toFloatValueExact((double) Float.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Double to Float", (double) Float.MAX_VALUE));
    // too big for float
    assertThatThrownBy(() -> toFloatValueExact(Double.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from Double to Float", Double.MAX_VALUE));
    // too many significant digits
    assertThatThrownBy(() -> toFloatValueExact(0.1234567891234d))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 0.1234567891234 from Double to Float");
    // too big for float
    BigDecimal bigDecimal = BigDecimal.valueOf(Double.MAX_VALUE);
    assertThatThrownBy(() -> toFloatValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Float", bigDecimal));
  }

  @Test
  void test_toDoubleValueExact() {
    assertThat(toDoubleValueExact((byte) 123)).isEqualTo(123d);
    assertThat(toDoubleValueExact((short) 123)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123L)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123d)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123f)).isEqualTo(123d);
    assertThat(toDoubleValueExact(BigInteger.valueOf(123L))).isEqualTo(123d);
    assertThat(toDoubleValueExact(BigDecimal.valueOf(123d))).isEqualTo(123d);
    assertThat(toDoubleValueExact(Float.NEGATIVE_INFINITY)).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(toDoubleValueExact(Float.POSITIVE_INFINITY)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(toDoubleValueExact(Float.NaN)).isNaN();
    // too big for double
    BigDecimal bigDecimal = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE);
    assertThatThrownBy(() -> toDoubleValueExact(bigDecimal))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining(
            String.format("Cannot convert %s from BigDecimal to Double", bigDecimal));
    // too many significant digits
    assertThatThrownBy(() -> toDoubleValueExact(new BigDecimal("0.1234567890123456789")))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("Cannot convert 0.1234567890123456789 from BigDecimal to Double");
  }

  @Test
  void test_toBigDecimal() {
    assertThat(toBigDecimal((byte) 123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal((short) 123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123L)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123d)).isEqualTo(new BigDecimal("123.0"));
    assertThat(toBigDecimal(123f)).isEqualTo(new BigDecimal("123.0"));
    assertThat(toBigDecimal(BigInteger.valueOf(123L))).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(new BigDecimal("123.0"))).isEqualTo(new BigDecimal("123.0"));
  }

  @Test
  void should_convert_number_to_instant() {
    assertThat(numberToInstant(null, MILLISECONDS, EPOCH)).isNull();

    assertThat(numberToInstant(0, MILLISECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(123));
    assertThat(numberToInstant(-123, MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(-123));
    assertThat(numberToInstant(-123, MILLISECONDS, ofEpochMilli(123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, MILLISECONDS, ofEpochMilli(-123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, SECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, SECONDS, EPOCH)).isEqualTo(ofEpochSecond(123));
    assertThat(numberToInstant(-123, SECONDS, EPOCH)).isEqualTo(ofEpochSecond(-123));
    assertThat(numberToInstant(-123, SECONDS, ofEpochSecond(123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, SECONDS, ofEpochSecond(-123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, 123));
    assertThat(numberToInstant(-123, NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, -123));
    assertThat(numberToInstant(-123, NANOSECONDS, ofEpochSecond(0, 123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, NANOSECONDS, ofEpochSecond(0, -123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123000000456L, NANOSECONDS, EPOCH))
        .isEqualTo(ofEpochSecond(123, 456));
    assertThat(numberToInstant(-123000000456L, NANOSECONDS, EPOCH))
        .isEqualTo(ofEpochSecond(-123, -456));
    assertThat(numberToInstant(-123000000456L, NANOSECONDS, ofEpochSecond(123, 456)))
        .isEqualTo(EPOCH);
    assertThat(numberToInstant(123000000456L, NANOSECONDS, ofEpochSecond(-123, -456)))
        .isEqualTo(EPOCH);

    assertThatThrownBy(() -> numberToInstant(1.234d, MILLISECONDS, EPOCH))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("Cannot convert 1.234 from Double to Long");

    // tests for numeric overflows (DAT-368)

    long n1 = 9999999999999999L;
    assertThat(numberToInstant(n1, NANOSECONDS, EPOCH)).isEqualTo(expectedInstant(n1, NANOSECONDS));
    assertThat(numberToInstant(n1, MICROSECONDS, EPOCH))
        .isEqualTo(expectedInstant(n1, MICROSECONDS));
    assertThat(numberToInstant(n1, MILLISECONDS, EPOCH))
        .isEqualTo(expectedInstant(n1, MILLISECONDS));
    assertThat(numberToInstant(n1, SECONDS, EPOCH)).isEqualTo(expectedInstant(n1, SECONDS));

    long n2 = 99999999999L;
    assertThat(numberToInstant(n2, MINUTES, EPOCH)).isEqualTo(Instant.ofEpochSecond(n2 * 60, 0L));
    assertThat(numberToInstant(n2, HOURS, EPOCH))
        .isEqualTo(Instant.ofEpochSecond(n2 * 60 * 60, 0L));
    assertThat(numberToInstant(n2, DAYS, EPOCH))
        .isEqualTo(Instant.ofEpochSecond(n2 * 60 * 60 * 24, 0L));

    assertThatThrownBy(() -> numberToInstant(999999999999L, DAYS, EPOCH))
        .isInstanceOf(DateTimeException.class)
        .hasMessage("Instant exceeds minimum or maximum instant");
  }

  private static Instant expectedInstant(long n, TimeUnit unit) {
    long seconds = SECONDS.convert(n, unit);
    long remainder = n - unit.convert(seconds, SECONDS);
    long nanoAdjustment = NANOSECONDS.convert(remainder, unit);
    return Instant.ofEpochSecond(seconds, nanoAdjustment);
  }

  @Test
  void should_convert_instant_to_number() {
    assertThat(instantToNumber(i1, MILLISECONDS, EPOCH)).isEqualTo(i1.toEpochMilli());
    assertThat(instantToNumber(i1, NANOSECONDS, EPOCH)).isEqualTo(i1.toEpochMilli() * 1_000_000);
    assertThat(instantToNumber(i1, SECONDS, EPOCH)).isEqualTo(i1.getEpochSecond());
    assertThat(instantToNumber(i1, MINUTES, EPOCH)).isEqualTo(i1.getEpochSecond() / 60);

    assertThat(instantToNumber(i1, MILLISECONDS, millennium))
        .isEqualTo(i1.toEpochMilli() - millennium.toEpochMilli());
    assertThat(instantToNumber(i1, NANOSECONDS, millennium))
        .isEqualTo(i1.toEpochMilli() * 1_000_000 - millennium.toEpochMilli() * 1_000_000);
    assertThat(instantToNumber(i1, SECONDS, millennium))
        .isEqualTo(i1.getEpochSecond() - millennium.getEpochSecond());
    assertThat(instantToNumber(i1, MINUTES, millennium))
        .isEqualTo(i1.getEpochSecond() / 60 - millennium.getEpochSecond() / 60);
  }

  @Test
  void should_parse_uuid() {
    @SuppressWarnings("unchecked")
    ConvertingCodec<String, Instant> instantCodec = mock(ConvertingCodec.class);
    assertThat(CodecUtils.parseUUID(null, instantCodec, MIN)).isNull();
    assertThat(CodecUtils.parseUUID("", instantCodec, MIN)).isNull();
    assertThat(CodecUtils.parseUUID("a15341ec-ebef-4eab-b91d-ff16bf801a79", instantCodec, MIN))
        .isEqualTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"));
    // time Uuids with MIN strategy
    Instant expected = ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant();
    when(instantCodec.externalToInternal(anyString())).thenReturn(expected);
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, MIN))
        .isEqualTo(Uuids.startOf(expected.toEpochMilli()));
    // time UUIDs with MAX strategy
    // the driver's endOf method takes milliseconds and sets all the sub-millisecond digits to their
    // max, that's why we add .000999999
    expected = ZonedDateTime.parse("2017-12-05T12:44:36.000999999+01:00").toInstant();
    when(instantCodec.externalToInternal(anyString())).thenReturn(expected);
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36.000999999+01:00", instantCodec, MAX))
        .isEqualTo(
            Uuids.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    // time UUIDs with FIXED strategy
    expected = ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant();
    when(instantCodec.externalToInternal(anyString())).thenReturn(expected);
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, FIXED).timestamp())
        .isEqualTo(Uuids.startOf(expected.toEpochMilli()).timestamp());
    // time UUIDs with RANDOM strategy
    expected = ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant();
    when(instantCodec.externalToInternal(anyString())).thenReturn(expected);
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, RANDOM).timestamp())
        .isEqualTo(Uuids.startOf(expected.toEpochMilli()).timestamp());
    // invalid UUIDs
    when(instantCodec.externalToInternal("not a valid UUID"))
        .thenThrow(IllegalArgumentException.class);
    assertThatThrownBy(() -> CodecUtils.parseUUID("not a valid UUID", instantCodec, MIN))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void should_parse_byte_buffer() {
    byte[] data = {1, 2, 3, 4, 5, 6};
    String data64 = Base64.getEncoder().encodeToString(data);
    String dataHex = ByteUtils.toHexString(data);
    assertThat(CodecUtils.parseByteBuffer(null)).isNull();
    // DAT-573: consider empty string as empty byte array
    assertThat(CodecUtils.parseByteBuffer("")).isEqualTo(ByteBuffer.wrap(new byte[] {}));
    assertThat(CodecUtils.parseByteBuffer("0x")).isEqualTo(ByteBuffer.wrap(new byte[] {}));
    assertThat(CodecUtils.parseByteBuffer(data64)).isEqualTo(ByteBuffer.wrap(data));
    assertThat(CodecUtils.parseByteBuffer(dataHex)).isEqualTo(ByteBuffer.wrap(data));
  }

  @Test
  void should_parse_point() {
    assertThat(CodecUtils.parsePoint(null)).isNull();
    assertThat(CodecUtils.parsePoint("")).isNull();
    assertThat(CodecUtils.parsePoint("POINT (-1.1 -2.2)"))
        .isEqualTo(new DefaultPoint(-1.1d, -2.2d));
    assertThat(CodecUtils.parsePoint("'POINT (-1.1 -2.2)'"))
        .isEqualTo(new DefaultPoint(-1.1d, -2.2d));
    assertThat(CodecUtils.parsePoint("{\"type\":\"Point\",\"coordinates\":[-1.1,-2.2]}"))
        .isEqualTo(new DefaultPoint(-1.1d, -2.2d));
    assertThat(CodecUtils.parsePoint("AQEAAACamZmZmZnxv5qZmZmZmQHA"))
        .isEqualTo(new DefaultPoint(-1.1d, -2.2d));
    assertThat(CodecUtils.parsePoint("0x01010000009a9999999999f1bf9a999999999901c0"))
        .isEqualTo(new DefaultPoint(-1.1d, -2.2d));
    assertThatThrownBy(() -> CodecUtils.parsePoint("not a valid point"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid point literal");
  }

  @Test
  void should_parse_line_string() {
    LineString lineString =
        new DefaultLineString(
            new DefaultPoint(30, 10), new DefaultPoint(10, 30), new DefaultPoint(40, 40));
    assertThat(CodecUtils.parseLineString(null)).isNull();
    assertThat(CodecUtils.parseLineString("")).isNull();
    assertThat(CodecUtils.parseLineString("LINESTRING (30 10, 10 30, 40 40)"))
        .isEqualTo(lineString);
    assertThat(CodecUtils.parseLineString("'LINESTRING (30 10, 10 30, 40 40)'"))
        .isEqualTo(lineString);
    assertThat(
            CodecUtils.parseLineString(
                "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}"))
        .isEqualTo(lineString);
    assertThat(
            CodecUtils.parseLineString(
                "AQIAAAADAAAAAAAAAAAAPkAAAAAAAAAkQAAAAAAAACRAAAAAAAAAPkAAAAAAAABEQAAAAAAAAERA"))
        .isEqualTo(lineString);
    assertThat(
            CodecUtils.parseLineString(
                "0x0102000000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440"))
        .isEqualTo(lineString);
    assertThatThrownBy(() -> CodecUtils.parseLineString("not a valid line string"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid line string literal");
  }

  @Test
  void should_parse_polygon() {
    Polygon polygon =
        new DefaultPolygon(
            new DefaultPoint(30, 10),
            new DefaultPoint(10, 20),
            new DefaultPoint(20, 40),
            new DefaultPoint(40, 40));
    assertThat(CodecUtils.parsePolygon(null)).isNull();
    assertThat(CodecUtils.parsePolygon("")).isNull();
    assertThat(CodecUtils.parsePolygon("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
        .isEqualTo(polygon);
    assertThat(CodecUtils.parsePolygon("'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'"))
        .isEqualTo(polygon);
    assertThat(
            CodecUtils.parsePolygon(
                "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}"))
        .isEqualTo(polygon);
    assertThat(
            CodecUtils.parsePolygon(
                "AQMAAAABAAAABQAAAAAAAAAAAD5AAAAAAAAAJEAAAAAAAABEQAAAAAAAAERAAAAAAAAANEAAAAAAAABEQAAAAAAAACRAAAAAAAAANEAAAAAAAAA+QAAAAAAAACRA"))
        .isEqualTo(polygon);
    assertThat(
            CodecUtils.parsePolygon(
                "0x010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444"
                    + "000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440"))
        .isEqualTo(polygon);
    assertThatThrownBy(() -> CodecUtils.parsePolygon("not a valid polygon"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid polygon literal");
  }

  @Test
  void should_parse_date_range() throws ParseException {
    DateRange dateRange = DateRange.parse("[* TO 2014-12-01]");
    assertThat(CodecUtils.parseDateRange(null)).isNull();
    assertThat(CodecUtils.parseDateRange("")).isNull();
    assertThat(CodecUtils.parseDateRange("[* TO 2014-12-01]")).isEqualTo(dateRange);
    assertThatThrownBy(() -> CodecUtils.parseDateRange("not a valid date range"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid date range literal");
  }
}
