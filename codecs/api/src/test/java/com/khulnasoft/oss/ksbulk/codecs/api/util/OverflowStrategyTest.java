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

import static com.khulnasoft.oss.ksbulk.codecs.api.util.OverflowStrategy.REJECT;
import static com.khulnasoft.oss.ksbulk.codecs.api.util.OverflowStrategy.TRUNCATE;
import static java.math.RoundingMode.UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

@SuppressWarnings("FloatingPointLiteralPrecision")
class OverflowStrategyTest {

  private final ArithmeticException e = new ArithmeticException("not really");

  @Test
  void should_apply_reject_strategy() {
    assertThatThrownBy(() -> REJECT.apply(null, e, null, null)).isSameAs(e);
  }

  @Test
  void should_apply_truncate_strategy() {
    assertThat(TRUNCATE.apply(1000, e, Byte.class, null)).isEqualTo(Byte.MAX_VALUE);
    assertThat(TRUNCATE.apply(-1000, e, Byte.class, null)).isEqualTo(Byte.MIN_VALUE);
    assertThat(TRUNCATE.apply(0.123, e, Byte.class, UP)).isEqualTo((byte) 1);
    assertThat(TRUNCATE.apply(0d, e, Byte.class, null)).isEqualTo((byte) 0);

    assertThat(TRUNCATE.apply(1_000_000, e, Short.class, null)).isEqualTo(Short.MAX_VALUE);
    assertThat(TRUNCATE.apply(-1_000_000, e, Short.class, null)).isEqualTo(Short.MIN_VALUE);
    assertThat(TRUNCATE.apply(0.123, e, Short.class, UP)).isEqualTo((short) 1);
    assertThat(TRUNCATE.apply(0d, e, Short.class, null)).isEqualTo((short) 0);

    assertThat(TRUNCATE.apply(1_000_000_000_000L, e, Integer.class, null))
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(TRUNCATE.apply(-1_000_000_000_000L, e, Integer.class, null))
        .isEqualTo(Integer.MIN_VALUE);
    assertThat(TRUNCATE.apply(0.123, e, Integer.class, UP)).isEqualTo(1);
    assertThat(TRUNCATE.apply(0d, e, Integer.class, null)).isEqualTo(0);

    assertThat(
            TRUNCATE.apply(
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), e, Long.class, null))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(
            TRUNCATE.apply(
                BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), e, Long.class, null))
        .isEqualTo(Long.MIN_VALUE);
    assertThat(TRUNCATE.apply(0.123, e, Long.class, UP)).isEqualTo(1L);
    assertThat(TRUNCATE.apply(0d, e, Long.class, null)).isEqualTo(0L);

    assertThat(TRUNCATE.apply(0.123, e, BigInteger.class, UP)).isEqualTo(BigInteger.ONE);
    assertThat(TRUNCATE.apply(0d, e, BigInteger.class, null)).isEqualTo(BigInteger.ZERO);

    assertThat(TRUNCATE.apply(Double.MAX_VALUE, e, Float.class, null)).isEqualTo(Float.MAX_VALUE);
    assertThat(TRUNCATE.apply(Double.MIN_VALUE, e, Float.class, null)).isEqualTo(Float.MIN_VALUE);
    assertThat(TRUNCATE.apply(0.123456789d, e, Float.class, null)).isEqualTo(0.123456789f);

    assertThat(
            TRUNCATE.apply(
                BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE), e, Double.class, null))
        .isEqualTo(Double.MAX_VALUE);
    assertThat(
            TRUNCATE.apply(
                BigDecimal.valueOf(Double.MIN_VALUE).subtract(BigDecimal.ONE),
                e,
                Double.class,
                null))
        .isEqualTo(Double.MIN_VALUE);
    assertThat(TRUNCATE.apply(new BigDecimal("0.123456789"), e, Double.class, null))
        .isEqualTo(0.123456789d);

    assertThat(TRUNCATE.apply(BigDecimal.ZERO, e, BigDecimal.class, null))
        .isEqualTo(BigDecimal.ZERO);

    assertThatThrownBy(() -> TRUNCATE.apply(0, e, AtomicInteger.class, null)).isSameAs(e);
  }
}
