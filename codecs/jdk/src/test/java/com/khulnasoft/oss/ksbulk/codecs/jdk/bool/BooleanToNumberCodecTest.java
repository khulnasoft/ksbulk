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
package com.khulnasoft.oss.ksbulk.codecs.jdk.bool;

import static com.khulnasoft.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static java.math.BigDecimal.ONE;

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import org.junit.jupiter.api.Test;

class BooleanToNumberCodecTest {

  @Test
  void should_convert_from_valid_external() {

    assertThat(new BooleanToNumberCodec<>(TypeCodecs.TINYINT, newArrayList(ONE, ONE.negate())))
        .convertsFromExternal(true)
        .toInternal((byte) 1)
        .convertsFromExternal(false)
        .toInternal((byte) -1)
        .convertsFromInternal((byte) 1)
        .toExternal(true)
        .convertsFromInternal((byte) -1)
        .toExternal(false)
        .cannotConvertFromInternal((byte) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
