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

import static com.khulnasoft.oss.ksbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.khulnasoft.oss.ksbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_TYPE;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;

import com.khulnasoft.oss.driver.api.core.type.DataTypes;
import com.khulnasoft.oss.ksbulk.codecs.api.ConversionContext;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodecFactory;
import com.khulnasoft.oss.ksbulk.codecs.text.TextConversionContext;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToLongCodecTest {

  private JsonNodeToLongCodec codec;

  @BeforeEach
  void setUp() {
    ConversionContext context =
        new TextConversionContext().setNullStrings("NULL").setFormatNumbers(true);
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (JsonNodeToLongCodec)
            codecFactory.<JsonNode, Long>createConvertingCodec(
                DataTypes.BIGINT, JSON_NODE_TYPE, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0L))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("9223372036854775807"))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-9223372036854775808"))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("9,223,372,036,854,775,807"))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-9,223,372,036,854,775,808"))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .toInternal(946684800000L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(1L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0L)
        .toExternal(JSON_NODE_FACTORY.numberNode(0L))
        .convertsFromInternal(Long.MAX_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .convertsFromInternal(Long.MIN_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid long"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("9223372036854775808"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("-9223372036854775809"));
  }
}
