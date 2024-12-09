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
package com.khulnasoft.oss.ksbulk.mapping;

import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.ALIASED_SELECTOR;
import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.INTERNAL;
import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.POSITIONAL_ASSIGNMENT;
import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.UNALIASED_SELECTOR;
import static com.khulnasoft.oss.ksbulk.mapping.CQLRenderMode.VARIABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.khulnasoft.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CQLWordTest {

  private static final CQLWord FOO = CQLWord.fromInternal("foo");
  private static final CQLWord FOO_UP = CQLWord.fromInternal("Foo");
  private static final CQLWord FOO_WS = CQLWord.fromInternal("foo bar");
  private static final CQLWord FOO_QUOTE = CQLWord.fromInternal("foo\"bar");
  private static final CQLWord FOO_KW = CQLWord.fromInternal("create");

  @ParameterizedTest
  @MethodSource
  void should_build_from_internal(String internal, CQLWord expected) {
    assertThat(CQLWord.fromInternal(internal)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_build_from_internal() {
    return Lists.newArrayList(
        Arguments.arguments("foo", FOO),
        Arguments.arguments("Foo", FOO_UP),
        Arguments.arguments("foo bar", FOO_WS),
        Arguments.arguments("foo\"bar", FOO_QUOTE),
        Arguments.arguments("create", FOO_KW));
  }

  @ParameterizedTest
  @MethodSource
  void should_build_from_valid_cql(String cql, CQLWord expected) {
    assertThat(CQLWord.fromCql(cql)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_build_from_valid_cql() {
    return Lists.newArrayList(
        Arguments.arguments("foo", FOO),
        Arguments.arguments("Foo", FOO),
        Arguments.arguments("\"Foo\"", FOO_UP),
        Arguments.arguments("\"foo bar\"", FOO_WS),
        Arguments.arguments("\"foo\"\"bar\"", FOO_QUOTE),
        Arguments.arguments("\"create\"", FOO_KW));
  }

  @Test
  void should_fail_to_build_from_valid_cql_if_special_characters() {
    assertThatThrownBy(() -> CQLWord.fromCql("foo bar"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CQL form [foo bar]: needs double quotes");
  }

  @Test
  void should_fail_to_build_from_valid_cql_if_reserved_keyword() {
    assertThatThrownBy(() -> CQLWord.fromCql("Create"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CQL form [Create]: needs double quotes");
  }

  @ParameterizedTest
  @MethodSource
  void should_render_identifier(CQLWord identifier, CQLRenderMode mode, String expected) {
    assertThat(identifier.render(mode)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_render_identifier() {
    return Lists.newArrayList(
        // INTERNAL
        Arguments.arguments(FOO, INTERNAL, "foo"),
        Arguments.arguments(FOO_UP, INTERNAL, "Foo"),
        Arguments.arguments(FOO_WS, INTERNAL, "foo bar"),
        Arguments.arguments(FOO_QUOTE, INTERNAL, "foo\"bar"),
        Arguments.arguments(FOO_KW, INTERNAL, "create"),
        // VARIABLE
        Arguments.arguments(FOO, VARIABLE, "foo"),
        Arguments.arguments(FOO_UP, VARIABLE, "\"Foo\""),
        Arguments.arguments(FOO_WS, VARIABLE, "\"foo bar\""),
        Arguments.arguments(FOO_QUOTE, VARIABLE, "\"foo\"\"bar\""),
        Arguments.arguments(FOO_KW, VARIABLE, "\"create\""),
        // NAMED_ASSIGNMENT
        Arguments.arguments(FOO, NAMED_ASSIGNMENT, ":foo"),
        Arguments.arguments(FOO_UP, NAMED_ASSIGNMENT, ":\"Foo\""),
        Arguments.arguments(FOO_WS, NAMED_ASSIGNMENT, ":\"foo bar\""),
        Arguments.arguments(FOO_QUOTE, NAMED_ASSIGNMENT, ":\"foo\"\"bar\""),
        Arguments.arguments(FOO_KW, NAMED_ASSIGNMENT, ":\"create\""),
        // POSITIONAL_ASSIGNMENT
        Arguments.arguments(FOO, POSITIONAL_ASSIGNMENT, "?"),
        Arguments.arguments(FOO_UP, POSITIONAL_ASSIGNMENT, "?"),
        Arguments.arguments(FOO_WS, POSITIONAL_ASSIGNMENT, "?"),
        Arguments.arguments(FOO_QUOTE, POSITIONAL_ASSIGNMENT, "?"),
        Arguments.arguments(FOO_KW, POSITIONAL_ASSIGNMENT, "?"),
        // UNALIASED_SELECTOR
        Arguments.arguments(FOO, UNALIASED_SELECTOR, "foo"),
        Arguments.arguments(FOO_UP, UNALIASED_SELECTOR, "\"Foo\""),
        Arguments.arguments(FOO_WS, UNALIASED_SELECTOR, "\"foo bar\""),
        Arguments.arguments(FOO_QUOTE, UNALIASED_SELECTOR, "\"foo\"\"bar\""),
        Arguments.arguments(FOO_KW, UNALIASED_SELECTOR, "\"create\""),
        // ALIASED_SELECTOR
        Arguments.arguments(FOO, ALIASED_SELECTOR, "foo"),
        Arguments.arguments(FOO_UP, ALIASED_SELECTOR, "\"Foo\""),
        Arguments.arguments(FOO_WS, ALIASED_SELECTOR, "\"foo bar\""),
        Arguments.arguments(FOO_QUOTE, ALIASED_SELECTOR, "\"foo\"\"bar\""),
        Arguments.arguments(FOO_KW, ALIASED_SELECTOR, "\"create\""));
  }
}
