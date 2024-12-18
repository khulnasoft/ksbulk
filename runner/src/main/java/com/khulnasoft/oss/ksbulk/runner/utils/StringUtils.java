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
package com.khulnasoft.oss.ksbulk.runner.utils;

import java.util.Collections;

public class StringUtils {

  /**
   * If the given string is surrounded by square brackets, return it intact, otherwise trim it and
   * surround it with square brackets.
   *
   * @param value The string to check.
   * @return A string surrounded by square brackets.
   */
  public static String ensureBrackets(String value) {
    value = value.trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      return value;
    }
    return "[" + value + "]";
  }

  /**
   * If the given string is surrounded by curly braces, return it intact, otherwise trim it and
   * surround it with curly braces.
   *
   * @param value The string to check.
   * @return A string surrounded by curly braces.
   */
  public static String ensureBraces(String value) {
    value = value.trim();
    if (value.startsWith("{") && value.endsWith("}")) {
      return value;
    }
    return "{" + value + "}";
  }

  /**
   * If the given string is surrounded by double-quotes, return it intact, otherwise trim it and
   * surround it with double-quotes.
   *
   * @param value The string to check.
   * @return A string surrounded by double-quotes.
   */
  public static String ensureQuoted(String value) {
    value = value.trim();
    if (value.startsWith("\"") && value.endsWith("\"")) {
      return value;
    }
    return "\"" + value + "\"";
  }

  /**
   * Returns a new string consisting of n copies of the given string.
   *
   * @param s String to copy.
   * @param count Number of times to copy it.
   * @return a new string consisting of n copies of the given string.
   */
  public static String nCopies(String s, int count) {
    return String.join("", Collections.nCopies(count, s));
  }
}
