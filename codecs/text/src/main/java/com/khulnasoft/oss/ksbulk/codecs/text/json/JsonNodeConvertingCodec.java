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
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public abstract class JsonNodeConvertingCodec<T> extends ConvertingCodec<JsonNode, T> {

  private final List<String> nullStrings;

  protected JsonNodeConvertingCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, JsonNode.class);
    this.nullStrings = nullStrings;
  }

  /**
   * Whether the input is null.
   *
   * <p>This method should be used to inspect external inputs that are meant to be converted <em>to
   * textual CQL types only (text, varchar and ascii)</em>.
   *
   * <p>It always considers the empty string as NOT equivalent to NULL, unless the user clearly
   * specifies that the empty string is to be considered as NULL, through the <code>
   * codec.nullStrings</code> setting.
   *
   * <p>Do NOT use this method for non-textual CQL types; use {@link #isNullOrEmpty(JsonNode)}
   * instead.
   */
  protected boolean isNull(JsonNode node) {
    return node == null
        || node.isNull()
        || node.isMissingNode()
        || (node.isValueNode() && nullStrings.contains(node.asText()));
  }

  /**
   * Whether the input is null or empty.
   *
   * <p>This method should be used to inspect external inputs that are meant to be converted <em>to
   * non-textual CQL types only</em>.
   *
   * <p>It always considers the empty string as equivalent to NULL, which is in compliance with the
   * documentation of <code>codec.nullStrings</code>: "Note that, regardless of this setting, KSBulk
   * will always convert empty strings to `null` if the target CQL type is not textual (i.e. not
   * text, varchar or ascii)."
   *
   * <p>Do NOT use this method for textual CQL types; use {@link #isNull(JsonNode)} instead.
   */
  protected boolean isNullOrEmpty(JsonNode node) {
    return isNull(node) || (node.isValueNode() && node.asText().isEmpty());
  }
}
