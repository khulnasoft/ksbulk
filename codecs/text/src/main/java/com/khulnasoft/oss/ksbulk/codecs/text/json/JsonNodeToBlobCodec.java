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

import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.format.binary.BinaryFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToBlobCodec extends JsonNodeConvertingCodec<ByteBuffer> {

  private final BinaryFormat binaryFormat;

  public JsonNodeToBlobCodec(BinaryFormat binaryFormat, List<String> nullStrings) {
    super(TypeCodecs.BLOB, nullStrings);
    this.binaryFormat = binaryFormat;
  }

  @Override
  public ByteBuffer externalToInternal(JsonNode node) {
    // Do not test isNullOrEmpty(), it returns true for empty binary nodes
    if (isNull(node)) {
      return null;
    }
    if (node.isBinary()) {
      try {
        return ByteBuffer.wrap(node.binaryValue());
      } catch (IOException ignored) {
        // try as a string below
      }
    }
    String s = node.asText();
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public JsonNode internalToExternal(ByteBuffer value) {
    return JsonCodecUtils.JSON_NODE_FACTORY.textNode(binaryFormat.format(value));
  }
}
