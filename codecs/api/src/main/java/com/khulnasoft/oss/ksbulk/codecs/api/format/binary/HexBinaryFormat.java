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
package com.khulnasoft.oss.ksbulk.codecs.api.format.binary;

import com.khulnasoft.oss.driver.api.core.data.ByteUtils;
import java.nio.ByteBuffer;

public class HexBinaryFormat implements BinaryFormat {

  public static final HexBinaryFormat INSTANCE = new HexBinaryFormat();

  private HexBinaryFormat() {}

  @Override
  public ByteBuffer parse(String s) {
    if (s == null) {
      return null;
    }
    // DAT-573: consider empty string as empty byte array
    if (s.isEmpty()) {
      return ByteBuffer.allocate(0);
    }
    return ByteUtils.fromHexString(s);
  }

  @Override
  public String format(ByteBuffer bb) {
    return ByteUtils.toHexString(bb);
  }
}
