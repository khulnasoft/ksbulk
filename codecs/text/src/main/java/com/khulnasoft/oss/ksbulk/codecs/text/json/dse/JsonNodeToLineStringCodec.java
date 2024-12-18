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
package com.khulnasoft.oss.ksbulk.codecs.text.json.dse;

import com.khulnasoft.dse.driver.api.core.data.geometry.LineString;
import com.khulnasoft.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.format.geo.GeoFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToLineStringCodec extends JsonNodeToGeometryCodec<LineString> {

  public JsonNodeToLineStringCodec(
      ObjectMapper objectMapper, GeoFormat geoFormat, List<String> nullStrings) {
    super(DseTypeCodecs.LINE_STRING, objectMapper, geoFormat, nullStrings);
  }

  @Override
  protected LineString parseGeometry(@NonNull String s) {
    return CodecUtils.parseLineString(s);
  }

  @Override
  protected LineString parseGeometry(@NonNull byte[] b) {
    return LineString.fromWellKnownBinary(ByteBuffer.wrap(b));
  }
}
