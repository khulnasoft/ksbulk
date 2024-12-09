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
package com.khulnasoft.oss.ksbulk.codecs.text.json.dse;

import com.khulnasoft.dse.driver.api.core.data.geometry.Polygon;
import com.khulnasoft.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.format.geo.GeoFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToPolygonCodec extends JsonNodeToGeometryCodec<Polygon> {

  public JsonNodeToPolygonCodec(
      ObjectMapper objectMapper, GeoFormat geoFormat, List<String> nullStrings) {
    super(DseTypeCodecs.POLYGON, objectMapper, geoFormat, nullStrings);
  }

  @Override
  protected Polygon parseGeometry(@NonNull String s) {
    return CodecUtils.parsePolygon(s);
  }

  @Override
  protected Polygon parseGeometry(@NonNull byte[] b) {
    return Polygon.fromWellKnownBinary(ByteBuffer.wrap(b));
  }
}
