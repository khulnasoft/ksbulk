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
package com.khulnasoft.oss.ksbulk.codecs.text.string.dse;

import com.khulnasoft.dse.driver.api.core.data.geometry.Point;
import com.khulnasoft.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.khulnasoft.oss.ksbulk.codecs.api.format.geo.GeoFormat;
import com.khulnasoft.oss.ksbulk.codecs.api.util.CodecUtils;
import com.khulnasoft.oss.ksbulk.codecs.text.string.StringConvertingCodec;
import java.util.List;

public class StringToPointCodec extends StringConvertingCodec<Point> {

  private final GeoFormat geoFormat;

  public StringToPointCodec(GeoFormat geoFormat, List<String> nullStrings) {
    super(DseTypeCodecs.POINT, nullStrings);
    this.geoFormat = geoFormat;
  }

  @Override
  public Point externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parsePoint(s);
  }

  @Override
  public String internalToExternal(Point value) {
    return geoFormat.format(value);
  }
}
