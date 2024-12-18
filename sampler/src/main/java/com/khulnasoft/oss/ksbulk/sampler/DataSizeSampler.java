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
package com.khulnasoft.oss.ksbulk.sampler;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.detach.AttachmentPoint;

public class DataSizeSampler {

  public static Histogram sampleWrites(
      AttachmentPoint attachmentPoint, Iterable<Statement<?>> statements) {
    Histogram histogram = new Histogram(new UniformReservoir());
    for (Statement<?> statement : statements) {
      long dataSize =
          DataSizes.getDataSize(
              statement, attachmentPoint.getProtocolVersion(), attachmentPoint.getCodecRegistry());
      histogram.update(dataSize);
    }
    return histogram;
  }

  public static Histogram sampleReads(Iterable<Row> rows) {
    Histogram histogram = new Histogram(new UniformReservoir());
    for (Row row : rows) {
      long dataSize = DataSizes.getDataSize(row);
      histogram.update(dataSize);
    }
    return histogram;
  }
}
