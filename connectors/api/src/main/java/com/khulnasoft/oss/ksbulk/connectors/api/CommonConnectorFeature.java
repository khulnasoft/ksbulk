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
package com.khulnasoft.oss.ksbulk.connectors.api;

public enum CommonConnectorFeature implements ConnectorFeature {

  /**
   * Indicates that the connector handles indexed records (i.e. records whose field identifiers are
   * zero-based indices).
   */
  INDEXED_RECORDS,

  /**
   * Indicates that the connector handles mapped records (i.e. records whose field identifiers are
   * strings).
   */
  MAPPED_RECORDS,

  /**
   * Indicates that the connector supports data sampling for performance optimization purposes. Data
   * size sampling is typically done when using the connector for reading, in which case a sample of
   * the data is read before the actual read operation begins. If the data source cannot be read
   * more than once, then data size sampling should be disallowed. This is notably the case when
   * reading live data streams such as {@linkplain System#in standard input}.
   */
  DATA_SIZE_SAMPLING
}
