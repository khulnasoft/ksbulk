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
package com.khulnasoft.oss.ksbulk.workflow.commons.schema;

import com.khulnasoft.oss.driver.api.core.CqlIdentifier;
import com.khulnasoft.oss.driver.api.core.cql.ColumnDefinition;
import com.khulnasoft.oss.driver.api.core.cql.ColumnDefinitions;
import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.oss.driver.api.core.type.DataType;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.driver.api.core.type.reflect.GenericType;
import com.khulnasoft.oss.ksbulk.connectors.api.DefaultErrorRecord;
import com.khulnasoft.oss.ksbulk.connectors.api.DefaultRecord;
import com.khulnasoft.oss.ksbulk.connectors.api.Field;
import com.khulnasoft.oss.ksbulk.connectors.api.Record;
import com.khulnasoft.oss.ksbulk.connectors.api.RecordMetadata;
import com.khulnasoft.oss.ksbulk.executor.api.result.ReadResult;
import com.khulnasoft.oss.ksbulk.mapping.CQLWord;
import com.khulnasoft.oss.ksbulk.mapping.Mapping;
import com.khulnasoft.oss.ksbulk.workflow.commons.statement.RangeReadStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import java.util.Set;

public class DefaultReadResultMapper implements ReadResultMapper {

  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean retainRecordSources;

  public DefaultReadResultMapper(
      Mapping mapping, RecordMetadata recordMetadata, boolean retainRecordSources) {
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.retainRecordSources = retainRecordSources;
  }

  @NonNull
  @Override
  public Record map(@NonNull ReadResult result) {
    Object source = retainRecordSources ? result : null;
    URI resource = ((RangeReadStatement) result.getStatement()).getResource();
    try {
      Row row = result.getRow().orElseThrow(IllegalStateException::new);
      ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
      DefaultRecord record = new DefaultRecord(source, resource, result.getPosition());
      for (ColumnDefinition def : columnDefinitions) {
        CQLWord variable = CQLWord.fromInternal(def.getName().asInternal());
        CqlIdentifier name = variable.asIdentifier();
        DataType cqlType = def.getType();
        Set<Field> fields = mapping.variableToFields(variable);
        for (Field field : fields) {
          GenericType<?> fieldType = null;
          try {
            fieldType = recordMetadata.getFieldType(field, cqlType);
            TypeCodec<?> codec = mapping.codec(variable, cqlType, fieldType);
            Object value = row.get(name, codec);
            record.setFieldValue(field, value);
          } catch (Exception e) {
            String msg =
                String.format(
                    "Could not deserialize column %s of type %s as %s",
                    name.asCql(true), cqlType, fieldType);
            throw new IllegalArgumentException(msg, e);
          }
        }
      }
      return record;
    } catch (Exception e) {
      return new DefaultErrorRecord(source, resource, -1, e);
    }
  }
}
