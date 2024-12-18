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

import com.khulnasoft.oss.driver.api.core.CqlIdentifier;
import com.khulnasoft.oss.driver.api.core.data.UdtValue;
import com.khulnasoft.oss.driver.api.core.type.UserDefinedType;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodec;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.Iterators;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.Sets;
import com.khulnasoft.oss.ksbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonNodeToUDTCodec extends JsonNodeConvertingCodec<UdtValue> {

  private final Map<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs;
  private final UserDefinedType definition;
  private final ObjectMapper objectMapper;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;

  public JsonNodeToUDTCodec(
      TypeCodec<UdtValue> udtCodec,
      Map<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs,
      ObjectMapper objectMapper,
      List<String> nullStrings,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    super(udtCodec, nullStrings);
    this.fieldCodecs = fieldCodecs;
    definition = (UserDefinedType) udtCodec.getCqlType();
    this.objectMapper = objectMapper;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
  }

  @Override
  public UdtValue externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!(node.isObject() || node.isArray())) {
      throw new IllegalArgumentException(
          "Expecting OBJECT or ARRAY node, got " + node.getNodeType());
    }
    if (node.size() == 0 && allowMissingFields) {
      return definition.newValue();
    }
    UdtValue value = definition.newValue();
    if (node.isObject()) {
      checkJsonObject(node);
      for (CqlIdentifier field : definition.getFieldNames()) {
        String name = field.asInternal();
        if (node.has(name)) {
          ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(field);
          Object o = fieldCodec.externalToInternal(node.get(name));
          value = value.set(field, o, fieldCodec.getInternalJavaType());
        }
      }
    } else {
      checkJsonArray(node);
      // The field iteration order is deterministic
      Iterator<CqlIdentifier> fields = definition.getFieldNames().iterator();
      for (int i = 0; i < node.size() && fields.hasNext(); i++) {
        JsonNode element = node.get(i);
        CqlIdentifier field = fields.next();
        ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(field);
        Object o = fieldCodec.externalToInternal(element);
        value = value.set(i, o, fieldCodec.getInternalJavaType());
      }
    }
    return value;
  }

  private void checkJsonObject(JsonNode node) {
    Set<String> udtFieldNames =
        definition.getFieldNames().stream()
            .map(CqlIdentifier::asInternal)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    Set<String> nodeFieldNames = new LinkedHashSet<>();
    Iterators.addAll(nodeFieldNames, node.fieldNames());
    if (!udtFieldNames.equals(nodeFieldNames)) {
      Set<String> extraneous = Sets.difference(nodeFieldNames, udtFieldNames);
      Set<String> missing = Sets.difference(udtFieldNames, nodeFieldNames);
      boolean hasExtras = !allowExtraFields && !extraneous.isEmpty();
      boolean hasMissing = !allowMissingFields && !missing.isEmpty();
      if (hasMissing && hasExtras) {
        throw JsonSchemaMismatchException.objectHasMissingAndExtraneousFields(extraneous, missing);
      } else if (hasExtras) {
        throw JsonSchemaMismatchException.objectHasExtraneousFields(extraneous);
      } else if (hasMissing) {
        throw JsonSchemaMismatchException.objectHasMissingFields(missing);
      }
    }
  }

  private void checkJsonArray(JsonNode node) {
    int udtSize = definition.getFieldNames().size();
    int nodeSize = node.size();
    if (nodeSize > udtSize && !allowExtraFields) {
      throw JsonSchemaMismatchException.arraySizeGreaterThanUDTSize(udtSize, nodeSize);
    }
    if (nodeSize < udtSize && !allowMissingFields) {
      throw JsonSchemaMismatchException.arraySizeLesserThanUDTSize(udtSize, nodeSize);
    }
  }

  @Override
  public JsonNode internalToExternal(UdtValue value) {
    if (value == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (CqlIdentifier field : definition.getFieldNames()) {
      ConvertingCodec<JsonNode, Object> eltCodec = fieldCodecs.get(field);
      Object o = value.get(field, eltCodec.getInternalJavaType());
      JsonNode node = eltCodec.internalToExternal(o);
      root.set(field.asInternal(), node);
    }
    return root;
  }
}
