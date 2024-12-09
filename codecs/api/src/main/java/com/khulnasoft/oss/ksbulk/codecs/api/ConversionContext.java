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
package com.khulnasoft.oss.ksbulk.codecs.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConversionContext {

  private final ConcurrentMap<String, Object> attributes = new ConcurrentHashMap<>();

  public void addAttribute(@NonNull String key, Object value) {
    attributes.put(key, value);
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public <T> T getAttribute(@NonNull String key) {
    return (T) attributes.get(key);
  }
}
