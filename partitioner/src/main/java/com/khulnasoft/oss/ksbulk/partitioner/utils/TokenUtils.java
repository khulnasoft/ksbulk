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
package com.khulnasoft.oss.ksbulk.partitioner.utils;

import com.khulnasoft.oss.driver.api.core.metadata.token.Token;
import com.khulnasoft.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.khulnasoft.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.khulnasoft.oss.driver.internal.core.metadata.token.RandomToken;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TokenUtils {

  @NonNull
  public static Object getTokenValue(@NonNull Token token) {
    Object value;
    if (token instanceof Murmur3Token) {
      value = ((Murmur3Token) token).getValue();
    } else if (token instanceof RandomToken) {
      value = ((RandomToken) token).getValue();
    } else if (token instanceof ByteOrderedToken) {
      value = ((ByteOrderedToken) token).getValue();
    } else {
      throw new IllegalArgumentException("Unknown token type: " + token.getClass());
    }
    return value;
  }
}
