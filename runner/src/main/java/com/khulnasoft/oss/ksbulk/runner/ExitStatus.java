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
package com.khulnasoft.oss.ksbulk.runner;

public enum ExitStatus {
  STATUS_OK(0),
  STATUS_COMPLETED_WITH_ERRORS(1),
  STATUS_ABORTED_TOO_MANY_ERRORS(2),
  STATUS_ABORTED_FATAL_ERROR(3),
  STATUS_INTERRUPTED(4),
  STATUS_CRASHED(5),
  ;

  private final int exitCode;

  ExitStatus(int exitCode) {
    this.exitCode = exitCode;
  }

  public int exitCode() {
    return exitCode;
  }
}
