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
package com.khulnasoft.oss.ksbulk.workflow.commons.statement;

import com.khulnasoft.oss.driver.api.core.cql.BoundStatement;
import com.khulnasoft.oss.ksbulk.connectors.api.Record;
import com.khulnasoft.oss.ksbulk.format.statement.BoundStatementPrinter;
import com.khulnasoft.oss.ksbulk.format.statement.StatementFormatVerbosity;
import com.khulnasoft.oss.ksbulk.format.statement.StatementWriter;
import com.khulnasoft.oss.ksbulk.workflow.commons.log.LogManagerUtils;

public class MappedBoundStatementPrinter extends BoundStatementPrinter {

  @Override
  public Class<MappedBoundStatement> getSupportedStatementClass() {
    return MappedBoundStatement.class;
  }

  @Override
  protected void printHeader(
      BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    super.printHeader(statement, out, verbosity);
    if (verbosity.compareTo(StatementFormatVerbosity.EXTENDED) >= 0) {
      MappedStatement mappedStatement = (MappedStatement) statement;
      appendRecord(mappedStatement, out);
    }
  }

  private void appendRecord(MappedStatement statement, StatementWriter out) {
    Record record = statement.getRecord();
    out.newLine()
        .indent()
        .append("Resource: ")
        .append(String.valueOf(record.getResource()))
        .newLine()
        .indent()
        .append("Position: ")
        .append(String.valueOf(record.getPosition()));
    if (record.getSource() != null) {
      out.newLine().indent().append("Source: ").append(LogManagerUtils.formatSource(record));
    }
  }
}
