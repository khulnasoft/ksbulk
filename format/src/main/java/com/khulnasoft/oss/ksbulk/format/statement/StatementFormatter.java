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
package com.khulnasoft.oss.ksbulk.format.statement;

import com.khulnasoft.oss.driver.api.core.ProtocolVersion;
import com.khulnasoft.oss.driver.api.core.cql.BatchStatement;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component to format instances of {@link Statement}.
 *
 * <p>Its main method is the {@link #format(Statement, StatementFormatVerbosity, ProtocolVersion,
 * CodecRegistry) format} method. It can format statements with different levels of verbosity, which
 * in turn determines which elements to include in the formatted string (query string, bound values,
 * custom payloads, inner statements for batches, etc.).
 *
 * <p>{@code StatementFormatter} also provides safeguards to prevent overwhelming your logs with
 * large query strings, queries with considerable amounts of parameters, batch queries with several
 * inner statements, etc.
 *
 * <p>{@code StatementFormatter} is fully customizable. To build a customized formatter, use the
 * {@link #builder()} method as follows:
 *
 * <pre>{@code
 * StatementFormatter formatter = StatementFormatter.builder()
 *      // customize formatter settings
 *      .withMaxBoundValues(42)
 *      .build()
 * }</pre>
 *
 * It is also possible to take full control over how a specific kind of statement should be
 * formatted. To do this, simply implement a {@link StatementPrinter StatementPrinter}:
 *
 * <pre>{@code
 * class MyCustomStatement extends StatementWrapper {...}
 *
 * class MyCustomStatementPrinter implements StatementPrinter<MyCustomStatement> {
 *      public Class<MyCustomStatement> getSupportedStatementClass() {
 *           return MyCustomStatement.class;
 *      }
 *      public void print(CustomStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
 *           // go crazy
 *      }
 * }
 * }</pre>
 *
 * Then add it to the resulting formatter as follows:
 *
 * <pre>{@code
 * StatementFormatter formatter = StatementFormatter.builder()
 *      .addStatementPrinter(new MyCustomStatementPrinter())
 *      .build()
 * }</pre>
 *
 * The driver ships with a set of printers that handle all the built-in statement types. It is
 * possible to completely override them by simply providing a {@code StatementPrinter} for the type
 * of {@code Statement} that you wish to override, using the same method outlined above.
 *
 * <p>Instances of this class are thread-safe.
 */
public final class StatementFormatter {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatementFormatter.class);

  /**
   * Creates a new {@link StatementFormatter.Builder} instance.
   *
   * @return the new StatementFormatter builder.
   */
  public static StatementFormatter.Builder builder() {
    return new StatementFormatter.Builder();
  }

  /** Helper class to build {@link StatementFormatter} instances with a fluent API. */
  public static class Builder {

    static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;
    static final int DEFAULT_MAX_BOUND_VALUE_LENGTH = 50;
    static final int DEFAULT_MAX_BOUND_VALUES = 10;
    static final int DEFAULT_MAX_INNER_STATEMENTS = 5;
    static final int DEFAULT_MAX_OUTGOING_PAYLOAD_ENTRIES = 10;
    static final int DEFAULT_MAX_OUTGOING_PAYLOAD_VALUE_LENGTH = 50;

    private int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;
    private int maxBoundValueLength = DEFAULT_MAX_BOUND_VALUE_LENGTH;
    private int maxBoundValues = DEFAULT_MAX_BOUND_VALUES;
    private int maxInnerStatements = DEFAULT_MAX_INNER_STATEMENTS;
    private int maxOutgoingPayloadEntries = DEFAULT_MAX_OUTGOING_PAYLOAD_ENTRIES;
    private int maxOutgoingPayloadValueLength = DEFAULT_MAX_OUTGOING_PAYLOAD_VALUE_LENGTH;

    private final List<StatementPrinter<?>> printers = new ArrayList<>();

    private Builder() {}

    /**
     * Adds the given {@link StatementPrinter}s to the list of available statement printers.
     *
     * <p>Note that built-in printers are always registered by default and they handle all the
     * driver built-in {@link Statement} subclasses. Calling this method is only useful if you need
     * to handle a special subclass of {@link Statement}; otherwise, the built-in printers should be
     * enough.
     *
     * @param printers The {@link StatementPrinter}s to add.
     * @return this (for method chaining).
     */
    public Builder addStatementPrinters(StatementPrinter<?>... printers) {
      this.printers.addAll(Arrays.asList(printers));
      return this;
    }

    /**
     * Sets the maximum length allowed for query strings. The default is {@value
     * DEFAULT_MAX_QUERY_STRING_LENGTH}.
     *
     * <p>If the query string length exceeds this threshold, printers should truncate it.
     *
     * @param maxQueryStringLength the maximum length allowed for query strings.
     * @throws IllegalArgumentException if the value is not &gt; 0, or {@value
     *     StatementFormatterLimits#UNLIMITED} (unlimited).
     * @return this (for method chaining).
     */
    public Builder withMaxQueryStringLength(int maxQueryStringLength) {
      if (maxQueryStringLength <= 0 && maxQueryStringLength != StatementFormatterLimits.UNLIMITED)
        throw new IllegalArgumentException(
            "Invalid maxQueryStringLength, should be > 0 or -1 (unlimited), got "
                + maxQueryStringLength);
      this.maxQueryStringLength = maxQueryStringLength;
      return this;
    }

    /**
     * Sets the maximum length, in numbers of printed characters, allowed for a single bound value.
     * The default is {@value DEFAULT_MAX_BOUND_VALUE_LENGTH}.
     *
     * <p>If the bound value length exceeds this threshold, printers should truncate it.
     *
     * @param maxBoundValueLength the maximum length, in numbers of printed characters, allowed for
     *     a single bound value.
     * @throws IllegalArgumentException if the value is not &gt; 0, or {@value
     *     StatementFormatterLimits#UNLIMITED} (unlimited).
     * @return this (for method chaining).
     */
    public Builder withMaxBoundValueLength(int maxBoundValueLength) {
      if (maxBoundValueLength <= 0 && maxBoundValueLength != StatementFormatterLimits.UNLIMITED)
        throw new IllegalArgumentException(
            "Invalid maxBoundValueLength, should be > 0 or -1 (unlimited), got "
                + maxBoundValueLength);
      this.maxBoundValueLength = maxBoundValueLength;
      return this;
    }

    /**
     * Sets the maximum number of printed bound values. The default is {@value
     * DEFAULT_MAX_BOUND_VALUES}.
     *
     * <p>If the number of bound values exceeds this threshold, printers should truncate it.
     *
     * @param maxBoundValues the maximum number of printed bound values.
     * @throws IllegalArgumentException if the value is not &gt; 0, or {@value
     *     StatementFormatterLimits#UNLIMITED} (unlimited).
     * @return this (for method chaining).
     */
    public Builder withMaxBoundValues(int maxBoundValues) {
      if (maxBoundValues <= 0 && maxBoundValues != StatementFormatterLimits.UNLIMITED)
        throw new IllegalArgumentException(
            "Invalid maxBoundValues, should be > 0 or -1 (unlimited), got " + maxBoundValues);
      this.maxBoundValues = maxBoundValues;
      return this;
    }

    /**
     * Sets the maximum number of printed inner statements of a {@link BatchStatement}. The default
     * is {@value DEFAULT_MAX_INNER_STATEMENTS}. Setting this value to zero should disable the
     * printing of inner statements.
     *
     * <p>If the number of inner statements exceeds this threshold, printers should truncate it.
     *
     * <p>If the statement to format is not a batch statement, then this withting should be ignored.
     *
     * @param maxInnerStatements the maximum number of printed inner statements of a {@link
     *     BatchStatement}.
     * @throws IllegalArgumentException if the value is not &gt;= 0, or {@value
     *     StatementFormatterLimits#UNLIMITED} (unlimited).
     * @return this (for method chaining).
     */
    public Builder withMaxInnerStatements(int maxInnerStatements) {
      if (maxInnerStatements < 0 && maxInnerStatements != StatementFormatterLimits.UNLIMITED)
        throw new IllegalArgumentException(
            "Invalid maxInnerStatements, should be >= 0 or -1 (unlimited), got "
                + maxInnerStatements);
      this.maxInnerStatements = maxInnerStatements;
      return this;
    }

    /**
     * Builds the {@link StatementFormatter} instance.
     *
     * @return the {@link StatementFormatter} instance.
     */
    @SuppressWarnings("unchecked")
    public StatementFormatter build() {
      StatementPrinterRegistry registry = new StatementPrinterRegistry();
      for (StatementPrinter<?> printer : printers) {
        registry.register(printer);
      }
      StatementFormatterLimits limits =
          new StatementFormatterLimits(
              maxQueryStringLength,
              maxBoundValueLength,
              maxBoundValues,
              maxInnerStatements,
              maxOutgoingPayloadEntries,
              maxOutgoingPayloadValueLength);
      return new StatementFormatter(registry, limits);
    }
  }

  private final StatementPrinterRegistry printerRegistry;
  private final StatementFormatterLimits limits;

  private StatementFormatter(
      StatementPrinterRegistry printerRegistry, StatementFormatterLimits limits) {
    this.printerRegistry = printerRegistry;
    this.limits = limits;
  }

  /**
   * Formats the given {@link Statement statement}.
   *
   * @param statement The statement to format; must not be {@code null}.
   * @param verbosity The verbosity to use.
   * @param protocolVersion The protocol version in use.
   * @param codecRegistry The codec registry in use.
   * @return The statement as a formatted string.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public String format(
      Statement<?> statement,
      StatementFormatVerbosity verbosity,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry) {
    try {
      StatementPrinter printer = printerRegistry.findPrinter(statement.getClass());
      assert printer != null : "Could not find printer for statement class " + statement.getClass();
      StatementWriter out =
          new StatementWriter(
              new StringBuilder(), 0, printerRegistry, limits, protocolVersion, codecRegistry);
      printer.print(statement, out, verbosity);
      return out.toString();
    } catch (Exception e) {
      try {
        LOGGER.error("Could not format statement: " + statement, e);
        return statement.toString();
      } catch (Exception e1) {
        LOGGER.error("statement.toString() failed", e1);
        return "statement[?]";
      }
    }
  }
}
