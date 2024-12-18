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
package com.khulnasoft.oss.ksbulk.runner.cli;

import com.khulnasoft.oss.driver.shaded.guava.common.collect.BiMap;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.Lists;
import com.khulnasoft.oss.ksbulk.config.ConfigUtils;
import com.khulnasoft.oss.ksbulk.config.shortcuts.ShortcutsFactory;
import com.khulnasoft.oss.ksbulk.io.IOUtils;
import com.khulnasoft.oss.ksbulk.runner.utils.StringUtils;
import com.khulnasoft.oss.ksbulk.workflow.api.WorkflowProvider;
import com.khulnasoft.oss.ksbulk.workflow.api.config.ConfigPostProcessor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigException.Missing;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;

public class CommandLineParser {

  private static final ConfigParseOptions COMMAND_LINE_ARGUMENTS =
      ConfigParseOptions.defaults()
          .setOriginDescription("command line argument")
          .setSyntax(ConfigSyntax.CONF);

  private static final Set<String> COMMON_DRIVER_LIST_TYPE_SETTINGS =
      ImmutableSet.of(
          "khulnasoft-java-driver.basic.contact-points",
          "khulnasoft-java-driver.advanced.ssl-engine-factory.cipher-suites",
          "khulnasoft-java-driver.advanced.metrics.session.enabled",
          "khulnasoft-java-driver.advanced.metrics.node.enabled",
          "khulnasoft-java-driver.advanced.metadata.schema.refreshed-keyspaces");

  private final List<String> args;

  public CommandLineParser(String... args) {
    this.args = Lists.newArrayList(args);
  }

  public ParsedCommandLine parse()
      throws ParseException, GlobalHelpRequestException, SectionHelpRequestException,
          VersionRequestException {

    if (args.isEmpty()) {
      throw new GlobalHelpRequestException();
    }

    checkVersionRequest();

    AnsiConfigurator.configureAnsi(args);

    Path applicationPath = resolveApplicationPath();
    String connectorName = resolveConnectorName();

    checkHelpRequest(connectorName);

    ConfigFactory.invalidateCaches();
    Config referenceConfig = ConfigUtils.createReferenceConfig();
    Config applicationConfig = ConfigUtils.createApplicationConfig(applicationPath);

    BiMap<String, String> shortcuts =
        ShortcutsFactory.createShortcutsMap(
            referenceConfig, connectorName == null ? "csv" : connectorName);

    WorkflowProvider workflowProvider = resolveWorkflowType();

    Config finalConfig = parseArguments(referenceConfig, applicationConfig, shortcuts).resolve();
    finalConfig = postProcess(finalConfig);

    return new ParsedCommandLine(workflowProvider, finalConfig);
  }

  /**
   * Attempts to identify an alternate location for the application configuration file, which can be
   * determined with the "-f" switch (e.g. -f "/path/to/file"). Returns null if no such path could
   * be found. This needs to be done before parsing any other command line argument.
   *
   * @return The alternate location of the application configuration file, or null if none found.
   * @throws ParseException If the parsing of application configuration file path fails.
   */
  @Nullable
  private Path resolveApplicationPath() throws ParseException {
    // Walk through args, looking for a -f option + value.
    Iterator<String> iterator = args.iterator();
    while (iterator.hasNext()) {
      String arg = iterator.next();
      if (arg.equals("-f")) {
        if (iterator.hasNext()) {
          // remove this argument as we don't need it anymore.
          iterator.remove();
          Path applicationPath = ConfigUtils.resolvePath(iterator.next());
          iterator.remove();
          IOUtils.assertAccessibleFile(applicationPath, "Application file");
          return applicationPath;
        } else {
          throw new ParseException("Expecting application configuration path after -f");
        }
      }
    }
    return null;
  }

  /**
   * Attempts to identify the connector name. Returns null if no connector name could be found. This
   * needs to be done before parsing any other command line argument.
   *
   * @return The connector name, or null if none found.
   * @throws ParseException If the parsing of the connector name fails.
   */
  @Nullable
  private String resolveConnectorName() throws ParseException {
    // Walk through args, looking for a -c / --connector.name option + value.
    Iterator<String> iterator = args.iterator();
    while (iterator.hasNext()) {
      String arg = iterator.next();
      if (arg.equals("-c")
          || arg.equals("--connector.name")
          || arg.equals("--ksbulk.connector.name")) {
        if (iterator.hasNext()) {
          return iterator.next();
        } else {
          throw new ParseException("Expecting connector name after " + arg);
        }
      }
    }
    return null;
  }

  private void checkVersionRequest() throws VersionRequestException {
    if (!args.isEmpty()) {
      if ("-v".equals(args.get(0)) || "--version".equals(args.get(0))) {
        // ignore the rest of the command line
        throw new VersionRequestException();
      }
    }
  }

  private void checkHelpRequest(@Nullable String connectorName)
      throws GlobalHelpRequestException, SectionHelpRequestException {
    ListIterator<String> it = args.listIterator();
    boolean firstArg = true;
    while (skipConnector(it)) {
      String arg = it.next();
      boolean helpAsCommand = "help".equals(arg) && firstArg;
      boolean helpAsLongOption = "--help".equals(arg);
      if (helpAsCommand || helpAsLongOption) {
        if (skipConnector(it)) {
          // consider the next remaining arg as the section to show help for,
          // and ignore the rest of the command line
          String sectionName = sanitizeSectionName(it.next());
          throw new SectionHelpRequestException(sectionName, connectorName);
        }
        // no more args: the help request was for the global help
        throw new GlobalHelpRequestException(connectorName);
      }
      firstArg = false;
    }
  }

  /**
   * Inspects the iterator and skips the connector name, if it's the next argument. Returns true if
   * the iterator has a next element after the inspection operation.
   *
   * <p>This is intended for use when checking for help requests only. This inspection is required
   * because the connector name is the only argument that can also appear in a help request; and to
   * complicate things, it can appear interleaved between the help command and the help section
   * name, e.g. "help -c json codec".
   */
  private boolean skipConnector(ListIterator<String> it) {
    if (it.hasNext()) {
      String arg = it.next();
      if (arg.equals("-c")
          || arg.equals("--connector.name")
          || arg.equals("--ksbulk.connector.name")) {
        it.next(); // already validated that there is at least one more arg
      } else {
        it.previous();
      }
    }
    return it.hasNext();
  }

  @NonNull
  private String sanitizeSectionName(@NonNull String sectionName) {
    if (sectionName.startsWith("driver")) {
      // driver -> khulnasoft-java-driver
      sectionName = sectionName.replaceFirst("driver", "khulnasoft-java-driver");
    } else if (!sectionName.startsWith("khulnasoft-java-driver")
        && !sectionName.startsWith("ksbulk")) {
      // foo -> ksbulk.foo
      sectionName = "ksbulk." + sectionName;
    }
    return sectionName;
  }

  @NonNull
  private WorkflowProvider resolveWorkflowType() throws ParseException {
    if (!args.isEmpty()) {
      String workflowTypeStr = args.remove(0);
      ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class);
      for (WorkflowProvider workflowProvider : loader) {
        if (workflowProvider.getTitle().equals(workflowTypeStr.toLowerCase())) {
          return workflowProvider;
        }
      }
    }
    throw new ParseException(
        String.format(
            "First argument must be subcommand \"%s\", or \"help\"", getAvailableCommands()));
  }

  @NonNull
  private Config postProcess(Config config) {
    ServiceLoader<ConfigPostProcessor> loader = ServiceLoader.load(ConfigPostProcessor.class);
    for (ConfigPostProcessor processor : loader) {
      config = processor.postProcess(config);
    }
    return config;
  }

  @NonNull
  private String getAvailableCommands() {
    ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class);
    List<String> commands = new ArrayList<>();
    for (WorkflowProvider workflowProvider : loader) {
      commands.add(workflowProvider.getTitle().toLowerCase());
    }
    return String.join("\", \"", commands);
  }

  @NonNull
  private Config parseArguments(
      Config referenceConfig, Config currentConfig, Map<String, String> shortcuts)
      throws ParseException {
    Iterator<String> iterator = args.iterator();
    int argumentIndex = 0;
    while (iterator.hasNext()) {
      String arg = iterator.next();
      argumentIndex++;
      try {
        String optionName = null;
        String optionValue = null;
        boolean wasLongOptionWithValue = false;
        try {
          if (arg.startsWith("--")) {
            // First, try long option in the form : --key=value; this is not the recommended
            // way to input long options for ksbulk, but it has been supported since the
            // beginning. Parse the option using TypeSafe Config since it has to be valid.
            // We will sanitize the parsed value after.
            Config config = ConfigFactory.parseString(arg.substring(2));
            Entry<String, ConfigValue> entry = config.entrySet().iterator().next();
            optionName = longNameToOptionName(entry.getKey(), referenceConfig);
            optionValue = entry.getValue().unwrapped().toString();
            wasLongOptionWithValue = true;
          }
        } catch (ConfigException.Parse ignored) {
          // could not parse option, assume it wasn't a long option with value and fall through
        }
        if (!wasLongOptionWithValue) {
          // Now try long or short option :
          // --long.option.name value
          // -shortcut value
          optionName = parseLongOrShortOptionName(arg, shortcuts, referenceConfig);
          if (iterator.hasNext()) {
            // there must be at least one remaining argument containing the option value
            optionValue = iterator.next();
            argumentIndex++;
          } else {
            throw new ParseException("Expecting value after: " + arg);
          }
        }
        ConfigValueType optionType = getOptionType(optionName, referenceConfig);
        if (optionType == ConfigValueType.OBJECT) {
          // the argument we are about to parse is a map (object), we want it to replace the
          // default value, not to be merged with it, so remove the path from the application
          // config.
          currentConfig = currentConfig.withoutPath(optionName);
        }
        String sanitizedOptionValue = sanitizeValue(optionValue, optionType);
        // Pre-validate that optionName and sanitizedOptionValue are syntactically valid, throw
        // immediately if something is not right to get a clear error message specifying the
        // incriminated argument. This won't validate if the value is semantically correct (i.e.
        // if value is of expected type): semantic validation is expected to occur later on in any
        // of the *Settings classes.
        try {
          // A hack to make the origin description contain the label "Command line argument"
          // followed by the line number, which corresponds to each argument index.
          String paddingBeforeOptionName =
              StringUtils.nCopies("\n", argumentIndex - (wasLongOptionWithValue ? 0 : 1));
          String paddingBeforeOptionValue = wasLongOptionWithValue ? "" : "\n";
          currentConfig =
              ConfigFactory.parseString(
                      paddingBeforeOptionName
                          + optionName
                          + '='
                          + paddingBeforeOptionValue
                          + sanitizedOptionValue,
                      COMMAND_LINE_ARGUMENTS)
                  .withFallback(currentConfig);
        } catch (ConfigException e) {
          IllegalArgumentException cause = ConfigUtils.convertConfigException(e, "");
          if (optionType == null) {
            throw new ParseException(
                String.format("Invalid value for %s: '%s'", optionName, optionValue), cause);
          } else {
            throw new ParseException(
                String.format(
                    "Invalid value for %s, expecting %s, got: '%s'",
                    optionName, optionType, optionValue),
                cause);
          }
        }
      } catch (RuntimeException e) {
        throw new ParseException("Could not parse argument: " + arg, e);
      }
    }
    return currentConfig;
  }

  /**
   * Returns the type of the given configuration path. Used mostly to sanitize argument values of
   * type STRING, LIST and MAP, and also to create better error messages.
   *
   * <p>This method is mostly intended for KSBulk paths, whose types are always clearly documented
   * (either by the default value, or with a @type meta-annotation). However it also honors a few
   * driver paths that are known to be of type LIST, as a convenience for the user.
   *
   * @param path The path to inspect.
   * @param referenceConfig The config where to look for the path.
   * @return The type of the given configuration path, or null if not found.
   */
  @Nullable
  private ConfigValueType getOptionType(String path, Config referenceConfig) {
    // Special-case common driver arguments of type LIST, since we usually cannot infer the right
    // type of most driver options (they are commented out in the reference file).
    if (COMMON_DRIVER_LIST_TYPE_SETTINGS.contains(path)) {
      return ConfigValueType.LIST;
    }
    ConfigValueType type = null;
    try {
      type = ConfigUtils.getValueType(referenceConfig, path);
    } catch (Missing ignored) {
    }
    return type;
  }

  /**
   * Parses an option without value. It parses both long options (introduced by a double dash) and
   * short options or shortcuts (introduced by a single dash).
   *
   * @param arg The argument to parse.
   * @param shortcuts The shortcuts map, used to resolve shortcuts.
   * @param referenceConfig The reference config, use to disambiguate driver options.
   * @return The parsed option name, always a valid configuration path.
   * @throws ParseException If the argument could not be parsed.
   */
  @NonNull
  private String parseLongOrShortOptionName(
      String arg, Map<String, String> shortcuts, Config referenceConfig) throws ParseException {
    String optionName;
    if (arg.startsWith("--") && arg.length() > 2) {
      // arg is a long option name
      String longName = arg.substring(2);
      optionName = longNameToOptionName(longName, referenceConfig);
    } else if (arg.startsWith("-") && arg.length() > 1) {
      // arg is a shortcut name
      String shortcut = arg.substring(1);
      optionName = shortcutToOptionName(arg, shortcut, shortcuts);
    } else {
      throw new ParseException(String.format("Expecting long or short option, got: '%s'", arg));
    }
    return optionName;
  }

  /**
   * Converts the long option name as input by the user to a valid configuration path.
   *
   * <p>On the command line, long options are allowed to bear a relaxed syntax:
   *
   * <ul>
   *   <li>The prefix <code>ksbulk.</code> can always be omitted;
   *   <li>The prefix <code>khulnasoft-java-driver.</code> can be abbreviated to <code>driver.</code>;
   *   <li>The prefix <code>driver.</code> being ambiguous – it can map to <code>ksbulk.driver.
   *       </code> or <code>khulnasoft-java-driver.</code> – this method disambiguates it.
   * </ul>
   *
   * @param longName The long path, as input by the user.
   * @param referenceConfig The reference config, used to disambiguate driver options.
   * @return A valid configuration path.
   */
  @NonNull
  private String longNameToOptionName(String longName, Config referenceConfig) {
    String optionName;
    if (longName.startsWith("ksbulk.")) {
      // already a fully qualified KSBulk setting name
      optionName = longName;
    } else if (longName.startsWith("khulnasoft-java-driver.")) {
      // already a fully qualified Driver setting name
      optionName = longName;
    } else if (longName.startsWith("driver.")) {
      if (isDeprecatedDriverSetting(referenceConfig, longName)) {
        // deprecated KSBulk driver setting (abbreviated without the prefix "ksbulk.")
        optionName = "ksbulk." + longName;
      } else {
        // abbreviated Driver setting name (with prefix "driver." instead of
        // "khulnasoft-java-driver.")
        optionName = longName.replaceFirst("driver\\.", "khulnasoft-java-driver.");
      }
    } else {
      // abbreviated KSBulk setting (without the prefix "ksbulk.")
      optionName = "ksbulk." + longName;
    }
    return optionName;
  }

  @NonNull
  private String shortcutToOptionName(String arg, String shortcut, Map<String, String> shortcuts)
      throws ParseException {
    if (!shortcuts.containsKey(shortcut)) {
      throw new ParseException("Unknown short option: " + arg);
    }
    return shortcuts.get(shortcut);
  }

  private boolean isDeprecatedDriverSetting(Config referenceConfig, String settingName) {
    return referenceConfig.getConfig("ksbulk").hasPathOrNull(settingName);
  }

  /**
   * Sanitizes the value of an argument provided on the command line.
   *
   * <p>All user input is expected to be already valid HOCON; however we accept a relaxed syntax for
   * strings, lists and maps. This methods reestablishes a valid input from a relaxed input of type
   * string (non-quoted strings are quoted), list (missing brackets are added) or map (missing
   * braces are added).
   *
   * <p>Note that this method cannot fix deep syntax problems such as list or map elements that were
   * not properly quoted; it can only perform a shallow fix for the common use cases mentioned
   * above.
   */
  @NonNull
  private String sanitizeValue(String optionValue, ConfigValueType optionType) {
    String formatted = optionValue;
    if (optionType == ConfigValueType.STRING) {
      // if the user did not surround the string with double-quotes, do it for him.
      formatted = StringUtils.ensureQuoted(optionValue);
    } else if (optionType == ConfigValueType.LIST) {
      // if the user did not surround the list elements with square brackets, do it for him.
      formatted = StringUtils.ensureBrackets(optionValue);
    } else if (optionType == ConfigValueType.OBJECT) {
      // if the user did not surround the map entries with curly braces, do it for him.
      formatted = StringUtils.ensureBraces(optionValue);
    }
    return formatted;
  }
}
