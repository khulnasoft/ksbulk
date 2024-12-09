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
package com.khulnasoft.oss.ksbulk.runner.help;

import com.khulnasoft.oss.ksbulk.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HelpEntryFactory {

  public static final HelpEntry CONFIG_FILE_OPTION =
      new HelpEntry(
          "f",
          null,
          null,
          "string",
          "Load options from the given file rather than from `<ksbulk_home>/conf/application.conf`.");

  static final HelpEntry HELP_OPTION =
      new HelpEntry(
          null,
          null,
          "help",
          null,
          "This help text. May be combined with -c <connectorName> to see short options for a "
              + "particular connector.");

  static final HelpEntry VERSION_OPTION =
      new HelpEntry("v", null, "version", null, "Show program's version number and exit.");

  static List<HelpEntry> createEntries(
      Collection<String> settings, Map<String, String> longToShortOptions, Config referenceConfig) {
    List<HelpEntry> entries = new ArrayList<>();
    for (String longOptionName : settings) {
      String argumentType =
          ConfigUtils.getTypeString(referenceConfig, longOptionName).orElse("arg");
      ConfigValue value = ConfigUtils.getNullSafeValue(referenceConfig, longOptionName);
      String abbreviatedOptionName = createAbbreviatedOptionName(longOptionName);
      HelpEntry entry =
          new HelpEntry(
              longToShortOptions.get(longOptionName),
              abbreviatedOptionName,
              longOptionName,
              argumentType,
              getSanitizedDescription(value));
      entries.add(entry);
    }
    return entries;
  }

  @Nullable
  private static String createAbbreviatedOptionName(String setting) {
    if (setting.startsWith("khulnasoft-java-driver.")) {
      return setting
          // the prefix "khulnasoft-java-driver." can be abbreviated to "driver."
          .replaceFirst("khulnasoft-java-driver\\.", "driver.");
    }
    if (setting.startsWith("ksbulk.")) {
      return setting
          // the prefix "ksbulk." can be abbreviated to ""
          .replaceFirst("ksbulk\\.", "");
    }
    return null;
  }

  /**
   * Adapts the configuration value comments and makes it suitable for rendering on the console.
   *
   * <p>TODO replace this with a proper Markdown render tool.
   *
   * @param value The configuration value to sanitize.
   * @return A sanitized description suitable for rendering on the console.
   */
  private static String getSanitizedDescription(ConfigValue value) {
    String desc = ConfigUtils.getComments(value);
    desc =
        desc
            // * Replace consecutive spaces with a single space.
            .replaceAll(" +", " ")
            // * Remove **'s, which have meaning in markdown but not useful here. However,
            //   we do have a legit case of ** when describing file patterns (e.g. **/*.csv).
            //   Those sorts of instances are preceded by ", so don't replace those.
            .replaceAll("([^\"])\\*\\*", "$1")
            // * Replace ``` with empty string
            .replaceAll("```\n", "")
            // * Replace ` with '
            .replaceAll("`", "'")
            .trim();
    String defaultValue = value.render(ConfigRenderOptions.concise());
    desc += "\nDefault: " + defaultValue;
    return desc;
  }
}
