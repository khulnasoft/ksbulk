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
package com.khulnasoft.oss.ksbulk.docs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.khulnasoft.oss.ksbulk.config.ConfigUtils;
import com.khulnasoft.oss.ksbulk.config.model.SettingsGroup;
import com.khulnasoft.oss.ksbulk.config.model.SettingsGroupFactory;
import com.khulnasoft.oss.ksbulk.docs.utils.StringUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.text.WordUtils;

public class ConfigurationFileCreator {

  private static final int LINE_LENGTH = 100;
  private static final int INDENT_LENGTH = 4;

  private static final String LINE_INDENT = StringUtils.nCopies(" ", INDENT_LENGTH);

  private static final String INDENTED_ROW_OF_HASHES =
      LINE_INDENT + StringUtils.nCopies("#", LINE_LENGTH - INDENT_LENGTH);
  private static final String ROW_OF_HASHES = StringUtils.nCopies("#", LINE_LENGTH);

  public static void main(String[] args) throws IOException {
    try {
      if (args.length != 1) {
        throw new IllegalArgumentException(
            "Usage: ConfigurationFileCreator \"/path/to/destination/directory\"");
      }
      Path dest = Paths.get(args[0]);
      Files.createDirectories(dest);
      Path ksbulkConfigurationFile = dest.resolve("application.template.conf");
      try (PrintWriter pw =
          new PrintWriter(
              Files.newBufferedWriter(
                  ksbulkConfigurationFile, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
        printKSBulkConfiguration(pw);
      }
      Path driverConfigurationPath = dest.resolve("driver.template.conf");
      try (PrintWriter pw =
          new PrintWriter(
              Files.newBufferedWriter(
                  driverConfigurationPath, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
        printDriverConfiguration(pw);
      }
    } catch (Exception e) {
      System.err.println("Error encountered generating template configuration file");
      e.printStackTrace();
      throw e;
    }
  }

  private static void printKSBulkConfiguration(PrintWriter pw) {
    pw.println(ROW_OF_HASHES);
    pw.println("# This is a template configuration file for the KhulnaSoft Bulk Loader (KSBulk).");
    pw.println("#");
    pw.println("# This file is written in HOCON format; see");
    pw.println("# https://github.com/typesafehub/config/blob/master/HOCON.md");
    pw.println("# for more information on its syntax.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Please make sure you've read the KhulnaSoft Bulk Loader documentation "
                + "included in this binary distribution:"));
    pw.println("# ../manual/README.md");
    pw.println("#");
    pw.println("# An exhaustive list of available settings can be found here:");
    pw.println("# ../manual/settings.md");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Also, two template configuration files meant to be used together can be "
                + "found here:"));
    pw.println("# ../manual/application.template.conf");
    pw.println("# ../manual/driver.template.conf");
    pw.println("#");
    pw.println(
        wrapLines(
            "# We recommend that this file be named application.conf and placed in the "
                + "/conf directory; these are indeed the default file name and path where "
                + "KSBulk looks for configuration files."));
    pw.println("#");
    pw.println(
        wrapLines(
            "# To use other file names, or another folder, you can use the -f command "
                + "line switch; consult the KhulnaSoft Bulk Loader online documentation for more "
                + "information:"));
    pw.println("# https://docs.khulnasoft.com/en/ksbulk/doc/ksbulk/ksbulkLoadConfigFile.html");
    pw.println(ROW_OF_HASHES);
    pw.println();
    pw.println(ROW_OF_HASHES);
    pw.println("# KhulnaSoft Java Driver settings.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# You can declare any Java Driver settings directly in this file, but for "
                + "maintainability sake, we placed them in a separate file, which is "
                + "expected to be named driver.conf and located in the same /conf directory."));
    pw.println(
        wrapLines(
            "# Use that file, for example, to define contact points, provide "
                + "authentication and encryption settings, modify timeouts, consistency levels, "
                + "page sizes, policies, and much more."));
    pw.println(
        wrapLines(
            "# If you decide to declare the driver settings in a different way, or in a "
                + "file named differently, make sure to test your setup to ensure that all "
                + "settings are correctly detected."));
    pw.println("#");
    pw.println("# You can also consult the Java Driver online documentation for more details:");
    pw.println("# https://docs.khulnasoft.com/en/developer/java-driver/latest/");
    pw.println("# https://docs.khulnasoft.com/en/developer/java-driver-dse/latest/");
    pw.println("include classpath(\"driver.conf\")");
    pw.println(ROW_OF_HASHES);
    pw.println();
    pw.println(ROW_OF_HASHES);
    pw.println("# KhulnaSoft Bulk Loader settings.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Settings for the KhulnaSoft Bulk Loader (KSBulk) are declared below. Use this "
                + "section, for example, to define which connector to use and how, to customize "
                + "logging, monitoring, codecs, to specify schema settings and mappings, "
                + "and much more."));
    pw.println("#");
    pw.println(
        "# You can also consult the KhulnaSoft Bulk Loader online documentation for more details:");
    pw.println("# https://docs.khulnasoft.com/en/ksbulk/doc/ksbulk/ksbulkRef.html");
    pw.println(ROW_OF_HASHES);
    pw.println("ksbulk {");
    pw.println();

    Config referenceConfig = ConfigUtils.standaloneKSBulkReference();

    Map<String, SettingsGroup> groups = SettingsGroupFactory.createKSBulkConfigurationGroups(false);

    for (Map.Entry<String, SettingsGroup> groupEntry : groups.entrySet()) {
      String section = groupEntry.getKey();
      if (section.equals("Common")) {
        // In this context, we don't care about the "Common" pseudo-section.
        continue;
      }
      pw.println(INDENTED_ROW_OF_HASHES);
      referenceConfig.getConfig(section).root().origin().comments().stream()
          .filter(line -> !ConfigUtils.isTypeHint(line))
          .filter(line -> !ConfigUtils.isLeaf(line))
          .forEach(
              l -> {
                pw.print(LINE_INDENT + "# ");
                pw.println(wrapIndentedLines(l, 1));
              });
      pw.println(INDENTED_ROW_OF_HASHES);

      for (String settingName : groupEntry.getValue().getSettings()) {
        ConfigValue value = ConfigUtils.getNullSafeValue(referenceConfig, settingName);

        pw.println();
        value.origin().comments().stream()
            .filter(line -> !ConfigUtils.isTypeHint(line))
            .filter(line -> !ConfigUtils.isLeaf(line))
            .forEach(
                l -> {
                  pw.print(LINE_INDENT + "# ");
                  pw.println(wrapIndentedLines(l, 1));
                });
        pw.print(LINE_INDENT + "# Type: ");
        pw.println(ConfigUtils.getTypeString(referenceConfig, settingName).orElse("arg"));
        pw.print(LINE_INDENT + "# Default value: ");
        pw.println(value.render(ConfigRenderOptions.concise()));
        pw.print(LINE_INDENT);
        pw.print("#");
        pw.print(settingName.substring("ksbulk.".length()));
        pw.print(" = ");
        pw.println(value.render(ConfigRenderOptions.concise()));
      }
      pw.println();
    }
    pw.println("}");
  }

  private static void printDriverConfiguration(PrintWriter pw) {
    pw.println(ROW_OF_HASHES);
    pw.println("# Java Driver configuration for KSBulk.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# The settings below are just a subset of all the configurable options of the "
                + "driver, and provide an optimal driver configuration for KSBulk for most use cases. "
                + "See the Java Driver configuration reference for instructions on how to configure "
                + "the driver properly:"));
    pw.println("# https://docs.khulnasoft.com/en/developer/java-driver/latest/");
    pw.println("# https://docs.khulnasoft.com/en/developer/java-driver-dse/latest/");
    pw.println("#");
    pw.println("# This file is written in HOCON format; see");
    pw.println("# https://github.com/typesafehub/config/blob/master/HOCON.md");
    pw.println("# for more information on its syntax.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# This file is not meant as the main configuration file for KSBulk, but "
                + "rather to be included from the main configuration file. We recommend that "
                + "this file be named driver.conf and placed in the /conf directory, alongside "
                + "with another configuration file for KSBulk itself, named application.conf. "
                + "Also, for this setup to work, application.conf should include driver.conf, "
                + "for example by using an import directive. For other ways to "
                + "configure this tool, refer to KhulnaSoft Bulk Loader online documentation:"));
    pw.println("# https://docs.khulnasoft.com/en/ksbulk/doc/ksbulk/ksbulkRef.html");
    pw.println(ROW_OF_HASHES);
    pw.println("");
    pw.println("khulnasoft-java-driver {");
    pw.println("");
    Config driverConfig = ConfigUtils.standaloneDriverReference().getConfig("khulnasoft-java-driver");
    printDriverSettings(pw, driverConfig.root(), 1);
    pw.println("}");
  }

  private static void printDriverSettings(
      @NonNull PrintWriter pw, @NonNull ConfigObject root, int indentation) {
    Set<Entry<String, ConfigValue>> entries = new TreeSet<>(SettingsGroupFactory.ORIGIN_COMPARATOR);
    entries.addAll(root.entrySet());
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      ConfigValue value = ConfigUtils.getNullSafeValue(root.toConfig(), key);
      String spaces = StringUtils.nCopies(" ", INDENT_LENGTH * indentation);
      if (ConfigUtils.isLeaf(value)) {
        value.origin().comments().stream()
            .filter(line -> !ConfigUtils.isTypeHint(line))
            .filter(line -> !ConfigUtils.isLeaf(line))
            .forEach(
                l -> {
                  pw.print(spaces + "# ");
                  pw.println(wrapIndentedLines(l, indentation));
                });
        pw.print(spaces + "# Type: ");
        pw.println(ConfigUtils.getTypeString(root.toConfig(), key).orElse("arg"));
        pw.print(spaces + "# Default value: ");
        pw.println(value.render(ConfigRenderOptions.concise()));
        pw.print(spaces);
        pw.print("#");
        pw.print(key);
        pw.print(" = ");
        pw.println(value.render(ConfigRenderOptions.concise()));
        pw.println();
      } else {
        value.origin().comments().stream()
            .filter(line -> !ConfigUtils.isTypeHint(line))
            .filter(line -> !ConfigUtils.isLeaf(line))
            .forEach(
                l -> {
                  pw.print(spaces + "# ");
                  pw.println(wrapIndentedLines(l, indentation));
                });
        pw.print(spaces);
        pw.print(key);
        pw.println(" {");
        pw.println();
        printDriverSettings(pw, ((ConfigObject) value), indentation + 1);
        pw.print(spaces);
        pw.println("}");
        pw.println();
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static String wrapLines(String text) {
    return WordUtils.wrap(text, LINE_LENGTH - 2, String.format("%n# "), false);
  }

  private static String wrapIndentedLines(String text, int indentation) {
    return WordUtils.wrap(
        text,
        (LINE_LENGTH - INDENT_LENGTH * indentation) - 2,
        String.format("%n%s# ", StringUtils.nCopies(" ", INDENT_LENGTH * indentation)),
        false);
  }
}
