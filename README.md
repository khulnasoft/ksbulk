# KhulnaSoft Bulk Loader Overview

The KhulnaSoft Bulk Loader tool (KSBulk) is a unified tool for loading into and unloading from 
Cassandra-compatible storage engines, such as OSS Apache Cassandra&reg;, KhulnaSoft Astra and KhulnaSoft 
Enterprise (DSE).

Out of the box, KSBulk provides the ability to:
 
1. Load (import) large amounts of data into the database efficiently and reliably; 
2. Unload (export) large amounts of data from the database efficiently and reliably; 
3. Count elements in a database table: how many rows in total, how many rows per replica and per 
   token range, and how many rows in the top N largest partitions.

Currently, CSV and Json formats are supported for both loading and unloading data.  

## Installation

KSBulk can be downloaded from several locations:

* From [KhulnaSoft Downloads](https://downloads.khulnasoft.com/#bulk-loader).
  * Available formats: zip, tar.gz.
* From [GitHub](https://github.com/khulnasoft/ksbulk/releases).
  * Available formats: zip, tar.gz and executable jar.
* From Maven Central: download the artifact `ksbulk-distribution`, for example from
  [here](https://repo.maven.apache.org/maven2/com/khulnasoft/oss/ksbulk-distribution/).
  * Available formats: zip, tar.gz and executable jar.

Please note: only the zip and tar.gz formats are considered production-ready. The executable jar is
provided as a convenience for users that want to try KSBulk, but it should not be deployed in
production environments.

To install KSBulk, simply unpack the zip or tar.gz archives.

The executable jar can be executed with a command like `java -jar ksbulk-distribution.jar
[subcommand] [options]`. See below for command line options.

## Documentation

The most up-to-date documentation is available [online][onlineDocs]. 

We also recommend reading the series of blog posts made by 
[Brian Hess](https://github.com/brianmhess); they target a somewhat older version of KSBulk, but
most of the contents are still valid and very useful:

1. [KhulnaSoft Bulk Loader Pt. 1 — Introduction and Loading]
2. [KhulnaSoft Bulk Loader Pt. 2 — More Loading]
3. [KhulnaSoft Bulk Loader Pt. 3 — Common Settings]
4. [KhulnaSoft Bulk Loader Pt. 4 — Unloading]
5. [KhulnaSoft Bulk Loader Pt. 5 — Counting]
5. [KhulnaSoft Bulk Loader: Examples for Loading From Other Locations]

Developers and contributors: please read our [Contribution Guidelines](./CONTRIBUTING.md).

## Basic Usage

Launch the tool with the appropriate script in the bin directory of your distribution. The help text 
of the tool provides summaries of all supported settings.

The `ksbulk` command takes a subcommand argument followed by options:

```
# Load data
ksbulk load <options>

# Unload data
ksbulk unload <options>

# Count rows
ksbulk count <options>
``` 

### Long options

Any KSBulk or Java Driver setting can be entered on the command line as a long-option argument of
the following general form:

    --full.path.of.setting "some-value"
    
KSBulk settings always start with `ksbulk`; for convenience, this prefix can be omitted in a long
option argument, so the following two options are equivalent and both map to KSBulk's 
`ksbulk.batch.mode` setting:

    --ksbulk.batch.mode PARTITION_KEY
    --batch.mode PARTITION_KEY
    
Java Driver settings always start with `khulnasoft-java-driver`; for convenience, this prefix can be
shortened to `driver` in a long option argument, so the following two options are equivalent and 
both map to the driver's `khulnasoft-java-driver.basic.cloud.secure-connect-bundle` setting:

    --khulnasoft-java-driver.basic.cloud.secure-connect-bundle /path/to/bundle
    --driver.basic.cloud.secure-connect-bundle /path/to/bundle
    
Most settings have default values, or values that can be inferred from the input data. However, 
sometimes the default value is not suitable for you, in which case you will have to specify the
desired value either in the application configuration file (see below), or on the command line. 

For example, the default value for `connector.csv.url` is to read from standard input or write to 
standard output; if that does not work for you, you need to override this value and specify the 
source path/url of the csv data to load (or path/url where to send unloaded data).

See the [Settings page](manual/settings.md) or KSBulk's 
[template configuration file](manual/application.template.conf) for details.

### Short options (Shortcuts)

For convenience, many options (prefaced with `--`), have shortcut variants (prefaced with `-`).
For example, `--ksbulk.schema.keyspace` has an equivalent short option `-k`. 

Connector-specific options also have shortcut variants, but they are only available when
the appropriate connector is chosen. This allows multiple connectors to overlap shortcut
options. For example, the JSON connector has a `--connector.json.url`
setting with a `-url` shortcut. This overlaps with the `-url` shortcut option for the CSV 
connector, that actually maps to `--connector.csv.url`. But in a given invocation of `ksbulk`, 
only the appropriate shortcut will be active.  

Run the tool with `--help` and specify the connector to see its short options:

```
ksbulk -c csv --help
```

## Configuration Files vs Command Line Options

All KSBulk options can be passed as command line arguments, or in a configuration file.

Using one or more configuration files is sometimes easier than passing all configuration 
options via the command line. 

By default, the configuration files are located under KSBulk's `conf` directory; the main
configuration file is named `application.conf`. This location can be modified via the `-f` 
switch. See examples below.

KSBulk ships with a default, empty `application.conf` file that users can customize to their 
needs; it also has a [template configuration file](manual/application.template.conf) that can 
serve as a starting point for further customization.

Configuration files are also required to be compliant with the [HOCON] syntax. This syntax
is very flexible and allows sections to be grouped together in blocks, e.g.:

```hocon
ksbulk {
  connector {
    name = "csv"
    csv {
      url = "C:\\Users\\My Folder"
      delimiter = "\t"
    }
  }
}
```

The above is equivalent to the following snippet using dotted notation instead of blocks:

```hocon
ksbulk.connector.name = "csv"
ksbulk.connector.csv.url = "C:\\Users\\My Folder"
ksbulk.connector.csv.delimiter = "\t"
```

You can split your configuration in more than one file using file inclusions; see the HOCON
documentation for details. The default configuration file includes another file called 
`driver.conf`, also located in the `conf` directory. This file should be used to configure 
the Java Driver for KSBulk. This file is empty as well; users can customize it to their needs. 
A [driver template configuration file](manual/driver.template.conf) can serve as a starting 
point for further customization.

**Important caveats:**

1. In configuration files, it is not possible to omit the prefix `ksbulk`. 
   For example, to select the connector to use in a configuration file,
   use `ksbulk.connector.name = csv`, as in the example above; on the command line, 
   however, you can use `--ksbulk.connector.name csv` or `--connector.name csv` to achieve 
   the same effect, as stated above.
2. In configuration files, it is not possible to abbreviate the prefix `khulnasoft-java-driver`
   to `driver`. For example, to select the consistency level to use in a configuration file,
   use `khulnasoft-java-driver.basic.request.consistency = QUORUM` in a configuration file; on 
   the command line, however, you can use both
   `--khulnasoft-java-driver.basic.request.consistency = QUORUM` or 
   `--driver.basic.request.consistency = QUORUM` to achieve the same effect.
3. Options specified through the command line _override options specified in configuration 
   files_. See examples for details.

## Escaping and Quoting Command Line Arguments

Regardless of whether they are supplied via the command line or in a configuration file, all option 
values are expected to be in valid [HOCON] syntax: control characters, the backslash character, 
and the double-quote character all need to be properly escaped.

For example, `\t` is the escape sequence that corresponds to the tab character:

```bash
ksbulk load -delim '\t'
```

In general, string values containing special characters (such as a colon or a whitespace) also 
need to be properly quoted with double-quotes, as required by the HOCON syntax:

```bash
ksbulk load -h '"host.com:9042"'
```

File paths on Windows systems usually contain backslashes; `\\` is the escape sequence for the 
backslash character, and since Windows paths also contain special characters, the whole path 
needs to be double-quoted: 

```bash
ksbulk load -url '"C:\\Users\\My Folder"'
```

However, when the expected type of an option is a string, it is possible to omit the
surrounding double-quotes, for convenience:

```bash
ksbulk load -url 'C:\\Users\\My Folder'
```

Similarly, when an argument is a list, it is possible to omit the surrounding square
brackets; making the following two lines equivalent:

```bash
ksbulk load --codec.nullStrings 'NIL, NULL'
ksbulk load --codec.nullStrings '[NIL, NULL]'
```

The same applies for arguments of type map: it is possible to omit the surrounding
curly braces, making the following two lines equivalent:

```bash
ksbulk load --connector.json.deserializationFeatures '{ USE_BIG_DECIMAL_FOR_FLOATS : true }'
ksbulk load --connector.json.deserializationFeatures 'USE_BIG_DECIMAL_FOR_FLOATS : true'
```

This syntactic sugar is only available for command line arguments of type string, list or map; 
all other option types, as well as all options specified in a configuration file _must_ be fully 
compliant with HOCON syntax, and it is the user's responsibility to ensure that such options are 
properly escaped _and_ quoted.

Also, note that this syntactic sugar is not capable of quoting single elements inside a list or a 
map; all elements in a list or a map must be individually quoted if they contain special characters.
For example, to specify a list with 2 contact points containing port numbers, it is necessary to 
quote each contact point individually, however the surrounding brackets, as explained above, can 
be omitted for brevity:

```bash
ksbulk load -h '"host1.com:9042","host2.com:9042"'
```

## Load Examples

* Load table `table1` in keyspace `ks1` from CSV data read from `stdin`.
  Use a cluster with a `localhost` contact point. Field names in the data match column names in the
  table. Field names are obtained from a *header row* in the data; by default the tool presumes 
  a header exists in each file being loaded:

  `ksbulk load -k ks1 -t table1`

* Load table `table1` in keyspace `ks1` from a gzipped CSV file by unzipping it to `stdout` and piping to
  `stdin` of the tool:

  `gzcat table1.csv.gz | ksbulk load -k ks1 -t table1`

* Load the file `export.csv` to table `table1` in keyspace `ks1` using the short form option for `url`
  and the tab character as a field delimiter:

  `ksbulk load -k ks1 -t table1 -url export.csv -delim '\t'`

* Specify a few hosts (initial contact points) that belong to the desired cluster and
  load from a local file, without headers. Map field indices of the input to table columns:
  
  `ksbulk load -url ~/export.csv -k ks1 -t table1 -h '10.200.1.3,10.200.1.4' -header false -m '0=col1,1=col3'`

* Specify port 9876 for the cluster hosts and load from an external source url:

  `ksbulk load -url https://192.168.1.100/data/export.csv -k ks1 -t table1 -h '10.200.1.3,10.200.1.4' -port 9876`

* Load all csv files from a directory. The files do not have header rows. Map field indices
  of the input to table columns:

  `ksbulk load -url ~/export-dir -k ks1 -t table1 -m '0=col1,1=col3' -header false`

* Load a file containing three fields per row. The file has no header row. Map all fields to
  table columns in field order. Note that field indices need not be provided.

  `ksbulk load -url ~/export-dir -k ks1 -t table1 -m 'col1, col2, col3' -header false`

* With default port for cluster hosts, keyspace, table, and mapping set in
  `conf/application.conf`:

  `ksbulk load -url https://192.168.1.100/data/export.csv -h '10.200.1.3,10.200.1.4'`

* With default port for cluster hosts, keyspace, table, and mapping set in `ksbulk_load.conf`:

  `ksbulk load -f ksbulk_load.conf -url https://192.168.1.100/data/export.csv -h '10.200.1.3,10.200.1.4'`

* Load table `table1` in keyspace `ks1` from a CSV file, where double-quote characters in fields are
  escaped with a double-quote; for example,  `"f1","value with ""quotes"" and more"` is a line in
  the CSV file:

  `ksbulk load -url ~/export.csv -k ks1 -t table1 -escape '\"'`

* Load table `table1` in keyspace `ks1` from an AWS S3 URL, passing the `region` and `profile` as
  [query parameters in the URL](./url/README.md):

  `ksbulk load -k ks1 -t table1 -url s3://bucket-name/key?region=us-west-1&profile=bucket-profile`

## Unload Examples

Unloading is simply the inverse of loading and due to the symmetry, many settings are
used in both load and unload.

* Unload data to `stdout` from the `ks1.table1` table in a cluster with a `localhost` contact 
  point. Column names in the table map to field names in the data. Field names must be emitted 
  in a *header row* in the output:

  `ksbulk unload -k ks1 -t table1`

* Unload data to `stdout` from the `ks1.table1` and gzip the result:

  `ksbulk unload -k ks1 -t table1 | gzip > table1.gz`

* Unload data to a local directory (which may not yet exist):
                                          
  `ksbulk unload -url ~/data-export -k ks1 -t table1`


## Count Examples

When counting rows in a table, no connector is required, and `schema.mapping` should not be present.

* Count the total rows in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `ksbulk count -k ks1 -t table1`

* Count the total number of rows per token range in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `ksbulk count -k ks1 -t table1 -stats ranges`

* Count the total number of rows per host in the `ks1.table1` table in a cluster with a `localhost` contact point. 

  `ksbulk count -k ks1 -t table1 -stats hosts`

* Count the total number of rows for the biggest 100 partitions in the `ks1.table1` table in a cluster with a `localhost` contact point (by default, KSBulk returns the number of rows for the 10 biggest partitions in the table). 

  `ksbulk count -k ks1 -t table1 -stats partitions -partitions 100`

* Count the total number of rows, the total number of rows per token range, the total number of rows per hosts in the `ks1.table1`, and the total number of rows for the biggest 10 partitions table in a cluster with a `localhost` contact point. 

  `ksbulk count -k ks1 -t table1 -stats global,ranges,hosts,partitions`


## Command-line Help

Available settings along with defaults are documented [here](manual/settings.md), they are also
documented in KSBulk's [template configuration file](manual/application.template.conf) and in
the driver [template configuration file](manual/driver.template.conf). This information is also
available on the command-line via the `help` subcommand.

* Get help for common options and a list of sections from which more help is available:

  `ksbulk help`
  
* Get help for all `connector.csv` options:

  `ksbulk help connector.csv`
  
[onlineDocs]:https://docs.khulnasoft.com/en/ksbulk/doc/
[HOCON]:https://github.com/lightbend/config/blob/master/HOCON.md
[KhulnaSoft Bulk Loader Pt. 1 — Introduction and Loading]: https://www.khulnasoft.com/blog/2019/03/khulnasoft-bulk-loader-introduction-and-loading
[KhulnaSoft Bulk Loader Pt. 2 — More Loading]: https://www.khulnasoft.com/blog/2019/04/khulnasoft-bulk-loader-more-loading
[KhulnaSoft Bulk Loader Pt. 3 — Common Settings]: https://www.khulnasoft.com/blog/2019/04/khulnasoft-bulk-loader-common-settings
[KhulnaSoft Bulk Loader Pt. 4 — Unloading]: https://www.khulnasoft.com/blog/2019/06/khulnasoft-bulk-loader-unloading
[KhulnaSoft Bulk Loader Pt. 5 — Counting]: https://www.khulnasoft.com/blog/2019/07/khulnasoft-bulk-loader-counting
[KhulnaSoft Bulk Loader: Examples for Loading From Other Locations]: https://www.khulnasoft.com/blog/2019/12/khulnasoft-bulk-loader-examples-loading-other-locations
 
## License
Copyright KhulnaSoft, Ltd.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
