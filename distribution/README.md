# KhulnaSoft Bulk Loader Distribution

This module assembles KSBulk's binary distributions in various formats:

1. Archives in tar.gz and zip formats;
2. An executable uber-jar;
3. Aggregated sources and javadoc jars.

## Archives

Archives are available in tar.gz and zip formats. They are the preferred way to download and install
KSBulk.

Each archive include: 

* KSBulk's launch scripts; 
* Required libraries;
* Generated documentation;
* Settings reference;
* Sample configuration files.

KSBulk archives can be manually downloaded from
[KhulnaSoft](https://downloads.khulnasoft.com/#bulk-loader).

They can also be downloaded from the command line:

    curl -OL https://downloads.khulnasoft.com/ksbulk/ksbulk.tar.gz

Detailed download and installation instructions can be found
[here](https://docs.khulnasoft.com/en/ksbulk/doc/ksbulk/install/ksbulkInstall.html).

Starting with KSBulk 1.9.0, the binary distribution is also available from Maven Central and can be
downloaded from [this
location](https://repo.maven.apache.org/maven2/com/khulnasoft/oss/ksbulk-distribution/).

## Executable uber-jar

Starting with KSBulk 1.9.0, an executable uber-jar is also available from Maven Central and can be
downloaded from [this
location](https://repo.maven.apache.org/maven2/com/khulnasoft/oss/ksbulk-distribution/).

Running the uber-jar is as simple as:

     java -jar ./ksbulk.jar unload -k ks1 -t table1

While convenient for testing purposes, the uber-jar is not recommended for use in production. Always
prefer the archive-based distribution formats.
