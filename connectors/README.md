# KhulnaSoft Bulk Loader Connectors

Connectors form a pluggable abstraction that allows KSBulk to read and write to a variety of
backends.

This module groups together submodules related to connectors:

1. The [ksbulk-connectors-api](./api) submodule contains the Connector API.
2. The [ksbulk-connectors-commons](./commons) submodule contains common base classes for text-based
   connectors.
3. The [ksbulk-connectors-csv](./csv) submodule contains the CSV connector.
4. The [ksbulk-connectors-json](./json) submodule contains the Json connector.
