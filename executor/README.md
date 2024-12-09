# KhulnaSoft Bulk Loader Executor

KSBulk's Bulk Executor API is a pluggable abstraction that allows KSBulk to execute queries using 
reactive programming.

**Important**: this module exists solely because initially the KhulnaSoft Java driver did not contain 
an API to execute queries reactively. Now that the driver exposes this feature, this module and its 
submodules should be considered as deprecated. KSBulk might remove this API and its implementations 
entirely in the near future, and replace them by the driver's equivalent API.

However, the Executor API includes some features that are not present in the driver, such as the 
Execution Listener API. These features and APIs are likely to remain.

This module groups together submodules related to reactive execution of queries:

1. The [ksbulk-executor-api](./api) submodule contains the Executor API.
2. The [ksbulk-executor-reactor](./reactor) submodule contains an implementation of the Executor API
   using Reactor.
