# KhulnaSoft Bulk Loader Workflows

Workflows form a pluggable abstraction that allows KSBulk to execute virtually any kind of 
operation.

This module groups together submodules related to workflows:

1. The [ksbulk-workflow-api](./api) submodule contains the Workflow API.
2. The [ksbulk-workflow-commons](./commons) submodule contains common base classes for workflows,
   and especially configuration utilities shared by KSBulk's built-in workflows (load, unload and 
   count).
3. The [ksbulk-workflow-load](./load) submodule contains the Load Workflow.
4. The [ksbulk-workflow-unload](./unload) submodule contains the Unload Workflow.
5. The [ksbulk-workflow-count](./count) submodule contains the Count Workflow.
