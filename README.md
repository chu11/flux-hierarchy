Setup
=====

* Clone, compile, and install these specific versions of flux-core/sched:
  * [stevwonder/flux-core:hierarchical](https://github.com/stevwonder/flux-core/tree/hierarchical)
  * [stevwonder/flux-sched:hierarchical](https://github.com/stevwonder/flux-sched/tree/hierarchical)
* Compile the `sched_proxy` module in this repo:
  * `cd src; make`

Usage
=====

* `flux start src/initial_program hierarchy_config.json root ${NUM_JOBS} ${COMMAND}`
  * The structure of the `hierarchy_config.json` is outlined in an [RFC wiki entry](https://github.com/flux-framework/rfc/wiki/Parent-Child-Communication-Strategies-for-Nested-Flux-Instances#hierarchy-configuration)
* See `examples/launch.sh` for a functioning example
