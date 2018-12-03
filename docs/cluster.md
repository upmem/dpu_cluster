# Dpu Cluster Layer

Note: All Dpus are supposed to be of the same type.

The Dpu Cluster Layer is an abstraction over a set of Dpu Ranks.

It provides:

* a protocol to communicate with the Dpus:
    * unicast/multicast/broadcast capabilities
    * generic requests, letting the user implements their application specific protocol
    * multi-direction capabilites (Host -> Dpu, Dpu -> Host, maybe Dpu -> Dpu)
* a data management system handling:
    * data location
    * data movements
* performance data collectors for the Cluster (load balancing info, communication overhead, etc...)
    * but no data analysis tool for now

It may be used as:

* a shared library (used by other higher level abstraction, or directly by user applications)
    + "easy" host debugging
    + simple integration
* a service (daemon process) with fast local inter-process communication with user applications
    + "easy" attach dpu (debug mechanism)
    + manage ALL Dpus of the machine -> resolve application collision issues

It is written in Rust. Interfaces may be provided for Java, Python, C++, ...

It depends on:

* the CNI library
* the C ELF loader module (currently part of the Host API)

