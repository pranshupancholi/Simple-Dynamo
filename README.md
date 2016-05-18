# Simple-Dynamo

Implemented a simplified version of Dynamo. There are three main pieces you need to implemented: 
1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation 
always perform read and write operations successfully even under failures. At the same time, a read operation always 
return the most recent value. Partitioning and replication, is done exactly the way Dynamo does.
