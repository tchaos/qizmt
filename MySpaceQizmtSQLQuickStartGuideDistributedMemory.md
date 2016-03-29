<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)




# Qizmt SQL INDEXs/Distributed Memory #

It is often useful to perform low latency queries and updates of large tables in distributed memory. In order to facilitate this, a few SQL-like commands are available. This is often useful for performing deep graph traversals, large look-up tables available to mapreducer jobs and integrating periodic updates into large distributed tables innear-real-time pipelines.

This next example generates random data and creates an indexed table. Every core in the cluster is given an even subset of the nodes in the graph; from there they traverse into distributed memory bringing 2 levels of related nodes into the local memory of each process.


## Massive Deep Graph Traversal Performance Test ##

This example was used to perform 2-level graph traversals on various data sets. Here is a summary of the findings for running this test on a 512 core cluster.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_DistributedMemoryPerfTest.png' alt='Performance Test' />


## Distributed Memory Example ##

[Example Code](MySpaceQizmtSQLQuickStartGuideDistributedMemoryExampleCode.md)
