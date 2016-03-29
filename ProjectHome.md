MySpace Qizmt is a mapreduce framework for executing and developing distributed computation applications on large clusters of Windows servers. The MySpace Qizmt project develops open-source software for reliable, scalable, super-easy, distributed computation software.


[![](http://qizmt.googlecode.com/svn/wiki/images/QizmtCloudImage.png)](http://code.google.com/p/qizmt/wiki/MySpaceQizmtOnEC2Tutorial)


MySpace Qizmt core features include:

![http://qizmt.googlecode.com/files/qizmt_core_feature_preview.png](http://qizmt.googlecode.com/files/qizmt_core_feature_preview.png)

## Highly Scalable Applications of MySpace Qizmt ##
  * Data Mining
  * Analytics
  * Bulk Media Processing
  * Content Indexing

## Core MySpace Qizmt Features ##
  * Rapidly develop mapreducer jobs in C#.Net
  * Easy Do-It-Yourself Installer
  * Built-in IDE/Debugger
    * Automatically colors heap allocations in red
    * Autocomplete for rapid mapreducer development
    * Step through and debug mapreducer jobs directly on target cluster
  * From any machine in a cluster:
    * Edit mapreducer jobs
    * Debug mapreducer jobs
    * Execute mapreducer jobs
    * Administer mapreducer jobs
  * Delta-only exchange option for Mapreduce jobs
  * Configurable data-redundancy/machine level failover
  * Easily add machines to a cluster to increase processing power and capacity
  * CAC (Cluster Assembly Cache) for exposing .Net DLLs to mapreduce jobs
  * Three kinds of jobs
    * Mapreduce - Set-based logic on large amounts of data
    * Remote - For problems that don't fit into the mapreducer mold
    * Local - For orchestrating a pipeline of Mapreducer and Remote jobs
  * Three ways to exhange data durring mapreduce
    * Sorted - key/value pairs are evenly sorted accross the cluster
    * Grouped - like key/value pairs make their way to same reducer but not sorted
    * Hashsorted - super fast way to sort random data

MySpace Qizmt currently supports .Net 3.5 SP1 on Windows 2003 Server, Windows 2008 Server, Windows Vista and Windows 7.

### MySpace Qizmt IDE/Debugger ###
![http://qizmt.googlecode.com/files/qizmt_IDE.png](http://qizmt.googlecode.com/files/qizmt_IDE.png)

## User Documentation ##

[MySpace Qizmt Tutorial](MySpaceQizmtTutorial.md)

[MySpace Qizmt Reference](MySpaceQizmtReference.md)

[MySpace Qizmt Administration](MySpaceQizmtAdministration.md)

[MySpace Qizmt FAQ](MySpaceQizmtFAQ.md)

## Qizmt SQL Documentation ##

[MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)

[MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md)

## Contributor Documentation ##

[MySpace Qizmt Open Source Contribution ](MySpaceQizmtOpenSourceContribution.md)

## Qizmt on EC2 Documentation ##

[Qizmt on EC2 Tutorial](MySpaceQizmtOnEC2Tutorial.md)