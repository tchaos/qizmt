<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)



# Qizmt Modeling #

Often when applications developers are designing pipelines, it is helpful to draw up a design for the pipeline. In order to facilitate this we have settled on a simple modeling technique for this. These models are often hand-drawn or created with Microsoft Visio. Qizmt modeling is often used to:

  * Design new pipelines
  * Communicate design of pipelines already created
  * Help achieve capability maturity certifications

## Qizmt Modeling Syntax ##

| **Concept** | **Narrative** |
|:------------|:--------------|
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingGroupedMR.png' alt='Modeling Grouped MR' /> | Grouped mapreduce, often used to re-pivot a large amount of data on some column of its tuples. Applying logic both before and after the data is pivoted on a field in its tuples. (Note: arrows always point to data in this methodology) |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingSortedMR.png' alt='Modeling Sorted MR' /> | Like grouped mapreducer, but data is fully sorted instead of hashed on the keys such that each reducer gets an even range of sorted data. Often used to sort data into an index for RINDEX creation, however this may also be done with INSERT INTO SELECT which will translate to a sorted mapreducer execution. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingLocal.png' alt='Modeling Local' /> | Function that executes on a single core in the cluster. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingRemote.png' alt='Modeling Remote' /> | Function that executes on a single or multiple cores in the cluster and has a stream both into and out of and MR.DFS file. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingContent.png' alt='Modeling Content' /> | An MR.DFS file or SQL table that has an underlying MR.DFS file. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingMRI.png' alt='Modeling MRI' /> | Mapreduce Input |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingMRO.png' alt='Modeling MRO' /> | Mapreduce Output |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingRI.png' alt='Modeling RI' /> | Remote Input  |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingRO.png' alt='Modeling RO' /> | Remote Output |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingRSelect.png' alt='Modeling RSelect' /> | ADO.NET RSELECT which returns tuples from distributed memory. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingBulkupdate.png' alt='Modeling Bulkupdate' /> | ADO.NET BULK UPDATE consisting of an ordered series of INSERTs and DELETES to apply to and index in distributed memory. |
| <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingADONET.png' alt='Modeling ADO.NET' /> | All other ADO.NET operations besides RSELECT and BULK UPDATE. |


## Qizmt Modeling Example ##

A simplified model to quickly describe the business logic of the Distributed Memory Performance Test might look something like this:

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ModelingExample.png' alt='Modeling Example' />




