<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)


# Qizmt SQL Translation #

There are three forms of SQL translation which occure. These include: translation to mapreduce, translation to MR.DFS administration and translation to direct data access. Much of the SQL92 standard is available for translation to mapreducer and MR.DFS administration, however there is no query optimizer or selection of algorithms for processing SQL. SQL statements are directly mapped to a series of mapreducer jobs which leverage the resources of the cluster. However there are a few SQL-like commands as well which do not translate to mapreducer jobs and may only be executed on sorted tables. Such jobs allow for very low latency queries and updates on sorted (indexed) tables and are often used from within map() and reduce() functions to to query SQL tables in distributed memory.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_Translation.png' alt='Qizmt SQL Translation' />


When SQL is executed in the Query Analyzer, it is sent via ADO.NET to the Qizmt SQL Extension which runs as a service on all the machines of the cluster. From there it is translated into a series of mapreducer jobs which are executed in the Qizmt cluster. Any resulting record sets are then published back to the ADO.NET Data Provider and displayed in the Query Analyzer’s list view. Note however that if the SQL Operation is selecting the Top 10 tuples from an inner join which produces 20TB of data, the mapreducers which do the inner join will produce the entire 20 TB of resulting data before returning the top 10 tuples. This is because SQL is simply translated to a set of mapreducers without any form of query optimizer logic.


In order to facilitate low latency SQL operations, there are also a few SQL-like commands which allow for muting and querying large sorted tables in distributed memory. These operations are described later on in this document.