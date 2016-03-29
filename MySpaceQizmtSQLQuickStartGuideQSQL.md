<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)


# Qizmt SQL #

The Qizmt SQL or (QSQL) is an infrastructure built on top of Qizmt which translates Standard Query Language (SQL) into set-based mapreducer jobs, MR.DFS commands and directly accessing/muting MR.DFS data. There are two packages available for installation: _Qizmt SQL Extension_, _Qizmt ADO.NET Data Provider_.

## Qizmt SQL Extension Package ##


[http://code.google.com/p/qizmt/source/browse/#svn/trunk/Extensions/QueryAnalyzer/QueryAnalyzer\_Setup](http://code.google.com/p/qizmt/source/browse/#svn/trunk/Extensions/QueryAnalyzer/QueryAnalyzer_Setup)


This package opens up ADO.NET connectivity into a Qizmt cluster. Once installed, any machine of the cluster may be used to launch the Query Analyzer, a windows application for editing and executing SQL commands. This install also includes a suite administration commands and examples. Once installed, mapreducer jobs within the cluster have access to the Qizmt ADO.NET Data Provider and may perform SQL queries during map() and reduce() operations.


## Qizmt ADO.NET Data Provider Package ##

[http://code.google.com/p/qizmt/source/browse/#svn/trunk/Extensions/QueryAnalyzer/Qizmt\_ADONET\_DataProvider](http://code.google.com/p/qizmt/source/browse/#svn/trunk/Extensions/QueryAnalyzer/Qizmt_ADONET_DataProvider)


This package should be installed on machines outside of the Qizmt cluster which need ADO.NET connectivity into the cluster. This is used for easily binding applications outside of the cluster to Qizmt data within a cluster. Such applications often include: .NET Web Applications, .NET Forms Applications, .NET Windows Services and MSSQL CLR functions/stored procedures.