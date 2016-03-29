<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension (contd) #





## 12.  Run Query ##

Click the **Run** button or hit **F5** to execute the query. Note that these queries have a fairly heavy amount of fixed overhead time, even if working with very small tables as this RDBMS is designed for executing queries over very large clusters of computers. Because the overhead time is fixed, large amounts of data can be streamed in and out via the _Qizmt ADO.NET Data Provider_ maximizing the bandwidth between data centers, but for small tasks like this, the overhead is not so negligible. This is because the SQL is translated to mapreduce and a number of network connections are made optimized to large-scale distributed data. While your job is running you can, however, view the progress of the underlying mapreducer jobs by discovering the jobID of the underlying mapreducer jobs `qizmt history –j` and attaching to the standard-out of a particular job with `qizmt –a viewjob <jobID>`


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_QaRun.png' alt='QSQL Qa Query Window' />


## 13.  Connect via ADO.NET ##

Next, let’s connect via ADO.NET in Visual Studio.Net. Open a new Visual Studio.Net from a machine within the LAN or VLAN and select


**File->New->Project->Console Application**



<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_CreateConsoleApp.png' alt='Create Console Application' />

[< PREV](MySpaceQizmtSQLQuickStartGuideWalkthrough4.md)
[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough6.md)