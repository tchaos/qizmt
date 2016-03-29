<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension (contd) #



## 16.  Naming of Rectangular Binary MR.DFS File ##

Now that the name of the _underlying rectangular binary MR.DFS file_ is known to be _dfs://RDBMS\_Table\_paintings@644_, a mapreducer can be written to produce another MR.DFS file containing the count of each unique word in the _title_ field of every tuple of the _paintings_ table.


## 17.  Launch the Qizmt IDE/Debugger ##


Launch the _Qizmt IDE/Debugger_ with a new job file called MyPaintingsWordCount.xml by issuing the following command at the windows command prompt:

**qizmt edit MyPaintingsWordCount.xml**


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_EditPaintingsWordCount.png' alt='Qizmt edit' />


## 18.  Qizmt IDE/Debugger ##


This will bring up the _Qizmt IDE/Debugger_ with a default mapreduce example, unless MyPaintingsWordCount.xml already exists in the _MR.DFS_.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_EditorPaintingsWordCount.png' alt='Qizmt edit' />

[< PREV](MySpaceQizmtSQLQuickStartGuideWalkthrough6.md)
[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough8.md)