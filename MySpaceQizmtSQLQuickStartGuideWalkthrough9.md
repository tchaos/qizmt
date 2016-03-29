<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension (contd) #



## 21.  Run Mapreducer ##

Run the mapreducer in debug mode by pressing **F5** or by clicking on the <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_StartDebugButton.png' alt='Start Debug Button' />  button.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_StartDebug.png' alt='Start Debug' />

## 22.  Observe Locals / Call Stack ##

In _Locals / Call Stack_ box you can see the values of the current reducer iteration each time you press **F5**.

## 23.  Remove Breakpoint ##

Click on <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_RemoveBreakPoint.png' alt='Remove Breakpoint' />  to remove the break point and then press F5 again to let the mapreducer finish executing in debug mode.

## 24.  Output File ##

Once the mapreducer finishes executing in debug mode, the resulting _MR.DFS file dfs://MyPaintingsWordCount\_Output_ is created containing the resulting words and number of occurrences for each word.

[< PREV](MySpaceQizmtSQLQuickStartGuideWalkthrough8.md)
[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough10.md)