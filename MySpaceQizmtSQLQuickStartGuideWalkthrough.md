<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension #



## 1.  Install Qizmt ##

Install the latest Qizmt on every machine in the cluster. (Note there is an install.js which may be used to avoid having to run the .msi installer on every machine of the cluster)

The latest mainline source may be found under:

**/trunk**


## 2. Build Qizmt SQL Extension ##

Build the Qizmt SQL Extension solution then zip the QueryAnalyzer\_Setup folder as QSQLExtension.zip

This folder contains all of the Qizmt Jobs, assemblies, services, etc. of the Qizmt SQL Extension.

The latest mainline source may be found under:

**/trunk/Extensions/QueryAnalyzer**


## 3.  Unzip Qizmt SQL Extension ##

Unzip QSQLExtension.zip on any machine in the cluster and navigate to the unzipped folder at the command line. It does not matter which machine in the cluster you use to install, once it is installed it will be available to the entire cluster.


## 4.  Install Qizmt SQL Extension ##

Issue the command: `RDBMS_Install.js “<username>” “<password>”`

(use the same windows credentials which Qizmt was installed with)

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_RDBMSIntall.png' alt='RDBMS_Install.js' />

[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough2.md)