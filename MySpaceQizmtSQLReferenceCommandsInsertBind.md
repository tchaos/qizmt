<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `INSERT BIND` #

```
INSERT INTO <table_name> BIND <dfs_file>;
```


Bind data into a table using a MR.DFS rectangular binary data file created using the DbRecordset object. The file is directly bound into the table, leaving the original file name inaccessible. This operation incurs no mapreducers and is used for re-casting MR.DFS data as a SQL table. See the Qizmt SQL Quick Start Guide for more information on DbRecordset.

### Examples ###

```
INSERT INTO Employees BIND 'dfs://db-company-roster';
```



