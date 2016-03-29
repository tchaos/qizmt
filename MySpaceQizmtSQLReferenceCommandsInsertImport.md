<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `INSERT IMPORT` #

```
INSERT INTO <table_name> IMPORT <dfs_file>;
```


Insert data into a table from a MR.DFS rectangular binary data file. The rectangular binary file must have the same schema as the target table for import. This operation translates to a single mapreducer.

### Examples ###

```
INSERT INTO Employees IMPORT 'dfs://company-roster';
```



