<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `INSERT IMPORTLINES` #

```
INSERT INTO <table_name> IMPORTLINES <dfs_file> [DELIMITER <character>];
```


Insert data into a table from a line-based MR.DFS file with a character delimiter. This operation incurs 1 mapreducer.

### Examples ###

```
INSERT INTO Employees IMPORTLINES 'dfs://company-roster-lines';
```



