<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `DROP TABLE` #

```
DROP TABLE <table_name>;
```


Remove all rows from the table and remove the table from sys.tables. No mapreduce overhead, all tuples are deleted in parallel across the cluster.

### Examples ###

```
DROP TABLE archive_data;
```



