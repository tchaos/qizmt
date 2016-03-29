<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `TRUNCATE TABLE` #

```
TRUNCATE TABLE <table_name>;
```


Remove all rows from the table. All tuples are deleted in parallel across the cluster. Much faster then DELETE command as no mapreduce overhead is incurred.

### Examples ###

```
TRUNCATE TABLE archive_data;
```



