<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `DELETE` #

```
DELETE FROM <table_name> WHERE <comparison> [[AND | OR] <...>];
```


Delete rows from a table under certain conditions. See SELECT WHERE for more information on the WHERE clause. This operation performs a full mapreduce on the table but only maps the tuples which match the WHERE clause. For ad-hoc deletes see the BULK UPDATE command.

### Examples ###

```
DELETE FROM Employees WHERE ISLIKE ‘%ferson';
```



