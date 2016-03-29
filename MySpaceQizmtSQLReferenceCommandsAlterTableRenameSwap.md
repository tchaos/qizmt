<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `ALTER TABLE RENAME SWAP` #

```
ALTER TABLE <table1> RENAME SWAP <table2>;
```


Fast way to swap the names of two tables which have identical schema. If a table is produced by offline sequence, this is a fast way to swap its name with the name of its predecessor which is already serving data.

### Examples ###

```
ALTER TABLE Employees RENAME SWAP Employees_alt;
```



