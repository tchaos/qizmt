<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `MAX` #

```
MAX(expression)
```

Returns the maximum value in a group.

### Examples ###

```
SELECT deptID, MAX(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, MAX(red) FROM colors GROUP BY blue;
```

# `MIN` #

```
MIN(expression)
```

Returns the minimum value in a group.

### Examples ###

```
SELECT deptID, MIN(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, MIN(red) FROM colors GROUP BY blue;
```



