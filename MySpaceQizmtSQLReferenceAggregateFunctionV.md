<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `VAR_POP` #

```
VAR(numeric_expression)
```

Returns the population variance of all values in a group.

### Examples ###

```
SELECT deptID, VAR_POP(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, VAR_POP(red) FROM colors GROUP BY blue;
```

# `VAR_SAMP` #

```
VAR_SAMP(numeric_expression)
```

Returns the sample variance of all values in a group.

### Examples ###

```
SELECT deptID, VAR_SAMP(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, VAR_SAMP(red) FROM colors GROUP BY blue;
```

