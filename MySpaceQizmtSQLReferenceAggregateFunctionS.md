<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `STD` #

```
STD(numeric_expression)
```

Returns the population standard deviation of all values in a group.

### Examples ###

```
SELECT deptID, STD(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, STD(red) FROM colors GROUP BY blue;
```

# `STD_SAMP` #

```
STD_SAMP(numeric_expression)
```

Returns the sample standard deviation of all values in a group.

### Examples ###

```
SELECT deptID, STD_SAMP(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, STD_SAMP(red) FROM colors GROUP BY blue;
```

# `SUM` #

```
SUM(numeric_expression)
```

Returns the sum of all values in a group.

### Examples ###

```
SELECT angle, SUM(area)  FROM Triangles GROUP BY angle;
```

```
SELECT angle, SUM(area) FROM Trapezoids GROUP BY angle;
```


