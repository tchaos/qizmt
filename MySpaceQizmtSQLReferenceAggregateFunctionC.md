<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `CHOOSERND` #

```
CHOOSERND(expression)
```

Returns a random value from a group.

### Examples ###

```
SELECT CHOOSERND(year) FROM Employees GROUP BY deptID;
```

```
SELECT CHOOSERND(red) FROM colors GROUP BY blue;
```

# `COUNT` #

```
COUNT(expression)
```

Returns the number of items in a group.

### Examples ###

```
SELECT year, COUNT(year) FROM Employees GROUP BY year;
```

```
SELECT year, COUNT(year) FROM Employees GROUP BY year;
```

# `COUNTDISTINCT` #

```
COUNTDISTINCT(expression)
```

Returns the number of distinct items in a group.

### Examples ###

```
SELECT year, COUNTDISTINCT(year) FROM Employees GROUP BY year;
```

```
SELECT blue, COUNTDISTINCT(blue) FROM colors GROUP BY blue;
```

