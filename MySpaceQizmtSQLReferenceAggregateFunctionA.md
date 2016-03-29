<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `AVG` #

```
AVG(numeric_expression) 
```


Returns the average of the values in a group.

### Examples ###

```
SELECT deptID, AVG(year) FROM Employees GROUP BY deptID;
```

```
SELECT blue, AVG(red) FROM colors GROUP BY blue;
```



