<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `MOD` #

```
MOD(numeric_expression1, numeric_expression2) 
```

Returns the remainder after dividing numeric\_expression1 by numeric\_expression2.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE MOD(area, 10) = 0;
```

```
SELECT TOP 2 MOD(base, 2), area FROM Trapezoids WHERE x = 1;
```

# `MONTHS_BETWEEN` #

```
MONTHS_BETWEEN(datetime1, datetime2) 
```

Returns a double that represents the number of months between the two datetime operands.

### Examples ###

```
SELECT TOP 100 * FROM Employees WHERE MONTHS_BETWEEN(hireDate, terminateDate) = 6;
```

```
SELECT TOP 100 BETWEEN(hireDate, terminateDate) FROM Employees ORDER BY EmployeeID;
```



