<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `EQUAL` #

```
EQUAL(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if they are equal, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE EQUAL(Years, 4) = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE EQUAL(red, blue) = 1 ORDER BY color;
```


# `EXP` #

```
EXP(numeric_expression) 
```

Returns the exponential value of the specified numeric expression.The constant e is the base of natural logarithms.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE EXP(Years) = 1.0 ORDER BY Name;
```

```
SELECT TOP 2 EXP(red) FROM colors WHERE blue = 9 ORDER BY color;
```
