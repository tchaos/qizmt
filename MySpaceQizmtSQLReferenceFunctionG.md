<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `GREATER` #

```
GREATER(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if expression1 is greater than expression2, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE GREATER(Name, 'M') = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE GREATER(red, blue) = 1 ORDER BY color;
```

# `GREATEREQUAL` #

```
GREATEREQUAL(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if expression1 is greater than or equal to expression2, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE GREATEREQUAL(Name, 'M') = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE GREATEREQUAL(red, blue) = 1 ORDER BY color;
```


