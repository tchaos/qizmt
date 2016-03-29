<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `TAN` #

```
TAN(numeric_expression)
```

Returns the tangent of the input numeric expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE TAN(angle) = 0.0;
```

```
SELECT TOP 2 TAN(angle) FROM Trapezoids WHERE base = 100;
```

# `TRUNC` #

```
TRUNC(double, int) 
```

Returns a double, rounded to the specified length or precision in int.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE TRUNC(area, 1) = 10.0;
```

```
SELECT TOP 2 TRUNC(area, 2), base  FROM Trapezoids WHERE y = 10.12 AND x < 1;
```


