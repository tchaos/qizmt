<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `SIGN` #

```
SIGN(numeric_expression)
```

Returns the positive (+1), zero (0), or negative (-1) sign of the specified expression.Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE SIGN(angle) = 1;
```

```
SELECT TOP 2 * FROM Trapezoids WHERE SIGN(angle) = 1;
```

# `SIN` #

```
SIN(numeric_expression)
```

Returns the trigonometric sine of the specified angle, in radians, and in numeric expression. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE SIN(angle) = 0.0;
```

```
SELECT TOP 2 SIN(angle), angle FROM Trapezoids WHERE x = 0.0;
```

# `SPACE` #

```
SPACE(int)
```

Returns a string of repeated spaces.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE Name = SPACE(1) ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE color = SPACE(1) ORDER BY color;
```

# `SQRT` #

```
SQRT(numeric_expression)
```

Returns the square root of the specified numeric expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE SQRT(area) = 2.0;
```

```
SELECT TOP 2 SQRT(area), x, y FROM Trapezoids WHERE x = 3.0;
```

# `SQUARE` #

```
SQUARE(numeric_expression)
```

Returns the square of the specified numeric expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE SQUARE(area) = 4.0;
```

```
SELECT TOP 2 SQUARE(area) FROM Trapezoids WHERE base = 9;
```

# `SUBSTRING` #

```
SUBSTRING(char(n), int startIndex, int length)
```

Returns part of a char(n) expression.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE SUBSTRING(Name, 0, 2) = 'Mr' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE SUBSTRING(color, 0, 1) = 'R' ORDER BY color;
```

# `SYSDATE` #

```
SYSDATE()
```

Returns the system datetime.

### Examples ###

```
SELECT TOP 10 area, SYSDATE() FROM Triangles WHERE SQUARE(area) = 4.0;
```

```
SELECT TOP 2 angle, SYSDATE() FROM Trapezoids WHERE SQUARE(area) = 9.0;
```