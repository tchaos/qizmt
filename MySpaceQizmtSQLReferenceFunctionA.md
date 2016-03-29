<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `ABS` #

```
ABS(numeric_expression) 
```

A mathematical function that returns the absolute (positive) value of the specified numeric expression. Returns the same type as numeric\_expression.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE ABS(Years) = 4 ORDER BY Name;
```

```
SELECT TOP 2 name, blue, ABS(red) FROM colors WHERE blue = 34 ORDER BY color;
```

# `ACOS` #

```
ACOS(numeric_expression) 
```

A mathematical function that returns the angle, in radians, whose cosine is the specified numeric expression; also called arccosine. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE ACOS(angle) = 0.0;
```

```
SELECT TOP 2 ACOS(angle) FROM Trapezoids WHERE base = 10;
```

# `ADD_MONTHS` #

```
ADD_MONTHS(datetime, int) 
```

Returns a datetime with the specified months added.

### Examples ###

```
SELECT TOP 100 * FROM Employees WHERE ADD_MONTHS(hireDate, 1) = '1/1/2000';
```

```
SELECT Name, ADD_MONTHS(hireDate, -6) FROM Employees where EmployeeID = 900;
```

# `ASIN` #

```
ASIN(numeric_expression) 
```

Returns the angle, in radians, whose sine is the specified numeric expression. This is also called arcsine. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE ASIN(angle) = 0.0;
```

```
SELECT TOP 2 ASIN(angle) FROM Trapezoids WHERE base = 20;
```

# `ATAN` #

```
ATAN(numeric_expression) 
```

Returns the angle in radians whose tangent is a specified numeric expression. This is also called arctangent. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE ATAN(angle) = 0.0;
```

```
SELECT TOP 2 ATAN(angle) FROM Trapezoids WHERE base < 190;
```

# `ATN2` #

```
AT2N(numeric_expression, numeric_expression) 
```

Returns the angle, in radians, between the positive x-axis and the ray from the origin to the point (y, x), where x and y are the values of the two specified numeric expressions. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE ATN2(x, y) = 0.0;
```

```
SELECT TOP 2 ATN2(x, y) FROM Trapezoids WHERE x > 15;
```