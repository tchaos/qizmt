<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `CAST` #

```
Converts a value from one type to another.
```

Converts a value from one type to another.

### Examples ###

```
SELECT TOP 10 CAST(x AS CHAR(20)) FROM Triangles WHERE ATN2(x, y) = 0.0;
```

```
SELECT TOP 2 CAST(x AS LONG) FROM Trapezoids WHERE ATN2(x, y) = 0.0;
```

# `CEILING` #

```
CEILING(double) 
```

Returns the smallest integral value that is greater than or equal to the specified double number. Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE CEILING(area) = 10.0;
```

```
SELECT TOP 2 y, CEILING(area) FROM Trapezoids WHERE y > 90;
```

# `CHARINDEX` #

```
CHARINDEX(expression1, expression2 [, start_location]) 
```

Searches expression2 for expression1 and returns its starting position if found. The search starts at start\_location (0-index based).It expression1 is not found, -1 is returned.Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE CHARINDEX('Mr', title) = 0 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE CHARINDEX('P', color) = 0 ORDER BY color;
```

# `COMPARE` #

```
COMPARE(expression1, expression2) 
```

Compares expression1 to expression2.  Returns 0 if they are the same, less than 0 if expression1 is less than expression2, greater than 0 if expression1 is greater than expression2.

### Examples ###

```
SELECT TOP 100 * FROM Employees WHERE COMPARE(EmployeeID, 900) = 0;
```

```
SELECT TOP 100 * FROM colors WHERE COMPARE(red, 0) = 0;
```

# `CONCAT` #

```
CONCAT(char(n), char(m))
```

Returns a concatenated string.

### Examples ###

```
SELECT CONCAT(Title, CONCAT(', ', Name)) FROM Employees WHERE CHARINDEX('Mr', Title) = 0;
```

```
SELECT CONCAT(DeptName, CONCAT(', ', Title)) FROM Employees;
```

# `COS` #

```
COS(numeric_expression) 
```

A mathematical function that returns the trigonometric cosine of the specified angle, in radians, in specified expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE COS(angle) = 1.0;
```

```
SELECT TOP 2 angle, COS(angle) FROM Trapezoids WHERE x = 100;
```


# `COT` #

```
COT(numeric_expression) 
```

A mathematical function that returns the trigonometric cotangent of the specified angle, in radians, in the specified expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE COT(angle) = 1.0;
```

```
SELECT TOP 2 angle, COT(angle) FROM Trapezoids WHERE base > 89;
```

