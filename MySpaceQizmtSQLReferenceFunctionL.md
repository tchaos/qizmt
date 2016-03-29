<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `LAST_DAY` #

```
LAST_DAY(datetime)
```

Returns the last day of the month.

### Examples ###

```
SELECT LAST_DAY(payDate) FROM Employees WHERE EmployeeID = 900;
```

```
SELECT LAST_DAY(hireDate) FROM Employees WHERE EmployeeID = 900;
```


# `LEFT` #

```
LEFT(char(n), int) 
```

Returns the left part of a character string with the specified number of characters.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE LEFT(Name, 4) = 'John' ORDER BY Name;
```

```
SELECT TOP 2 blue, LEFT(color, 3) FROM colors WHERE blue > 9 ORDER BY color;
```

# `LEN` #

```
LEN(char(n)) 
```

Returns the number of characters of the specified string expression. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE LEN(Name) = 4 ORDER BY Name;
```

```
SELECT TOP 2 LEN(color), red, blue FROM colors WHERE blue > 10 ORDER BY color;
```


# `LESSER` #

```
LESSER(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if expression1 is less than expression2, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE LESSER(Name, 'M') = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE LESSER(red, blue) = 1 ORDER BY color;
```

# `LESSEREQUAL` #

```
LESSEREQUAL(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if expression1 is less than or equal to expression2, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE LESSEREQUAL(Name, 'M') = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE LESSEREQUAL(red, blue) = 1 ORDER BY color;
```

# `LOG` #

```
LOG(numeric_expression)
```

Returns the natural logarithm of the specified numeric expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE LOG(angle) = 0.0;
```

```
SELECT TOP 2 LOG(angle), x, y FROM Trapezoids WHERE base = 10;
```

# `LOG10` #

```
LOG10(numeric_expression)
```

Returns the base-10 logarithm of the specified numeric expression.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE LOG10(angle) = 0.0;
```

```
SELECT TOP 2 LOG10(angle), x, y FROM Trapezoids WHERE base = 9;
```

# `LOWER` #

```
LOWER(char(n)) 
```

Returns a character expression after converting uppercase character data to lowercase.

### Examples ###

```
SELECT TOP 10 LOWER(Name) FROM Employees WHERE EmployeeID > 90 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE LOWER(color) = 'red' ORDER BY color;
```

# `LPAD` #

```
LPAD(str, length, padstr) 
```

Left pad str to the specified length using padstr. Returns the padded string.

### Examples ###

```
SELECT TOP 10 LPAD(Title, 100, '- ') FROM Employees;
```

```
SELECT TOP 2 * FROM colors WHERE LPAD(Name, 10, '. ') = '…….Blue';
```


# `LTRIM` #

```
LTRIM(char(n)) 
```

Returns a character expression after it removes leading blanks.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE LTRIM(Name) = 'john' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE LTRIM(color) = 'red' ORDER BY color;
```