<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `RADIANS` #

```
RADIANS(numeric_expression)) 
```

Returns radians when a numeric expression, in degrees, is entered.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE RADIANS(angle) = 0.0;
```

```
SELECT TOP 2 RADIANS(angle), angle FROM Trapezoids;
```

# `RAND` #

```
RAND() 
```

Returns a double number greater than or equal to 0.0, and less than 1.0.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE area = RAND();
```

```
SELECT TOP 2 x, y, RAND() FROM Trapezoids WHERE x > y;
```

# `REPLACE` #

```
REPLACE(str, pattern, replacement) 
```

Replaces all occurrences of a specified string value with another string value.

### Examples ###

```
SELECT TOP 10 REPLACE(Title, 'Trainer', 'T.R.') FROM Employees;
```

```
SELECT TOP 2 REPLACE(Name, 'v2', '-') FROM colors;
```

# `REVERSE` #

```
REVERSE(char(n)) 
```

Returns the reverse of a string value.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE REVERSE(Name) = 'yraM' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE REVERSE(color) = 'deR' ORDER BY color;
```

# `RIGHT` #

```
RIGHT(char(n), int) 
```

Returns the right part of a character string with the specified number of characters.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE RIGHT(Name, 4) = 'John' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE RIGHT(color, 3) = 'Red' ORDER BY color;
```

# `ROUND` #

```
ROUND(double, int) 
```

Returns a double, rounded to the specified length or precision in int.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE ROUND(area, 1) = 10.0;
```

```
SELECT TOP 2 ROUND(area, 2) FROM Trapezoids WHERE area < 10;
```


# `RPAD` #

```
RPAD(str, length, padstr) 
```

Right pad str to the specified length using padstr. Returns the padded string.

### Examples ###

```
SELECT TOP 10 RPAD(Title, 100, '- ') FROM Employees;
```

```
SELECT * FROM colors WHERE RPAD(Name, 10, '. ') = 'Red……. ';
```

# `RTRIM` #

```
RTRIM(char(n))
```

Returns a character string after truncating all trailing blanks.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE RTRIM(Name) = 'john' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE RTRIM(color) = 'red' ORDER BY color;
```

