<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `PATINDEX` #

```
PATINDEX(char(n) pattern, char(n) text)
```

Returns the starting position (0-index based) of the first occurrence of a pattern in a specified expression, or -1 if the pattern is not found, on char(n) data types.


A pattern can include regular characters and wildcard characters.


Wildcard character (%): Matches any string of zero or more characters.


Wildcard character(_): Matches any single character._


Wildcard character([.md](.md)): Matches any single character within the specified range ([a-f]) or set ([abcdef](abcdef.md)).


Wildcard character([<sup>]): Matches any single character not within the specified range ([</sup>a-f]) or set ([^abcdef]).


Return type is int.


### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE PATINDEX('%PhD%', Name) = 0 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE PATINDEX('%R%', color) = 0 ORDER BY color;
```

# `PI` #

```
PI()) 
```

Returns the constant value of PI.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE height = PI();
```

```
SELECT TOP 2 * FROM Trapezoids WHERE height = PI();
```

# `POWER` #

```
POWER(numeric_expression, numeric_expression power)) 
```

Returns the value of the specified expression to the specified power.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE POWER(height, 2) = 4.0;
```

```
SELECT TOP 2 POWER(height, 3), area FROM Trapezoids WHERE area > 89;
```


