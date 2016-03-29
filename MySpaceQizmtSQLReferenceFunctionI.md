<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `INSTR` #

```
INSTR(str, substr) 
```

Returns the index of the first occurrence of the substring substr in string str.

### Examples ###

```
SELECT * FROM Employees WHERE GREATER(INSTR(Title, 'Manager'), -1) = 1;
```

```
SELECT * FROM colors WHERE GREATER(INSTR(Name, 'purple'), -1) = 1;
```

# `ISLIKE` #

```
ISLIKE(char(n) text, char(n) pattern) 
```

Determines whether a specific character string matches a specified pattern.

A pattern can include regular characters and wildcard characters.

Wildcard character (%): Matches any string of zero or more characters.


Wildcard character(_): Matches any single character._


Wildcard character([.md](.md)): Matches any single character within the specified range ([a-f]) or set ([abcdef](abcdef.md)).


Wildcard character([<sup>]): Matches any single character not within the specified range ([</sup>a-f]) or set ([^abcdef]).Returns 1 if a match is found, otherwise, returns 0.Return type is int.


### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE ISLIKE(Name, 'john%') = 1 ORDER BY Name;
```

```
SELECT TOP 10 * FROM Employees WHERE ISLIKE(Name, 'john%') = 1 ORDER BY Name;
```


# `ISNOTNULL` #

```
ISNOTNULL(expression) 
```

Returns 1 if the expression is not null, returns 0 otherwise.

### Examples ###

```
SELECT TOP 100 * FROM Employees WHERE ISNOTNULL(terminateDate) = 1;
```

```
SELECT TOP 10 * FROM colors WHERE ISNOTNULL(Name) = 0;
```

# `ISNULL` #

```
ISNULL(expression) 
```

Returns 1 if the expression is null, returns 0 otherwise.

### Examples ###

```
SELECT TOP 100 * FROM Employees WHERE ISNULL(terminateDate) = 0;
```

```
SELECT TOP 10 * FROM colors WHERE ISNULL(Name) = 1;
```


