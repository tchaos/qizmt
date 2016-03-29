<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `NEXT_DAY` #

```
NEXT_DAY(datetime) 
```

Returns the next day of the specified datetime.

### Examples ###

```
SELECT TOP 10 * NEXT_DAY(payDate) FROM Employees;
```

```
SELECT TOP 100 * NEXT_DAY(hireDate) FROM Employees;
```

# `NOTEQUAL` #

```
NOTEQUAL(expression1, expression2) 
```

Compares expression1 with expression2.Returns 1 if they are not equal, otherwise, returns 0. Return type is int.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE NOTEQUAL(Years, 4) = 1 ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE NOTEQUAL(red, blue) = 1 ORDER BY color;
```

# `NULLIF` #

```
NULLIF(expression1, expression2) 
```

Returns a null value of the type of expression1 if expression1 is equal to expression2, otherwise returns expression1.

### Examples ###

```
SELECT TOP 10 * NULLIF(hireDate, terminateDate) FROM Employee;
```

```
SELECT TOP 2 * NULLIF(red, blue) FROM colors;
```

# `NVL` #

```
NVL(expression1, expression2) 
```

Returns expression2 if expression1 is null, otherwise returns expression1.

### Examples ###

```
SELECT TOP 10 * NVL(MiddleName, ' ') FROM Employees;
```

```
SELECT TOP 10 * NVL(Address, 'None') FROM Employees;
```

# `NVL2` #

```
NVL2(expression1, expression2, expression3) 
```

Returns expression3 if expression1 is null, otherwise returns expression2.

### Examples ###

```
SELECT TOP 10 * NVL2(MiddleName, MiddleName, ' ') FROM Employees;
```

```
SELECT TOP 10 * NVL2(Address, Address, 'None') FROM Employees;
```


