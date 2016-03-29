<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `FLOOR` #

```
FLOOR(double) 
```

Returns the largest integer less than or equal to the specified double number.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE FLOOR(area) = 10.0;
```

```
SELECT TOP 2 * FROM Trapezoids WHERE FLOOR(area) = 1.0;
```

# `FORMAT` #

```
FORMAT(char(n) format, dt datetime)
```

Returns a string representation of the datetime.

### Valid Format ###

This is the same used for C# `DateTime` format.

### Examples ###

```
SELECT TOP 10 FORMAT('dd/MM/yy', hireDate) FROM Employees;
```

```
SELECT TOP 10 * FROM Employees WHERE FORMAT('yyyy', hireDate) = '1999';
```


