<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `DATEADD` #

```
DATEADD(char(n) datepart, number int, dt datetime) 
```

Returns a datetime with the specified number interval added to the specified date part.

### Valid datepart ###

| **datepart** | **Abbreviations** |
|:-------------|:------------------|
| year         | yy, yyyy          |
| quarter      | quarter           |
| month        | m, mm             |
| day          | dd, d             |
| week         | wk, ww            |
| hour         | hh                |
| minute       | mi, n             |
| second       | ss, s             |
| millisecond  | ms                |

### Examples ###

```
SELECT DATEADD('year', 1, hireDate) FROM Employees WHERE EmployeeID = 900;
```

```
SELECT TOP 100 * FROM Employees WHERE DATEDIFF('year', DATEADD('month', 6, hireDate), '1/1/2000') = 2;
```

# `DATEDIFF` #

```
DATEDIFF(char(n) datepart, dt1 datetime, dt2 datetime) 
```

Returns a double that represents the difference in the specified datepart between datetime1 and datetime2.

### Valid datepart ###

| **datepart** | **Abbreviations** |
|:-------------|:------------------|
| year         | yy, yyyy          |
| quarter      | quarter           |
| month        | m, mm             |
| day          | dd, d             |
| week         | wk, ww            |
| hour         | hh                |
| minute       | mi, n             |
| second       | ss, s             |
| millisecond  | ms                |

### Examples ###

```
SELECT DATEDIFF('month', hireDate, terminateDate) FROM Employees WHERE active = 0;
```

```
SELECT TOP 100 * FROM Employees WHERE DATEDIFF('year', DATEADD('month', 6, hireDate), '1/1/2000') = 2;
```

# `DEGREES` #

```
DEGREES(numeric_expression) 
```

Returns the corresponding angle in degrees for an angle specified in radians.Return type is double.

### Examples ###

```
SELECT TOP 10 * FROM Triangles WHERE DEGREES(angle) = 90.0;
```

```
SELECT TOP 2 angle, DEGREES(angle) FROM Trapezoids WHERE x > 10;
```

