<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Functions](MySpaceQizmtSQLReferenceFunction.md)



# `UPPER` #

```
UPPER(char(n)) 
```

Returns a character expression with lowercase character data converted to uppercase.

### Examples ###

```
SELECT TOP 10 * FROM Employees WHERE UPPER(Name) = 'JOHN' ORDER BY Name;
```

```
SELECT TOP 2 * FROM colors WHERE UPPER(color) = 'RED' ORDER BY color;
```



