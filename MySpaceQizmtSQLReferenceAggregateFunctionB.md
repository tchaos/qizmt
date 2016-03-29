<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Aggregate Functions](MySpaceQizmtSQLReferenceAggregateFunction.md)



# `BIT_AND` #

```
BIT_AND(expression)
```

Performs bitwise AND operations on the values in a group.

### Examples ###

```
SELECT angle, BIT_AND(length) FROM Triangles GROUP BY angle;
```

```
SELECT angle, BIT_AND(base) FROM Trapezoids GROUP BY angle;
```

# `BIT_OR` #

```
BIT_OR(expression)
```

Performs bitwise OR operations on the values in a group.

### Examples ###

```
SELECT angle, BIT_OR(length) FROM Triangles GROUP BY angle;
```

```
SELECT angle, BIT_OR(base) FROM Trapezoids GROUP BY angle;
```

# `BIT_XOR` #

```
BIT_XOR(expression)
```

Performs bitwise exclusive OR operations on the values in a group.

### Examples ###

```
SELECT angle, BIT_XOR(length) FROM Triangles GROUP BY angle;
```

```
SELECT angle, BIT_XOR(base) FROM Trapezoids GROUP BY angle;
```



