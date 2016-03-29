<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `CREATE TABLE` #

```
CREATE TABLE <table_name> (<column_name> <type> [, ...]);
```

Creates a table with application specific schema of columns. Data types supported include:

### CHAR(N) ###

**Description:**  Data-type for a fixed-length string. String length must be <= n. Representing a single quote in a string literal is done by adding a second adjacent single quote. Storage: `((n * 2) + 1)` Bytes

**Usage:**  `'<text>'`;

**Example 1:**  `'hello world'`

**Example 2:**  `'normal ''single quotes'' normal'`

### INT ###

**Description:**  Data-type for a signed 32-bit integer. An integer literal can include a preceding + or - sign character. Range: -2<sup>31</sup> (-2,147,483,648) to 2<sup>31-1</sup> (2,147,483,647). Storage: (4 + 1) Bytes

**Usage:**  `[+|-]<integer>`;

**Example 1:**  `9`

**Example 2:**  `-409983`

### LONG ###

**Description:**  Data-type for a signed 64-bit integer, or long integer. A long integer literal can include a preceding + or - sign character. Range: -2<sup>63</sup> (-9,223,372,036,854,775,808) to 2<sup>63-1</sup> (9,223,372,036,854,775,807).  Storage: (8 + 1) Bytes

**Usage:**  `[+|-]<long_integer>`;

**Example 1:**  `7`

**Example 2:**  `-2021432403507`

### DOUBLE ###

**Description:**  Data-type for a signed 64-bit floating-point number, or double-precision floating-point number. A double literal can include a preceding + or - sign character.Storage: (8 + 1) Bytes

**Usage:**  `[+|-]<dec>.<dec>`;

**Example 1:**  `6.1`

**Example 2:**  `-300.991`

### DATETIME ###

**Description:**  Data-type for date and time. Storage: (8 + 1) Bytes

**Usage:**  `<date time>`;

**Example 1:**  `'8/7/2010 12:38:15 PM'`

**Example 2:**  `'8/7/2009 12:38:15 PM'`

### Examples ###

```
CREATE TABLE Employees (Name char(200), Years int);
```

```
CREATE TABLE colors (color char(50), red int, green int, blue int);
```



