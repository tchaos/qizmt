<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `SELECT` #

```
SELECT  [TOP <limit>] 
        <[table.]columns/nested scalars/aggregates> 
        FROM [<table_name> 
        [INNER JOIN | LEFT OUTER JOIN | RIGHT OUTER JOIN]…] 
        [WHERE <[table.]comparison/nested scalars/aggregates > 
        [AND <...> | OR <…>]...] 
        [[ORDER BY <[table.]columns>] | [ORDER BY <[table.]columns>]…];
```


SELECT statements are translated into a series of mapreducers on the tables. For example, if performing a complex select on 10TB of data to obtain the top 10 rows, the generated mapreducers will apply the operation on the entire 10TB of data. Indexes, etc. are not considered when executing this statement. It is simple translation to brute force mapreduce.

| **Senario** | **Mapreduce Translation** |
|:------------|:--------------------------|
| SIMPLE SELECT | 1 mapreduce               |
| SELECT that JOINs 2 tables | 2 mapreducers             |
| SELECT that JOINs 5 tables | 4 mapreducers             |
| SELECT with ORDER BY or GROUP BY clause | 2 mapreducers             |
| SELECT with ORDER BY and GROUP BY clause | 3 mapreducers             |
| SELECT TOP that JOINs 5 tables with ORDER BY clause and GROUP BY clause | 7 mapreducers             |
| SELECT with lots of nested aggregate and scalar functions | No impact on total number of resulting mapreducers.  |

It is often better to write custom mapreducers when a SQL query is going to create many of them as the mapreduce solution will likely be more optimal. See the Qizmt Quick Start Guide for a walkthrough on Qizmt mapreduce development.

### Examples ###

```
SELECT TOP 10 Name FROM Employees WHERE Years = 4 ORDER BY Name;
```

```
SELECT TOP 2 UPPER(color),red FROM colors WHERE red = 34 ORDER BY color;
```

```
SELECT TOP 1 * FROM Sys.Tables WHERE Table = 'Employees' ORDER BY Table;
```

```
SELECT TOP 10 * FROM Employees INNER JOIN Jobs ON Employees.JobID = Jobs.ID;
```

```
SELECT TOP 2 * FROM colors LEFT OUTER JOIN decorations ON colors.ID = decorations.colorID WHERE red > 100 ORDER BY color;
```

```
SELECT TOP 100
        FIRST(LargeSQL_Employees.ID),
        CONCAT(CONCAT(SUBSTRING(LAST(FirstName),0,1),'. '),LAST(LastName)),
        FIRST(LargeSQL_Jobs.Name),
        FIRST(YearHired),
        ROUND(DEGREES(ACOS(ASIN(ATN2(ATAN(ASIN(
        COT(COS(SIN(TAN(FIRST(1.1))))))),PI())))),2),
        LEN(RPAD(LPAD(REVERSE(LOWER(UPPER(RTRIM(
        LTRIM(REPLACE(SUBSTRING(LAST(FirstName),1,5),'-',' ')))))),10,' '),10,' ')),
        FLOOR(LOG10(POWER(SQRT(EXP(CEILING(
        TRUNC(ABS(LOG(RADIANS(CHOOSERND(5.5)))),1)))),1.1))),
        PATINDEX('e%e', LAST(LastName)),
        INSTR('ee', LAST(LastName)),
        AVG(LEN(LargeSQL_Jobs.Name)),
        MIN(LEN(LargeSQL_Jobs.Name)),
        MAX(LEN(LargeSQL_Jobs.Name)),
        BIT_AND(LEN(LargeSQL_Jobs.Name)),
        BIT_OR(LEN(LargeSQL_Jobs.Name)),
        BIT_XOR(LEN(LargeSQL_Jobs.Name)),
        STD(LEN(LargeSQL_Jobs.Name)),
        STD_SAMP(LEN(LargeSQL_Jobs.Name)),
        VAR_POP(LEN(LargeSQL_Jobs.Name)),
        VAR_SAMP(LEN(LargeSQL_Jobs.Name)),
        CHARINDEX('ee', LAST(LastName))
    FROM LargeSQL_Employees
    INNER JOIN LargeSQL_Jobs ON LargeSQL_Employees.JobID = LargeSQL_Jobs.ID
    WHERE
        SUBSTRING(LOWER(LargeSQL_Jobs.Name),1,3) = UPPER('ann')
        OR LargeSQL_Jobs.Name LIKE '%teacher%'
        OR YearHired >= 2000
        OR (YearHired < 1900 AND NOT (NOT MiddleInitial LIKE '%e%'))
    GROUP BY LargeSQL_Employees.ID
    ORDER BY LastName,FirstName,MiddleInitial,LargeSQL_Employees.ID

```
