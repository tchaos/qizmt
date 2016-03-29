<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `INSERT` #

```
INSERT  INTO <table_name> 
        SELECT <columns> 
        FROM <table_name> 
        [WHERE <comparison> [AND <...> | OR <…>]...] 
        [ORDER BY <order_columns>];
```


Copy tuples from one or more tables or literal into another table. For inserting literals into another table ad-hoc, see BULK UPDATE. If you have a large amount of data to put into a table, first put it into MR.DFS with then bind it into a table. See fput in qizmt documentation and INSERT BIND commands.


### Examples ###

```
INSERT INTO Employees VALUES('George Jefferson', 4);
```

```
INSERT INTO Employees VALUES('George Jefferson', 4);
```

```
INSERT INTO EmployeeJobs
SELECT
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
