<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md)


# `RSELECT` #

```
RSELECT * 
        FROM <indexName> 
        [SAMPLE <sampleSize>] 
        WHERE KEY = <keyValue> 
        [OR KEY = <keyValue>]
```



This is a very low latency way to perform ad-hoc queries on very large tables. This can only be done on an RINDEX and the RINDEX can only be created on an already sorted table. (Usually created with a sorted mapreducer job or a SQL with an ORDER BY statement.


In order to use this command, the ADO.NET connection string must specify RINDEX=POOLED.


There is no limit to the number of clients which may RSELECT from an RINDEX in parallel, however RSELECT may not be run in parallel with BULK UPDATE.


The where clause of RSELECT only supports OR statements. The order of tuples returned is random unless KEEPVALUEORDER is specified. If you wish to return a random subset of the tuples, the SAMPLE switch may be used. If a machine is lost during execution, the remaining machines are used to fulfill the query and an exception will be thrown.


### Examples ###

```
System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

using (DbConnection conn = fact.CreateConnection())
{
    conn.ConnectionString = "Data Source = localhost; RINDEX=POOLED";
    conn.Open();
    DbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "RSELECT * FROM idxGraph WHERE KEY = 1 OR KEY = 99 OR KEY = 434928";
    DbDataReader reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        int empID = reader.GetInt32(0);
        string empName = reader.GetString(1);
    }
    reader.Close();
    conn.Close();
}  


```





