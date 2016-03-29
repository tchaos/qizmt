<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md)


# `BULKUPDATE` #

```
BEGIN BULKUPDATE 
  [RINSERT INTO indTest VALUES (<literal value>,<literal value>,…) WHERE KEY = <literal value>\0]
  |
  [RDELETE FROM indTest WHERE KEY = <literal value> AND <column> = <literal value>]
  .
  .
  .
END BULKUPDATE
```


An order of inserts and deletes are used to perform a low latency update. If the table is in memory, both the memory copy and any replicates on disk are updated before execution returns. If a machine is lost during execution, the remaining machines will be used to fulfill the update and an exception will be thrown.

Only one client may bulk-update an RINDEX at a time and no RSELECTs may be run in parallel.

In order to use this command, the ADO.NET connection string must specify RINDEX=POOLED.


### Examples ###

```
System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

using (DbConnection conn = fact.CreateConnection())
{
    conn.ConnectionString = "Data Source = localhost";
    conn.Open();
    DbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "BEGIN BULKUPDATE\0 " +
        "RDELETE FROM idxGraph WHERE KEY = 1 \0" +
        "RINSERT INTO idxGraph VALUES (2, 'Jane') WHERE KEY = 2 \0" +
        "RDELETE FROM idxGraph WHERE KEY = 3 AND empName = 'Jon' \0" +
        "END BULKUPDATE";
    cmd.ExecuteNonQuery();
    conn.Close();
}
```





