<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension (contd) #



## 14.  Sample Code to Connect via ADO.NET ##

Use regular ADO.NET to connect to the cluster an execute SQL from your Main() function.

```
public static void Main(string[] args)
{
   System.Data.Common.DbProviderFactory fact = 
        System.Data.Common.DbProviderFactories
        .GetFactory("Qizmt_DataProvider");
   using(System.Data.Common.DbConnection conn = fact.CreateConnection())
   {
      conn.ConnectionString = "Data Source = localhost; Batch Size = 64MB";
      conn.Open();
      System.Data.Common.DbCommand cmd = conn.CreateCommand();
      cmd.CommandText = "SELECT TOP 3 * from paintings ORDER BY year";
      System.Data.Common.DbDataReader reader = cmd.ExecuteReader();
      while (reader.Read())
      {
         Console.WriteLine("----------------------------------------");
         Console.WriteLine("num = {0}", (int)reader["paintingID"]);
         Console.WriteLine("num = {0}", (int)reader["year"]);
         Console.WriteLine("num = {0}", (string)reader["title"]);
         Console.WriteLine("num = {0}", (double)reader["size"]);
         Console.WriteLine("num = {0}", (long)reader["pixel"]);
         Console.WriteLine("num = {0}", (int)reader["artistID"]);
      }
      Console.WriteLine("----------------------------------------");
      reader.Close();
      conn.Close();
   }
   System.Console.ReadLine();
}

```


## 15.  Rectangular Binary MR.DFS File ##

Next, to write a _Qizmt mapreducer_ to count the words in all the _title_ field of every tuple of the _paintings_ table. Although mapreduce development is outside the scope of this Guide, it is included in this tutorial for completeness and the steps can be followed as an introduction to mapreduce development. To start, find the _underlying rectangular binary MR.DFS file_ for the _paintings_ table. Do this by issuing the command:


**qizmt dir RDBMS\_Table\_paintings`*`**


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_DirPaintings.png' alt='Qizmt ls' />


Although, two MR.DFS files matched the wild card, the file to use is in the pattern:

`RDBMS_TABLE_<tablename>@<tuplesize>`


[< PREV](MySpaceQizmtSQLQuickStartGuideWalkthrough5.md)
[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough7.md)