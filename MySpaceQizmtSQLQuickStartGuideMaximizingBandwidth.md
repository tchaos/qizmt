<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)


# Maximizing Bandwidth Usage between Data Centers with ADO.NET #


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_Bandwidth.png' alt='Bandwidth' />


Given each server in a data center has a 1, 2 or 5 Gb/s network card, it is generally not possible for a one server on each data center to max out the bandwidth between the data centers. This problem is easily solvable by having every ADO.NET client list most or all of the host names of the target cluster. The _Qizmt ADO.NET Data Provider_ will automatically orchestrate the connectivity for optimal bandwidth use. In this way, the clients writing ADO.NET applications to talk to Qizmt Clusters with SQL do not have to worry about which servers in the remote cluster are up, or which servers in the remote cluster are busy. Rather, they may specify most or all of the hostnames in the remote cluster and let the _Qizmt ADO.NET Data Provider_ manage failover and bandwidth maximization and to avoid bottlenecks.

```

   System.Data.Common.DbProviderFactory fact = 
        System.Data.Common.DbProviderFactories
        .GetFactory("QueryAnalyzer_DataProvider");
   using(System.Data.Common.DbConnection conn = fact.CreateConnection())
   {
      conn.ConnectionString = "Data Source = MACHINE0,MACHINE1,MACHINE2,MACHINE3,
                              MACHINE4,MACHINE5,MACHINE6,MACHINE7,MACHINE8,
                              MACHINE9,MACHINE10,MACHINE11,MACHINE12,MACHINE13,
                              MACHINE14,MACHINE15,MACHINE16,MACHINE17,MACHINE18,
                              .
                              .
                              .
                              MACHINE99; Batch Size = 64MB";
      conn.Open();
```


In this example code, all of the machines in a Qizmt Cluster are listed in the connect string. The ADO.NET Data Provider will select one of these machines on each connect, and if that machine is unavailable, it will failover to another one to use as an entry point in the cluster. Outside of the Qizmt cluster, MSSQL CLR’s, Windows Services, or Web Applications connecting to the same Qizmt Cluster listing the same hostnames in their connection string will also exhibit the same benefits. Connecting to a Qizmt Cluster with ADO.NET, of course, requires that the cluster be installed with the _Qizmt SQL Extension_ first. The ADO.NET client on the other hand needs only install the _Qizmt ADO.NET Data Provider_.