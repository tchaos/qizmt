using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        public static void DbAggregators_COUNTDISTINCT(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing countdistinct(int)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, countdistinct(id) from " + tablename + " group by id";

                        Dictionary<int, int> expected = new Dictionary<int, int>();
                        expected[10] = 1;
                        expected[20] = 1;
                        expected[30] = 1;

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            int count = (int)reader.GetInt64(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from countdistinct group by is invalid.");
                            }
                            if (expected[id] != count)
                            {
                                throw new Exception("Expected countdistinct: " + expected[id].ToString() + ", but got " + count.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(id))
                            {
                                resultCount[id] = 0;
                            }
                            resultCount[id]++;
                        }
                        reader.Close();

                        int cnt = 0;
                        foreach (int c in resultCount.Values)
                        {
                            cnt += c;
                        }

                        if (cnt != 3)
                        {
                            throw new Exception("Expected row count: 3, but got " + cnt.ToString() + " instead.");
                        }
                        Console.WriteLine("Expected results received.");
                    }
                }
                finally
                {
                    conn.Close();
                }
            }

            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing countdistinct(double)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, countdistinct(cost) from " + tablename + " group by id";

                        Dictionary<int, int> expected = new Dictionary<int, int>();
                        expected[10] = 3;
                        expected[20] = 2;
                        expected[30] = 2;

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            int count = (int)reader.GetInt64(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from countdistinct group by is invalid.");
                            }
                            if (expected[id] != count)
                            {
                                throw new Exception("Expected countdistinct: " + expected[id].ToString() + ", but got " + count.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(id))
                            {
                                resultCount[id] = 0;
                            }
                            resultCount[id]++;
                        }
                        reader.Close();

                        int cnt = 0;
                        foreach (int c in resultCount.Values)
                        {
                            cnt += c;
                        }

                        if (cnt != 3)
                        {
                            throw new Exception("Expected row count: 3, but got " + cnt.ToString() + " instead.");
                        }
                        Console.WriteLine("Expected results received.");
                    }
                }
                finally
                {
                    conn.Close();
                }
            }
        }
    }
}