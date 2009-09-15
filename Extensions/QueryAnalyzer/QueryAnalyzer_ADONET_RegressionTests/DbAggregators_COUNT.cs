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
        public static void DbAggregators_COUNT(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing count(int)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, count(id) from " + tablename + " group by id";

                        Dictionary<int, int> expected = new Dictionary<int, int>();
                        expected[10] = 3;
                        expected[20] = 2;
                        expected[30] = 3;

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            int count = reader.GetInt32(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from count group by is invalid.");
                            }
                            if(expected[id] != count)
                            {
                                throw new Exception("Expected count: " + expected[id].ToString() + ", but got " + count.ToString() + " instead.");
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
                        Console.WriteLine("Testing count(char(n))...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select name, count(name) from " + tablename + " group by name";

                        Dictionary<string, int> expected = new Dictionary<string, int>();
                        expected["x"] = 1;
                        expected["p"] = 3;
                        expected["h"] = 1;
                        expected["o"] = 1;
                        expected["j"] = 1;
                        expected["k"] = 1;

                        Dictionary<string, int> resultCount = new Dictionary<string, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            string name = reader.GetString(0);
                            int count = reader.GetInt32(1);
                            if (!expected.ContainsKey(name))
                            {
                                throw new Exception("name returned from count group by is invalid.");
                            }
                            if (expected[name] != count)
                            {
                                throw new Exception("Expected count: " + expected[name].ToString() + ", but got " + count.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(name))
                            {
                                resultCount[name] = 0;
                            }
                            resultCount[name]++;
                        }
                        reader.Close();

                        int cnt = 0;
                        foreach (int c in resultCount.Values)
                        {
                            cnt += c;
                        }

                        if (cnt != 6)
                        {
                            throw new Exception("Expected row count: 6, but got " + cnt.ToString() + " instead.");
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