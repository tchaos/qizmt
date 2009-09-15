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
        public static void DbAggregators_AVG(string tablename)
        {           
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing avg(long)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, avg(costl) from " + tablename + " group by id";

                        Dictionary<int, double> expected = new Dictionary<int, double>();
                        expected[10] = (double)(100 + 200 + 400) / 3d;
                        expected[20] = (double)(705 + 900) / 2d;
                        expected[30] = (double)(100 + 705 + 1000) / 3d;

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            double avg = reader.GetDouble(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from avg group by is invalid.");
                            }
                            if (!Utils.IsEqual(avg, expected[id]))
                            {
                                throw new Exception("Expected avg: " + expected[id].ToString() + ", but got " + avg.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(id))
                            {
                                resultCount[id] = 0;
                            }
                            resultCount[id]++;
                        }
                        reader.Close();

                        int count = 0;
                        foreach (int c in resultCount.Values)
                        {
                            count += c;
                        }

                        if (count != 3)
                        {
                            throw new Exception("Expected row count: 3, but got " + count.ToString() + " instead.");
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
                        Console.WriteLine("Testing avg(double)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, avg(cost) from " + tablename + " group by id";

                        Dictionary<int, double> expected = new Dictionary<int, double>();
                        expected[10] = (double)(9.1 + 10.02 + 7.8) / 3d;
                        expected[20] = (double)(20.3 + 9.78) / 2d;
                        expected[30] = (double)(7.8 + 7.8 + 20.3) / 3d;

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            double avg = reader.GetDouble(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from avg group by is invalid.");
                            }
                            if (!Utils.IsEqual(avg, expected[id]))
                            {
                                throw new Exception("Expected avg: " + expected[id].ToString() + ", but got " + avg.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(id))
                            {
                                resultCount[id] = 0;
                            }
                            resultCount[id]++;
                        }
                        reader.Close();

                        int count = 0;
                        foreach (int c in resultCount.Values)
                        {
                            count += c;
                        }

                        if (count != 3)
                        {
                            throw new Exception("Expected row count: 3, but got " + count.ToString() + " instead.");
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