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
        public static void DbAggregators_SUM(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing SUM(int)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select cost, SUM(id) from " + tablename + " group by cost";

                        Dictionary<double, int> expected = new Dictionary<double, int>();
                        expected[9.1] = 10;
                        expected[10.02] = 10;
                        expected[7.8] = 70;
                        expected[20.3] = 50;
                        expected[9.78] = 20;

                        Dictionary<double, int> resultCount = new Dictionary<double, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            double cost = reader.GetDouble(0);
                            int sum = reader.GetInt32(1);
                            if (!expected.ContainsKey(cost))
                            {
                                throw new Exception("cost returned from sum group by is invalid.");
                            }
                            if (expected[cost] != sum)
                            {
                                throw new Exception("Expected sum: " + expected[cost].ToString() + ", but got " + sum.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(cost))
                            {
                                resultCount[cost] = 0;
                            }
                            resultCount[cost]++;
                        }
                        reader.Close();

                        int cnt = 0;
                        foreach (int c in resultCount.Values)
                        {
                            cnt += c;
                        }

                        if (cnt != 5)
                        {
                            throw new Exception("Expected row count: 5, but got " + cnt.ToString() + " instead.");
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