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
        public static void DbAggregators_MAX(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing MAX(datetime)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, MAX(bday) from " + tablename + " group by id";

                        Dictionary<int, DateTime> expected = new Dictionary<int, DateTime>();
                        expected[10] = DateTime.Parse("1/2/2000 10:00:00 AM");
                        expected[20] = DateTime.Parse("3/4/2001 7:00:00 PM");
                        expected[30] = DateTime.Parse("9/10/2008 9:00:00 PM");

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            DateTime max = reader.GetDateTime(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from max group by is invalid.");
                            }
                            if (expected[id] != max)
                            {
                                throw new Exception("Expected max: " + expected[id].ToString() + ", but got " + max.ToString() + " instead.");
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