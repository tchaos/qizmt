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
        public static void DbAggregators_MIN(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing MIN(datetime)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, MIN(bday) from " + tablename + " group by id";

                        Dictionary<int, DateTime> expected = new Dictionary<int, DateTime>();
                        expected[10] = DateTime.Parse("10/3/1900 10:01:01 PM");
                        expected[20] = DateTime.Parse("1/2/2000 10:00:00 AM");
                        expected[30] = DateTime.Parse("1/2/2000 10:00:00 AM");

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            DateTime min = reader.GetDateTime(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from MIN group by is invalid.");
                            }
                            if (expected[id] != min)
                            {
                                throw new Exception("Expected MIN: " + expected[id].ToString() + ", but got " + min.ToString() + " instead.");
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