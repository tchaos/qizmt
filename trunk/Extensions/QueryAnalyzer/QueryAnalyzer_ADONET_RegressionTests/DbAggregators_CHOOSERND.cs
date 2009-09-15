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
        public static void DbAggregators_CHOOSERND(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing CHOOSERND(datetime)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, CHOOSERND(bday) from " + tablename + " group by id";

                        Dictionary<int, DateTime[]> expected = new Dictionary<int, DateTime[]>();
                        List<DateTime> dt = new List<DateTime>();
                        dt.Add(DateTime.Parse("1/2/2000 10:00:00 AM"));
                        dt.Add(DateTime.Parse("10/3/1900 10:01:01 PM"));
                        dt.Add(DateTime.Parse("5/7/1987 5:00:00 AM"));
                        expected[10] = dt.ToArray();

                        dt.Clear();
                        dt.Add(DateTime.Parse("1/2/2000 10:00:00 AM"));
                        dt.Add(DateTime.Parse("3/4/2001 7:00:00 PM"));
                        expected[20] = dt.ToArray();

                        dt.Clear();
                        dt.Add(DateTime.Parse("3/4/2001 7:00:00 PM"));
                        dt.Add(DateTime.Parse("9/10/2008 9:00:00 PM"));
                        dt.Add(DateTime.Parse("1/2/2000 10:00:00 AM"));
                        expected[30] = dt.ToArray();

                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            DateTime first = reader.GetDateTime(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from CHOOSERND group by is invalid.");
                            }
                            bool found = false;
                            for (int i = 0; i < expected[id].Length; i++)
                            {
                                if (expected[id][i] == first)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("Invalid CHOOSERND received: " + first.ToString() + " instead.");
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