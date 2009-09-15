using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        public static void DbAggregators_BIT_OR(string tablename)
        {
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing BIT_OR(long)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, BIT_OR(costl) from " + tablename + " group by id";

                        Dictionary<int, byte[]> expected = new Dictionary<int, byte[]>();
                        List<long> values = new List<long>();
                        values.Add(100);
                        values.Add(200);
                        values.Add(400);
                        expected[10] = bitop(values.ToArray(), 2);

                        values.Clear();
                        values.Add(705);
                        values.Add(900);
                        expected[20] = bitop(values.ToArray(), 2);

                        values.Clear();
                        values.Add(705);
                        values.Add(1000);
                        values.Add(100);
                        expected[30] = bitop(values.ToArray(), 2);
                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            long op = reader.GetInt64(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from BIT_OR group by is invalid.");
                            }
                            long expop = Entry.ToInt64((UInt64)Entry.BytesToLong(expected[id], 0));

                            if (op != expop)
                            {
                                throw new Exception("Expected BIT_OR: " + expop.ToString() + ", but got " + op.ToString() + " instead.");
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