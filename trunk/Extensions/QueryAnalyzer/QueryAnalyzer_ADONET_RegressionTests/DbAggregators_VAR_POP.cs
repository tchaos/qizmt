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
        public static void DbAggregators_VAR(string tablename, bool sample)
        {
            string aggname = "var_pop";
            if (sample)
            {
                aggname = "var_samp";
            }
         
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing " + aggname + "(int)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select costl, " + aggname + "(id) from " + tablename + " group by costl";

                        Dictionary<long, double> expected = new Dictionary<long, double>();
                        List<double> values = new List<double>();
                        values.Add(10);
                        values.Add(30);
                        expected[100] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10);
                        expected[200] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10);
                        expected[400] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20);
                        values.Add(30);
                        expected[705] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20);
                        expected[900] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(30);
                        expected[1000] = var(values.ToArray(), sample);

                        Dictionary<long, int> resultCount = new Dictionary<long, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            long costl = reader.GetInt64(0);
                            double sd = reader.GetDouble(1);
                            if (!expected.ContainsKey(costl))
                            {
                                throw new Exception("costl returned from var group by is invalid.");
                            }
                            if (!Utils.IsEqual(sd, expected[costl]))
                            {
                                throw new Exception("Expected var: " + expected[costl].ToString() + ", but got " + sd.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(costl))
                            {
                                resultCount[costl] = 0;
                            }
                            resultCount[costl]++;
                        }
                        reader.Close();

                        int count = 0;
                        foreach (int c in resultCount.Values)
                        {
                            count += c;
                        }

                        if (count != 6)
                        {
                            throw new Exception("Expected row count: 6, but got " + count.ToString() + " instead.");
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
                        Console.WriteLine("Testing " + aggname + "(double)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select costl, " + aggname + "(cost) from " + tablename + " group by costl";

                        Dictionary<long, double> expected = new Dictionary<long, double>();
                        List<double> values = new List<double>();
                        values.Add(9.1);
                        values.Add(7.8);
                        expected[100] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10.02);
                        expected[200] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(7.8);
                        expected[400] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20.3);
                        values.Add(7.8);
                        expected[705] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(9.78);
                        expected[900] = var(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20.3);
                        expected[1000] = var(values.ToArray(), sample);

                        Dictionary<long, int> resultCount = new Dictionary<long, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            long costl = reader.GetInt64(0);
                            double sd = reader.GetDouble(1);
                            if (!expected.ContainsKey(costl))
                            {
                                throw new Exception("costl returned from var group by is invalid.");
                            }
                            if (!Utils.IsEqual(sd, expected[costl]))
                            {
                                throw new Exception("Expected var: " + expected[costl].ToString() + ", but got " + sd.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(costl))
                            {
                                resultCount[costl] = 0;
                            }
                            resultCount[costl]++;
                        }
                        reader.Close();

                        int count = 0;
                        foreach (int c in resultCount.Values)
                        {
                            count += c;
                        }

                        if (count != 6)
                        {
                            throw new Exception("Expected row count: 6, but got " + count.ToString() + " instead.");
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