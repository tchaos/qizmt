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
        private static double var(double[] values, bool sample)
        {
            double sum = 0;
            foreach (double v in values)
            {
                sum += v;
            }

            double avg = sum / (double)values.Length;

            double dev = 0;
            foreach (double v in values)
            {
                dev += Math.Pow(v - avg, 2);
            }
            return dev / (double)(sample ? values.Length - 1 : values.Length);
        }

        private static double std(double[] values, bool sample)
        {
            return Math.Sqrt(var(values, sample));
        }

        public static void DbAggregators_STD(string tablename, bool sample)
        {
            string aggname = "std";
            if (sample)
            {
                aggname = "std_samp";
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
                        expected[100] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10);
                        expected[200] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10);
                        expected[400] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20);
                        values.Add(30);
                        expected[705] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20);
                        expected[900] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(30);
                        expected[1000] = std(values.ToArray(), sample);

                        Dictionary<long, int> resultCount = new Dictionary<long, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            long costl = reader.GetInt64(0);
                            double sd = reader.GetDouble(1);
                            if (!expected.ContainsKey(costl))
                            {
                                throw new Exception("costl returned from std group by is invalid.");
                            }
                            if (!Utils.IsEqual(sd, expected[costl]))
                            {
                                throw new Exception("Expected std: " + expected[costl].ToString() + ", but got " + sd.ToString() + " instead.");
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
                        expected[100] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(10.02);
                        expected[200] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(7.8);
                        expected[400] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20.3);
                        values.Add(7.8);
                        expected[705] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(9.78);
                        expected[900] = std(values.ToArray(), sample);

                        values.Clear();
                        values.Add(20.3);
                        expected[1000] = std(values.ToArray(), sample);

                        Dictionary<long, int> resultCount = new Dictionary<long, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            long costl = reader.GetInt64(0);
                            double sd = reader.GetDouble(1);
                            if (!expected.ContainsKey(costl))
                            {
                                throw new Exception("costl returned from std group by is invalid.");
                            }
                            if (!Utils.IsEqual(sd, expected[costl]))
                            {
                                throw new Exception("Expected std: " + expected[costl].ToString() + ", but got " + sd.ToString() + " instead.");
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