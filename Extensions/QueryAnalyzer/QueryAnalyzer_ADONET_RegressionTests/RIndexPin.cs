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
        public static void RIndexPin()
        {
            string tablename = "rselect_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenameSorted = "rselect_test_sorted" + Guid.NewGuid().ToString().Replace("-", "");
            string indexname = Guid.NewGuid().ToString().Replace("-", "") + "apple";           
            Dictionary<long, List<KeyValuePair<long, long>>> expected = new Dictionary<long, List<KeyValuePair<long, long>>>();
            Dictionary<long, int> testvalues = new Dictionary<long, int>(5);
            const int TESTSIZE = 5;

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            {
                Console.WriteLine("Preparing data...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablename + " (num1 LONG, num2 LONG, num3 LONG)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num1, @num2, @num3)";

                    DbParameter num1 = cmd.CreateParameter();
                    num1.ParameterName = "@num1";
                    num1.DbType = DbType.Int64;

                    DbParameter num2 = cmd.CreateParameter();
                    num2.ParameterName = "@num2";
                    num2.DbType = DbType.Int64;

                    DbParameter num3 = cmd.CreateParameter();
                    num3.ParameterName = "@num3";
                    num3.DbType = DbType.Int64;

                    Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    int rowcount = 5000;// (1024 * 1024 * 10) / (9 * 3);
                    int min = -100;
                    int max = 100;
                    for (int i = 0; i < rowcount; i++)
                    {
                        long key = rnd.Next(min, max);
                        if (testvalues.Count < TESTSIZE && !testvalues.ContainsKey(key))
                        {
                            testvalues.Add(key, 0);
                        }
                        num1.Value = key;
                        num2.Value = rnd.Next(min, max);
                        num3.Value = rnd.Next(min, max);
                        cmd.ExecuteNonQuery();
                    }
                }
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenameSorted + " (num1 LONG, num2 LONG, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenameSorted + " SELECT * FROM " + tablename + " ORDER BY num1";
                    cmd.ExecuteNonQuery();
                }
                ///////! cannot create index here.  
                conn.Close();
                Console.WriteLine("Data prepared.");
            }

            {
                Console.WriteLine("Querying data using SELECT...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                foreach (long key in testvalues.Keys)
                {
                    cmd.CommandText = "select * from " + tablenameSorted + " where num1 = " + key.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        long num1 = reader.GetInt64(0);
                        long num2 = reader.GetInt64(1);
                        long num3 = reader.GetInt64(2);
                        if (!expected.ContainsKey(num1))
                        {
                            expected.Add(num1, new List<KeyValuePair<long, long>>());
                        }
                        expected[num1].Add(new KeyValuePair<long, long>(num2, num3));
                    }
                    reader.Close();
                }
                conn.Close();
                Console.WriteLine("SELECT completed.");
            }

            {
                Console.WriteLine("Creating RIndexes...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname + " FROM " + tablenameSorted + " PINMEMORY";
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created pinned.");
            }

            Console.WriteLine("Querying data using RSELECT...");
            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                foreach (long key in expected.Keys)
                {
                    List<KeyValuePair<long, long>> results = new List<KeyValuePair<long, long>>();
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        long num1 = reader.GetInt64(0);
                        long num2 = reader.GetInt64(1);
                        long num3 = reader.GetInt64(2);
                        results.Add(new KeyValuePair<long, long>(num2, num3));
                    }
                    reader.Close();

                    //compare results
                    List<KeyValuePair<long, long>> xlist = expected[key];
                    if (xlist.Count != results.Count)
                    {
                        throw new Exception("Result count: " + results.Count.ToString() + " is different from that of expected: " + xlist.Count.ToString());
                    }
                    foreach (KeyValuePair<long, long> rpair in results)
                    {
                        bool found = false;
                        foreach (KeyValuePair<long, long> xpair in xlist)
                        {
                            if (rpair.Key == xpair.Key && rpair.Value == xpair.Value)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            throw new Exception("RSelect returned a row which was not located in expected results. num2=" + rpair.Key.ToString() + " num3=" + rpair.Value.ToString());
                        }
                    }
                    foreach (KeyValuePair<long, long> xpair in xlist)
                    {
                        bool found = false;
                        foreach (KeyValuePair<long, long> rpair in results)
                        {
                            if (rpair.Key == xpair.Key && rpair.Value == xpair.Value)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            throw new Exception("RSelect did not return an expected row. num2=" + xpair.Key.ToString() + " num3=" + xpair.Value.ToString());
                        }
                    }
                }
                conn.Close();
            }
            Console.WriteLine("RIndexPin completed.");

            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop table " + tablenameSorted;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
