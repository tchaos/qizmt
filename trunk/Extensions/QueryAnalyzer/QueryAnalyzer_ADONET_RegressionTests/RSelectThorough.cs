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
        public static void RSelectThorough()
        {
            string tablename = "regression_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenameSorted = "regression_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string indexname = Guid.NewGuid().ToString().Replace("-", "") + "apple";
            string tablenamedummy = "regression_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string indexnamedummy = Guid.NewGuid().ToString().Replace("-", "");
            Dictionary<long, List<KeyValuePair<long, long>>> expected = new Dictionary<long, List<KeyValuePair<long, long>>>();

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

                    Random rnd = new Random();
                    int rowcount = 5000;
                    int min = -1000;
                    int max = 1000;
                    for (int i = 0; i < rowcount; i++)
                    {
                        num1.Value = rnd.Next(min, max);
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
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "select * from " + tablenameSorted;
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

                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenamedummy + " (num1 LONG, num2 LONG, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenamedummy + " VALUES (1, 1, 1)";
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
                Console.WriteLine("Completed preparing data.");
            }

            {
                Console.WriteLine("Creating RIndexes...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname + " FROM " + tablenamedummy;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "CREATE RINDEX " + indexnamedummy + " FROM " + tablenameSorted;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "ALTER RINDEX " + indexnamedummy + " RENAME SWAP " + indexname;
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            {
                Console.WriteLine("Querying data using RSELECT...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                foreach (long key in expected.Keys)
                {
                    Dictionary<long, List<KeyValuePair<long, long>>> results = new Dictionary<long, List<KeyValuePair<long, long>>>();
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key.ToString();
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            long num1 = reader.GetInt64(0);
                            long num2 = reader.GetInt64(1);
                            long num3 = reader.GetInt64(2);
                            if (!results.ContainsKey(num1))
                            {
                                results.Add(num1, new List<KeyValuePair<long, long>>());
                            }
                            results[num1].Add(new KeyValuePair<long, long>(num2, num3));
                        }
                        reader.Close();

                        if (!results.ContainsKey(key))
                        {
                            throw new Exception("RSelect did not return rows with key = " + key.ToString());
                        }

                        List<KeyValuePair<long, long>> resultlist = results[key];
                        List<KeyValuePair<long, long>> expectedlist = expected[key];

                        if (resultlist.Count != expectedlist.Count)
                        {
                            throw new Exception(resultlist.Count.ToString() + " number of rows are returned from RSELECT.  This number did not match that of expected results: " + expectedlist.Count.ToString());
                        }

                        foreach (KeyValuePair<long, long> rpair in resultlist)
                        {
                            bool found = false;
                            foreach (KeyValuePair<long, long> xpair in expectedlist)
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

                        foreach (KeyValuePair<long, long> xpair in expectedlist)
                        {
                            bool found = false;
                            foreach (KeyValuePair<long, long> rpair in resultlist)
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
                }
                conn.Close();
            }

            Console.WriteLine("RSelectThorough passed");

            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop table " + tablenameSorted;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop table " + tablenamedummy;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexnamedummy;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}