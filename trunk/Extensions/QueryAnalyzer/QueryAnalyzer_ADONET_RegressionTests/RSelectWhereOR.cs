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
        public static void RSelectWhereOR()
        {
            RSelectWhereOR(false, false);
        }

        public static void RSelectWhereOR(bool SavedTable, bool NoCreateTable)
        {
            bool CreateTable = !NoCreateTable;
            string uniq;
            if (SavedTable)
            {
                uniq = "SavedTable";
            }
            else
            {
                uniq = Guid.NewGuid().ToString().Replace("-", "");
            }
            string tablename = "rselectwhereor_test_" + uniq;
            string tablenameSorted = "rselectwhereor_test_sorted" + uniq;
            string indexname = uniq + "apple";
            string tablenamedummy = "rselectwhereor_test_dummy" + uniq;
            string indexnamedummy = "dummy_" + uniq;
            Dictionary<long, List<KeyValuePair<long, long>>> expected = new Dictionary<long, List<KeyValuePair<long, long>>>();
            Dictionary<long, int> testvalues = new Dictionary<long, int>(5);
            const int TESTSIZE = 5;

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            if (SavedTable && CreateTable)
            {
                try
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
                catch
                {
                }
            }

            {
                DbConnection conn = null;
                if (CreateTable)
                {
                    Console.WriteLine("Preparing data...");
                    conn = fact.CreateConnection();
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();
                }
                {
                    DbCommand cmd = null;
                    DbParameter num1 = null;
                    DbParameter num2 = null;
                    DbParameter num3 = null;
                    if (CreateTable)
                    {
                        cmd = conn.CreateCommand();
                        cmd.CommandText = "CREATE TABLE " + tablename + " (num1 LONG, num2 LONG, num3 LONG)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num1, @num2, @num3)";

                        num1 = cmd.CreateParameter();
                        num1.ParameterName = "@num1";
                        num1.DbType = DbType.Int64;

                        num2 = cmd.CreateParameter();
                        num2.ParameterName = "@num2";
                        num2.DbType = DbType.Int64;

                        num3 = cmd.CreateParameter();
                        num3.ParameterName = "@num3";
                        num3.DbType = DbType.Int64;
                    }

                    //Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    Random rnd = new Random(82416); // Same seed to allow reusing table.
                    int rowcount = 5000;// (1024 * 1024 * 10) / (9 * 3);
                    int min = -50;
                    int max = 50;
                    for (int i = 0; i < rowcount; i++)
                    {
                        long key = rnd.Next(min, max);
                        if (testvalues.Count < TESTSIZE && !testvalues.ContainsKey(key))
                        {
                            testvalues.Add(key, 0);
                        }
                        long lnum1 = key;
                        long lnum2 = rnd.Next(min, max);
                        long lnum3 = rnd.Next(min, max);
                        if (CreateTable)
                        {
                            num1.Value = lnum1;
                            num2.Value = lnum2;
                            num3.Value = lnum3;
                            cmd.ExecuteNonQuery();
                        }
                        {
                            if (!expected.ContainsKey(lnum1))
                            {
                                expected.Add(lnum1, new List<KeyValuePair<long, long>>());
                            }
                            expected[lnum1].Add(new KeyValuePair<long, long>(lnum2, lnum3));
                        }
                    }
                }
                if(CreateTable)
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenameSorted + " (num1 LONG, num2 LONG, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenameSorted + " SELECT * FROM " + tablename + " ORDER BY num1";
                    cmd.ExecuteNonQuery();
                }
                ///////! cannot create index here.       
                if(CreateTable)
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenamedummy + " (num1 LONG, num2 LONG, num3 LONG)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "INSERT INTO " + tablenamedummy + " VALUES (1, 1, 1)";
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    Console.WriteLine("Data prepared.");
                }
            }

            if(CreateTable)
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

            Console.WriteLine("Querying data using RSELECT...WHERE...OR...");
            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                long prevkey = -42424242;
                List<long> orvalues = new List<long>(); // Values of both keys OR'd.
                foreach (long key in expected.Keys)
                {
                    {
                        List<KeyValuePair<long, long>> kvexpected = expected[key];
                        if (-42424242 == prevkey)
                        {
                            prevkey = key;
                            orvalues.Clear();
                            foreach (KeyValuePair<long, long> kvp in kvexpected)
                            {
                                orvalues.Add(kvp.Value);
                            }
                            continue;
                        }
                        foreach (KeyValuePair<long, long> kvp in kvexpected)
                        {
                            orvalues.Add(kvp.Value);
                        }
                    }

                    List<KeyValuePair<long, long>> results = new List<KeyValuePair<long, long>>();
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key.ToString() + " OR key = " + prevkey.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    bool some = false;
                    while (reader.Read())
                    {
                        some = true;
                        long num1 = reader.GetInt64(0);
                        long num2 = reader.GetInt64(1);
                        long num3 = reader.GetInt64(2);
                        results.Add(new KeyValuePair<long, long>(num2, num3));
                    }
                    reader.Close();
                    if (!some)
                    {
                        throw new Exception("Did not return any results: " + cmd.CommandText);
                    }

                    //compare results
                    if (orvalues.Count != results.Count)
                    {
                        throw new Exception("Result count: " + results.Count.ToString() + " is different from that of expected: " + orvalues.Count.ToString());
                    }
                    foreach (KeyValuePair<long, long> rpair in results)
                    {
                        bool found = false;
                        foreach (KeyValuePair<long, long> xpair in expected[key])
                        {
                            if (rpair.Key == xpair.Key && rpair.Value == xpair.Value)
                            {
                                found = true;
                                break;
                            }
                        }
                        foreach (KeyValuePair<long, long> xpair in expected[prevkey])
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
                    foreach (KeyValuePair<long, long> xpair in expected[key])
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
                    foreach (KeyValuePair<long, long> xpair in expected[prevkey])
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
                    prevkey = -42424242; // Reset.
                }
                conn.Close();
            }
            Console.WriteLine("RSelect completed.");

            if(!SavedTable)
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
