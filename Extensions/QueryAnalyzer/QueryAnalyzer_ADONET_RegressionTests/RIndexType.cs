﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        private static OneRow[] RIndexType_PrepareSourceTable(string sourcetablename, int testsize)
        {
            string[] words = @"Apple's good for you.  $apple$ with 2 dollar signs. Whether you want to build an audience or 
communicate with your closest friends, the MySpace Privacy Control Panel allows you to control what 
information is shared, who can contact you, how you are notified and the tools to eliminate unwanted 
spam. Here you control who can see you are online, who knows your birthday, who can view your profile, 
who can share your photos and how to block specific users and users by age. Here you control spam security 
levels, who can message you, who can request to be friends, approval of comments, group invitations, event and 
IM invitations. Here you control your subscription to the MySpace newsletter featuring exclusive content and 
promotions and email notifications about friend requests, comments, blog subscriptions and event invitations. 
Here you control SMS (text message) alerts for MySpace messages, friend requests, content and when you 
receive mobile content subscriptions. Here you control how your calendar is set up and how you are reminded 
of important events. Here you control how groups are displayed on your profile, if and where you accept HTML 
comments, how your music player works and your away message when you are on vacation or are 
taking a break.".Split(new string[] { " ", Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

            for (int wi = 0; wi < words.Length; wi++)
            {
                words[wi] = words[wi].Trim();
                if (words[wi].Length == 0)
                {
                    words[wi] = "xx";
                }
            }

            Dictionary<int, OneRow> testrows = new Dictionary<int, OneRow>();
            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            Console.WriteLine("Preparing source table...");
            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE " + sourcetablename + " (num1 INT, num2 DOUBLE, str CHAR(200), dt DATETIME, num3 LONG)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO " + sourcetablename + " VALUES (@num1, @num2, @str, @dt, @num3)";

            DbParameter num1 = cmd.CreateParameter();
            num1.ParameterName = "@num1";
            num1.DbType = DbType.Int32;

            DbParameter num2 = cmd.CreateParameter();
            num2.ParameterName = "@num2";
            num2.DbType = DbType.Double;

            DbParameter str = cmd.CreateParameter();
            str.ParameterName = "@str";
            str.DbType = DbType.String;
            str.Size = 200;

            DbParameter dt = cmd.CreateParameter();
            dt.ParameterName = "@dt";
            dt.DbType = DbType.DateTime;

            DbParameter num3 = cmd.CreateParameter();
            num3.ParameterName = "@num3";
            num3.DbType = DbType.Int64;

            Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
            int rowcount = 50000;// (1024 * 1024 * 10) / (9 * 3);
            int min = -5000;
            int max = 5000;
            DateTime dtseed = DateTime.Now;

            //Make sure that I get the edge case in there.
            {
                OneRow row;
                row.num1 = min;
                row.num2 = rnd.Next(min, max); //rnd.NextDouble();
                row.num3 = rnd.Next(min, max);
                row.dt = dtseed.AddDays(rnd.Next(min, max));
                row.str = words[rnd.Next() % words.Length];
                testrows.Add(row.num1, row);
                num1.Value = row.num1;
                num2.Value = row.num2;
                num3.Value = row.num3;
                str.Value = row.str;
                dt.Value = row.dt;
                cmd.ExecuteNonQuery();
            }

            //Make sure that I get the edge case in there.
            {
                OneRow row;
                row.num1 = max;
                row.num2 = rnd.Next(min, max); //rnd.NextDouble();
                row.num3 = rnd.Next(min, max);
                row.dt = dtseed.AddDays(rnd.Next(min, max));
                row.str = words[rnd.Next() % words.Length];
                testrows.Add(row.num1, row);
                num1.Value = row.num1;
                num2.Value = row.num2;
                num3.Value = row.num3;
                str.Value = row.str;
                dt.Value = row.dt;
                cmd.ExecuteNonQuery();
            }

            for (int i = 0; i < rowcount; i++)
            {
                OneRow row;
                row.num1 = rnd.Next(min, max);
                row.num2 = rnd.Next(min, max); //rnd.NextDouble();
                row.num3 = rnd.Next(min, max);
                row.dt = dtseed.AddDays(rnd.Next(min, max));
                row.str = words[rnd.Next() % words.Length];
                if (testrows.Count < testsize && !testrows.ContainsKey(row.num1))
                {
                    testrows.Add(row.num1, row);
                }
                num1.Value = row.num1;
                num2.Value = row.num2;
                num3.Value = row.num3;
                str.Value = row.str;
                dt.Value = row.dt;
                cmd.ExecuteNonQuery();
            }

            conn.Close();
            Console.WriteLine("Source table created.");
            return (new List<OneRow>(testrows.Values)).ToArray();
        }

        public static void RIndexType()
        {
            string tablename = "rselect_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenamedummy = "rselect_test_dummy" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenameSorted = "rselect_test_sorted_" + Guid.NewGuid().ToString().Replace("-", "");
            string indexname_default = "default_" + Guid.NewGuid().ToString().Replace("-", "") + "apple";
            string indexname_pinmemory = "pinmem_" + Guid.NewGuid().ToString().Replace("-", "") + "apple";
            string indexname_pinmemoryhash = "pinmemhash_" + Guid.NewGuid().ToString().Replace("-", "") + "apple";
            string indexname_diskonly = "diskonly_" + Guid.NewGuid().ToString().Replace("-", "") + "apple";
            string indexname_dummy = "dummy_" + Guid.NewGuid().ToString().Replace("-", "") + "apple";
            const int TESTSIZE = 3;
            OneRow[] testrows = null;

            testrows = RIndexType_PrepareSourceTable(tablename, TESTSIZE);

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            //Key column = string.           
            {
                Console.WriteLine("Sorting table by key column int...");

                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenameSorted + " (num1 INT, num2 DOUBLE, str CHAR(200), dt DATETIME, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenameSorted + " SELECT * FROM " + tablename + " ORDER BY num1";
                    cmd.ExecuteNonQuery();
                    
                    cmd.CommandText = "CREATE TABLE " + tablenamedummy + " (num1 INT, num2 DOUBLE, str CHAR(200), dt DATETIME, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenamedummy + " VALUES(1,1,'x','10/22/2009 8:15:47 AM',1)";
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
                Console.WriteLine("Table sorted by int");
            }

            {
                Console.WriteLine("Creating RIndexes DEFAULT on key column = int...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname_default + " FROM " + tablenameSorted + " ON NUM1";
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            {
                Console.WriteLine("Creating RIndexes PINMEMORY on key column = int...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname_pinmemory + " FROM " + tablenameSorted + " PINMEMORY ON NUM1";
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            {
                Console.WriteLine("Creating RIndexes PINMEMORYHASH on key column = int...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname_pinmemoryhash + " FROM " + tablenameSorted + " PINMEMORYHASH ON NUM1";
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            {
                Console.WriteLine("Creating RIndexes DISKONLY on key column = int...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname_dummy + " FROM " + tablenameSorted + " DISKONLY ON NUM1";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "CREATE RINDEX " + indexname_diskonly + " FROM " + tablenamedummy + " DISKONLY ON NUM1";
                cmd.ExecuteNonQuery();
                conn.Close();

                conn.Open();
                cmd.CommandText = "ALTER RINDEX " + indexname_dummy + " RENAME SWAP " + indexname_diskonly;
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }



            Dictionary<int, List<OneRow>> expected = new Dictionary<int, List<OneRow>>();
            {
                Console.WriteLine("Querying data using SELECT...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                foreach (OneRow row in testrows)
                {
                    cmd.CommandText = "select * from " + tablenameSorted + " where num1 = " + row.num1.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!expected.ContainsKey(r.num1))
                        {
                            expected.Add(r.num1, new List<OneRow>());
                        }
                        expected[r.num1].Add(r);
                    }
                    reader.Close();
                }
                conn.Close();
                Console.WriteLine("SELECT completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR FROM DEFAULT RINDEX");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                string whereor = "";
                foreach (int key in expected.Keys)
                {
                    if (whereor.Length > 0)
                    {
                        whereor += " OR ";
                    }
                    whereor += " key = " + key.ToString() + " ";
                }

                {
                    Dictionary<int, List<OneRow>> results = new Dictionary<int, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname_default.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num1))
                        {
                            results.Add(r.num1, new List<OneRow>());
                        }
                        results[r.num1].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (int key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                        }
                        foreach (OneRow r in results[key])
                        {
                            bool found = false;
                            foreach (OneRow x in xlist)
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                        foreach (OneRow x in xlist)
                        {
                            bool found = false;
                            foreach (OneRow r in results[key])
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL/OR completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR FROM PINMEMORY RINDEX");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                string whereor = "";
                foreach (int key in expected.Keys)
                {
                    if (whereor.Length > 0)
                    {
                        whereor += " OR ";
                    }
                    whereor += " key = " + key.ToString() + " ";
                }

                {
                    Dictionary<int, List<OneRow>> results = new Dictionary<int, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname_pinmemory.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num1))
                        {
                            results.Add(r.num1, new List<OneRow>());
                        }
                        results[r.num1].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (int key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                        }
                        foreach (OneRow r in results[key])
                        {
                            bool found = false;
                            foreach (OneRow x in xlist)
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                        foreach (OneRow x in xlist)
                        {
                            bool found = false;
                            foreach (OneRow r in results[key])
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL/OR completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR FROM PINMEMORYHASH RINDEX");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                string whereor = "";
                foreach (int key in expected.Keys)
                {
                    if (whereor.Length > 0)
                    {
                        whereor += " OR ";
                    }
                    whereor += " key = " + key.ToString() + " ";
                }

                {
                    Dictionary<int, List<OneRow>> results = new Dictionary<int, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname_pinmemoryhash.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num1))
                        {
                            results.Add(r.num1, new List<OneRow>());
                        }
                        results[r.num1].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (int key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                        }
                        foreach (OneRow r in results[key])
                        {
                            bool found = false;
                            foreach (OneRow x in xlist)
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                        foreach (OneRow x in xlist)
                        {
                            bool found = false;
                            foreach (OneRow r in results[key])
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL/OR completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR FROM DISKONLY RINDEX");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                string whereor = "";
                foreach (int key in expected.Keys)
                {
                    if (whereor.Length > 0)
                    {
                        whereor += " OR ";
                    }
                    whereor += " key = " + key.ToString() + " ";
                }

                {
                    Dictionary<int, List<OneRow>> results = new Dictionary<int, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname_diskonly.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num1))
                        {
                            results.Add(r.num1, new List<OneRow>());
                        }
                        results[r.num1].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (int key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                        }
                        foreach (OneRow r in results[key])
                        {
                            bool found = false;
                            foreach (OneRow x in xlist)
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                        foreach (OneRow x in xlist)
                        {
                            bool found = false;
                            foreach (OneRow r in results[key])
                            {
                                if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ". Query=" + cmd.CommandText);
                            }
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL/OR completed.");
            }

            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop table " + tablenameSorted;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname_default;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname_pinmemory;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname_pinmemoryhash;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname_diskonly;
                cmd.ExecuteNonQuery();
                conn.Close();
           }
        }
    }
}
