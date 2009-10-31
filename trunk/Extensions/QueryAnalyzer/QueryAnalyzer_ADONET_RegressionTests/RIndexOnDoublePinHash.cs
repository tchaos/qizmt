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
        private static OneRow[] RIndexOnDoublePinHash_PrepareSourceTable(string sourcetablename, int testsize)
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

            Dictionary<double, OneRow> testrows = new Dictionary<double, OneRow>();
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
            double excludedkey = 0.01;
            DateTime dtseed = DateTime.Now;
            for (int i = 0; i < rowcount; i++)
            {
                OneRow row;
                row.num1 = rnd.Next(min, max);

                for (; ; )
                {
                    double thiskey = Math.Round(rnd.NextDouble(), 2);
                    if (rnd.Next() % 2 == 0)
                    {
                        thiskey = thiskey * -1d;
                    }
                    if (thiskey != excludedkey)
                    {
                        row.num2 = thiskey;
                        break;
                    }   
                }
                
                row.num3 = rnd.Next(min, max);
                row.dt = dtseed.AddDays(rnd.Next(min, max));
                row.str = words[rnd.Next() % words.Length];
                if (testrows.Count < testsize && !testrows.ContainsKey(row.num2))
                {
                    testrows.Add(row.num2, row);
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

        public static void RIndexOnDoublePinHash()
        {
            string tablename = "rselect_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenameSorted = "rselect_test_sorted_" + Guid.NewGuid().ToString().Replace("-", "");
            string indexname = Guid.NewGuid().ToString().Replace("-", "") + "apple";
            const int TESTSIZE = 10;
            OneRow[] testrows = null;

            if (TESTSIZE < 5)
            {
                throw new Exception("TESTSIZE must be greater than 5.");
            }

            testrows = RIndexOnDoublePinHash_PrepareSourceTable(tablename, TESTSIZE);

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            //Key column = string.           
            {
                Console.WriteLine("Sorting table by key column double...");

                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tablenameSorted + " (num1 INT, num2 DOUBLE, str CHAR(200), dt DATETIME, num3 LONG)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tablenameSorted + " SELECT * FROM " + tablename + " ORDER BY num2";
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
                Console.WriteLine("Table sorted by double");
            }

            {
                Console.WriteLine("Creating RIndexes pinmemoryhash on key column = double...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname + " FROM " + tablenameSorted + " pinmemoryhash ON NUM2";
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            Dictionary<double, List<OneRow>> expected = new Dictionary<double, List<OneRow>>();
            {
                Console.WriteLine("Querying data using SELECT...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                foreach (OneRow row in testrows)
                {
                    cmd.CommandText = "select * from " + tablenameSorted + " where num2 = " + row.num2.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!expected.ContainsKey(r.num2))
                        {
                            expected.Add(r.num2, new List<OneRow>());
                        }
                        expected[r.num2].Add(r);
                    }
                    reader.Close();
                }
                conn.Close();
                Console.WriteLine("SELECT completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT NOPOOL...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                foreach (double key in expected.Keys)
                {
                    List<OneRow> results = new List<OneRow>();
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        results.Add(r);
                    }
                    reader.Close();

                    //compare results
                    List<OneRow> xlist = expected[key];
                    if (xlist.Count != results.Count)
                    {
                        throw new Exception("Result count: " + results.Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ".  Query= " + cmd.CommandText);
                    }
                    foreach (OneRow r in results)
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
                            throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ".  Query= " + cmd.CommandText);
                        }
                    }
                    foreach (OneRow x in xlist)
                    {
                        bool found = false;
                        foreach (OneRow r in results)
                        {
                            if (r.num1 == x.num1 && r.num2 == x.num2 && r.num3 == x.num3 && r.str == x.str && r.dt == x.dt)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ".  Query= " + cmd.CommandText);
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect NOPOOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT NOPOOL, where key is out of the lower bound...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = -2000.3";
                DbDataReader reader = cmd.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    OneRow r;
                    r.num1 = reader.GetInt32(0);
                    r.num2 = reader.GetDouble(1);
                    r.str = reader.GetString(2);
                    r.dt = reader.GetDateTime(3);
                    r.num3 = reader.GetInt64(4);
                    found = true;
                }
                reader.Close();
                conn.Close();
                if (found)
                {
                    throw new Exception("Rows are returned when no rows are expected.");
                }
                Console.WriteLine("RSelect NOPOOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT NOPOOL, where key is out of the upper bound...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = 2000.3";
                DbDataReader reader = cmd.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    OneRow r;
                    r.num1 = reader.GetInt32(0);
                    r.num2 = reader.GetDouble(1);
                    r.str = reader.GetString(2);
                    r.dt = reader.GetDateTime(3);
                    r.num3 = reader.GetInt64(4);
                    found = true;
                }
                reader.Close();
                conn.Close();
                if (found)
                {
                    throw new Exception("Rows are returned when no rows are expected.");
                }
                Console.WriteLine("RSelect NOPOOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                string whereor = "";
                foreach (double key in expected.Keys)
                {
                    if (whereor.Length > 0)
                    {
                        whereor += " OR ";
                    }
                    whereor += " key = " + key.ToString() + " ";
                }

                {
                    Dictionary<double, List<OneRow>> results = new Dictionary<double, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num2))
                        {
                            results.Add(r.num2, new List<OneRow>());
                        }
                        results[r.num2].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (double key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
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
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
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
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
                            }
                        }
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL/OR completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT POOL/OR, with a key that is out of the lower bound, out of the upper bound, and within bound but not in table.");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();

                string whereor = " key = -2000.9 "; //out of lower bound.

                List<double> keys = new List<double>(expected.Keys);

                whereor += " OR key = " + keys[0].ToString() + " ";

                whereor += " OR key = 2000.9  "; //out of upper bound.

                whereor += " OR key = " + keys[1].ToString() + " ";

                whereor += " OR key = 0.01  "; //within bound but not in table.

                whereor += " OR key = -2000.9 "; //out of lower bound.

                whereor += " OR key = -2000.9 "; //out of lower bound.

                whereor += " OR key = " + keys[2].ToString() + " ";

                for(int i = 3; i < keys.Count; i++)
                {                   
                    whereor += " OR key = " + keys[i].ToString() + " ";
                }

                {
                    Dictionary<double, List<OneRow>> results = new Dictionary<double, List<OneRow>>();
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where " + whereor;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        OneRow r;
                        r.num1 = reader.GetInt32(0);
                        r.num2 = reader.GetDouble(1);
                        r.str = reader.GetString(2);
                        r.dt = reader.GetDateTime(3);
                        r.num3 = reader.GetInt64(4);
                        if (!results.ContainsKey(r.num2))
                        {
                            results.Add(r.num2, new List<OneRow>());
                        }
                        results[r.num2].Add(r);
                    }
                    reader.Close();

                    //compare results
                    foreach (double key in expected.Keys)
                    {
                        List<OneRow> xlist = expected[key];
                        if (xlist.Count != results[key].Count)
                        {
                            throw new Exception("Result count: " + results[key].Count.ToString() + " is different from that of expected: " + xlist.Count.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
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
                                throw new Exception("RSelect returned a row which was not located in expected results. num2=" + r.num2.ToString() + " num3=" + r.num3.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
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
                                throw new Exception("RSelect did not return an expected row. num2=" + x.num2.ToString() + " num3=" + x.num3.ToString() + ". Key=" + key.ToString() + ".  Query:" + cmd.CommandText);
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
                cmd.CommandText = "drop rindex " + indexname;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
