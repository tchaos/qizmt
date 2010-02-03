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
        public static void SelectDistinct()
        {
            string tablename = Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                           
            {
                Console.WriteLine("Preparing data...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
            
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE " + tablename + " (num1 LONG, num2 double, num3 int, str char(200), dt datetime)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num1, @num2, @num3, @str, @dt)";

                DbParameter num1 = cmd.CreateParameter();
                num1.ParameterName = "@num1";
                num1.DbType = DbType.Int64;

                DbParameter num2 = cmd.CreateParameter();
                num2.ParameterName = "@num2";
                num2.DbType = DbType.Double;

                DbParameter num3 = cmd.CreateParameter();
                num3.ParameterName = "@num3";
                num3.DbType = DbType.Int32;

                DbParameter str = cmd.CreateParameter();
                str.ParameterName = "@str";
                str.Size = 200;
                str.DbType = DbType.String;

                DbParameter dt = cmd.CreateParameter();
                dt.ParameterName = "@dt";
                dt.DbType = DbType.DateTime;

                num1.Value = 10;
                num2.Value = 0.1;
                num3.Value = 1;
                str.Value = "apple";
                dt.Value = DateTime.Parse("10/30/2009 10:00:00 AM");
                cmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();

                num1.Value = 20;
                num2.Value = 0.2;
                num3.Value = 2;
                str.Value = "APPLE";
                dt.Value = DateTime.Parse("10/29/2009 10:00:00 AM");
                cmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();                

                num1.Value = 30;
                num2.Value = 0.3;
                num3.Value = 3;
                str.Value = "lemon";
                dt.Value = DateTime.Parse("10/15/2009 10:00:00 AM");
                cmd.ExecuteNonQuery();

                conn.Close();
                Console.WriteLine("Data prepared.");
            }                           
                
            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT * FROM tbl...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct * from " + tablename;
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    string str = reader.GetString(3);
                    DateTime dt = reader.GetDateTime(4);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 3)
                {
                    throw new Exception("Expected 3 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }


            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT columns FROM tbl...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct num1, num2, num3 from " + tablename;
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 3)
                {
                    throw new Exception("Expected 3 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }

            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT TOP 3 * FROM tbl...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct  top 3 * from " + tablename;
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    string str = reader.GetString(3);
                    DateTime dt = reader.GetDateTime(4);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 3)
                {
                    throw new Exception("Expected 3 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }

            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT TOP 3 columns FROM tbl...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct top 3 num1, num2, num3 from " + tablename;
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 3)
                {
                    throw new Exception("Expected 3 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }

            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT TOP 3 columns FROM tbl WHERE...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct top 3 num1, num2, num3 from " + tablename + " where num1 = 10";
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 1)
                {
                    throw new Exception("Expected 1 row.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }

            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT * FROM tbl WHERE FUNCTION(column) = value");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct * from " + tablename + " WHERE LOWER(str) = 'apple'";
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long num1 = reader.GetInt64(0);
                    double num2 = reader.GetDouble(1);
                    int num3 = reader.GetInt32(2);
                    if (!expected.ContainsKey(num1))
                    {
                        expected.Add(num1, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate num1 is returned:" + num1.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 2)
                {
                    throw new Exception("Expected 2 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("SELECT completed.");
            }

            using (DbConnection conn = fact.CreateConnection())
            {
                Console.WriteLine("Querying data using SELECT DISTINCT aggregate(column) FROM tbl GROUP BY...");
                Dictionary<long, int> expected = new Dictionary<long, int>();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select distinct COUNT(num1) from " + tablename + " GROUP BY num1";
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long count = reader.GetInt64(0);
                    if (!expected.ContainsKey(count))
                    {
                        expected.Add(count, 1);
                    }
                    else
                    {
                        throw new Exception("Duplicate count is returned:" + count.ToString());
                    }
                }
                reader.Close();
                conn.Close();
                if (expected.Count != 2)
                {
                    throw new Exception("Expected 2 rows.  Only " + expected.Count.ToString() + " rows are returned.");
                }
                Console.WriteLine("Completed");
            }
           
            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
