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
        public static void RSelectLotsOfRows()
        {
            string tablename = "rselect_test_" + Guid.NewGuid().ToString().Replace("-", "");
            string tablenameSorted = "rselect_test_sorted" + Guid.NewGuid().ToString().Replace("-", "");
            string indexname = Guid.NewGuid().ToString().Replace("-", "") + "apple";
            long thekey = 10;
            int expectedRowCount = (1024 * 1024 * 65) / (9 * 3);

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
                    int min = -5000;
                    int max = 5000;
                    for (int i = 0; i < expectedRowCount; i++)
                    {
                        num1.Value = thekey;
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
                Console.WriteLine("Creating RIndexes...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();                
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE RINDEX " + indexname + " FROM " + tablenameSorted;
                cmd.ExecuteNonQuery();
                conn.Close();
                Console.WriteLine("RIndexes created.");
            }

            Console.WriteLine("Querying data using RSELECT...");
            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + thekey.ToString();
                DbDataReader reader = cmd.ExecuteReader();
                bool isfirstread = true;
                int resultRowCount = 0;
                Console.WriteLine("Reader to load all rows....");
                while (reader.Read())
                {
                    if (isfirstread)
                    {
                        Console.WriteLine("Reader finished loading all rows...");
                        isfirstread = false;
                    }
                    resultRowCount++;
                    long num1 = reader.GetInt64(0);
                    long num2 = reader.GetInt64(1);
                    long num3 = reader.GetInt64(2);
                }
                reader.Close();
                conn.Close();

                //compare results
                if (expectedRowCount != resultRowCount)
                {
                    throw new Exception("Expected row count = " + expectedRowCount.ToString() + " but got " + resultRowCount.ToString());
                }

                Console.WriteLine("Rows returned: {0}", resultRowCount);
            }

            Console.WriteLine("RSelectLotsOfRows completed.");

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
