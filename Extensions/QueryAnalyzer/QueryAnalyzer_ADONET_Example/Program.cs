using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_Example
{
    class Program
    {
        public static void Main(string[] args)
        {
            string datasource = "localhost";
            string tablename = "ExampleTable";

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = " + datasource + "; Batch Size = 64MB";
            conn.Open();
            Console.WriteLine("Data source is set to {0}", datasource);

            DbCommand cmd = conn.CreateCommand();

            //Delete old table
            try
            {
                cmd.CommandText = "DROP TABLE " + tablename;
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine("Old table dropped.");
            }
            catch
            {
            }

            //Create table
            {
                cmd.CommandText = "CREATE TABLE " + tablename + " (num INT, str CHAR(100), num2 long, num3 double)";
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine("Table created.");
            }

            //Insert data
            {
                cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num, @str, @num2, @num3)";

                DbParameter num = cmd.CreateParameter();
                num.ParameterName = "@num";
                num.DbType = DbType.Int32;

                DbParameter str = cmd.CreateParameter();
                str.ParameterName = "@str";
                str.DbType = DbType.String;
                str.Size = 100;

                DbParameter num2 = cmd.CreateParameter();
                num2.ParameterName = "@num2";
                num2.DbType = DbType.Int64;

                DbParameter num3 = cmd.CreateParameter();
                num3.ParameterName = "@num3";
                num3.DbType = DbType.Double;
               
                num.Value = -99;
                str.Value = "apple";
                num2.Value = 5987654;
                num3.Value = 0.123456d;
                int rows = cmd.ExecuteNonQuery();

                num.Value = 89;
                str.Value = "apple\r\nis \"very\" good & 'tasty'.";
                num2.Value = 900000;
                num3.Value = 13.8098d;
                rows = cmd.ExecuteNonQuery();

                num.Value = 89;
                str.Value = "apple";
                num2.Value = 900000;
                num3.Value = 13.8098d;
                rows = cmd.ExecuteNonQuery();

                num.Value = 1;
                str.Value = "banana";
                num2.Value = -6583521;
                num3.Value = 30.1353d;
                rows = cmd.ExecuteNonQuery();

                num.Value = 3;
                str.Value = "kiwi";
                num2.Value = -6583521;
                num3.Value = -0.98732d;
                rows = cmd.ExecuteNonQuery();

                Console.WriteLine("Sample data inserted");
            }

            //Select from table
            {
                cmd.CommandText = "SELECT TOP 100 * FROM " + tablename + " WHERE str = 'apple' ORDER BY num";
                Console.WriteLine(cmd.CommandText);
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                string str = null;
                long num2 = 0;
                double num3 = 0;
                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = (string)reader[1];
                    num2 = (Int64)reader["num2"];
                    num3 = reader.GetDouble(3);
                    Console.WriteLine("=====Row=====");
                    Console.WriteLine("num = {0}", num);
                    Console.WriteLine("str = {0}", str);
                    Console.WriteLine("num2 = {0}", num2);
                    Console.WriteLine("num3 = {0}", num3);
                }
                //Must close reader before other executions.
                reader.Close();

                cmd.CommandText = "SELECT TOP 100 * FROM " + tablename + " WHERE num = 89 ORDER BY num";
                Console.WriteLine(cmd.CommandText);
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = reader.GetString(1);
                    num2 = reader.GetInt64(2);
                    num3 = (double)reader["num3"];
                    Console.WriteLine("=====Row=====");
                    Console.WriteLine("num = {0}", num);
                    Console.WriteLine("str = {0}", str);
                    Console.WriteLine("num2 = {0}", num2);
                    Console.WriteLine("num3 = {0}", num3);
                }
                reader.Close();
            }

            conn.Close();
            Console.WriteLine("Connection closed");
        }
    }
}
