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
        public static void Param()
        {
            string tablename = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            //Create table
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE " + tablename + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                int rows = cmd.ExecuteNonQuery();
            
                cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num, @str, @num2, @num3, @dt)";

                DbParameter num = cmd.CreateParameter();
                num.ParameterName = "@num";
                num.DbType = DbType.Int32;

                DbParameter str = cmd.CreateParameter();
                str.ParameterName = "@str";
                str.DbType = DbType.String;
                str.Size = 40;

                DbParameter num2 = cmd.CreateParameter();
                num2.ParameterName = "@num2";
                num2.DbType = DbType.Int64;

                DbParameter num3 = cmd.CreateParameter();
                num3.ParameterName = "@num3";
                num3.DbType = DbType.Double;

                DbParameter dt = cmd.CreateParameter();
                dt.ParameterName = "@dt";
                dt.DbType = DbType.DateTime;

                num.Value = -99;
                str.Value = "ap'p'le";
                num2.Value = 100;
                num3.Value = 3.14d;
                dt.Value = DateTime.Parse("12/30/2009 12:00:00 AM");
                rows = cmd.ExecuteNonQuery();

                num.Value = 3;
                str.Value = "kiwi";
                num2.Value = 2;
                num3.Value = 3.15d;
                dt.Value = DateTime.Parse("12/1/2009 12:00:00 AM");
                rows = cmd.ExecuteNonQuery();
            }

            //Select from table
            bool ok = false;
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT TOP 100 * FROM " + tablename + " ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                string str = null;
                long num2 = 0;
                double num3 = 0;
                DateTime dt = DateTime.MinValue;
                bool row1 = false;
                bool row2 = false;
                int rowcount = 0;
                
                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = (string)reader[1];
                    num2 = (Int64)reader["num2"];
                    num3 = reader.GetDouble(3);
                    dt = reader.GetDateTime(4);
                    if (num == -99 && str == "ap'p'le" && num2 == 100 && num3 == 3.14 && dt == DateTime.Parse("12/30/2009 12:00:00 AM"))
                    {
                        row1 = true;
                    }
                    else if (num == 3 && str == "kiwi" && num2 == 2 && num3 == 3.15 && dt == DateTime.Parse("12/1/2009 12:00:00 AM"))
                    {
                        row2 = true;
                    }
                    rowcount++;
                }
                //Must close reader before other executions.
                reader.Close();

                ok = (rowcount == 2 && row1 == true && row2 == true);
            }

            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
            }

            conn.Close();
            
            if (!ok)
            {
                throw new Exception("param test failed.");
            }      
        }
    }
}
