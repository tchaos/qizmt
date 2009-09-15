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
        public static void CmdExecuteNonQuery()
        {
            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            DbCommand cmd = conn.CreateCommand();

            //Create table
            {
                cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                int rows = cmd.ExecuteNonQuery();
            }

            //Insert data
            {
                cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, 'PPP', 2, 3.14, '12/30/2009 12:00:00 AM')";
                int rows = cmd.ExecuteNonQuery();
            }

            {
                cmd.CommandText = "SELECT TOP 100 * FROM " + tblname + " WHERE str = 'PPP' ORDER BY num";               
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                string str = null;
                long num2 = 0;
                double num3 = 0;
                DateTime dt = DateTime.MinValue;
                int rowcount = 0;
                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = (string)reader[1];
                    num2 = (Int64)reader["num2"];
                    num3 = reader.GetDouble(3);
                    dt = reader.GetDateTime(4);
                    if (num == 1 && str == "PPP" && num2 == 2 && num3 == 3.14 && dt == DateTime.Parse("12/30/2009 12:00:00 AM"))
                    {
                        rowcount++;
                    }                    
                }
                reader.Close();

                {
                    cmd.CommandText = "drop table " + tblname;
                    cmd.ExecuteNonQuery();
                }

                conn.Close();

                if (rowcount != 1)
                {
                    throw new Exception("Expected rowcount to be 1.  But only " + rowcount.ToString() + " row is returned.");
                }
            }            
        }
    }
}
