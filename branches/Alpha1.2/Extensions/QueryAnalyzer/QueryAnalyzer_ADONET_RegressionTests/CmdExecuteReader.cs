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
        public static void CmdExecuteReader()
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
            string guid = Guid.NewGuid().ToString().Substring(0, 20);
            {                
                cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, '" + guid + "', 2, 3.14, '12/30/2009 12:00:00 AM')";
                int rows = cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, '" + guid + "', 3, 3.14, '12/30/2009 12:00:00 AM')";
                rows = cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, '" + guid + "', 4, 3.14, '12/30/2009 12:00:00 AM')";
                rows = cmd.ExecuteNonQuery();
            }

            {
                cmd.CommandText = "SELECT TOP 100 * FROM " + tblname + " WHERE str = '" + guid + "' ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                string str = null;
                int num2 = 0;
                double num3 = 0;
                DateTime dt = DateTime.MinValue;
                int totalrows = 0;
                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = (string)reader[1];
                    //num2 = (Int32)reader["num2"];
                    try
                    {
                        num2 = (Int32)(Int64)reader["num2"];
                    }
                    catch(Exception e)
                    {
                        throw new Exception("Type is actually: " + reader["num2"].GetType().FullName, e);
                    }
                    num3 = reader.GetDouble(3);
                    dt = reader.GetDateTime(4);
                    totalrows++;
                }
                reader.Close();

                {
                    cmd.CommandText = "drop table " + tblname;
                    cmd.ExecuteNonQuery();
                }

                conn.Close();

                if (totalrows != 3)
                {
                    throw new Exception("Expected rowcount to be 3.  But only " + totalrows.ToString() + " row is returned.");
                }
            }            
        }
    }
}
