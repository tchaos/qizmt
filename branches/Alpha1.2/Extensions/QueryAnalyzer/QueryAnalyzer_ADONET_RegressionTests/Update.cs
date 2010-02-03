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
        public static void Update()
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

            //Update data
            {
                cmd.CommandText = "UPDATE " + tblname + " SET str = 'QQQ' , dt = '1/2/2009 12:00:00 AM' WHERE num = 1";
                int rows = cmd.ExecuteNonQuery();
            }

            {
                cmd.CommandText = "SELECT TOP 100 * FROM " + tblname + " ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                string str = null;
                long num2 = 0;
                double num3 = 0;
                DateTime dt = DateTime.MinValue;
                bool ok = false;
                int rowcount = 0;
                string err = "";
                while (reader.Read())
                {
                    num = reader.GetInt32(0);
                    str = (string)reader[1];
                    num2 = (Int64)reader["num2"];
                    num3 = reader.GetDouble(3);
                    dt = reader.GetDateTime(4);
                    if (num == 1 && str == "QQQ" && num2 == 2 && num3 == 3.14 && dt == DateTime.Parse("1/2/2009 12:00:00 AM"))
                    {
                        ok = true;
                    }
                    else
                    {
                        err = "Did not receive updated row.";
                    }
                    rowcount++;
                }
                reader.Close();

                {
                    cmd.CommandText = "drop table " + tblname;
                    cmd.ExecuteNonQuery();
                }

                conn.Close();

                if (rowcount != 1)
                {
                    ok = false;
                    err = "Row count expected: 1, but received: " + rowcount.ToString();
                }

                if (!ok)
                {
                    throw new Exception("Update test failed: " + err);
                }
            }
           
        }
    }
}
