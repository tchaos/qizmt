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
        public static void Truncate()
        {
            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            DbCommand cmd = conn.CreateCommand();

            //Create table
            {
                cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE)";
                int rows = cmd.ExecuteNonQuery();
            }

            //Insert data
            {
                cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, 'PPP', 2, 3.14)";
                cmd.ExecuteNonQuery();
            }

            bool ok = false;
            {
                cmd.CommandText = "SELECT TOP 100 * FROM " + tblname + " ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();                      
                int rowcount = 0;
                while (reader.Read())
                {
                    rowcount++;
                }
                reader.Close();

                ok = (rowcount == 1);
            }

            if (ok)
            {
                cmd.CommandText = "TRUNCATE TABLE " + tblname;
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT TOP 100 * FROM " + tblname + " ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();
                int rowcount = 0;
                while (reader.Read())
                {
                    rowcount++;
                }
                if (rowcount > 0)
                {
                    ok = false;
                }
                reader.Close();
            }

            {
                cmd.CommandText = "drop table " + tblname;
                cmd.ExecuteNonQuery();
            }

            conn.Close();                

            if (!ok)
            {
                throw new Exception("Didn't truncate table.");
            }           
           
        }
    }
}
