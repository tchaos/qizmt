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
        public static void Drop()
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

            //Drop table
            {
                cmd.CommandText = "DROP TABLE " + tblname;
                cmd.ExecuteNonQuery();
            }

            //Select from table
            bool ok = false;
            {
                try
                {
                    cmd.CommandText = "SELECT TOP 1 * FROM  " + tblname + " ORDER BY num";
                    DbDataReader reader = cmd.ExecuteReader();
                    reader.Close();
                }
                catch
                {
                    ok = true;
                }
            }

            conn.Close();

            if (!ok)
            {
                throw new Exception("Table didn't get dropped.");
            }
        }
    }
}
