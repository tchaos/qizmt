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
        public static void PinFor()
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

            //Select from table
            {
                cmd.CommandText = "SELECT PIN FOR 10 * FROM  " + tblname;
                DbDataReader reader = cmd.ExecuteReader();
                reader.Close();
            }

            //Drop table
            {
                cmd.CommandText = "DROP PIN FOR 20 TABLE " + tblname + "";
                cmd.ExecuteNonQuery();
            }

            //Select from table
            bool ok = false;
            {
                try
                {
                    cmd.CommandText = "SELECT PIN FOR 10 * FROM  " + tblname;
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
