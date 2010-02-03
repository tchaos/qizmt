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
        public static void BatchingOff()
        {
            string tablename = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost; Batch Size = 0B";
            conn.Open();

            //Create table
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE " + tablename + " (num INT, str CHAR(40), num2 INT, dt DATETIME)";
                int rows = cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num, @str, @num2, @dt)";

                DbParameter num = cmd.CreateParameter();
                num.ParameterName = "@num";
                num.DbType = DbType.Int32;

                DbParameter str = cmd.CreateParameter();
                str.ParameterName = "@str";
                str.DbType = DbType.String;
                str.Size = 40;

                DbParameter num2 = cmd.CreateParameter();
                num2.ParameterName = "@num2";
                num2.DbType = DbType.Int32;

                DbParameter dt = cmd.CreateParameter();
                dt.ParameterName = "@dt";
                dt.DbType = DbType.DateTime;

                num.Value = -99;
                str.Value = "ap'p'le";
                num2.Value = 100;
                dt.Value = DateTime.Parse("12/30/2009 12:00:00 AM");
                rows = cmd.ExecuteNonQuery();
            }

            //Select
            bool ok = false;
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT TOP 100 * from " + tablename + " WHERE num = -99 ORDER BY num";
                DbDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if ((Int32)reader["num"] == -99 && (Int32)reader["num2"] == 100
                        && (string)reader["str"] == "ap'p'le" && (DateTime)reader["dt"] == DateTime.Parse("12/30/2009 12:00:00 AM"))
                    {
                        ok = true;
                    }
                }
                reader.Close();
            }

            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
            }

            conn.Close();

            if (!ok)
            {
                throw new Exception("Returned values don't match.");
            }
        }
    }
}
