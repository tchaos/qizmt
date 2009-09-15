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
        public static void InsertIntoSelectTop()
        {
            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");
            string tblname2 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");
            string tblname3 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";

            try
            {
                conn.Open();
                try
                {
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "CREATE TABLE " + tblname2 + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "CREATE TABLE " + tblname3 + " (xnum INT, xnum2 INT, num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                        cmd.ExecuteNonQuery();
                    }
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, 'a', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname + " VALUES (2, 'b', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (3, 'f', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (4, 'e', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (5, 'd', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (6, 'c', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "INSERT INTO " + tblname3 + " VALUES (1, 1, 9, 'f', 2, 3.14, '12/30/2009 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname3 + " VALUES (2, 2, 8, 'e', 2, 3.14, '12/30/2000 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "INSERT INTO " + tblname3 + " VALUES (3, 3, 7, 'd', 2, 3.14, '12/30/1999 12:00:00 AM')";
                        cmd.ExecuteNonQuery();
                    }
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "INSERT INTO " + tblname + " SELECT TOP 2 * FROM " + tblname2 + " ORDER BY str";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "INSERT INTO " + tblname + " SELECT TOP 2 num, str, num2, num3, dt FROM " + tblname3 + " ORDER BY dt";
                        cmd.ExecuteNonQuery();
                    }
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT * FROM " + tblname;
                        DbDataReader reader = cmd.ExecuteReader();

                        int num = 0;
                        Dictionary<int, int> results = new Dictionary<int, int>();
                        while (reader.Read())
                        {
                            num = reader.GetInt32(0);

                            if (num == 1 || num == 2 || num == 5 || num == 6 || num == 7 || num == 8)
                            {
                                if (!results.ContainsKey(num))
                                {
                                    results.Add(num, 1);
                                }
                                else
                                {
                                    throw new Exception("Duplicate row is returned.");
                                }
                            }
                        }
                        reader.Close();

                        if (results.Count != 6)
                        {
                            throw new Exception("Expected 6 rows to be returned.  Received " + results.Count.ToString() + " instead.");
                        }
                    }
                }
                finally
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "drop table " + tblname;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "drop table " + tblname2;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "drop table " + tblname3;
                    cmd.ExecuteNonQuery();
                }                
            }
            finally
            {
                conn.Close();
            }
        }
    }
}