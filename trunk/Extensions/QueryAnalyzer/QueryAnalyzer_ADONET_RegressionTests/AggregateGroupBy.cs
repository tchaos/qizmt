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
        public static void AggregateGroupBy()
        {
            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();
            try
            {
                DbCommand cmd = conn.CreateCommand();

                //Create table
                {
                    cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(40), X LONG, Y DOUBLE, Z DOUBLE)";
                    int rows = cmd.ExecuteNonQuery();
                }

                //Insert data
                {
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11388275,'a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}',312111,-11223344,3111);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11388276,'B{745B9042-77B2-4633-B765-59A1B0DA7194}',312111, 0       ,3111);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11388277,'c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}',312111, 11223344,3111);";
                    cmd.ExecuteNonQuery();
                }

                {
                    cmd.CommandText = "SELECT MIN(num) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((Int32)reader["MIN(num)"] == 11388275)
                        {
                            ok = true;
                        }
                    }
                    reader.Close();
                    if (rowcount != 1)
                    {
                        throw new Exception("Expected exactly 1 row output");
                    }
                    if (!ok)
                    {
                        throw new Exception("Did not find expected value");
                    }
                }

                {
                    cmd.CommandText = "SELECT MAX(str) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["MAX(str)"] == "c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                        {
                            ok = true;
                        }
                    }
                    reader.Close();
                    if (rowcount != 1)
                    {
                        throw new Exception("Expected exactly 1 row output");
                    }
                    if (!ok)
                    {
                        throw new Exception("Did not find expected value");
                    }
                }

                {
                    cmd.CommandText = "SELECT x,MIN(num),MAX(str),x FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["MAX(str)"] == "c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}"
                            && (Int32)reader["MIN(num)"] == 11388275)
                        {
                            ok = true;
                        }
                    }
                    reader.Close();
                    if (rowcount != 1)
                    {
                        throw new Exception("Expected exactly 1 row output");
                    }
                    if (!ok)
                    {
                        throw new Exception("Did not find expected values");
                    }
                }

                {
                    cmd.CommandText = "SELECT   x ,  Min  (  num  )  ,  Max  (  str  )  ,  x    FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["MAX(str)"] == "c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}"
                            && (Int32)reader["MIN(num)"] == 11388275)
                        {
                            ok = true;
                        }
                    }
                    reader.Close();
                    if (rowcount != 1)
                    {
                        throw new Exception("Expected exactly 1 row output");
                    }
                    if (!ok)
                    {
                        throw new Exception("Did not find expected values");
                    }
                }

            }
            finally
            {
                conn.Close();
            }

        }
    }
}
