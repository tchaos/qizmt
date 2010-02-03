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
        public static void SelectNestedFunctions()
        {

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

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
                    cmd.CommandText = "SELECT MIN(ABS(y)) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((double)reader["MIN(ABS(y))"] == 0)
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
                    cmd.CommandText = "SELECT LOWER(MAX(UPPER(str))) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["LOWER(MAX(UPPER(str)))"] == "c{512441f2-0afe-42d3-a4ba-9dd26a9e553a}")
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
                    cmd.CommandText = "SELECT x,MIN(num),UPPER(MAX(str)),x,SIGN(x) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["UPPER(MAX(str))"] == "C{512441F2-0AFE-42D3-A4BA-9DD26A9E553A}"
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
                    cmd.CommandText = "SELECT x,MIN(num),UPPER(MAX(str)),x,ROUND(PI(),2) FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["UPPER(MAX(str))"] == "C{512441F2-0AFE-42D3-A4BA-9DD26A9E553A}"
                            && (Int32)reader["MIN(num)"] == 11388275
                            && (double)reader["ROUND(PI(),2)"] == 3.14)
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
                    cmd.CommandText = "SELECT   x ,  Min  (  num  )  ,  Min  ( LOWER ( str )  )  ,  x    FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((string)reader["MIN(LOWER(str))"] == "a{bc54afdd-34ae-445b-820a-bfcbc716ff77}"
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
                    // All literal values!
                    cmd.CommandText = "SELECT 88 , -909 , 'foo''bar baz' , 4503599627370496, 11.1, 'i' FROM " + tblname + " WHERE x = 312111 GROUP BY x;";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    bool ok = false;
                    while (reader.Read())
                    {
                        rowcount++;
                        if ((Int32)reader["88"] == 88
                            && (Int32)reader["-909"] == -909
                            && (string)reader["'foo''bar baz'"] == "foo'bar baz"
                            && (Int64)reader["4503599627370496"] == 4503599627370496
                            && (double)reader["11.1"] == 11.1
                            && (string)reader["'i'"] == "i")
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
                try
                {
                    conn.Close();
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(" *** Close error: {0}", e.Message);
                }
            }

        }
    }
}
