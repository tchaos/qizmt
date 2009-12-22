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
        public static void OuterJoin()
        {

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");
            string tblname2 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();
            try
            {
                DbCommand cmd = conn.CreateCommand();

#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif

                //Create table 1
                {
                    cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(60), X LONG, Y DOUBLE, Z DOUBLE, ID INT)";
                    int rows = cmd.ExecuteNonQuery();
                }

                //Insert data 1
                {
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11380001,'a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}ONE',312111,-11223344,3111, 1);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11380002,'B{745B9042-77B2-4633-B765-59A1B0DA7194}TWO',312111, 0       ,3111, 2);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11380003,'c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}THREE',312111, 11223344,3111, 3);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES(11380003,'d{6F380F7F-A70C-4b22-AA87-E480955B3579}FOUR',312111, 11223344,3111, 4);";
                    cmd.ExecuteNonQuery();
                }

                {
                    cmd.CommandText = "SELECT * FROM " + tblname;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                    }
                    reader.Close();
                }

                //Create table 2
                {
                    cmd.CommandText = "CREATE TABLE " + tblname2 + " (foo INT, fooID INT)";
                    int rows = cmd.ExecuteNonQuery();
                }

                //Insert data 2
                {
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES(2000, 2);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES(3000, 3);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES(1000, 1);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES(5000, 5);";
                    cmd.ExecuteNonQuery();
                }

                {
                    cmd.CommandText = "SELECT * FROM " + tblname;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                    }
                    reader.Close();
                }

                {
                    cmd.CommandText = "SELECT * FROM " + tblname2;
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                    }
                    reader.Close();
                }

                Console.WriteLine("LEFT OUTER JOIN...");
                {
                    cmd.CommandText = "SELECT * FROM " + tblname + " LEFT OUTER JOIN " + tblname2 + " ON " + tblname + ".ID = " + tblname2 + ".fooID";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    while (reader.Read())
                    {
                        rowcount++;
                        string field;
                        {
                            field = tblname + ".x";
                            if ((long)reader[field] != 312111)
                            {
                                throw new Exception("Unexpected value for " + field);
                            }
                        }
                        /*{
                            field = "x";
                            if ((long)reader[field] != 312111)
                            {
                                throw new Exception("Unexpected value for " + field);
                            }
                        }*/
                        {
                            field = tblname + ".id";
                            int id = (int)reader[field];
                            if ((id < 0 || id > 3)
                                && id != 4)
                            {
                                throw new Exception("ID out of range: " + id);
                            }
                            if (id != 4)
                            {
                                {
                                    //field = "fooID";
                                    field = tblname2 + ".fooID";
                                    if ((int)reader[field] != id)
                                    {
                                        throw new Exception("Unexpected value for " + field);
                                    }
                                }
                                {
                                    field = tblname2 + ".foo";
                                    if ((int)reader[field] != id * 1000)
                                    {
                                        throw new Exception("Unexpected value for " + field);
                                    }
                                }
                            }
                        }
                    }
                    reader.Close();
                    if (rowcount != 4)
                    {
                        throw new Exception("Expected exactly 4 rows output");
                    }

                }

                Console.WriteLine("RIGHT OUTER JOIN...");
                {
                    cmd.CommandText = "SELECT * FROM " + tblname + " RIGHT OUTER JOIN " + tblname2 + " ON " + tblname + ".ID = " + tblname2 + ".fooID";
                    DbDataReader reader = cmd.ExecuteReader();
                    int rowcount = 0;
                    while (reader.Read())
                    {
                        rowcount++;
                        string field;
                        {
                            //field = "fooID";
                            field = tblname2 + ".fooID";
                            int id = (int)reader[field];
                            if ((id < 0 || id > 3)
                                && id != 5)
                            {
                                throw new Exception("ID out of range: " + id);
                            }
                            if (id != 5)
                            {
                                {
                                    field = tblname + ".id";
                                    if ((int)reader[field] != id)
                                    {
                                        throw new Exception("Unexpected value for " + field);
                                    }
                                }
                                {
                                    field = tblname + ".x";
                                    if ((long)reader[field] != 312111)
                                    {
                                        throw new Exception("Unexpected value for " + field);
                                    }
                                }
                                /*{
                                    field = "x";
                                    if ((long)reader[field] != 312111)
                                    {
                                        throw new Exception("Unexpected value for " + field);
                                    }
                                }*/
                            }
                            {
                                field = tblname2 + ".foo";
                                if ((int)reader[field] != id * 1000)
                                {
                                    throw new Exception("Unexpected value for " + field);
                                }
                            }
                        }
                    }
                    reader.Close();
                    if (rowcount != 4)
                    {
                        throw new Exception("Expected exactly 4 rows output");
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
