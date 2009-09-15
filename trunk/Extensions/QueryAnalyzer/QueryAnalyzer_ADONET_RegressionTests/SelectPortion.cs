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
        public static void SelectPortion()
        {
            string tblname = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");
            string tblname2 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            DbConnection conn = fact.CreateConnection();
            try
            {
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();

                //Create table
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tblname + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE)";
                    cmd.ExecuteNonQuery();
                }                

                //Insert data
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES (1, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES (2, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " VALUES (3, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                }

                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE " + tblname2 + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE)";
                    cmd.ExecuteNonQuery();
                }

                {
                    DbCommand cmd = conn.CreateCommand();                    
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (4, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (5, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (6, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (7, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (8, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (9, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (10, 'PPP', 2, 3.14)";
                    cmd.ExecuteNonQuery();
                }

                //Insert into select to create multiple parts.
                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 4";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 5";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 6";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 7";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 8";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 9";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO " + tblname + " SELECT * FROM " + tblname2 + " WHERE num= 10";
                    cmd.ExecuteNonQuery();
                }

                {
                    int totalPortions = 3;
                   
                    Dictionary<int, int> results = new Dictionary<int, int>();
                    for (int i = 1; i <= totalPortions; i++)
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT PORTION " + i.ToString() + " OF " + totalPortions.ToString() + " * FROM " + tblname;
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            if (!results.ContainsKey(id))
                            {
                                results[id] = 1;
                            }
                            else
                            {
                                results[id]++;
                            }
                        }
                        reader.Close();
                    }

                    if (results.Count != 10)
                    {
                        throw new Exception("Not all rows are returned.");
                    }

                    //Check all rows are distinct.
                    foreach (int cnt in results.Values)
                    {
                        if (cnt != 1)
                        {
                            throw new Exception("Rows are not distinct.");
                        }
                    }
                }

                {
                    int totalPortions = 20;

                    Dictionary<int, int> results = new Dictionary<int, int>();
                    for (int i = 1; i <= totalPortions; i++)
                    {
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT PORTION " + i.ToString() + " OF " + totalPortions.ToString() + " * FROM " + tblname;
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            if (!results.ContainsKey(id))
                            {
                                results[id] = 1;
                            }
                            else
                            {
                                results[id]++;
                            }
                        }
                        reader.Close();
                    }

                    if (results.Count != 10)
                    {
                        throw new Exception("Not all rows are returned.");
                    }

                    //Check all rows are distinct.
                    foreach (int cnt in results.Values)
                    {
                        if (cnt != 1)
                        {
                            throw new Exception("Rows are not distinct.");
                        }
                    }
                }

                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "drop table " + tblname;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "drop table " + tblname2;
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
