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
        public static void AlterTableRenameSwap()
        {
            string tblname1 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");
            string tblname2 = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            DbCommand cmd = conn.CreateCommand();

            //Create table1
            {
                cmd.CommandText = "CREATE TABLE " + tblname1 + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                cmd.ExecuteNonQuery();
            }
            {
                cmd.CommandText = "INSERT INTO " + tblname1 + " VALUES (1, '{73DBDBC7-EE31-4fe2-B6F8-57872B81E93D}', 2, 3.14, '12/30/2009 12:00:00 AM')";
                cmd.ExecuteNonQuery();
            }

            //Create table2
            {
                cmd.CommandText = "CREATE TABLE " + tblname2 + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE, dt DATETIME)";
                cmd.ExecuteNonQuery();
            }
            {
                cmd.CommandText = "INSERT INTO " + tblname2 + " VALUES (1, '{1D21F967-4BFB-4ad1-ACB9-C25DE07665FF}', 2, 3.14, '12/30/2009 12:00:00 AM')";
                cmd.ExecuteNonQuery();
            }

            // Swap tables!
            {
                cmd.CommandText = "ALTER TABLE " + tblname1 + " RENAME SWAP " + tblname2;
                cmd.ExecuteNonQuery();
            }

            // Ensure swapped by selecting...
            {

                {
                    cmd.CommandText = "SELECT * FROM " + tblname1;
                    DbDataReader reader = cmd.ExecuteReader();
                    if (!reader.Read())
                    {
                        throw new Exception("Expected to read a row from table 1");
                    }
                    string str = (string)reader[1];
                    if (str != "{1D21F967-4BFB-4ad1-ACB9-C25DE07665FF}")
                    {
                        throw new Exception("Table 1 does not have value originally inserted into table 2");
                    }
                    if (reader.Read())
                    {
                        throw new Exception("Expected to read one row from table 1, not two");
                    }
                    reader.Close();
                }

                {
                    cmd.CommandText = "SELECT * FROM " + tblname2;
                    DbDataReader reader = cmd.ExecuteReader();
                    if (!reader.Read())
                    {
                        throw new Exception("Expected to read a row from table 2");
                    }
                    string str = (string)reader[1];
                    if (str != "{73DBDBC7-EE31-4fe2-B6F8-57872B81E93D}")
                    {
                        throw new Exception("Table 2 does not have value originally inserted into table 1");
                    }
                    if (reader.Read())
                    {
                        throw new Exception("Expected to read one row from table 2, not two");
                    }
                    reader.Close();
                }

            }

            // All done!
            {
                cmd.CommandText = "drop table " + tblname1;
                cmd.ExecuteNonQuery();
            }
            conn.Close();


        }
    }
}
