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
        public static void NullParam()
        {
            string tablename = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            //Create table
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE " + tablename + " (num INT, str CHAR(40), num2 LONG, num3 DOUBLE)";
                int rows = cmd.ExecuteNonQuery();
            }

            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO " + tablename + " VALUES (@num, @str, @num2, @num3)";

                DbParameter num = cmd.CreateParameter();
                num.ParameterName = "@num";
                num.DbType = DbType.Int32;

                DbParameter str = cmd.CreateParameter();
                str.ParameterName = "@str";
                str.DbType = DbType.String;
                str.Size = 40;

                DbParameter num2 = cmd.CreateParameter();
                num2.ParameterName = "@num2";
                num2.DbType = DbType.Int64;

                DbParameter num3 = cmd.CreateParameter();
                num3.ParameterName = "@num3";
                num3.DbType = DbType.Double;

                num.Value = 33;
                str.Value = DBNull.Value;
                num2.Value = DBNull.Value;
                num3.Value = 331332333334335;

                cmd.ExecuteNonQuery();
            }
           
            //Select from table
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT TOP 100 * FROM " + tablename + " where num3=331332333334335";
                DbDataReader reader = cmd.ExecuteReader();

                int num = 0;
                //string str = null;
                DBNull str = null;
                //long num2 = 0;
                DBNull num2 = null;
                double num3 = 0;
                int rowcount = 0;

                try
                {
                    while (reader.Read())
                    {
                        num = reader.GetInt32(0);
                        if (33 != num)
                        {
                            throw new Exception("Expected num to be 33, got " + num.ToString());
                        }
                        str = reader[1] as DBNull;
                        if (null == str || !DBNull.Value.Equals(str))
                        {
                            throw new Exception("Expected str to be DBNull");
                        }
                        num2 = reader["num2"] as DBNull;
                        if (null == num2 || !DBNull.Value.Equals(num2))
                        {
                            throw new Exception("Expected num2 to be DBNull");
                        }
                        num3 = reader.GetDouble(3);
                        if (331332333334335 != num3)
                        {
                            throw new Exception("Expected num3 to be 33, got " + num3.ToString());
                        }

                        rowcount++;
                    }

                    if (1 != rowcount)
                    {
                        throw new Exception("Expected exactly 1 row, got " + rowcount.ToString() + " rows");
                    }

                }
                catch (Exception e)
                {
                    throw new Exception("Error getting values: " + e.Message);
                }
                //Must close reader before other executions.
                reader.Close();
            }

            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
            }
                
            conn.Close();
         }
    }
}
