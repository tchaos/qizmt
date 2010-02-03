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
        public static string DbAggregators_CreateTable()
        {
            string tablename = "RDBMS_Table_ADONET_RegressionTest_" + Guid.NewGuid().ToString().Replace("-", "");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            DbConnection conn = fact.CreateConnection();
            try
            {
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();

                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "create table " + tablename + " (id int, name char(40),  cost double, costl long, bday datetime)";
                    cmd.ExecuteNonQuery();
                }

                {
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "insert into " + tablename + " values(@id, @name, @cost, @costl, @bday)";

                    DbParameter id = cmd.CreateParameter();
                    id.ParameterName = "@id";
                    id.DbType = DbType.Int32;

                    DbParameter name = cmd.CreateParameter();
                    name.ParameterName = "@name";
                    name.DbType = DbType.String;
                    name.Size = 40;

                    DbParameter cost = cmd.CreateParameter();
                    cost.ParameterName = "@cost";
                    cost.DbType = DbType.Double;

                    DbParameter costl = cmd.CreateParameter();
                    costl.ParameterName = "@costl";
                    costl.DbType = DbType.Int64;

                    DbParameter bday = cmd.CreateParameter();
                    bday.ParameterName = "@bday";
                    bday.DbType = DbType.DateTime;

                    id.Value = 10;
                    name.Value = "x";
                    cost.Value = 9.1;
                    costl.Value = 100;
                    bday.Value = DateTime.Parse("1/2/2000 10:00:00 AM");
                    cmd.ExecuteNonQuery();

                    id.Value = 10;
                    name.Value = "p";
                    cost.Value = 10.02;
                    costl.Value = 200;
                    bday.Value = DateTime.Parse("10/3/1900 10:01:01 PM");
                    cmd.ExecuteNonQuery();

                    id.Value = 10;
                    name.Value = "h";
                    cost.Value = 7.8;
                    costl.Value = 400;
                    bday.Value = DateTime.Parse("5/7/1987 5:00:00 AM");
                    cmd.ExecuteNonQuery();

                    id.Value = 20;
                    name.Value = "o";
                    cost.Value = 20.3;
                    costl.Value = 705;
                    bday.Value = DateTime.Parse("1/2/2000 10:00:00 AM");
                    cmd.ExecuteNonQuery();

                    id.Value = 20;
                    name.Value = "p";
                    cost.Value = 9.78;
                    costl.Value = 900;
                    bday.Value = DateTime.Parse("3/4/2001 7:00:00 PM");
                    cmd.ExecuteNonQuery();

                    id.Value = 30;
                    name.Value = "j";
                    cost.Value = 7.8;
                    costl.Value = 100;
                    bday.Value = DateTime.Parse("3/4/2001 7:00:00 PM");
                    cmd.ExecuteNonQuery();

                    id.Value = 30;
                    name.Value = "k";
                    cost.Value = 7.8;
                    costl.Value = 705;
                    bday.Value = DateTime.Parse("9/10/2008 9:00:00 PM");
                    cmd.ExecuteNonQuery();

                    id.Value = 30;
                    name.Value = "p";
                    cost.Value = 20.3;
                    costl.Value = 1000;
                    bday.Value = DateTime.Parse("1/2/2000 10:00:00 AM");
                    cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                conn.Close();
            }
            return tablename;
        }

        public static void DbAggregators_DropTable(string tablename)
        {
            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "drop table " + tablename;
            cmd.ExecuteNonQuery();
            conn.Close();
        }
    }
}
