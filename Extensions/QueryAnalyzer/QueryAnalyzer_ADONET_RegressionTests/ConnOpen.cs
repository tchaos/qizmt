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
        public static void ConnOpen()
        {
            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("FAILED: State = {0}" + conn.State.ToString());
            }

            conn.Close();
        }
    }
}
