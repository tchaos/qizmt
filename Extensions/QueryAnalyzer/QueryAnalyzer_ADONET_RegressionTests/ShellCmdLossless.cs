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
        public static void ShellCmdLossless()
        {
            string expected = Exec.Shell("Qizmt ls");

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost";
            conn.Open();
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SHELL 'Qizmt ls'";
            DbDataReader reader = cmd.ExecuteReader();
            StringBuilder sb = new StringBuilder();
            while (reader.Read())
            {
                sb.Append(reader.GetString(0));
            }
            reader.Close();
            conn.Close();
            
            if (expected != sb.ToString())
            {
                throw new Exception("Shell command failed.  Not lossless.");
            }
        }
    }
}
