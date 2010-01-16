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
            {
                string guid = "{FFA94574-D63F-44ae-920E-5DFA841F6A69}";
                while (guid.Length < 300)
                {
                    guid += "a";
                }

                string job = (@"<SourceCode>
      <Jobs>
        <Job Name=`` Custodian=`` Email=``>
          <IOSettings>
            <JobType>local</JobType>
          </IOSettings>
          <Local>
            <![CDATA[
                public virtual void Local()
                {                     
                    DSpace_Log(`" + guid + @"`);               
                }
            ]]>
          </Local>
        </Job>
      </Jobs>
    </SourceCode>").Replace('`', '"');

                string tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\" + Guid.NewGuid().ToString().Replace("-", "");
                if (System.IO.Directory.Exists(tempdir))
                {
                    System.IO.Directory.Delete(tempdir, true);
                }
                System.IO.Directory.CreateDirectory(tempdir);
                string tempjobname = Guid.NewGuid().ToString();
                System.IO.File.WriteAllText(tempdir + @"\" + tempjobname, job);
                Exec.Shell("Qizmt importdir \"" + tempdir + "\"");                

                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SHELL 'Qizmt exec " + tempjobname + "'";
                DbDataReader reader = cmd.ExecuteReader();
                StringBuilder sb = new StringBuilder();
                while (reader.Read())
                {
                    sb.Append(reader.GetString(0));
                }
                reader.Close();
                conn.Close();

                //Clean up
                Exec.Shell(@"Qizmt del " + tempjobname);
                System.IO.Directory.Delete(tempdir, true);

                if (sb.ToString().IndexOf(guid) == -1)
                {
                    throw new Exception("Shell command failed.  Not lossless.");
                }
            }

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
}
