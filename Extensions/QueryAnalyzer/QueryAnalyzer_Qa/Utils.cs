using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace RDBMS_qa
{
    public class Utils
    {
        private static Dictionary<string, string> hostToRIndexConnStr = new Dictionary<string, string>();

        public static string GetRIndexConnStr(string host)
        {
            string _host = host.ToLower();
            lock (hostToRIndexConnStr)
            {
                if (hostToRIndexConnStr.ContainsKey(_host))
                {
                    return hostToRIndexConnStr[_host];
                }
                else
                {
                    StringBuilder sb = new StringBuilder();
                    System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");
                    using (DbConnection conn = fact.CreateConnection())
                    {
                        //Get all host names of the Qizmt cluster
                        conn.ConnectionString = "data source = " + _host;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SHELL 'Qizmt slaveinstalls' ";
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            sb.Append(reader.GetString(0));
                        }
                        conn.Close();
                    }

                    string[] lines = sb.ToString().Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    string hosts = "";
                    for (int i = 0; i < lines.Length; i++)
                    {
                        string line = lines[i].Trim();
                        hosts += "," + line.Split(' ')[0];
                    }
                    hosts = hosts.Substring(1);
                    string connstr = "data source = " + hosts + "; rindex=pooled";
                    hostToRIndexConnStr.Add(_host, connstr);
                    return connstr;
                }
            }            
        }
    }
}
