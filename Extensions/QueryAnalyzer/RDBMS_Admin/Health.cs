using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void Health(string[] args)
        {
            string[] hosts = Utils.GetQizmtHosts();
            List<string> badhosts = new List<string>();
            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            foreach (string host in hosts)
            {
                try
                {
                    using (DbConnection conn = fact.CreateConnection())
                    {
                        conn.ConnectionString = "data source = " + host;
                        conn.Open();
                        conn.Close();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error while connecting to " + host + ": " + e.ToString());
                    badhosts.Add(host);
                }                
            }

            if (badhosts.Count == 0)
            {
                Console.WriteLine("All healthy");
            }
            else
            {
                Console.WriteLine("Bad hosts:");
                foreach (string bad in badhosts)
                {
                    Console.WriteLine(bad);
                }
            }
        }
    }
}
