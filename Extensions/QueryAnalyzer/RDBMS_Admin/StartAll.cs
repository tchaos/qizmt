using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void StartAll(string[] args)
        {
            string[] hosts = Utils.GetQizmtHosts();

            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            int threadcount = hosts.Length > 12 ? 12 : hosts.Length;

            RDBMS_Admin.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                try
                {
                    lock (hosts)
                    {
                        Console.Write("Starting service on {0}... ", host);
                        Console.WriteLine(Exec.Shell(@"sc \\" + host + " start QueryAnalyzer_Protocol", false));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Problem starting service on {0}: {1}", host, e.Message);
                }

                System.Threading.Thread.Sleep(1000 * 2);
            }
            ), hosts, threadcount);

            Console.WriteLine("Done");
        }
    }
}
