using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void KillAll(string[] args)
        {
            bool forceflag = false;

            if (args.Length > 1)
            {
                if (args[1].ToLower() == "-f")
                {
                    forceflag = true;
                }
            }

            if (!forceflag)
            {
                Console.Error.WriteLine("WARNING: about to terminate protocol services.");
                Console.Error.WriteLine("To continue, use:  RDBMS_admin killall -f");
                return;
            }

            string[] hosts = Utils.GetQizmtHosts();

            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            int threadcount = hosts.Length > 12 ? 12 : hosts.Length;

            Console.WriteLine("Stopping services...");
            RDBMS_Admin.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                string result = Exec.Shell(@"sc \\" + host + " stop QueryAnalyzer_Protocol", true); 
                lock (hosts)
                {
                    Console.Write("{0}: ", host);
                    Console.WriteLine(result);
                }
                System.Threading.Thread.Sleep(1000 * 2);
            }
            ), hosts, threadcount);

            List<string> badhosts = new List<string>();

            Console.WriteLine("Starting services...");
            RDBMS_Admin.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                try
                {
                    string result = Exec.Shell(@"sc \\" + host + " start QueryAnalyzer_Protocol", false); 
                    lock (hosts)
                    {
                        Console.Write("{0}: ", host);
                        ConsoleColor oldc = Console.ForegroundColor;
                        if (-1 == result.IndexOf("STATE") || -1 == result.IndexOf("START"))
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            badhosts.Add(host);
                        }
                        Console.WriteLine(result);
                        Console.ForegroundColor = oldc;
                    }
                }
                catch (Exception e)
                {
                    lock (hosts)
                    {
                        Console.WriteLine("Start service error for {0}: {1}", host, e.ToString());
                        badhosts.Add(host);
                    }
                }
            }
            ), hosts, threadcount);

            Console.WriteLine("---KILLALL RESULTS---");
            if (badhosts.Count > 0)
            {
                Console.WriteLine("Error while starting services on these machines:");
                foreach (string bad in badhosts)
                {
                    Console.WriteLine("  {0}", bad);
                }
            }
            else
            {
                Console.WriteLine("Killall completed successfully.");
            }
            Console.WriteLine("---KILLALL RESULTS---");
        }
    }
}
