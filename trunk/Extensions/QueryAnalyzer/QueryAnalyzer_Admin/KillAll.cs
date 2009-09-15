using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Admin
{
    partial class Program
    {
        private static void KillAll(string[] args)
        {
            bool forceflag = false;
            string[] hosts = null;

            for (int i = 1; i < args.Length; i++)
            {
                string arg = args[i].ToLower();
                if (arg[0] == '-')
                {
                    if (arg == "-f")
                    {
                        forceflag = true;
                    }
                }
                else
                {
                    hosts = ParseHostList(arg);
                }
            }

            if (hosts == null || hosts.Length == 0)
            {
                Console.Error.WriteLine("Missing argument: <hosts>");
                return;
            }
            if (!forceflag)
            {
                Console.Error.WriteLine("WARNING: about to terminate protocol services.");
                Console.Error.WriteLine("To continue, use:  QueryAnalyzer_Admin killall -f <hosts>");
                return;
            }

            int threadcount = hosts.Length > 12 ? 12 : hosts.Length;

            QueryAnalyzer_Admin.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                string result = Exec.Shell(@"sc \\" + host + " stop QueryAnalyzer_Protocol", true); // Suppress error.
                lock (hosts)
                {
                    Console.Write("{0}: ", host);
                    Console.WriteLine(result);
                }
                System.Threading.Thread.Sleep(1000 * 2);                
            }
            ), hosts, threadcount);

            List<string> badhosts = new List<string>();

            QueryAnalyzer_Admin.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {       
                try
                {
                    string result = Exec.Shell(@"sc \\" + host + " start QueryAnalyzer_Protocol", false); // Throws on error.
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

        private static string[] ParseHostList(string arg)
        {          
            if (arg.StartsWith("@"))
            {
                List<string> hosts = new List<string>();
                string filePath = arg.Substring(1);
                try
                {
                    string[] lines = System.IO.File.ReadAllLines(filePath);
                    for (int i = 0; i < lines.Length; i++)
                    {
                        string line = lines[i].Trim();
                        if (line[0] != '#')
                        {
                            hosts.Add(line);
                        }
                    }
                    return hosts.ToArray(); 
                }
                catch
                {
                    Console.Error.WriteLine("Error while reading file: {0}", filePath);
                    return null;
                }                
            }
            else
            {
                return arg.Trim().Split(new string[] { ",", ";", Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            }
        }
    }
}
