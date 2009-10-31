using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void ClearLog(string[] args)
        {
            string[] hosts = Utils.GetQizmtHosts();

            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            const int MAX_TRIES = 10;
            List<string> errs = new List<string>(hosts.Length);
            string logpath = CurrentDir.Replace(':', '$') + @"\errors.txt";
           
            RDBMS_Admin.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string host)
                {
                    int triesremain = MAX_TRIES;
                    string fn = @"\\" + host + @"\" + logpath;

                    for (; ; )
                    {
                        try
                        {
                            System.IO.File.Delete(fn);
                            lock (hosts)
                            {
                                Console.Write('.');
                            }
                            return;
                        }
                        catch (Exception e)
                        {
                            if (--triesremain <= 0)
                            {
                                lock (hosts)
                                {
                                    errs.Add(host);
                                }
                                break;
                            }
                        }
                    }
                }
            ), hosts, hosts.Length);

            Console.WriteLine();

            if (errs.Count > 0)
            {
                Console.WriteLine("Errors encountered while trying to clear logs from these machines:");
                foreach (string e in errs)
                {
                    Console.WriteLine(e);
                }
            }
            else
            {
                Console.WriteLine("Done");
            }
        }
    }
}
