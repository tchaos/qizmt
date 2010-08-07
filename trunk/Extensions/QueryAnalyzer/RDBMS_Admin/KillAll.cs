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
            bool proxy = false;
            string cmd = "";

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i].ToLower();
                switch (arg)
                {
                    case "-f":
                        forceflag = true;
                        cmd += " " + arg;
                        break;
                    
                    case "-p":
                        proxy = true;
                        break;

                    default:
                        cmd += " " + arg;
                        break;
                }
            }

            if (!forceflag)
            {
                Console.Error.WriteLine("WARNING: about to terminate protocol services.");
                Console.Error.WriteLine("To continue, use:  RDBMS_admin killall -f");
                return;
            }

            if (proxy)
            {
                RunViaJob(cmd);
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

        internal static void RunViaJob(string cmd)
        {
            string tempdir = CurrentDir + @"\" + Guid.NewGuid().ToString();
            string jobname = Guid.NewGuid().ToString();

            try
            {
                System.IO.Directory.CreateDirectory(tempdir);
                string job = (@"<SourceCode>
  <Jobs>
    <Job Name=`rdbms_admin` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Qizmt_Log(Shell(@`rdbms_admin " + cmd + @"`));   
            }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"');
                
                System.IO.File.WriteAllText(tempdir + @"\" + jobname, job);
                Exec.Shell("Qizmt importdir \"" + tempdir + "\"");
                Console.WriteLine(Exec.Shell("Qizmt exec " + jobname));                
            }
            finally
            {
                try
                {
                    Exec.Shell("Qizmt del " + jobname);
                }
                catch
                {
                }

                try
                {                    
                    if (System.IO.Directory.Exists(tempdir))
                    {
                        System.IO.Directory.Delete(tempdir, true);
                    }      
                }
                catch
                {
                }                         
            }            
        }
    }
}
