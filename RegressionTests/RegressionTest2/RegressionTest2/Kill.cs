using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using MySpace.DataMining.AELight;

namespace RegressionTest2
{
    public partial class Program
    {

        static void Kill(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];

            string masterdir;
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(dfsxmlpath);
                masterdir = fi.DirectoryName; // Directory's full path.
            }
            Surrogate.SetNewMetaLocation(masterdir);

            dfs dc = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            string masterhost = System.Net.Dns.GetHostName();
            string[] allmachines;
            {
                string[] sl = dc.Slaves.SlaveList.Split(';');
                List<string> aml = new List<string>(sl.Length + 1);
                aml.Add(masterhost);
                foreach (string slave in sl)
                {
                    if (0 != string.Compare(IPAddressUtil.GetName(slave), IPAddressUtil.GetName(masterhost), StringComparison.OrdinalIgnoreCase))
                    {
                        aml.Add(slave);
                    }
                }
                allmachines = aml.ToArray();
            }

            {

                Console.WriteLine("Ensure cluster is perfectly healthy...");
                EnsurePerfectQizmtHealtha();

                Console.WriteLine("Run job in one thread, kill from another...");

                //System.Threading.Thread.Sleep(1000 * 8);

                string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_Kill-" + Guid.NewGuid().ToString();
                if (!System.IO.Directory.Exists(exectempdir))
                {
                    System.IO.Directory.CreateDirectory(exectempdir);
                }
                string execfp = exectempdir + @"\exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}";
                System.IO.File.WriteAllText(execfp, (@"<SourceCode>
<Jobs>
<Job Name=`exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}_Preprocessing 1`>
  <IOSettings>
    <JobType>local</JobType>
  </IOSettings>
  <Local>
    <![CDATA[
    public virtual void Local()
    {
        // Sleep forever so that kill will take it down.
        System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
    }
    ]]>
  </Local>
</Job>
<Job Name=`exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}_Preprocessing 2`>
  <IOSettings>
    <JobType>local</JobType>
  </IOSettings>
  <Local>
    <![CDATA[
    public virtual void Local()
    {
        Qizmt_Log(`{DC46FA81-A69F-4d46-9A30-54869168916B}`);
    }
    ]]>
  </Local>
</Job>
</Jobs>
</SourceCode>
").Replace('`', '"'));
                Exec.Shell("Qizmt importdir " + exectempdir);
                try
                {
                    try
                    {
                        System.IO.File.Delete(execfp);
                        System.IO.Directory.Delete(exectempdir);
                    }
                    catch
                    {
                    }

                    bool execdone = false;
                    bool execx = false;
                    System.Threading.Thread execthread = new System.Threading.Thread(
                        new System.Threading.ThreadStart(
                        delegate()
                        {
                            try
                            {
                                Console.WriteLine("    Running exec...");
                                string output = Exec.Shell("Qizmt exec exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}");
                                Console.WriteLine("exec output: {0}", output.Trim());
                                if (-1 != output.IndexOf("{DC46FA81-A69F-4d46-9A30-54869168916B}"))
                                {
                                    lock (typeof(Program))
                                    {
                                        execx = true;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Console.Error.WriteLine("Warning: exec exception: {0}", e.ToString());
                            }
                            lock (typeof(Program))
                            {
                                execdone = true;
                            }
                        }));
                    execthread.Start();

                    // Wait a few seconds to give the exec a chance to get started.
                    System.Threading.Thread.Sleep(1000 * 5);

                    lock (typeof(Program))
                    {
                        if (execx)
                        {
                            throw new Exception("exec completed; problem with test");
                        }
                        if (execdone)
                        {
                            throw new Exception("exec finished too early, did not get a chance to call kill");
                        }
                    }

                    int execjid = 0;
                    string execsjid = "0";
                    string psexecline = "N/A";
                    {
                        foreach (string psln in Exec.Shell("qizmt ps").Split('\n'))
                        {
                            if (-1 != psln.IndexOf("exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}"))
                            {
                                psexecline = psln.Trim();
                                {
                                    int isp = psexecline.IndexOf(' ');
                                    if (-1 != isp)
                                    {
                                        execsjid = psexecline.Substring(0, isp);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    if (!int.TryParse(execsjid, out execjid) || execjid < 1)
                    {
                        throw new Exception("JID for job not valid: " + execsjid);
                    }
                    execsjid = execjid.ToString(); // Normalize.

                    Console.WriteLine("    Running kill... ({0})", psexecline);
                    Exec.Shell("Qizmt kill " + execsjid);

                    lock (typeof(Program))
                    {
                        if (execx)
                        {
                            throw new Exception("exec completed; problem with test");
                        }
                        if (!execdone)
                        {
                            throw new Exception("kill completed but exec has not yet returned");
                        }
                    }

                    // Wait a couple seconds to give the services a chance to come back up.
                    System.Threading.Thread.Sleep(1000 * 2);

                }
                finally
                {
                    try
                    {
                        Exec.Shell("Qizmt del exec{3EAE6884-28BB-4340-8F94-6E9421B68C92}");
                    }
                    catch
                    {
                    }
                }

                Console.WriteLine("[PASSED] - " + string.Join(" ", args));


            }

        }


    }
}
