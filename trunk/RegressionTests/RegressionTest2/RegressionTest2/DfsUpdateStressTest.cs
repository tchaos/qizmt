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

        static void DfsUpdateStressTest(string[] args)
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

                Console.WriteLine("Stressing DFS updates...");

                //System.Threading.Thread.Sleep(1000 * 8);

                string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_DfsUpdateStressTest-" + Guid.NewGuid().ToString();
                if (!System.IO.Directory.Exists(exectempdir))
                {
                    System.IO.Directory.CreateDirectory(exectempdir);
                }
                string execfp = exectempdir + @"\exec{8B8F731B-3BEC-4e99-B08F-BDEB81525172}";
                const int NUMBER_OF_JOBS = 50; // <--STRESS-NUMBER--
                for (int njob = 0; njob < NUMBER_OF_JOBS; njob++)
                {
                    System.IO.File.WriteAllText(execfp + njob.ToString(), (@"<SourceCode><Jobs></Jobs></SourceCode>").Replace('`', '"'));
                }
                try
                {
                    Exec.Shell("Qizmt importdirmt " + exectempdir);

                    Console.WriteLine("Confirming updates...");
                    {
                        string lsoutput = Exec.Shell("Qizmt ls");
                        int njobs = 0;
                        for (int i = 0; ; )
                        {
                            if (njobs == NUMBER_OF_JOBS)
                            {
                                break; // Good!
                            }
                            i = lsoutput.IndexOf("{8B8F731B-3BEC-4e99-B08F-BDEB81525172}", i);
                            if (-1 == i)
                            {
                                throw new Exception("Not all updates to DFS were written (only found " + njobs.ToString() + " jobs imported, expected " + NUMBER_OF_JOBS.ToString() + ")");
                            }
                            i += "{8B8F731B-3BEC-4e99-B08F-BDEB81525172}".Length;
                            njobs++;
                        }
                    }
                }
                finally
                {
                    try
                    {
                        for (int njob = 0; njob < NUMBER_OF_JOBS; njob++)
                        {
                            System.IO.File.Delete(execfp + njob.ToString());
                        }
                        System.IO.Directory.Delete(exectempdir);
                    }
                    catch
                    {
                    }
                    try
                    {
                        Exec.Shell("Qizmt del exec{8B8F731B-3BEC-4e99-B08F-BDEB81525172}*");
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
