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

        static void ReplicationChecks(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];
            string dfsxmlpathbackup = dfsxmlpath + "$" + Guid.NewGuid().ToString();

            string masterdir;
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(dfsxmlpath);
                masterdir = fi.DirectoryName; // Directory's full path.
            }
            Surrogate.SetNewMetaLocation(masterdir);

            dfs olddfs = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            string masterhost = System.Net.Dns.GetHostName();
            string[] allmachines;
            {
                string[] sl = olddfs.Slaves.SlaveList.Split(';');
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

            Console.WriteLine("Backing up DFS.xml to: {0} ...", dfsxmlpathbackup);
            try
            {
                System.IO.File.Delete(dfsxmlpathbackup);
            }
            catch
            {
            }
            System.IO.File.Move(dfsxmlpath, dfsxmlpathbackup);
            try
            {
                {
                    Console.WriteLine("Formatting DFS for test...");
                    Exec.Shell("Qizmt @format Machines=" + string.Join(",", allmachines));
                }

                {
                    // Test logic:

                    {
                        long XBYTES = (long)4194304 * (long)allmachines.Length;

                        Console.WriteLine("Generating data...");
                        Console.Write("    ");
                        Exec.Shell("Qizmt gen data{476D6FE8-D645-41cc-83A1-3AB5E2DE23E7} " + (XBYTES / 4).ToString());
                        Console.Write("25%");
                        Exec.Shell("Qizmt gen data{61136275-16EC-4ff9-84CE-ACC967550181} " + (XBYTES / 4).ToString());
                        Console.Write("..50%");
                        Exec.Shell("Qizmt gen data{C76F6C06-EFC8-4808-B214-DB4D167171EB} " + (XBYTES / 2).ToString());
                        Console.Write("..100%");
                        Console.WriteLine();
                    }

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    Console.WriteLine("Raising replication factor to 2...");
                    Exec.Shell("Qizmt replicationupdate 2");

                    {
                        Console.WriteLine("Raising replication factor too high (ensure fail)...");
                        bool ok = false;
                        System.Threading.Thread thd = new System.Threading.Thread(
                            new System.Threading.ThreadStart(
                            delegate
                            {
                                try
                                {
                                    Exec.Shell("Qizmt replicationupdate 999999999");
                                }
                                catch (Exception e)
                                {
                                    ok = true;
                                    Console.WriteLine("Got exception as expected: {0}", e.Message);
                                }
                            }));
                        thd.Start();
                        if (!thd.Join(1000 * 10))
                        {
                            thd.Abort();
                        }
                        if (!ok)
                        {
                            throw new Exception("Test failed: expected exception");
                        }
                    }

                }

                Console.WriteLine("[PASSED] - " + string.Join(" ", args));

            }
            finally
            {
                Console.WriteLine("Restoring DFS.xml backup...");
                // Note: these are safe; the try/finally only wraps the new dfs.
                try
                {
                    Exec.Shell("Qizmt del *");
                }
                catch
                {
                }
                try
                {
                    // Delete temp dfs.xml, it's being replaced with the good one.
                    System.IO.File.Delete(dfsxmlpath);
                }
                catch
                {
                }
                System.IO.File.Move(dfsxmlpathbackup, dfsxmlpath);
            }

        }


    }
}
