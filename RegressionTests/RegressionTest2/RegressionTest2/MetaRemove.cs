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

        static void MetaRemoveMachine(string[] args)
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
            string masterslavedat = masterdir + @"\slave.dat";

            dfs olddfs = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            int iarg = 2;

            string sreplication = "2";
            /*if (args.Length > iarg)
            {
                if (args[iarg].StartsWith("#"))
                {
                    sreplication = args[iarg++].Substring(1);
                }
            }*/

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

            //if (three2two)
            {
                if (allmachines.Length < 3)
                {
                    //throw new Exception("Need >= 3 machines for 3to2");
                    throw new Exception("Need >= 3 machines for this test");
                }
                allmachines = new string[] { allmachines[0], allmachines[1], allmachines[2] };
            }

            if (allmachines.Length < 3)
            {
                throw new Exception("Cluster needs at least 3 machines for this test");
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
                Console.WriteLine("Formatting DFS for test...");
                {
                    string fmtcmd = "Qizmt @format Machines=" + string.Join(",", allmachines) + " Replication=" + sreplication;
                    Console.WriteLine("    {0}", fmtcmd);
                    Exec.Shell(fmtcmd);
                }

                Console.WriteLine("Adding some files to DFS...");
                Console.Write("    ");
                Exec.Shell("Qizmt bingen 1MB 1MB 50");
                Console.Write("10%");
                Exec.Shell("Qizmt examples");
                Console.Write("..15%");
                Exec.Shell("Qizmt wordgen 10MB 10MB 100");
                Console.Write("..50%");
                Exec.Shell("Qizmt asciigen 50MB 50MB 500");
                Console.Write("..100%");
                Console.WriteLine();
                int ls_output_linecount = Exec.Shell("Qizmt ls").Split('\n').Length;

                Console.WriteLine("Ensure the cluster is perfectly healthy...");
                EnsurePerfectQizmtHealtha();

                Console.WriteLine("Run test job, save output...");
                string md5_10MB_output = Exec.Shell("Qizmt md5 10MB");

                {
                    string rmachine = allmachines[allmachines.Length - 1];
                    Console.WriteLine("Removing machine (metaremovemachine -s {0}) ...", rmachine);
                    Console.WriteLine(Exec.Shell("Qizmt metaremovemachine -s " + rmachine));
                }

                Console.WriteLine("Run test job, confirm output...");
                if (md5_10MB_output != Exec.Shell("Qizmt md5 10MB"))
                {
                    throw new Exception("Test job output does not match previous run");
                }

                Console.WriteLine("Ensuring meta-removing another machine fails (no replicationphase)");
                {
                    bool failed = false;
                    string rmachine = allmachines[allmachines.Length - 2];
                    try
                    {
                        
                        Console.WriteLine("Removing machine (metaremovemachine -s {0}) ...", rmachine);
                        Console.WriteLine(Exec.Shell("Qizmt metaremovemachine -s " + rmachine));
                    }
                    catch
                    {
                        failed = true;
                    }
                    if (!failed)
                    {
                        throw new Exception("metaremovemachine -s " + rmachine + " was supposed to fail");
                    }

                    Console.WriteLine("Forcing meta-removal (metaremovemachine -s -f {0})", rmachine);
                    Console.WriteLine(Exec.Shell("Qizmt metaremovemachine -s -f " + rmachine));
                }

                Console.WriteLine("Run test job yet again, confirm output is different, due to missing parts...");
                try
                {
                    if (md5_10MB_output == Exec.Shell("Qizmt md5 10MB"))
                    {
                        throw new Exception("Test job output was not supposed to match previous run, but it matches");
                    }
                    Console.WriteLine("Doesn't match; as expected");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Doesn't match due to exception: {0}", e.Message);
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
