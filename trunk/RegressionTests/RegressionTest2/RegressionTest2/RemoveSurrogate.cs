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

        static void RemoveSurrogate(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];
            string dfsxmlpathbackup = dfsxmlpath + "$" + Guid.NewGuid().ToString();

            bool incluster;
            if (args[2] == "incluster")
            {
                incluster = true;
            }
            else if (args[2] == "isolated")
            {
                incluster = false;
            }
            else
            {
                throw new Exception("Expected: incluster or isolated");
            }

            string masterdir;
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(dfsxmlpath);
                masterdir = fi.DirectoryName; // Directory's full path.
            }
            Surrogate.SetNewMetaLocation(masterdir);
            string masterslavedat = masterdir + @"\slave.dat";

            dfs olddfs = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            string masterhost = System.Net.Dns.GetHostName();
            List<string> otherhosts = new List<string>(); // Non-surrogate machines, reguardless if participating surrogate or not.
            foreach (string slave in olddfs.Slaves.SlaveList.Split(';'))
            {
                if (0 != string.Compare(IPAddressUtil.GetName(slave), IPAddressUtil.GetName(masterhost), StringComparison.OrdinalIgnoreCase))
                {
                    otherhosts.Add(slave);
                }
            }
            string newmaster = otherhosts[0];

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
                StringBuilder sbmachines = new StringBuilder(1000);
                if (incluster)
                {
                    sbmachines.Append(masterhost);
                }
                foreach (string host in otherhosts)
                {
                    if (0 != sbmachines.Length)
                    {
                        sbmachines.Append(',');
                    }
                    sbmachines.Append(host);
                }

                Console.WriteLine("Formatting DFS for test...");
                Exec.Shell("Qizmt @format Machines=" + sbmachines.ToString());

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

                Console.WriteLine("Removing Surrogate (removemachine {0}) ...", masterhost);
                Console.WriteLine(Exec.Shell("Qizmt removemachine " + masterhost));

                Console.WriteLine("Interface with new surrogate...");
                System.IO.File.WriteAllText(masterslavedat, "master=" + newmaster + Environment.NewLine);
                {
                    // Not comparing contents because of the free disk space line.
                    int new_ls_output_linecount = Exec.Shell("Qizmt ls").Split('\n').Length;
                    if (ls_output_linecount != new_ls_output_linecount)
                    {
                        throw new Exception("Cluster does not contain the same files as before removemachine " + masterdir + ", or problem issuing commands on new surrogate");
                    }
                }

                Console.WriteLine("Ensure the cluster is perfectly healthy...");
                EnsurePerfectQizmtHealtha();

                Console.WriteLine("Run test job, confirm output...");
                if (md5_10MB_output != Exec.Shell("Qizmt md5 10MB"))
                {
                    throw new Exception("Test job output does not match previous run");
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
                    System.IO.File.Delete(masterslavedat);
                }
                catch
                {
                }
                for (int si = 0; si < otherhosts.Count; si++)
                {
                    try
                    {
                        System.IO.File.Delete(Surrogate.NetworkPathForHost(otherhosts[si]) + @"\slave.dat");
                        // Deleting dfs.xml should go last because it'll usually fail.
                        System.IO.File.Delete(Surrogate.NetworkPathForHost(otherhosts[si]) + @"\dfs.xml");
                    }
                    catch
                    {
                    }
                }
                try
                {
                    System.IO.File.Delete(dfsxmlpath);
                }
                catch
                {
                }
                try
                {
                    // Reformat the cluster so stuff like slave.dat is correct...
                    Exec.Shell("Qizmt @format Machines=" + string.Join(",", otherhosts.ToArray()));
                }
                catch(Exception exf)
                {
                    Console.Error.WriteLine("Problem during reformat, there may be an issue with the cluster", exf);
                }
                try
                {
                    // Delete the dfs.xml just written, it's being replaced with the good one.
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
