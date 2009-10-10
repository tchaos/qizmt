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

        static void ReplicationFailover(string[] args)
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
                    // Note: added replication and much lower DataNodeBaseSize!
                    Console.WriteLine("Formatting DFS with Replication=3 for test...");
                    Exec.Shell("Qizmt @format Machines=" + string.Join(",", allmachines) + " Replication=3 DataNodeBaseSize=1048576");
                }

                {
                    // Test logic:

                    if (allmachines.Length < 3)
                    {
                        throw new Exception("This test needs a cluster of at least 3 machines!");
                    }

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

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    Console.WriteLine("Running job on healthy cluster...");
                    string exec_md5;
                    {
                        string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_ReplicationFailover-" + Guid.NewGuid().ToString();
                        if (!System.IO.Directory.Exists(exectempdir))
                        {
                            System.IO.Directory.CreateDirectory(exectempdir);
                        }
                        string execfp = exectempdir + @"\exec{FA19CAB0-5225-4cc8-8728-9BFC3A1B834C}";
                        System.IO.File.WriteAllText(execfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`exec{FA19CAB0-5225-4cc8-8728-9BFC3A1B834C}`>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://data{*}</DFSInput>
        <DFSOutput>dfs://output{04454992-E2CD-4342-AEEB-1D0607B32D84}</DFSOutput>
        <KeyMajor>8</KeyMajor>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              output.Add(line, ByteSlice.Prepare());
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(key);
              }
          }
       ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                        Exec.Shell("Qizmt importdir " + exectempdir);
                        try
                        {
                            System.IO.File.Delete(execfp);
                            System.IO.Directory.Delete(exectempdir);
                        }
                        catch
                        {
                        }
                        Exec.Shell("Qizmt exec exec{FA19CAB0-5225-4cc8-8728-9BFC3A1B834C}");
                        exec_md5 = DfsSum("md5", "output{04454992-E2CD-4342-AEEB-1D0607B32D84}");
                        Exec.Shell("Qizmt del output{04454992-E2CD-4342-AEEB-1D0607B32D84}");
                    }

                    try
                    {
                        Console.WriteLine("Disrupting 2 machines...");
                        {
                            string badmachine = allmachines[allmachines.Length - 1];
                            Console.WriteLine("    Bad disk on {0}", badmachine);
                            string netpath = Surrogate.NetworkPathForHost(badmachine);
                            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zd.*.zd"))
                            {
                                if (!fi.Name.StartsWith("zd.!."))
                                {
                                    System.IO.File.Move(fi.FullName, fi.DirectoryName + @"\zd.!." + fi.Name.Substring(3));
                                }
                            }
                        }
                        {
                            string badmachine = allmachines[allmachines.Length - 2];
                            Console.WriteLine("    Bad network connection on {0}", badmachine);
                            Exec.Shell(@"sc \\" + badmachine + @" stop DistributedObjects");
                        }

                        Console.WriteLine("Ensure the cluster is NOT perfectly healthy...");
                        {
                            bool healthy;
                            try
                            {
                                EnsurePerfectQizmtHealtha();
                                healthy = true;
                            }
                            catch
                            {
                                healthy = false;
                            }
                            if (healthy)
                            {
                                throw new Exception("Cluster is still healthy");
                            }
                        }

                        Console.WriteLine("Running job on unhealthy cluster...");
                        {
                            try
                            {
                                Exec.Shell("Qizmt exec exec{FA19CAB0-5225-4cc8-8728-9BFC3A1B834C}");
                            }
                            catch
                            {
                                // Replication will output a warning and throw an exception,
                                // so we need to ignore that exception.
                                // The MD5 check will ensure it ran fine.
                            }
                            string new_exec_md5 = DfsSum("md5", "output{04454992-E2CD-4342-AEEB-1D0607B32D84}");
                            Exec.Shell("Qizmt del output{04454992-E2CD-4342-AEEB-1D0607B32D84}");
                            if (new_exec_md5 != exec_md5)
                            {
                                throw new Exception("Output files from before and after disrupting cluster do not match");
                            }
                        }

                    }
                    finally
                    {
                        {
                            Console.WriteLine("Repairing disrupted disk");
                            string badmachine = allmachines[allmachines.Length - 1];
                            string netpath = Surrogate.NetworkPathForHost(badmachine);
                            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zd.!.*.zd"))
                            {
                                System.IO.File.Move(fi.FullName, fi.DirectoryName + @"\zd." + fi.Name.Substring(5));
                            }
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

                {
                    // Note: killall issued to fix disrupted machines
                    Console.WriteLine("Running killall to repair");
                    Exec.Shell("Qizmt killall -f");
                }

            }

        }


    }
}
