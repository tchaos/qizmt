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

        static void EnableReplication(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            bool withcache = -1 != args[0].IndexOf("withcache", StringComparison.OrdinalIgnoreCase);
            string dfsxmlpath = args[1];
            string dfsxmlpathbackup = dfsxmlpath + "$" + Guid.NewGuid().ToString();

            long bytes_to_add = 0;
            if (args.Length > 2)
            {
                // To-do: ParseCapacity.
                bytes_to_add = long.Parse(args[2]);
                if (bytes_to_add < 0)
                {
                    throw new Exception("Invalid bytes-to-add (" + bytes_to_add.ToString() + " bytes)");
                }
                if (bytes_to_add < 1048576)
                {
                    throw new Exception("bytes-to-add must be at least 1 MB");
                }
            }

            int num_files = 0;
            if (args.Length > 3)
            {
                num_files = int.Parse(args[3]);
                if (num_files < 0 || num_files > bytes_to_add / 20)
                {
                    throw new Exception("Invalid #files");
                }
            }

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

                    Console.WriteLine("Adding some files to DFS...");
                    Console.Write("    ");
                    Exec.Shell("Qizmt bingen 1MB 1MB 50");
                    Console.Write("10%");
                    Exec.Shell("Qizmt examples");
                    Console.Write("..15%");
                    Exec.Shell("Qizmt wordgen 10MB 10MB 100"); // Note: also used by Cacher.
                    Console.Write("..50%");
                    Exec.Shell("Qizmt asciigen 50MB 50MB 500");
                    Console.Write("..100%");
                    Console.WriteLine();
                    if (bytes_to_add > 0)
                    {
                        Console.WriteLine("Adding {0} bytes as requested (bytes-to-add)...", bytes_to_add);
                        long bta10 = bytes_to_add / 10;
                        Console.Write("    ");
                        Exec.Shell("Qizmt gen bta10-" + Guid.NewGuid().ToString() + " " + bta10.ToString());
                        Console.Write("10%");
                        Exec.Shell("Qizmt gen bta20-" + Guid.NewGuid().ToString() + " " + (bta10 * 2).ToString());
                        Console.Write("..30%");
                        {
                            long totsz = (bta10 * 3);
                            if (num_files > 1)
                            {
                                long onesz = totsz / num_files;
                                //for (int inf = 0; inf < num_files; inf++)
                                MySpace.DataMining.Threading.ThreadTools.Parallel(
                                    new Action<int>(
                                    delegate(int inf)
                                    {
                                        Exec.Shell("Qizmt gen bta30." + inf.ToString() + "-" + Guid.NewGuid().ToString() + " " + onesz.ToString());
                                    }), num_files, 15);
                            }
                            else
                            {
                                Exec.Shell("Qizmt gen bta30-" + Guid.NewGuid().ToString() + " " + totsz.ToString());
                                Console.Write("..60%");
                            }
                        }
                        Exec.Shell("Qizmt gen bta40-" + Guid.NewGuid().ToString() + " " + (bta10 * 4).ToString());
                        Console.Write("..100%");
                        Console.WriteLine();
                    }

                    if (withcache)
                    {
                        Console.WriteLine("Generating cache files...");
                        string cachertempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_Cacher-" + Guid.NewGuid().ToString();
                        if(!System.IO.Directory.Exists(cachertempdir))
                        {
                            System.IO.Directory.CreateDirectory(cachertempdir);
                        }
                        string cacherfp = cachertempdir + @"\Cacher";
                        System.IO.File.WriteAllText(cacherfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`Cacher`>
        <Delta>
            <Name>Cacher_cache</Name>
            <DFSInput>dfs://10MB</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://Cacher_output</DFSOutput>
        <KeyMajor>8</KeyMajor>
        <OutputMethod>grouped</OutputMethod>
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
                        Exec.Shell("Qizmt importdir " + cachertempdir);
                        try
                        {
                            System.IO.File.Delete(cacherfp);
                            System.IO.Directory.Delete(cachertempdir);
                        }
                        catch
                        {
                        }
                        Exec.Shell("Qizmt exec Cacher"); // Creates cache file Cacher_cache
                        Exec.Shell("Qizmt del Cacher_output");
                    }

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    string ls_output = Exec.Shell("Qizmt ls");
                    int ls_output_linecount = ls_output.Split('\n').Length;

                    Console.WriteLine("*** ls output before replication:");
                    Console.WriteLine(ls_output);

                    Console.WriteLine("Updating Replication Factor...");
                    const int replicationfactor = 2;
                    Console.WriteLine(Exec.Shell("Qizmt replicationupdate " + replicationfactor.ToString()));

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    if (withcache)
                    {
                        Console.WriteLine("Validate cache files...");
                        Exec.Shell("Qizmt exec Cacher"); // Uses existing cache file Cacher_cache
                        Exec.Shell("Qizmt del Cacher_output");
                    }

                    Console.WriteLine("Ensure data is replicated...");
                    EnsureReplication(dfsxmlpath, replicationfactor);
                    {
                        // Not comparing contents because of the free disk space line.
                        string new_ls_output = Exec.Shell("Qizmt ls");
                        Console.WriteLine("*** ls output after replication:");
                        Console.WriteLine(new_ls_output);
                        int new_ls_output_linecount = new_ls_output.Split('\n').Length;
                        if (ls_output_linecount != new_ls_output_linecount)
                        {
                            throw new Exception("Cluster does not contain the same files as before replication");
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
