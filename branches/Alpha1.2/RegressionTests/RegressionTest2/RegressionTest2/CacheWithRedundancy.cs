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

        static void CacheWithRedundancy(string[] args)
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
                    // Note: added replication.
                    Console.WriteLine("Formatting DFS with Replication for test...");
                    Exec.Shell("Qizmt @format Machines=" + string.Join(",", allmachines) + " Replication=2");
                }

                {
                    // Test logic:

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    Console.WriteLine("Adding files to DFS...");
                    Exec.Shell("Qizmt wordgen Cacher_input 10MB 100");


                    Console.WriteLine("Generating cache files...");
                    string cachertempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_Cacher-" + Guid.NewGuid().ToString();
                    if (!System.IO.Directory.Exists(cachertempdir))
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
            <DFSInput>dfs://Cacher_input</DFSInput>
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
                    string cacher_output_sum = DfsSum("Cacher_output");
                    string cacher_output_md5 = DfsSum("md5", "Cacher_output");
                    if (cacher_output_sum != DfsSum("Cacher_input"))
                    {
                        throw new Exception("Output file does not have same checksum as input file");
                    }
                    Exec.Shell("Qizmt del Cacher_output");

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    Console.WriteLine("Validate cache files...");
                    Exec.Shell("Qizmt exec Cacher"); // Uses existing cache file Cacher_cache
                    if (cacher_output_sum != DfsSum("Cacher_output"))
                    {
                        throw new Exception("Output file not the same when using cache (sum)");
                    }
                    if (cacher_output_md5 != DfsSum("md5", "Cacher_output"))
                    {
                        throw new Exception("Output file not the same when using cache (md5)");
                    }
                    Exec.Shell("Qizmt del Cacher_output");

                    Console.WriteLine("Ensure the cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();


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
