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

        static void Replication(string[] args)
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

                    Console.WriteLine(">>> RUNNING TESTS WITH REPLICATION=2");
                    _ReplicationRunAllCommands(allmachines);
                    EnsureReplication(dfsxmlpath, 2);

                    Console.WriteLine(">>> DISABLING REPLICATION");
                    Console.Write(Exec.Shell("Qizmt replicationupdate 1"));
                    EnsureReplication(dfsxmlpath, 1);
                    Exec.Shell("Qizmt del *"); // Have to delete everything so next run can recreate.

                    Console.WriteLine(">>> RUNNING TESTS WITH REPLICATION=1");
                    _ReplicationRunAllCommands(allmachines);
                    EnsureReplication(dfsxmlpath, 1);

                    Console.WriteLine(">>> DONE");

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


        static void EnsureReplication(string dfsxmlpath, int replicationfactor)
        {
            bool toomany = false;
            Console.WriteLine("Ensure data is replicated...");
            {
                dfs dc = dfs.ReadDfsConfig_unlocked(dfsxmlpath);
                foreach (dfs.DfsFile df in dc.Files)
                {
                    if (0 == string.Compare(DfsFileTypes.NORMAL, df.Type, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(DfsFileTypes.BINARY_RECT, df.Type, StringComparison.OrdinalIgnoreCase))
                    {
                        foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                        {
                            string[] nhosts = fn.Host.Split(';', ',');
                            if (nhosts.Length < replicationfactor)
                            {
                                throw new Exception("dfs://" + df.Name + " node " + fn.Name + " does not live on " + replicationfactor.ToString() + " machines");
                            }
                            else if (nhosts.Length > replicationfactor)
                            {
                                if (!toomany)
                                {
                                    toomany = true;
                                    Console.Error.WriteLine("Warning: too many replicates for one or more DFS file chunks");
                                }
                            }
                            for (int ni = 0; ni < nhosts.Length; ni++)
                            {
                                string np = Surrogate.NetworkPathForHost(nhosts[ni]) + @"\" + fn.Name;
                                if (!System.IO.File.Exists(np))
                                {
                                    throw new Exception("dfs://" + df.Name + " node " + fn.Name + " does not actually live on host " + nhosts[ni] + " as indicated by meta-data [" + np + "]");
                                }
                                if (df.HasZsa)
                                {
                                    if (!System.IO.File.Exists(np + ".zsa"))
                                    {
                                        throw new Exception("Sample data for dfs://" + df.Name + " node " + fn.Name + " (" + fn.Name + ".zsa) does not actually live on host " + nhosts[ni] + " as indicated by meta-data [" + np + ".zsa]");
                                    }
                                }
                            }
                        }
                    }

                }
            }
        }


        static void _ReplicationRunAllCommands(string[] allmachines)
        {

            //long XBYTES = (long)67108864 * (long)allmachines.Length;
            long XBYTES = (long)4194304 * (long)allmachines.Length;

            {
                Console.WriteLine("RT: wordgen");
                Console.Write(Exec.Shell("Qizmt wordgen wordgen{92D6883B-D950-4beb-8281-8599F352EAF7} " + XBYTES.ToString()));
            }

            if (allmachines.Length <= 2)
            {
                Console.WriteLine(" *** Skipping removemachine and addmachine: not enough machines ***");
            }
            else
            {
                {
                    Console.WriteLine("RT: removemachine");
                    Console.Write(Exec.Shell("Qizmt removemachine " + allmachines[allmachines.Length - 1]));
                }

                {
                    Console.WriteLine("RT: addmachine");
                    Console.Write(Exec.Shell("Qizmt addmachine " + allmachines[allmachines.Length - 1]));
                }
            }

            {
                Console.WriteLine("RT: asciigen");
                Console.Write(Exec.Shell("Qizmt asciigen asciigen{161C30D0-2810-4861-B517-AAFB207B8887} " + XBYTES.ToString()));
            }

            {
                Console.WriteLine("RT: bingen");
                Console.Write(Exec.Shell("Qizmt bingen bingen{E59A0DA4-9D69-4e62-A03B-4085B75956D0} " + XBYTES.ToString()));
            }

            {
                Console.WriteLine("RT: combine");
                Console.Write(Exec.Shell("Qizmt wordgen combine1part{CB302F9E-6291-4e60-8D95-EA816A116912} 1MB"));
                Console.Write(Exec.Shell("Qizmt wordgen combine2part{D035B44D-210C-4969-B8DE-3E8FDEE0FA32} 1MB"));
                Console.Write(Exec.Shell("Qizmt combine combine1part{CB302F9E-6291-4e60-8D95-EA816A116912} combine2part{D035B44D-210C-4969-B8DE-3E8FDEE0FA32} + combine{8FAD1E43-0F6F-4d81-9D96-22D095994374}"));
            }

            {
                Console.WriteLine("RT: info");
                Console.Write(Exec.Shell("Qizmt info"));
                Console.Write(Exec.Shell("Qizmt info wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}"));
                Console.Write(Exec.Shell("Qizmt info wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}:" + allmachines[allmachines.Length - 1]));
            }

            // Ensure temp dir exists.
            string tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\c$\temp\qizmt" + Guid.NewGuid().ToString();
            if (!System.IO.Directory.Exists(tempdir))
            {
                System.IO.Directory.CreateDirectory(tempdir);
            }

            {
                Console.WriteLine("RT: put");
                string fsfn = "put{6619C2FF-1816-4adc-8EE9-12F84F290DCC}";
                string fsfile = tempdir + @"\" + fsfn;
                System.IO.File.WriteAllText(fsfile, "Replication test for Qizmt" + Environment.NewLine
                    + "{C3CBD581-58C7-42bb-B5AE-70ABAD806B65}" + Environment.NewLine);
                Console.Write(Exec.Shell("Qizmt put " + fsfile + " " + fsfn));
                System.IO.File.Delete(fsfile);
            }

            {
                Console.WriteLine("RT: putbinary");
                string fsfn = "putbinary{B581025F-9695-4433-804B-CA9E05D80E3D}";
                string fsfile = tempdir + @"\" + fsfn;
                System.IO.File.WriteAllText(fsfile, "Replication test for Qizmt" + Environment.NewLine
                    + "{E8406AF3-5482-4428-A973-46FF9E6D3F6A}" + Environment.NewLine);
                Console.Write(Exec.Shell("Qizmt putbinary " + fsfile + " " + fsfn));
                System.IO.File.Delete(fsfile);
            }

            {
                Console.WriteLine("RT: head");
                string output = Exec.Shell("Qizmt head put{6619C2FF-1816-4adc-8EE9-12F84F290DCC} 1");
                Console.Write(output);
                string oneline = output.Trim();
                string[] lines = oneline.Split('\n');
                if (1 != lines.Length)
                {
                    throw new Exception("Expected one line from head <file> 1");
                }
            }

            {
                Console.WriteLine("RT: get");
                string fsfn = "get{F6179A4C-8FC1-41f1-895E-05B23023F01A}";
                string fsfile = tempdir + @"\" + fsfn;
                Console.Write(Exec.Shell("Qizmt get put{6619C2FF-1816-4adc-8EE9-12F84F290DCC} " + fsfile));
                if (-1 == System.IO.File.ReadAllText(fsfile).IndexOf("{C3CBD581-58C7-42bb-B5AE-70ABAD806B65}"))
                {
                    throw new Exception("Qizmt get failure");
                }
                System.IO.File.Delete(fsfile);
            }

            {
                Console.WriteLine("RT: getbinary");
                string fsfile = tempdir + @"\putbinary{B581025F-9695-4433-804B-CA9E05D80E3D}";
                Console.Write(Exec.Shell("Qizmt getbinary putbinary{B581025F-9695-4433-804B-CA9E05D80E3D} " + tempdir));
                if (-1 == System.IO.File.ReadAllText(fsfile).IndexOf("{E8406AF3-5482-4428-A973-46FF9E6D3F6A}"))
                {
                    throw new Exception("Qizmt getbinary failure");
                }
                System.IO.File.Delete(fsfile);
            }

            {
                Console.WriteLine("RT: rename");
                Console.Write(Exec.Shell("Qizmt rename asciigen{161C30D0-2810-4861-B517-AAFB207B8887} rename{0AD29C98-D5F9-4501-B5EA-735BFDD119A7}"));
                // NOTE: asciigen file is gone now!
            }

            {
                Console.WriteLine("RT: exec");
                string cachertempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_Cacher-" + Guid.NewGuid().ToString();
                if (!System.IO.Directory.Exists(cachertempdir))
                {
                    System.IO.Directory.CreateDirectory(cachertempdir);
                }
                string cacherfp = cachertempdir + @"\exec{B9C92E20-9B5F-47a1-8081-B07560EE606E}";
                System.IO.File.WriteAllText(cacherfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`Cacher`>
        <Delta>
            <Name>cacheB9DE42AC-2823-4ad9-9101-54DE52BDCECE</Name>
            <DFSInput>dfs://wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}</DFSInput>
        </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://exec-output{5E745D71-DF26-4332-A023-69DF5D99502B}</DFSOutput>
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
                Exec.Shell("Qizmt importdir " + cachertempdir);
                try
                {
                    System.IO.File.Delete(cacherfp);
                    System.IO.Directory.Delete(cachertempdir);
                }
                catch
                {
                }
                Console.Write(Exec.Shell("Qizmt exec exec{B9C92E20-9B5F-47a1-8081-B07560EE606E}"));
            }

            {
                Console.WriteLine("RT: del");
                Console.Write(Exec.Shell("Qizmt del rename{0AD29C98-D5F9-4501-B5EA-735BFDD119A7}")); // Data
                Console.Write(Exec.Shell("Qizmt del cacheB9DE42AC-2823-4ad9-9101-54DE52BDCECE")); // DeltaCache
                Console.Write(Exec.Shell("Qizmt del exec{B9C92E20-9B5F-47a1-8081-B07560EE606E}")); // Job
            }

            {
                Console.WriteLine("RT: sorted");
                if (-1 == Exec.Shell("Qizmt sorted wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}").IndexOf("Not sorted"))
                {
                    throw new Exception("Expected unsorted content: wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}");
                }
                if (-1 == Exec.Shell("Qizmt sorted exec-output{5E745D71-DF26-4332-A023-69DF5D99502B}").IndexOf("Sorted"))
                {
                    throw new Exception("Expected sorted content: exec-output{5E745D71-DF26-4332-A023-69DF5D99502B}");
                }
            }

            {
                Console.WriteLine("RT: md5");
                if (32 != DfsSum("md5", "exec-output{5E745D71-DF26-4332-A023-69DF5D99502B}").Length)
                {
                    throw new Exception("Expected 32 bytes of MD5 hex string");
                }
            }

            {
                Console.WriteLine("RT: checksum");
                if (DfsSum("wordgen{92D6883B-D950-4beb-8281-8599F352EAF7}") != DfsSum("exec-output{5E745D71-DF26-4332-A023-69DF5D99502B}"))
                {
                    throw new Exception("Checksum of input file (unsorted) does not match checksum of output file (sorted) for exec job");
                }
            }

            {
                Console.WriteLine("RT: ls");
                Console.Write(Exec.Shell("Qizmt ls"));
            }

            {
                Console.WriteLine("RT: health");
                EnsurePerfectQizmtHealtha();
            }

            try
            {
                System.IO.Directory.Delete(tempdir);
            }
            catch
            {
            }

        }


    }
}
