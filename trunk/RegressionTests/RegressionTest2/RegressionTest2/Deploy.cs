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

        static void Deploy(string[] args)
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

                // Run a job...
                string exec_md5;
                {
                    // Generate some data to operate on.
                    Exec.Shell("Qizmt gen data{AE7E8F7E-AE48-40e7-B5B2-7E07E39B46F9} " + 1048576.ToString());

                    string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_Deploy-" + Guid.NewGuid().ToString();
                    if (!System.IO.Directory.Exists(exectempdir))
                    {
                        System.IO.Directory.CreateDirectory(exectempdir);
                    }
                    string execfp = exectempdir + @"\exec{07E2B469-80F9-4776-908F-E504A906E3B6}";
                    System.IO.File.WriteAllText(execfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`exec{07E2B469-80F9-4776-908F-E504A906E3B6}`>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://data{*}</DFSInput>
        <DFSOutput>dfs://output{A785E7D1-9017-45fe-9E07-57695192A5DC}</DFSOutput>
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
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              while(values.MoveNext())
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
                    Exec.Shell("Qizmt exec exec{07E2B469-80F9-4776-908F-E504A906E3B6}");
                    exec_md5 = DfsSum("md5", "output{A785E7D1-9017-45fe-9E07-57695192A5DC}");
                    Exec.Shell("Qizmt del output{A785E7D1-9017-45fe-9E07-57695192A5DC}");
                }

                try
                {

                    const string TEMP_DLLS_PATTERN = "temp_????????-????-????-????-????????????.dll";

                    // Prepare to detect leaked DLLs:
                    string lmachine = allmachines[allmachines.Length - 1];
                    string[] dummyleaknames = new string[] {
                        TEMP_DLLS_PATTERN.Replace('?', 'x'),
                        //"dummy1D48A66FD2EF41e3B6266C06D320A17D.dll",
                        //"dummy1D48A66FD2EF41e3B6266C06D320A17D.exe"
                    };

                    try
                    {
                        // Delete leaked DLLs on lmachine...
                        foreach (string fn in System.IO.Directory.GetFiles(Surrogate.NetworkPathForHost(lmachine), TEMP_DLLS_PATTERN))
                        {
                            System.IO.File.Delete(fn);
                        }

                        // Delete planted files from lmachine...
                        //foreach (string host in allmachines)
                        {
                            string host = lmachine;
                            string netdir = Surrogate.NetworkPathForHost(host);
                            foreach (string dummyleakname in dummyleaknames)
                            {
                                try
                                {
                                    System.IO.File.Delete(netdir + @"\" + dummyleakname);
                                }
                                catch
                                {
                                }
                            }
                        }
                        // Plant some new leaked files on surrogate...
                        foreach (string dummyleakname in dummyleaknames)
                        {
                            System.IO.File.WriteAllText(masterdir + @"\" + dummyleakname, "Dummy file for deploy leak detector" + Environment.NewLine);
                        }
                    }
                    catch (Exception e)
                    {
                        lmachine = null;
                        throw new Exception("Failed to prepare for deploy leak detector", e);
                    }

                    {
                        Console.WriteLine("Deleting critical files across cluster to ensure deploy will succeed...");
                        int nfailed = 0;
                        string failreason = "";
                        //foreach (string host in allmachines)
                        if (allmachines.Length > 1) // Important; can't delete slave.exe on surrogate or it can't deploy it.
                        {
                            string host = allmachines[allmachines.Length - 1];
                            try
                            {
                                string netdir = Surrogate.NetworkPathForHost(host);
                                System.IO.File.Delete(netdir + @"\MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe");
                            }
                            catch (Exception fe)
                            {
                                nfailed++;
                                failreason = fe.ToString();
                            }
                        }
                        if (nfailed > 0)
                        {
                            Console.WriteLine("Warning: {0} files failed to be deleted; {0}", failreason);
                        }
                    }

                    try
                    {
                        Console.WriteLine("Deploying...");
                        Exec.Shell("aelight deploy");
                        System.Threading.Thread.Sleep(1000 * 5); // Wait a bit for the services to come back up.
                    }
                    catch(Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.Error.WriteLine("    WARNING: cluster may be in a bad state; may need to reinstall");
                        throw;
                    }

                    Console.WriteLine("Ensuring deploy succeeded...");
                    Console.WriteLine("(Note: if this hangs indefinitely, deploy failed and need to reinstall)");

                    if (lmachine != null)
                    {
                        //foreach (string host in allmachines)
                        {
                            string host = lmachine;
                            string netdir = Surrogate.NetworkPathForHost(host);
                            foreach (string dummyleakname in dummyleaknames)
                            {
                                {
                                    string fp = netdir + @"\" + dummyleakname;
                                    if (System.IO.File.Exists(fp))
                                    {
                                        throw new Exception("Deployed dummy/leaked file: " + fp);
                                    }
                                }
                            }
                        }

                        {
                            string[] leaks = System.IO.Directory.GetFiles(Surrogate.NetworkPathForHost(lmachine), TEMP_DLLS_PATTERN);
                            if (leaks.Length > 0)
                            {
                                throw new Exception("Deployed leaked dll: " + leaks[0] + " (" + leaks.Length.ToString() + " in total)");
                            }
                        }

                        // Delete the planted dummy files from surrogate!
                        foreach (string dummyleakname in dummyleaknames)
                        {
                            System.IO.File.Delete(masterdir + @"\" + dummyleakname);
                        }
                    }

                    Console.WriteLine("Ensure cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    // Re-run job, confirm good...
                    {
                        Exec.Shell("Qizmt exec exec{07E2B469-80F9-4776-908F-E504A906E3B6}");
                        string new_exec_md5 = DfsSum("md5", "output{A785E7D1-9017-45fe-9E07-57695192A5DC}");
                        Exec.Shell("Qizmt del output{A785E7D1-9017-45fe-9E07-57695192A5DC}");
                        if (new_exec_md5 != exec_md5)
                        {
                            throw new Exception("Output files from before and after deploy do not match");
                        }
                    }

                }
                finally
                {
                    try
                    {
                        Console.WriteLine("Cleaning temporary test data...");
                        Exec.Shell("Qizmt del exec{07E2B469-80F9-4776-908F-E504A906E3B6}");
                        Exec.Shell("Qizmt del data{AE7E8F7E-AE48-40e7-B5B2-7E07E39B46F9}");
                        Exec.Shell("Qizmt del output{A785E7D1-9017-45fe-9E07-57695192A5DC}");
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
