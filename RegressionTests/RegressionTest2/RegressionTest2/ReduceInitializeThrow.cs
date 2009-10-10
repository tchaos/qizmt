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

        static void ReduceInitializeThrow(string[] args)
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
                // Test logic:

                {
                    string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_ReduceInitializeThrow-" + Guid.NewGuid().ToString();
                    if (!System.IO.Directory.Exists(exectempdir))
                    {
                        System.IO.Directory.CreateDirectory(exectempdir);
                    }
                    string execfp = exectempdir + @"\baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}";
                    System.IO.File.WriteAllText(execfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`PrepJob` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt -dfs del baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}_*.txt`,true); // Clean previous run.            
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Load sample data` Name=`baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(@`
MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!
`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}_Input.txt</DFSInput>
        <DFSOutput>dfs://baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine= mstring.Prepare(line);
              mstringarray parts = sLine.SplitM(' ');
              
              for(int i=0; i < parts.Length; i++)
             {
                    mstring word = parts[i];
                    
                    if(word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
                    {                        
                        output.Add(word.ToLowerM(), mstring.Prepare(1)); 
                    }                                 
             }
          }
        ]]>
        </Map>

        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize()
          {
              Qizmt_Log(`ReduceInitialize {3AF77789-4D38-4fde-B5DD-DC5115A909F5}`);
              throw new Exception(`Exception from ReduceInitialize {D5C5B240-537E-4b80-AD70-6463F632EE7B}`);
          }
        ]]>
        </ReduceInitialize>

        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              mstring sLine = mstring.Prepare(UnpadKey(key));
              sLine = sLine.AppendM(',').AppendM(values.Length);              
              output.Add(sLine);
          }
        ]]>
        </Reduce>

        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize()
          {
              Qizmt_Log(`ReduceFinalize {3C677456-22C5-46cd-A1E7-47383274C0C5}`);
          }
        ]]>
        </ReduceFinalize>
        
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
                    // Run it twice: first suppressing errors to get stdout, then again to get stderr exception.
                    string output = Exec.Shell("Qizmt exec baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}", true);
                    try
                    {
                        Exec.Shell("Qizmt exec baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}");
                        throw new Exception("<<< Job completed without errors; this is wrong! >>>");
                    }
                    catch (Exception e)
                    {
                        string err = e.ToString();
                        string badstr = "System.FormatException: Expected 16 hex digits, got \"-000000000000000\"";
                        if (-1 != err.IndexOf(badstr) || -1 != output.IndexOf(badstr))
                        {
                            throw new Exception("Test failed: broken protocol!", e);
                        }
                        if (-1 == err.IndexOf("{D5C5B240-537E-4b80-AD70-6463F632EE7B}"))
                        {
                            throw new Exception("Test failed: did not get expected exception from exec", e);
                        }
                    }
                    finally
                    {
                        Exec.Shell("Qizmt del baderrordup{2E829DB6-E853-4fc2-B0B4-82ADAF637312}*");
                    }
                }

            }

            Console.WriteLine("[PASSED] - " + string.Join(" ", args));

        }


    }
}
