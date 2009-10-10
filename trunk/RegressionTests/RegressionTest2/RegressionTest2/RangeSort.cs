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

        static void RangeSort(string[] args)
        {
            string sortmethod = args[0];

            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];

            // Checking for arg[2]==pause later...

            string masterdir;
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(dfsxmlpath);
                masterdir = fi.DirectoryName; // Directory's full path.
            }
            Surrogate.SetNewMetaLocation(masterdir);

            dfs dc = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            string masterhost = System.Net.Dns.GetHostName();
            string[] slaves = dc.Slaves.SlaveList.Split(';');
            string[] allmachines;
            {
                List<string> aml = new List<string>(slaves.Length + 1);
                aml.Add(masterhost);
                foreach (string slave in slaves)
                {
                    if (0 != string.Compare(IPAddressUtil.GetName(slave), IPAddressUtil.GetName(masterhost), StringComparison.OrdinalIgnoreCase))
                    {
                        aml.Add(slave);
                    }
                }
                allmachines = aml.ToArray();
            }

            string pausefile = "";
            if (args.Length > 2 && "pause" == args[2])
            {
                string pausedir = @"\\" + masterhost + @"\c$\temp\qizmt";
                try
                {
                    System.IO.Directory.CreateDirectory(pausedir);
                }
                catch
                {
                }
                pausefile = pausedir + @"\" + sortmethod + @"-pause.txt";
                System.IO.File.WriteAllText(pausefile, "Delete this file to un-pause..." + Environment.NewLine);
                Console.WriteLine();
                Console.WriteLine("Delete the file '{0}' to un-pause...", pausefile);
                Console.WriteLine();
            }

            {

                Console.WriteLine("Ensure cluster is perfectly healthy...");
                EnsurePerfectQizmtHealtha();

                {
                    string fguid = "{" + Guid.NewGuid().ToString() + "}";
                    // Generate some data to operate on.
                    Console.WriteLine("Generating data...");
                    // Note: this test depends on wordgen, and wordgen lines always starting with uppercase!
                    long gensize = 1048576 * dc.Blocks.SortedTotalCount; // 1MB * #processes
                    string gencmd = "wordgen";
                    int keymajor = 8;
                    if (-1 != sortmethod.IndexOf("hash", StringComparison.OrdinalIgnoreCase))
                    {
                        //gencmd = "bingen"; // Will write crazy files to c:\temp
                        keymajor = 2;
                    }
                    Exec.Shell("Qizmt " + gencmd + " data" + fguid + " " + gensize.ToString());
                    try
                    {
                        string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_" + sortmethod + @"-" + Guid.NewGuid().ToString();
                        if (!System.IO.Directory.Exists(exectempdir))
                        {
                            System.IO.Directory.CreateDirectory(exectempdir);
                        }
                        string execfp = exectempdir + @"\exec" + fguid;
                        // Note: using c:\temp instead of IOUtils.GetTempDirectory() in the following test
                        // because I can't get the IOUtils.GetTempDirectory() for other machines.
                        string scguid = Guid.NewGuid().ToString();
                        System.IO.File.WriteAllText(execfp, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`exec" + fguid + @"`>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://data" + fguid + @"</DFSInput>
        <DFSOutput>dfs://output" + fguid + @"</DFSOutput>
        <KeyMajor>" + keymajor.ToString() + @"</KeyMajor>
        <OutputMethod>" + sortmethod + @"</OutputMethod>
        <Setting name=`Subprocess_TotalPrime` value=`0` /> <!-- Don't use grouped. -->
        <Setting name=`Subprocess_SortedTotalCount` value=`" + slaves.Length.ToString() + @"` />
            <!-- ^ One process per participating machine. -->
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
        <ReduceInitialize><![CDATA[
            public virtual void ReduceInitialize() { }
        ]]></ReduceInitialize>
        <Reduce>
          <![CDATA[
            string dir = null;
            Dictionary<char, System.IO.StreamWriter> files = new Dictionary<char, System.IO.StreamWriter>();
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
                if(null == dir)
                {
                    dir = @`\\` + Qizmt_MachineHost + @`\c$\temp\qizmt\" + sortmethod + @"-" + scguid + @"`;
                    if(!System.IO.Directory.Exists(dir))
                    {
                        System.IO.Directory.CreateDirectory(dir);
                    }
                }
                System.IO.StreamWriter stmw;
                if(!files.ContainsKey((char)key[0]))
                {
                    stmw = new System.IO.StreamWriter(dir + @`\` + (char)key[0] + `.txt`, true); // append=true
                    files[(char)key[0]] = stmw;
                }
                stmw = files[(char)key[0]];
                stmw.WriteLine(key.ToString());
          }
       ]]>
        </Reduce>
        <ReduceFinalize><![CDATA[
            public virtual void ReduceFinalize()
            {
                foreach(KeyValuePair<char, System.IO.StreamWriter> kvp in files)
                {
                    kvp.Value.Close();
                }
            }
        ]]></ReduceFinalize>
      </MapReduce>
    </Job>
    <Job Name=`Verify Sort Range` >
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        readonly string[] slaves = `" + dc.Slaves.SlaveList + @"`.Split(';');
        readonly string pausefile = @`" + pausefile + @"`;
        public virtual void Local()
        {
            if(!string.IsNullOrEmpty(pausefile))
            {
                bool bb = false;
                while(System.IO.File.Exists(pausefile))
                {
                    if(!bb)
                    {
                        bb = true;
                        try
                        {
                            System.IO.File.AppendAllText(pausefile, `Ready!` + Environment.NewLine);
                        }
                        catch
                        {
                        }
                    }
                    System.Threading.Thread.Sleep(1000);
                }
            }
            bool failed = false;
            char bound = '\0';
            foreach(string slave in slaves)
            {
                string dir = @`\\` + slave + @`\c$\temp\qizmt\" + sortmethod + @"-" + scguid + @"`;
                try
                {
                    char thishighest = '\0';
                    foreach(System.IO.FileInfo fi in (new System.IO.DirectoryInfo(dir).GetFiles()))
                    {
                        char c = fi.Name[0];
                        if(c < bound)
                        {
                            failed = true;
                            throw new Exception(`Data is not range sorted (" + sortmethod + @") starting on machine ` + slave + `    (Error 1FC8AB58-4DBD-4d56-9587-96312F9A5886)`);
                        }
                        if(c > thishighest)
                        {
                            thishighest = c;
                        }
                        fi.Delete();
                    }
                    if(thishighest > bound)
                    {
                        bound = thishighest;
                    }
                    System.IO.Directory.Delete(dir);
                }
                catch(Exception e)
                {
                    Qizmt_Log(`Exception: ` + e.ToString());
                }
            }
            if(!failed && bound > '\0')
            {
                Qizmt_Log(`Success!    (OK 55D106EA-AD09-4503-96BA-387795EDEECB)`);
            }
        }
        ]]>
      </Local>
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
                        try
                        {
                            Console.WriteLine("Running " + sortmethod + " job...");
                            string output = Exec.Shell("Qizmt exec exec" + fguid);
                            Console.WriteLine(output.Trim());
                            if (-1 == output.IndexOf("55D106EA-AD09-4503-96BA-387795EDEECB"))
                            {
                                throw new Exception("Sort range order verification (" + sortmethod + ") did not succeed");
                            }
                        }
                        finally
                        {
                            Exec.Shell("Qizmt del output" + fguid);
                            Exec.Shell("Qizmt del exec" + fguid);
                        }
                    }
                    finally
                    {
                        Exec.Shell("Qizmt del data" + fguid);
                    }
                }

                Console.WriteLine("[PASSED] - " + string.Join(" ", args));


            }

        }


    }
}
