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

        static void SortedCache(string[] args)
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

                string fguid = "{" + Guid.NewGuid().ToString() + "}";

                string jobfn = "regression_test_SortedCache-" + Guid.NewGuid().ToString();
                SortedCacheCleanup(jobfn); // Cleanup previous run.
                try
                {

                    {
                        Console.WriteLine("Generating data and jobs...");
                        string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_SortedCache" + fguid;
                        if (!System.IO.Directory.Exists(exectempdir))
                        {
                            System.IO.Directory.CreateDirectory(exectempdir);
                        }
                        string execfp = exectempdir + @"\" + jobfn;
                        string scguid = Guid.NewGuid().ToString();
                        string ECODE = (@"<SourceCode>
  <Jobs>
    <Job Name=`CS` Custodian=`Chris Miller` Email=``>
    <Delta>
            <Name>{D7D3A6FE-8472-4320-9144-486E436D4542}CS_cache</Name>
            <DFSInput>{D7D3A6FE-8472-4320-9144-486E436D4542}a.txt;dfs://{D7D3A6FE-8472-4320-9144-486E436D4542}b?.txt</DFSInput>
    </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>1</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://{D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt</DFSOutput>
        <OutputMethod>{9235036E-4A47-4ee5-985F-F19D2F2DE85C}</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          List<byte> foo = new List<byte>();
          List<byte> bar = new List<byte>();
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              foo.Clear();
              bar.Clear();
              foo.Add((byte)('A' + line[0] % 16)); // A-F only.
              bar.Add(line[1]);
                output.Add(ByteSlice.Prepare(foo), ByteSlice.Prepare(bar));
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              long result = 0;
              while(values.MoveNext())
              {
                  ByteSlice v = values.Current;
                  result += v[0];
              }
              mstring ms = mstring.Prepare();
              ms.AppendM((char)key[0]);
              ms.AppendM(result);
              output.Add(ms);
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"');
                        System.IO.File.WriteAllText(execfp + ".grouped", ECODE.Replace("{9235036E-4A47-4ee5-985F-F19D2F2DE85C}", "grouped"));
                        System.IO.File.WriteAllText(execfp + ".sorted", ECODE.Replace("{9235036E-4A47-4ee5-985F-F19D2F2DE85C}", "sorted"));
                        Exec.Shell("Qizmt importdir " + exectempdir);
                        try
                        {
                            System.IO.File.Delete(execfp + ".grouped");
                            System.IO.File.Delete(execfp + ".sorted");
                            System.IO.Directory.Delete(exectempdir);
                        }
                        catch
                        {
                        }

                        Exec.Shell("Qizmt asciigen {D7D3A6FE-8472-4320-9144-486E436D4542}a.txt 16KB 2B");
                        Exec.Shell("Qizmt asciigen {D7D3A6FE-8472-4320-9144-486E436D4542}b1.txt 8KB 2B");
                        Exec.Shell("Qizmt asciigen {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt_ 8KB 2B");

                    }

                    string checksum_grouped = "";
                    {
                        Console.WriteLine("Running grouped job...");

                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_cache");
                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Exec.Shell("Qizmt exec " + jobfn + ".grouped");
                        Exec.Shell("Qizmt rename {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt_ {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt");
                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Exec.Shell("Qizmt exec " + jobfn + ".grouped");
                        Exec.Shell("Qizmt rename {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt_");
                        checksum_grouped = DfsSum("Sum2", "{D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Console.WriteLine("    checksum2 = {0}", checksum_grouped);
                    }

                    string checksum_sorted = "";
                    {
                        Console.WriteLine("Running sorted job...");

                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_cache");
                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Exec.Shell("Qizmt exec " + jobfn + ".sorted");
                        Exec.Shell("Qizmt rename {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt_ {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt");
                        Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Exec.Shell("Qizmt exec " + jobfn + ".sorted");
                        Exec.Shell("Qizmt rename {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt {D7D3A6FE-8472-4320-9144-486E436D4542}b2.txt_");
                        checksum_sorted = DfsSum("Sum2", "{D7D3A6FE-8472-4320-9144-486E436D4542}CS_Output.txt");
                        Console.WriteLine("    checksum2 = {0}", checksum_sorted);
                    }

                    if (checksum_grouped != checksum_sorted)
                    {
                        throw new Exception("Checksums do not match; sort with cache test failed!");
                    }

                }
                finally
                {
                    SortedCacheCleanup(jobfn);
                }

            }

            Console.WriteLine("[PASSED] - " + string.Join(" ", args));

        }


        static void SortedCacheCleanup(string jobfn)
        {
            Exec.Shell("Qizmt del " + jobfn + "*");
            Exec.Shell("Qizmt del {D7D3A6FE-8472-4320-9144-486E436D4542}*");
        }


    }
}
