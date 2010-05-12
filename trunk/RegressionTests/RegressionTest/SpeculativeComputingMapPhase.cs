using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class SpeculativeComputingMapPhase
    {
        static string[] GetQizmtHosts(out string qizmtdir)
        {
            string[] lines = Exec.Shell("qizmt slaveinstalls").Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            string[] hosts = new string[lines.Length];
            qizmtdir = null;
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                int del = line.IndexOf(' ');
                string host = line.Substring(0, del);
                string netpath = line.Substring(del + 1);
                hosts[i] = host;
                
                del = netpath.IndexOf(@"\", 2);
                qizmtdir = netpath.Substring(del + 1);
            }
            return hosts;
        }

        static int GetReplicationFactor()
        {
            string[] lines = Exec.Shell("qizmt replicationfactorview").Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            int rep = -1;
            foreach (string line in lines)
            {
                if (line.StartsWith("Replication factor is set to", StringComparison.OrdinalIgnoreCase))
                {
                    int del = line.Trim().LastIndexOf(' ');
                    rep = Int32.Parse(line.Substring(del + 1));
                    break;
                }
            }
            return rep;
        }

        static bool IsClusterHealthy()
        {
            string output = Exec.Shell("qizmt health");
            return output.IndexOf("100%") != -1;
        }

        public static void TestHDFailureBeforeMapStarts(string[] args)
        {
            Console.WriteLine("====TestHDFailureBeforeMapStarts====");
            
            string qizmtdir = null;
            string[] hosts = GetQizmtHosts(out qizmtdir);
            if (hosts.Length < 4)
            {
                throw new Exception("There must be more than 4 machines in the Qimzt cluster to test.");
            }

            int replication = GetReplicationFactor();
            if (replication < 2)
            {
                throw new Exception("Replication factor must be greater than 2.");
            }

            if (!IsClusterHealthy())
            {
                throw new Exception("Cluster must be 100% healthy to begin with.");
            }

            string guid = "10DF6995-B6C1-4c9a-9770-C46B6DF6DE26";

            Console.WriteLine("Importing jobs...");
            ImportJobs();
            Console.WriteLine("Done");

            Console.WriteLine("Running job in normal mode...");
            Exec.Shell("qizmt exec reg_job1_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
            Console.WriteLine("Done");

            //Speculative computing with InputOrder = next
            {
                //Simulate hd failure
                string fhost = hosts[1];
                Console.WriteLine("Simulating HDF at {0}...", fhost);
                SimulateHDFailure(fhost);
                Console.WriteLine("Done");

                //Restart a machine
                string rhost = hosts[2];
                Console.WriteLine("Restarting host {0}...", rhost);
                RestartHost(rhost);
                Console.WriteLine("Done");

                {
                    Console.WriteLine("Running job in speculative computing mode with inputOrder = next...");
                    string output = Exec.Shell("qizmt exec \"//Job[@Name='mr']/Computing/InputOrder=next\" reg_job2_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
                    Console.WriteLine("Done");

                    if (output.IndexOf("HWFailure:Recovered:" + fhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("HWFailure at host " + fhost + " was not captured.");
                    }

                    if (output.IndexOf("Warning: excluding '" + rhost, StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Excluded host " + rhost + " was not captured.");
                    }
                }                

                {
                    UnsimulateHDFailure(fhost);
                    Console.WriteLine("Checking results...");
                    string output = Exec.Shell("qizmt exec reg_job3_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml \"" + qizmtdir + "\"");
                    Console.WriteLine("Done");

                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                }
            }

            //Speculative computing with InputOrder = shuffle
            {
                //Simulate hd failure
                string fhost = hosts[1];
                Console.WriteLine("Simulating HDF at {0}...", fhost);
                SimulateHDFailure(fhost);
                Console.WriteLine("Done");

                //Restart a machine
                string rhost = hosts[2];
                Console.WriteLine("Restarting host {0}...", rhost);
                RestartHost(rhost);
                Console.WriteLine("Done");

                {
                    Console.WriteLine("Running job in speculative computing mode with inputOrder = shuffle...");
                    string output = Exec.Shell("qizmt exec \"//Job[@Name='mr']/Computing/InputOrder=shuffle\" reg_job2_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
                    Console.WriteLine("Done");

                    if (output.IndexOf("HWFailure:Recovered:" + fhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("HWFailure at host " + fhost + " was not captured.");
                    }

                    if (output.IndexOf("Warning: excluding '" + rhost, StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Excluded host " + rhost + " was not captured.");
                    }
                }                

                {
                    UnsimulateHDFailure(fhost);
                    Console.WriteLine("Checking results...");
                    string output = Exec.Shell("qizmt exec reg_job3_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml \"" + qizmtdir + "\"");
                    Console.WriteLine("Done");

                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                }
            }
           
            Console.WriteLine("[PASSED] - " + string.Join(" ", args));
        }

        public static void TestHDFailureAfterMapStarts(string[] args)
        {
            Console.WriteLine("====TestHDFailureAfterMapStarts====");

            string qizmtdir = null;
            string[] hosts = GetQizmtHosts(out qizmtdir);
            if (hosts.Length < 4)
            {
                throw new Exception("There must be more than 4 machines in the Qimzt cluster to test.");
            }

            int replication = GetReplicationFactor();
            if (replication < 2)
            {
                throw new Exception("Replication factor must be greater than 2.");
            }

            if (!IsClusterHealthy())
            {
                throw new Exception("Cluster must be 100% healthy to begin with.");
            }                       

            Console.WriteLine("Importing jobs...");
            ImportJobs();
            Console.WriteLine("Done");

            Console.WriteLine("Running job in normal mode...");
            Exec.Shell("qizmt exec reg_job1_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
            Console.WriteLine("Done");

            //Speculative computing with InputOrder=next            
            {
                List<string> errors = new List<string>();

                //Simulate hd failure
                string fhost = hosts[1];

                //Restart host
                string rhost = hosts[2];

                System.Threading.Thread thjob = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                    {
                        Console.WriteLine("Running job in speculative computing mode with inputOrder = next...");       
                        string output = Exec.Shell("qizmt exec \"//Job[@Name='mr']/Computing/InputOrder=next\" reg_job2_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
                        Console.WriteLine("Done");                        

                        if (output.IndexOf("HWFailure:Recovered:" + fhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                        {
                            errors.Add("HWFailure at host " + fhost + " was not captured.");
                        }
                        if (output.IndexOf("HWFailure:Recovered:" + rhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                        {
                            errors.Add("HWFailure at host " + rhost + " was not captured.");
                        }
                    }));
                thjob.Start();

                //Let the job run for 1 min before simulating hd failures.
                System.Threading.Thread.Sleep(60 * 1000);                
                SimulateHDFailure(fhost);
                RestartHost(rhost);

                thjob.Join();

                if (errors.Count > 0)
                {
                    string allerr = string.Join(";", errors.ToArray());
                    throw new Exception("Error during job2: " + allerr);
                }

                {
                    UnsimulateHDFailure(fhost);
                    Console.WriteLine("Checking results...");
                    string output = Exec.Shell("qizmt exec reg_job3_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml \"" + qizmtdir + "\"");
                    Console.WriteLine("Done");

                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                }
            }
            
            //Speculative computing with InputOrder=shuffle            
            {
                List<string> errors = new List<string>();

                //Simulate hd failure
                string fhost = hosts[1];

                //Restart host
                string rhost = hosts[2];

                System.Threading.Thread thjob = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                {
                    Console.WriteLine("Running job in speculative computing mode with inputOrder = shuffle...");
                    string output = Exec.Shell("qizmt exec \"//Job[@Name='mr']/Computing/InputOrder=shuffle\" reg_job2_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
                    Console.WriteLine("Done");

                    if (output.IndexOf("HWFailure:Recovered:" + fhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        errors.Add("HWFailure at host " + fhost + " was not captured.");
                    }
                    if (output.IndexOf("HWFailure:Recovered:" + rhost + ":", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        errors.Add("HWFailure at host " + rhost + " was not captured.");
                    }
                }));
                thjob.Start();

                //Let the job run for 1 min before simulating hd failures.
                System.Threading.Thread.Sleep(60 * 1000);
                SimulateHDFailure(fhost);
                RestartHost(rhost);

                thjob.Join();

                if (errors.Count > 0)
                {
                    string allerr = string.Join(";", errors.ToArray());
                    throw new Exception("Error during job2: " + allerr);
                }

                {
                    UnsimulateHDFailure(fhost);
                    Console.WriteLine("Checking results...");
                    string output = Exec.Shell("qizmt exec reg_job3_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml \"" + qizmtdir + "\"");
                    Console.WriteLine("Done");

                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                }
            }

            Console.WriteLine("[PASSED] - " + string.Join(" ", args));
        }
             
        static void RestartHost(string host)
        {
            Exec.Shell(@"Shutdown /m \\" + host + " /r");

            int tryremains = 24;
            for (; ; )
            {
                Console.Write(".");
                System.Threading.Thread.Sleep(5000);
                string output = Exec.Shell("ping " + host + " -n 1");
                if (output.IndexOf("Reply from ", StringComparison.OrdinalIgnoreCase) == -1)
                {
                    break;
                }

                if (--tryremains <= 0)
                {
                    throw new Exception("Still getting reply from host " + host + " after shutdown.");
                }
            }
        }

        static void SimulateHDFailure(string host)
        {
            string controlfile = @"\\" + host + @"\c$\temp\HealthFailure.txt";
            if (!System.IO.File.Exists(controlfile))
            {
                System.IO.File.WriteAllText(controlfile, "simulate hd failure");
            }
        }

        static void UnsimulateHDFailure(string host)
        {
            string controlfile = @"\\" + host + @"\c$\temp\HealthFailure.txt";
            if (System.IO.File.Exists(controlfile))
            {
                System.IO.File.Delete(controlfile);
            }
        }

        static void ImportJobs()
        {
            string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\" + Guid.NewGuid().ToString();
            System.IO.Directory.CreateDirectory(dir);

            #region normalmode
            {
                string job = @"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input*`);
                Shell(@`Qizmt del reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output*`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 50000000;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        int num2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        dfsoutput.WriteLine(num.ToString() + `,apple,` + num2.ToString() + `,lemon`);                        
                    }        
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 50000000;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        int num2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        dfsoutput.WriteLine(num.ToString() + `,apple,` + num2.ToString() + `,lemon`);                        
                    }        
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 50000000;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        int num2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        dfsoutput.WriteLine(num.ToString() + `,apple,` + num2.ToString() + `,lemon`);                        
                    }        
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt combine reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1_*.txt +reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1.txt`);    
                Shell(@`Qizmt combine reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2_*.txt +reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2.txt`);     
                Shell(@`Qizmt combine reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3_*.txt +reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3.txt`);     
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`oo` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1.txt;dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2.txt;reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3.txt</DFSInput>
        <DFSOutput>dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output_not_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {     
                if(StaticGlobals.MapIteration % 12195 == 0)
                {
                    //System.Threading.Thread.Sleep(120000);
                   //System.Threading.Thread.Sleep(1000);
                }
                    
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                int num2 = sLine.NextItemToInt(',');
                mstring title2 = sLine.NextItemToString(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(num);
                
                int num3 = -1;
                
                if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1.txt`)
                {
                    num3 = 1;
                }
                else if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2.txt`)
                {
                    num3 = 2;
                }
                else if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3.txt`)
                {
                    num3 = 3;
                }
                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(num2);
                rValue.PutString(title);
                rValue.PutString(title2);
                rValue.PutInt(num3);
                
                output.Add(rKey, rValue);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int num = rKey.GetInt();
                
                List<KeyValuePair<int, string>> list = new List<KeyValuePair<int, string>>(values.Length);
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int num2 = rValue.GetInt();
                    mstring title = rValue.GetString();
                    mstring title2 = rValue.GetString();
                    int num3 = rValue.GetInt();
                    
                    KeyValuePair<int, string> pair = new KeyValuePair<int, string>(num2, title.ToString() + `,` + title2.ToString() + `,` + num3.ToString());                    
                    list.Add(pair);
                }
                
                list.Sort(delegate(KeyValuePair<int, string> x, KeyValuePair<int, string> y)
                {
                    int comp = x.Key.CompareTo(y.Key);
                    if(comp != 0)
                    {
                        return comp;
                    }
                    
                    return x.Value.CompareTo(y.Value);
                });
                
                foreach(KeyValuePair<int, string> pair in list)
                {
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(pair.Key)
                        .AppendM(',')
                        .AppendM(pair.Value);
                    
                    output.Add(sLine);
                }       
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>".Replace('`', '"');
                System.IO.File.WriteAllText(dir + @"\reg_job1_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml", job);
            }
            #endregion

            #region speculativeComputing
            {
                string job = @"<SourceCode>
  <Jobs>
  <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output_sp.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1.txt;dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2.txt;reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3.txt</DFSInput>
        <DFSOutput>dfs://reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Computing>
        <Mode>speculative</Mode>
        <MapInputOrder>shuffle</MapInputOrder>
      </Computing>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {     
                if(StaticGlobals.MapIteration % 12195 == 0)
                {
                   System.Threading.Thread.Sleep(1000);
                }
                    
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                int num2 = sLine.NextItemToInt(',');
                mstring title2 = sLine.NextItemToString(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(num);
                
                int num3 = -1;
                
                if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input1.txt`)
                {
                    num3 = 1;
                }
                else if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input2.txt`)
                {
                    num3 = 2;
                }
                else if(StaticGlobals.DSpace_InputFileName == `reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Input3.txt`)
                {
                    num3 = 3;
                }
                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(num2);
                rValue.PutString(title);
                rValue.PutString(title2);
                rValue.PutInt(num3);
                
                output.Add(rKey, rValue);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int num = rKey.GetInt();
                
                List<KeyValuePair<int, string>> list = new List<KeyValuePair<int, string>>(values.Length);
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int num2 = rValue.GetInt();
                    mstring title = rValue.GetString();
                    mstring title2 = rValue.GetString();
                    int num3 = rValue.GetInt();
                    
                    KeyValuePair<int, string> pair = new KeyValuePair<int, string>(num2, title.ToString() + `,` + title2.ToString() + `,` + num3.ToString());                    
                    list.Add(pair);
                }
                
                list.Sort(delegate(KeyValuePair<int, string> x, KeyValuePair<int, string> y)
                {
                    int comp = x.Key.CompareTo(y.Key);
                    if(comp != 0)
                    {
                        return comp;
                    }
                    
                    return x.Value.CompareTo(y.Value);
                });
                
                foreach(KeyValuePair<int, string> pair in list)
                {
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(pair.Key)
                        .AppendM(',')
                        .AppendM(pair.Value);
                    
                    output.Add(sLine);
                }       
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>".Replace('`', '"');
                System.IO.File.WriteAllText(dir + @"\reg_job2_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml", job);
            }
            #endregion

            #region check
            {
                string job = @"<SourceCode>
  <Jobs>
    <Job Name=`check` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                if(Qizmt_ExecArgs.Length == 0)
                {
                    throw new Exception(`Qizmt dir required.`);
                }
                string qizmtdir = Qizmt_ExecArgs[0];
                
                string f1 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                string f2 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                
                Shell(@`qizmt bulkget ` + f1 + ` reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output_not_sp.txt`);
                Shell(@`qizmt bulkget ` + f2 + ` reg_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26_Output_sp.txt`);
                                
                {                    
                    string[] lines1 = System.IO.File.ReadAllLines(f1);
                    string[] lines2 = System.IO.File.ReadAllLines(f2);
                    
                    if(lines1.Length != lines2.Length)
                    {
                        throw new Exception(`Parts count is different. lines1.len=` + lines1.Length.ToString() + `; lines2.len=` + lines2.Length.ToString());
                    }
                    
                    List<string> err = new List<string>();
                    List<System.Threading.Thread> thds = new List<System.Threading.Thread>();
                    
                    for(int li = 0; li < lines1.Length; li++)
                    {
                        string[] parts1 = lines1[li].Split(' ');
                        string[] parts2 = lines2[li].Split(' ');
                        
                        string chunk1 = @`\\` + parts1[0].Split(';')[0] + @`\` + qizmtdir + @`\` + parts1[1];
                        string chunk2 = @`\\` + parts2[0].Split(';')[0] + @`\` + qizmtdir + @`\` + parts2[1];
                       
                        //DSpace_Log(`chunk1=` + chunk1 + `;chunk2=` + chunk2);
                        
                         TP tp = new TP();
                         tp.chunk1 = chunk1;
                         tp.chunk2 = chunk2;
                         tp.err = err;
                         System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ThreadStart(tp.ThreadProc));   
                         th.Start();
                         thds.Add(th);
                    } 
                    
                    foreach(System.Threading.Thread th in thds)
                    {
                        th.Join();
                    }
                    
                   DSpace_Log(`error count=` + err.Count.ToString());        
                }
               
                System.IO.File.Delete(f1);
                System.IO.File.Delete(f2);
            }
        
        public class TP
        {
            public string chunk1;
            public string chunk2;
            public List<string> err;
            
            public void ThreadProc()
            {
                if(!CompareFiles(chunk1, chunk2))
                {
                    lock(err)
                    {
                       err.Add(chunk1);  
                    }                    
                }                
            }
            
            private static bool CompareFiles(string f1, string f2)
            {
                System.IO.FileInfo info1 = new System.IO.FileInfo(f1);
                System.IO.FileInfo info2 = new System.IO.FileInfo(f2);
                if (info1.Length != info2.Length)
                {
                    return false;
                }

                System.IO.FileStream fs1 = new System.IO.FileStream(f1, System.IO.FileMode.Open);
                System.IO.FileStream fs2 = new System.IO.FileStream(f2, System.IO.FileMode.Open);
                bool ok = true;
                for (int i = 0; i < info1.Length; i++)
                {
                    int b1 = fs1.ReadByte();
                    int b2 = fs2.ReadByte();
                    if (b1 != b2)
                    {
                        ok = false;
                        break;
                    }
                }

                fs1.Close();
                fs2.Close();
                return ok;
            }
        }
        ]]>
      </Local>
    </Job>    
  </Jobs>
</SourceCode>".Replace('`', '"'); ;
                System.IO.File.WriteAllText(dir + @"\reg_job3_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml", job);
            }
            #endregion
                        
            Exec.Shell("qizmt del reg_job*_10DF6995-B6C1-4c9a-9770-C46B6DF6DE26.xml");
            Exec.Shell("qizmt importdir \"" + dir + "\"");            
            System.IO.Directory.Delete(dir, true);
        }
    }
}
