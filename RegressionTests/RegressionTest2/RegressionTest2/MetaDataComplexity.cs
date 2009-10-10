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

        static void MetaDataComplexity(string[] args)
        {

            bool IsLargeSort = -1 != args[0].IndexOf("LargeSort", StringComparison.OrdinalIgnoreCase);

            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];
            string dfsxmlpathbackup = dfsxmlpath + "$" + Guid.NewGuid().ToString();

            int iarg = 2;

            long metadatabytes = 0;
            if (args.Length > iarg)
            {
                if (!args[iarg].StartsWith("#"))
                {
                    metadatabytes = long.Parse(args[iarg++]);
                }
            }
            if(metadatabytes <= 0)
            {
                metadatabytes = 1024 * 1024 * 1;
            }

            int replication = 1;
            if (args.Length > iarg)
            {
                if (args[iarg].StartsWith("#"))
                {
                    replication = int.Parse(args[iarg++].Substring(1));
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
                    // Modified format to include DataNodeBaseSize
                    // It is set low enough so that each line goes into a new chunk file.
                    // Includes replication count too.
                    string fmtcmd = "qizmt @format Machines=" + string.Join(",", allmachines) + " DataNodeBaseSize=128 Replication=" + replication.ToString();
                    Console.WriteLine("    {0}", fmtcmd);
                    Exec.Shell(fmtcmd);
                }

                {
                    Console.WriteLine("Ensure cluster is perfectly healthy...");
                    EnsurePerfectQizmtHealtha();

                    List<string> othertests = new List<string>();

                    {
                        Console.WriteLine("Generating meta-data...");
                        {
                            string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\qizmt\regression_test_MetaDataComplexity-" + Guid.NewGuid().ToString();
                            if (!System.IO.Directory.Exists(exectempdir))
                            {
                                System.IO.Directory.CreateDirectory(exectempdir);
                            }
                            string execfp1 = exectempdir + @"\regression_test_MetaDataComplexity_exec{886AA08F-A089-460a-9E69-E1909E1FEBBC}";
                            System.IO.File.WriteAllText(execfp1, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`MetaDataComplexity`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_MetaDataComplexity_{this gets replaced}</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                byte[] bline = System.Text.Encoding.UTF8.GetBytes(`Access the new MySpace mobile for free - Any phone, any network! Simply log into m.myspace.com from your mobile phone web browser.`);
                for(int i = 0; i < 128; i++)
                {
                    dfsoutput.WriteLine(bline);
                }
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            #region other tests

                            string execfp2 = exectempdir + @"\regression_test_iocompareTHROW.xml";
                            // Not a test, but a dependency.
                            System.IO.File.WriteAllText(execfp2, (@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>regression_test_iocompareTHROW_Preprocessing</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string f1 = Qizmt_ExecArgs[0];       
            string f2 = Qizmt_ExecArgs[1];
            string jobname = Qizmt_ExecArgs[2];
            
            Shell(@`Qizmt del ` + jobname + `.????ED`); // Previous PASSED/FAILED
            bool failed = true;
            
            string hostname = System.Net.Dns.GetHostName();
            string guid = Guid.NewGuid().ToString();
            string f1Local = @`\\` + hostname + @`\c$\temp\qizmt\` + guid + f1;
            string f2Local = @`\\` + hostname + @`\c$\temp\qizmt\` + guid  + f2;
            string tempLocal = @`\\` + hostname + @`\c$\temp\qizmt\` + guid + `.txt`;
            
            try
            {
                
                if(0 == string.Compare(f1, f2, true))
                {
                  throw new Exception(`iocompareTHROW: cannot compare a file with itself: ` + f1);
                }
            
                if(!System.IO.Directory.Exists(@`\\` + hostname + @`\c$\temp`))
                {
                    System.IO.Directory.CreateDirectory(@`\\` + hostname + @`\c$\temp`);
                }
                
                if(!System.IO.Directory.Exists(@`\\` + hostname + @`\c$\temp\qizmt`))
                {
                    System.IO.Directory.CreateDirectory(@`\\` + hostname + @`\c$\temp\qizmt`);
                }
                
                System.IO.File.Delete(f1Local);
                System.IO.File.Delete(f2Local);
                
                Shell(@`Qizmt get ` + f1 + ` ` + f1Local);
                Shell(@`Qizmt get ` + f2 + ` ` + f2Local);
                
                List<string> arr1 = new List<string>();

                using (System.IO.StreamReader reader = new System.IO.StreamReader(f1Local))
                {
                    while (reader.Peek() > -1)
                    {
                        string line = reader.ReadLine();
                        arr1.Add(line);
                    }
                    reader.Close();
                }

                List<string> arr2 = new List<string>();

                using (System.IO.StreamReader reader = new System.IO.StreamReader(f2Local))
                {
                    while (reader.Peek() > -1)
                    {
                        string line = reader.ReadLine();                  
                        arr2.Add(line);
                    }
                    reader.Close();
                }

                arr1.Sort();
                arr2.Sort();

                bool diff = false;

                if (arr1.Count != arr2.Count)
                {
                    diff = true;
                }
                else
                {
                    for (int i = 0; i < arr1.Count; i++)
                    {
                        if (arr1[i] != arr2[i])
                        {
                            diff = true;
                            break;
                        }
                    }
                }
                
                if(!diff)
                {
                    failed = false;
                }
            }
            catch(Exception e)
            {
                Console.Error.WriteLine(e.ToString());
                failed = true;
            }
            using(System.IO.StreamWriter writer = new System.IO.StreamWriter(tempLocal))
            {
                //Divide by zero error if there is no sample.
                writer.Write(`.`);
                writer.Close();
            }
            if(failed)
            {
                Shell(@`Qizmt put ` + tempLocal + ` ` + jobname + `.FAILED`);
            }
            else
            {
                Shell(@`Qizmt put ` + tempLocal + ` ` + jobname + `.PASSED`);
            }
            System.IO.File.Delete(f1Local);
            System.IO.File.Delete(f2Local);
            System.IO.File.Delete(tempLocal);
            if(failed)
            {
                throw new Exception(tempLocal + ` ` + jobname + `.FAILED`);
            }
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            string execfp3 = exectempdir + @"\regression_test_mapreduce_output_sorted_small.xml";
                            othertests.Add((new System.IO.FileInfo(execfp3).Name));
                            System.IO.File.WriteAllText(execfp3, (@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>regression_test_mapreduce_output_sorted_small_Preprocessing</Name>
        <Custodian>Christopher Miller</Custodian>
        <email>cmiller@myspace.com</email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_mapreduce_output_sorted_small_Input.txt`);
            Shell(@`Qizmt del regression_test_mapreduce_output_sorted_small_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Create sample data`>
      <Narrative>
        <Name>regression_test_mapreduce_output_sorted_small_CreateSampleData</Name>
        <Custodian>Christopher Miller</Custodian>
        <email>cmiller@myspace.com</email>
      </Narrative>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_mapreduce_output_sorted_small_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`samekey 1) a few lines`);
                dfsoutput.WriteLine(`samekey 2) testing`);
                dfsoutput.WriteLine(`samekey 3) a small mapreduce job!`);
            }
        ]]>
      </Remote>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_mapreduce_output_sorted_small</Name>
        <Custodian>Christopher Miller</Custodian>
        <email>cmiller@myspace.com</email>
      </Narrative>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <DFSInput>dfs://regression_test_mapreduce_output_sorted_small_Input.txt</DFSInput>
        <DFSOutput>dfs://regression_test_mapreduce_output_sorted_small_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              output.Add(ByteSlice.Prepare(line, 0, 4), line);
          }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize() { }
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              IEnumerator<ByteSlice> evalues = values;
              while(evalues.MoveNext())
              {
                  output.Add(evalues.Current);
              }
          }
        ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize() { }
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_mapreduce_output_sorted_small_Postprocessing</Name>
        <Custodian>Christopher Miller</Custodian>
        <email>cmiller@myspace.com</email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt exec regression_test_iocompareTHROW.xml regression_test_mapreduce_output_sorted_small_Input.txt regression_test_mapreduce_output_sorted_small_Output.txt regression_test_mapreduce_output_sorted_small.xml`);          
            
            Shell(@`Qizmt del regression_test_mapreduce_output_sorted_small_Input.txt`);
            Shell(@`Qizmt del regression_test_mapreduce_output_sorted_small_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            string execfp4 = exectempdir + @"\regression_test_exec_xpath.xml";
                            othertests.Add((new System.IO.FileInfo(execfp4).Name));
                            System.IO.File.WriteAllText(execfp4, (@"<SourceCode>
  <Jobs>
    <Job Name=`regression_test_exec_xpath_Preprocessing`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(Qizmt_ExecArgs.Length > 1)
            {
            }
            else
            {
                Shell(@`Qizmt del regression_test_exec_xpath_*.txt`);
            }
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`regression_test_exec_xpath_remote`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_exec_xpath_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`a few lines`);
                dfsoutput.WriteLine(`testing`);
                dfsoutput.WriteLine(`xpath stuff!`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`regression_test_exec_xpath_mapreduce`>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <DFSInput>dfs://regression_test_exec_xpath_Input.txt</DFSInput>
        <DFSOutput>dfs://regression_test_exec_xpath_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              output.Add(ByteSlice.Prepare(line, 0, 4), line);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              IEnumerator<ByteSlice> evalues = values;
              while(evalues.MoveNext())
              {
                  output.Add(evalues.Current);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`regression_test_exec_xpath_Postprocessing`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(Qizmt_ExecArgs.Length > 1)
            {
                // New intput/output files were supploied, so check the new stuff and finish.
                // Compare outputs of first (normal) and second (modified) runs:
                Shell(@`Qizmt exec regression_test_iocompareTHROW.xml regression_test_exec_xpath_Output.txt ` + Qizmt_ExecArgs[1] + ` regression_test_exec_xpath.xml`); 
                Shell(@`Qizmt del regression_test_exec_xpath_*.txt`);
            }
            else
            {
                // Input/output files aren't supplied in the args, so re-run these jobs with these modifications:
                //Qizmt exec `//Job[@Name='regression_test_exec_xpath_remote']//DFSWriter=regression_test_exec_xpath_Input{new}.txt` `//Job[@Name='regression_test_exec_xpath_mapreduce']//DFSInput=regression_test_exec_xpath_Input{new}.txt` `//Job[@Name='regression_test_exec_xpath_mapreduce']//DFSOutput=regression_test_exec_xpath_Output{new}.txt` regression_test_exec_xpath.xml regression_test_exec_xpath_Input{new}.txt regression_test_exec_xpath_Output{new}.txt
                string output = Shell(@`Qizmt exec`
    + @` ``//Job[@Name='regression_test_exec_xpath_remote']//DFSWriter=regression_test_exec_xpath_Input{new}.txt```
    + @` ``//Job[@Name='regression_test_exec_xpath_mapreduce']//DFSInput=regression_test_exec_xpath_Input{new}.txt```
    + @` ``//Job[@Name='regression_test_exec_xpath_mapreduce']//DFSOutput=regression_test_exec_xpath_Output{new}.txt```
    + @` regression_test_exec_xpath.xml regression_test_exec_xpath_Input{new}.txt regression_test_exec_xpath_Output{new}.txt`);
                
                
                Qizmt_Log(`Output of jobs modified by xpath:`);
                Qizmt_Log(output);
            }
            
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            string execfp5 = exectempdir + @"\regression_test_mstring.MReplace(mstring_mstring).xml";
                            othertests.Add((new System.IO.FileInfo(execfp5).Name));
                            System.IO.File.WriteAllText(execfp5, (@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>regression_test_mstring.MReplace(mstring_mstring)_Preprocessing</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Input.txt`, true);
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output.txt`, true);   
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output1.txt`, true);    
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output2.txt`, true);    
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Create sample data`>
      <Narrative>
        <Name>regression_test_mstring.MReplace(mstring_mstring)_CreateSampleData</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_mstring.MReplace(mstring_mstring)_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                int stringLength = 100;
                int rowCount = 2000;               
                               
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
            
                List<byte> onerow = new List<byte>();
                
                for(long rn = 0; rn < rowCount; rn++)
                {
                    onerow.Clear();
                    
                    //string  
                    for(int cnt = 0; cnt < stringLength; cnt++)
                    {
                        byte b = (byte)rnd.Next((int)' ' + 1, (int)'~' + 1);   
                        
                       onerow.Add(b);   
                    }
                    
                    dfsoutput.WriteLine(onerow);        
                }     
           }
        ]]>
      </Remote>
    </Job>
    <Job>
      <Narrative>
        <Name>mstring.MReplace(mstring_mstring)</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>1</KeyLength>
        <DFSInput>dfs://regression_test_mstring.MReplace(mstring_mstring)_Input.txt</DFSInput>
        <DFSOutput>dfs://regression_test_mstring.MReplace(mstring_mstring)_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                string sLine = line.ToString();   
                char key = sLine[0];
                
                mstring oldStr = mstring.Prepare(sLine.Substring(10, 2));
                mstring newStr = mstring.Prepare(sLine.Substring(30, 2));               
                mstring ms = mstring.Prepare(line);                
                mstring val = ms.MReplace(ref oldStr, ref newStr); 
                output.Add(mstring.Prepare(key), val);     
                
                oldStr = mstring.Prepare(sLine.Substring(20, 10));
                newStr = mstring.Prepare(sLine.Substring(50, 30));
                ms = mstring.Prepare(line);                
                val = ms.MReplace(ref oldStr, ref newStr);  
                output.Add(mstring.Prepare(key), val);                   
          }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize() { }
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {                          
                for(int i = 0; i < values.Length; i++)
                {
                    string sLine = values.Items[i].ToString();  
                    
                    mstring oldStr = mstring.Prepare(sLine.Substring(8, 3));
                    mstring newStr = mstring.Prepare(sLine.Substring(90, 3));               
                    mstring ms = mstring.Prepare(values.Items[i]);                
                    mstring val = ms.MReplace(ref oldStr, ref newStr); 
                    output.Add(val);     
                    
                    oldStr = mstring.Prepare(sLine.Substring(80, 10));
                    newStr = mstring.Prepare(sLine.Substring(20, 30));
                    ms = mstring.Prepare(values.Items[i]);                
                    val = ms.MReplace(ref oldStr, ref newStr);  
                    output.Add(val);                   
                }                        
          }
        ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize() { }
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_mstring.MReplace(mstring_mstring)_Imitate_Mapper</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://regression_test_mstring.MReplace(mstring_mstring)_Input.txt</DFSReader>
          <DFSWriter>dfs://regression_test_mstring.MReplace(mstring_mstring)_Output1.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                StringBuilder sb = new StringBuilder();
                
                while(dfsinput.ReadLineAppend(sb))
                {
                    string sLine = sb.ToString();
                      
                    string oldStr = sLine.Substring(10, 2);
                    string newStr = sLine.Substring(30, 2); 
                    dfsoutput.WriteLine(sLine.Replace(oldStr, newStr));     
                    
                    oldStr = sLine.Substring(20, 10);
                    newStr = sLine.Substring(50, 30);                   
                    dfsoutput.WriteLine(sLine.Replace(oldStr, newStr));                  
                    
                    sb.Length = 0;                    
                }
           }
        ]]>
      </Remote>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_mstring.MReplace(mstring_mstring)_Imitate_Reducer</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://regression_test_mstring.MReplace(mstring_mstring)_Output1.txt</DFSReader>
          <DFSWriter>dfs://regression_test_mstring.MReplace(mstring_mstring)_Output2.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                StringBuilder sb = new StringBuilder();
                
                while(dfsinput.ReadLineAppend(sb))
                {
                    string sLine = sb.ToString();
                      
                    string oldStr = sLine.Substring(8, 3);
                    string newStr = sLine.Substring(90, 3); 
                    dfsoutput.WriteLine(sLine.Replace(oldStr, newStr));     
                    
                    oldStr = sLine.Substring(80, 10);
                    newStr = sLine.Substring(20, 30);                   
                    dfsoutput.WriteLine(sLine.Replace(oldStr, newStr));                  
                    
                    sb.Length = 0;                   
                }
           }
        ]]>
      </Remote>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_mstring.MReplace(mstring_mstring)_post-processing</Name>
        <Custodian>Cynthia Lok</Custodian>
        <email>clok@myspace-inc.com</email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            //Compare input and output file, pass if they are the same.
            Shell(@`Qizmt exec regression_test_iocompareTHROW.xml regression_test_mstring.MReplace(mstring_mstring)_Output.txt regression_test_mstring.MReplace(mstring_mstring)_Output2.txt regression_test_mstring.MReplace(mstring_mstring).xml`);          
            
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Input.txt`, true);
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output.txt`, true); 
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output1.txt`, true); 
            Shell(@`Qizmt del regression_test_mstring.MReplace(mstring_mstring)_Output2.txt`, true); 
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            string execfp6 = exectempdir + @"\regression_test_put_dll.xml";
                            othertests.Add((new System.IO.FileInfo(execfp6).Name));
                            System.IO.File.WriteAllText(execfp6, (@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>regression_test_put_dll_Preprocessing</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_testdll.dll`);
            Shell(@`Qizmt del regression_test_put_dll_Input.txt`);
            Shell(@`Qizmt del regression_test_put_dll_Output1.txt`);
            Shell(@`Qizmt del regression_test_put_dll_Output2.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_put_dll PUT DLL</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string localdir = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\qizmt`;
            if(!System.IO.Directory.Exists(localdir))
            {
                System.IO.Directory.CreateDirectory(localdir);
            }
            
            string fn = `regression_test_testdll.dll`;
            string localfn = localdir + @`\` + Guid.NewGuid().ToString() + fn;
            /*
            // Code for regression_test_testdll.dll:
namespace testdll
{
    public class Test
    {
        static int x = 0;
        public static void reset()
        {
            x = 0;
        }
        public static int next()
        {
            return x++;
        }
    }
}
            */
            string testdlldatab64 = `TVqQAAMAAAAEAAAA//8AALgAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAA4fug4AtAnNIbgBTM0hVGhpcyBwcm9ncmFtIGNhbm5vdCBiZSBydW4gaW4gRE9TIG1vZGUuDQ0KJAAAAAAAAABQRQAATAEDACjM90kAAAAAAAAAAOAAAiELAQgAAAgAAAAGAAAAAAAA7iYAAAAgAAAAQAAAAABAAAAgAAAAAgAABAAAAAAAAAAEAAAAAAAAAACAAAAAAgAAAAAAAAMAQIUAABAAABAAAAAAEAAAEAAAAAAAABAAAAAAAAAAAAAAAJQmAABXAAAAAEAAADgDAAAAAAAAAAAAAAAAAAAAAAAAAGAAAAwAAAAMJgAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAACAAAAAAAAAAAAAAACCAAAEgAAAAAAAAAAAAAAC50ZXh0AAAA9AYAAAAgAAAACAAAAAIAAAAAAAAAAAAAAAAAACAAAGAucnNyYwAAADgDAAAAQAAAAAQAAAAKAAAAAAAAAAAAAAAAAABAAABALnJlbG9jAAAMAAAAAGAAAAACAAAADgAAAAAAAAAAAAAAAAAAQAAAQgAAAAAAAAAAAAAAAAAAAADQJgAAAAAAAEgAAAACAAUAdCAAAJgFAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB4WgAEAAAQqOn4BAAAEJRdYgAEAAAQqBioeAigQAAAKKgAAAEJTSkIBAAEAAAAAAAwAAAB2Mi4wLjUwNzI3AAAAAAUAbAAAAOABAAAjfgAATAIAAHACAAAjU3RyaW5ncwAAAAC8BAAACAAAACNVUwDEBAAAEAAAACNHVUlEAAAA1AQAAMQAAAAjQmxvYgAAAAAAAAACAAABVxQAAAkAAAAA+gEzABYAAAEAAAARAAAAAgAAAAEAAAAEAAAAEAAAAA0AAAABAAAAAQAAAAAACgABAAAAAAAGAEMAPAAGAG8AXQAGAIYAXQAGAKMAXQAGAMIAXQAGANsAXQAGAPQAXQAGAA8BXQAGACoBXQAGAGIBQwEGAHYBQwEGAIQBXQAGAJ0BXQAGAM0BugE7AOEBAAAGABAC8AEGADAC8AEAAAAAAQAAAAAAAQABAAEAEAAmACsABQABAAEAEQBKAAoAUCAAAAAAlgBMAA0AAQBYIAAAAACWAFIAEQABAGkgAAAAAIYYVwAVAAEAZyAAAAAAkRhmAg0AAQARAFcAGQAZAFcAGQAhAFcAGQApAFcAGQAxAFcAGQA5AFcAGQBBAFcAGQBJAFcAGQBRAFcAHgBZAFcAGQBhAFcAGQBpAFcAGQBxAFcAIwCBAFcAKQCJAFcAFQAJAFcAFQAuAAsALgAuABMAOwAuABsAOwAuACMAOwAuACsALgAuADMAQQAuADsAOwAuAEsAOwAuAFMAWQAuAGMAgwAuAGsAkAAuAHMAmQAuAHsAogAEgAAAAQAAAAAAAAAAAAAAAABOAgAAAgAAAAAAAAAAAAAAAQAzAAAAAAAAAAA8TW9kdWxlPgByZWdyZXNzaW9uX3Rlc3RfdGVzdGRsbC5kbGwAVGVzdAB0ZXN0ZGxsAG1zY29ybGliAFN5c3RlbQBPYmplY3QAeAByZXNldABuZXh0AC5jdG9yAFN5c3RlbS5SZWZsZWN0aW9uAEFzc2VtYmx5VGl0bGVBdHRyaWJ1dGUAQXNzZW1ibHlEZXNjcmlwdGlvbkF0dHJpYnV0ZQBBc3NlbWJseUNvbmZpZ3VyYXRpb25BdHRyaWJ1dGUAQXNzZW1ibHlDb21wYW55QXR0cmlidXRlAEFzc2VtYmx5UHJvZHVjdEF0dHJpYnV0ZQBBc3NlbWJseUNvcHlyaWdodEF0dHJpYnV0ZQBBc3NlbWJseVRyYWRlbWFya0F0dHJpYnV0ZQBBc3NlbWJseUN1bHR1cmVBdHRyaWJ1dGUAU3lzdGVtLlJ1bnRpbWUuSW50ZXJvcFNlcnZpY2VzAENvbVZpc2libGVBdHRyaWJ1dGUAR3VpZEF0dHJpYnV0ZQBBc3NlbWJseVZlcnNpb25BdHRyaWJ1dGUAQXNzZW1ibHlGaWxlVmVyc2lvbkF0dHJpYnV0ZQBTeXN0ZW0uRGlhZ25vc3RpY3MARGVidWdnYWJsZUF0dHJpYnV0ZQBEZWJ1Z2dpbmdNb2RlcwBTeXN0ZW0uUnVudGltZS5Db21waWxlclNlcnZpY2VzAENvbXBpbGF0aW9uUmVsYXhhdGlvbnNBdHRyaWJ1dGUAUnVudGltZUNvbXBhdGliaWxpdHlBdHRy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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==`;
            byte[] testdlldata = System.Convert.FromBase64String(testdlldatab64);
            System.IO.File.WriteAllBytes(localfn, testdlldata);
            try
            {
                Shell(@`Qizmt dfs put ` + localfn + ` ` + fn);
            }
            finally
            {
                System.IO.File.Delete(localfn);
            }
        }
        ]]>
      </Local>
    </Job>

    <Job Name=`testdll` Custodian=`` email=``>
      <Add Reference=`regression_test_testdll.dll` Type=`dfs` />
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            testdll.Test.reset();
            if(0 != testdll.Test.next())
            {
                throw new Exception(`Local: (0 != testdll.Test.next())`);
            }
            if(1 != testdll.Test.next())
            {
                throw new Exception(`Local: (1 != testdll.Test.next())`);
            }
            if(2 != testdll.Test.next())
            {
                throw new Exception(`Local: (2 != testdll.Test.next())`);
            }
            if(3 != testdll.Test.next())
            {
                throw new Exception(`Local: (3 != testdll.Test.next())`);
            }
            const string want = `4513`;
            string str = testdll.Test.next().ToString() + testdll.Test.next().ToString()
                + (testdll.Test.next() + testdll.Test.next()).ToString(); // 4.ToString() + 5.ToString() + (6 + 7).ToString()
            if(want != str)
            {
                throw new Exception(`Local: (want != str)`);
            }
            string localdir = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\qizmt`;
            string fn = `regression_test_put_dll_Input.txt`;
            string localfn = localdir + @`\` + Guid.NewGuid().ToString() + fn;
            System.IO.File.WriteAllText(localfn, want + Environment.NewLine);
            try
            {
                Shell(@`Qizmt dfs put ` + localfn + ` ` + fn);
            }
            finally
            {
                System.IO.File.Delete(localfn);
            }
        }
        ]]>
      </Local>
    </Job>

    <Job description=`Create sample data` Name=`testdll_CreateSampleData` Custodian=`` email=``>
      <Add Reference=`regression_test_testdll.dll` Type=`dfs` />
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://regression_test_put_dll_Input.txt</DFSReader>
          <DFSWriter>dfs://regression_test_put_dll_Output1.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {
              List<byte> line = new List<byte>();
              dfsinput.ReadLineAppend(line);
              
              testdll.Test.reset();
              const string want2 = `03`;
              string str2 = testdll.Test.next().ToString()
                    + (testdll.Test.next() + testdll.Test.next()).ToString(); // 0.ToString() + (1 + 2).ToString()
              if(want2 != str2)
              {
                throw new Exception(`Remote: (want2 != str2)`);
              }
              
              dfsoutput.WriteLine(line);
           }
        ]]>
      </Remote>
    </Job>

    <Job Name=`testdll` Custodian=`` email=``>
      <Add Reference=`regression_test_testdll.dll` Type=`dfs` />
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>1</KeyLength>
        <DFSInput>dfs://regression_test_put_dll_Output1.txt</DFSInput>
        <DFSOutput>dfs://regression_test_put_dll_Output2.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              const string want = `4513`;
              if(want == line.ToString())
              {
                    testdll.Test.reset();
                    testdll.Test.next(); // 0
                    testdll.Test.next(); // 1
                    testdll.Test.next(); // 2
                    
                    string want3 = `39`;
                    string str3 = testdll.Test.next().ToString()
                            + (testdll.Test.next() + testdll.Test.next()).ToString(); // 3.ToString() + (4+ 5).ToString()
                    if(want3 != str3)
                    {
                        throw new Exception(`Map: (want3 != str3)`);
                    }
                    
                    {
                        testdll.Test.reset();
                        if(0 != testdll.Test.next())
                        {
                            throw new Exception(`Map: (0 != testdll.Test.next())`);
                        }
                    }
                    
                    output.Add(ByteSlice.Prepare(`x`), line);
              }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
                testdll.Test.reset();
                if(0 != testdll.Test.next())
                {
                    throw new Exception(`Reduce: (0 != testdll.Test.next())`);
                }
                
                while(values.MoveNext())
                {
                    output.Add(values.Current);
                }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>

    <Job>
      <Narrative>
        <Name>regression_test_put_dll_Postprocessing</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt exec regression_test_iocompareTHROW.xml regression_test_put_dll_Input.txt regression_test_put_dll_Output2.txt regression_test_put_dll.xml`);
            
            Shell(@`Qizmt del regression_test_testdll.dll`);
            Shell(@`Qizmt del regression_test_put_dll_Input.txt`);
            Shell(@`Qizmt del regression_test_put_dll_Output1.txt`);
            Shell(@`Qizmt del regression_test_put_dll_Output2.txt`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));

                            string execfp7 = exectempdir + @"\ssorterarg";
                            // Not adding to othertests becuase this one is different.
                            System.IO.File.WriteAllText(execfp7, (@"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
      <Job Name=`GenData` >
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string[] args = Qizmt_ExecArgs;
            string scapacity;
            if(args.Length > 0)
            {
                scapacity = args[0];
            }
            else
            {
                throw new Exception(`Argument expected!`);
            }
            
            //Shell(`Qizmt del ssorter_input`);
            //Shell(`Qizmt del ssorter_output`);
            
            Shell(`Qizmt wordgen ssorter_input \`` + scapacity + `\``);
            
        }
        
        ]]>
      </Local>
    </Job>
    <Job>
      <Narrative>
        <Name>SSorter</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <!--<Delta>
            <Name>ssorter_cache</Name>
            <DFSInput>dfs://ssorter_input</DFSInput>
      </Delta>-->
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://ssorter_input</DFSInput>
        <DFSOutput>dfs://ssorter_output</DFSOutput>
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

                            #endregion

                            Exec.Shell("Qizmt importdirmt " + exectempdir);
                            try
                            {
                                System.IO.File.Delete(execfp1);
                                System.IO.File.Delete(execfp2);
                                System.IO.File.Delete(execfp3);
                                System.IO.File.Delete(execfp4);
                                System.IO.File.Delete(execfp5);
                                System.IO.File.Delete(execfp6);
                                System.IO.File.Delete(execfp7);
                                System.IO.Directory.Delete(exectempdir);
                            }
                            catch
                            {
                            }
                            {
                                long fiMax = metadatabytes;
                                if (fiMax > 1024 * 1024 / 4)
                                {
                                    fiMax = 1024 * 1024 / 4;
                                }
                                const int MIN_PERCENT = 0;
                                const int MAX_PERCENT = 75;
                                Console.Write("    ");
                                for (; ; )
                                {
                                    {
                                        Exec.Shell(@"Qizmt exec ""//Job[@Name='MetaDataComplexity']/IOSettings/DFS_IO/DFSWriter=regression_test_MetaDataComplexity_output{" + Guid.NewGuid() + @"}"" regression_test_MetaDataComplexity_exec{886AA08F-A089-460a-9E69-E1909E1FEBBC}");
                                    }
                                    long fiLength = (new System.IO.FileInfo(dfsxmlpath)).Length;
                                    if (fiLength >= fiMax)
                                    {
                                        break;
                                    }
                                    //Console.Write("{0}%..", MIN_PERCENT + (fiLength * (MAX_PERCENT - MIN_PERCENT) / fiMax));
                                    Console.Write("\r    {0}%...", MIN_PERCENT + (fiLength * (MAX_PERCENT - MIN_PERCENT) / fiMax));
                                }
                                //Console.WriteLine("{0}%", MAX_PERCENT);
                                Console.WriteLine("\r    {0}%   ", MAX_PERCENT);
                            }
                            {
                                // We created N of valid metadata, now duplicate it with fake stuff for the remainder.
                                System.Xml.XmlElement firstdfsfiles;
                                {
                                    System.Xml.XmlDocument firstxd = new XmlDocument();
                                    firstxd.Load(dfsxmlpath);
                                    firstdfsfiles = firstxd["dfs"]["Files"];
                                }
                                {
                                    long fiMax = metadatabytes;
                                    const int MIN_PERCENT = 75;
                                    const int MAX_PERCENT = 100;
                                    Console.Write("    ");
                                    System.Xml.XmlDocument xd = new XmlDocument();
                                    xd.Load(dfsxmlpath);
                                    System.Xml.XmlElement dfsfiles = xd["dfs"]["Files"];
                                    for (int iter = 1; ; iter++)
                                    {
                                        {
                                            string iterstring = "." + iter.ToString();
                                            foreach (System.Xml.XmlNode xn in firstdfsfiles.SelectNodes("DfsFile"))
                                            {
                                                System.Xml.XmlNode df = xd.ImportNode(xn, true);
                                                df["Name"].InnerText += iterstring;
                                                foreach (System.Xml.XmlNode xnode in df["Nodes"].SelectNodes("FileNode"))
                                                {
                                                    xnode["Name"].InnerText += iterstring;
                                                }
                                                dfsfiles.AppendChild(df);
                                            }
                                            xd.Save(dfsxmlpath);
                                        }
                                        long fiLength = (new System.IO.FileInfo(dfsxmlpath)).Length;
                                        if (fiLength >= fiMax)
                                        {
                                            break;
                                        }
                                        //Console.Write("{0}%..", MIN_PERCENT + (fiLength * (MAX_PERCENT - MIN_PERCENT) / fiMax));
                                        Console.Write("\r    {0}%...", MIN_PERCENT + (fiLength * (MAX_PERCENT - MIN_PERCENT) / fiMax));
                                    }
                                    //Console.WriteLine("{0}%", MAX_PERCENT);
                                    Console.WriteLine("\r    {0}%   ", MAX_PERCENT);
                                }

                            }
                        }
                    }

                    {
                        Console.WriteLine("Running other regression tests...");
                        {
                            // First put the DataNodeBaseSize back to something sane for the tests.
                            Exec.Shell("Qizmt clusterconfigupdate Mrdfs_BlockBaseSize 67108864");
                        }
                        List<TimeSpan> times = new List<TimeSpan>(othertests.Count);
                        TimeSpan tdur = new TimeSpan(0);
                        int totaljobs = 0;
                        if (IsLargeSort)
                        {
                            foreach (string scapacity in new string[] { "10GB", "20GB", "30GB" })
                            {
                                Exec.Shell("Qizmt del ssorter_*"); // Needs to be done outside of job or times will be off!
                                string x = "Qizmt exec ssorterarg " + scapacity;
                                Console.WriteLine("    {0}", x);
                                DateTime tstart = DateTime.Now;
                                Exec.Shell(x);
                                TimeSpan dur = DateTime.Now - tstart;
                                times.Add(dur);
                                tdur += dur;
                                Console.WriteLine("        '-Duration: {0}", dur.ToString());
                                totaljobs++;
                            }
                        }
                        else
                        {
                            foreach (string fn in othertests)
                            {
                                string x = "Qizmt exec " + fn;
                                Console.WriteLine("    {0}", x);
                                DateTime tstart = DateTime.Now;
                                Exec.Shell(x);
                                TimeSpan dur = DateTime.Now - tstart;
                                times.Add(dur);
                                tdur += dur;
                                Console.WriteLine("        '-Duration: {0}", dur.ToString());
                                totaljobs++;
                            }
                        }
                        {
                            Console.WriteLine("    * Total duration:   {0}", tdur.ToString());
                            Console.WriteLine("    * Average duration: {0}", (new TimeSpan(tdur.Ticks / totaljobs)).ToString());
                        }
                    }

                }

                Console.WriteLine("[PASSED] - " + string.Join(" ", args));

            }
            finally
            {
                Console.WriteLine("Restoring DFS.xml backup...");
                // Note: these are safe; the try/finally only wraps the new dfs.
                /* // Not calling del here due to all the copied metadata failing to delete.
                 * // (it's slow)... calling killall after instead!
                try
                {
                    Exec.Shell("Qizmt del *");
                }
                catch
                {
                }
                */
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
                    // Note: killall issued to clean leaked data.
                    Console.WriteLine("Running killall to cleanup");
                    Exec.Shell("Qizmt killall -f");
                }
            }

        }


    }
}
