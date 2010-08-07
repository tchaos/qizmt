using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public partial class Program
    {
        static void FaultTolerantExecutionTest(string[] args)
        {
            bool verbose = false;
            bool skipsplitsort = false;

            for (int ai = 1; ai < args.Length; ai++)
            {
                switch (args[ai].ToLower())
                {
                    case "-verbose":
                        verbose = true;
                        Console.WriteLine("verbose=true");
                        break;

                    case "-skipsplitsort":
                        skipsplitsort = true;
                        Console.WriteLine("skipsplitsort=true");
                        break;

                    default:
                        throw new Exception("Unknown argument: " + args[ai]);
                }
            }  

            Console.WriteLine("====FaultTolerantExecutionTest====");

            if (!MySpace.DataMining.AELight.FTTest.enabled)
            {
                throw new Exception("TESTFAULTTOLERANT is not #defined.  Need Qizmt build with all #define TESTFAULTTOLERANT uncommented.");
            }

            if (GetReplicationFactor() < 2)
            {
                throw new Exception("Replication factor must be 2 or greater.");
            }

            if (!IsClusterHealthy())
            {
                throw new Exception("Cluster must be 100% healthy to begin with.");
            }

            {
                Console.WriteLine("Importing jobs...");
                string tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\" + Guid.NewGuid().ToString();
                System.IO.Directory.CreateDirectory(tempdir);

                #region job1
                string txt = @"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input*`);
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output*`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 50000;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        int valuescount = rnd.Next(10,50);
                        for(int vi = 0; vi < valuescount; vi++)
                        {
                            int num2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                            dfsoutput.WriteLine(num.ToString() + `,apple,` + num2.ToString() + `,lemon`);        
                        }                                       
                    }        
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 50000;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        int valuescount = rnd.Next(10,50);
                        for(int vi = 0; vi < valuescount; vi++)
                        {
                            int num2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                            dfsoutput.WriteLine(num.ToString() + `,apple,` + num2.ToString() + `,lemon`);        
                        }                                       
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
                Shell(@`Qizmt combine oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1_*.txt +oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1.txt`);    
                Shell(@`Qizmt combine oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2_*.txt +oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2.txt`);    
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1.txt;oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2.txt</DFSInput>
        <DFSOutput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output_not_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {     
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                int num2 = sLine.NextItemToInt(',');
                mstring title2 = sLine.NextItemToString(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(num);
                
                int num3 = -1;
                
                if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1.txt`)
                {
                    num3 = 1;
                }
                else if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2.txt`)
                {
                    num3 = 2;
                }
                else if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input3.txt`)
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
               
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int num2 = rValue.GetInt();
                    mstring title = rValue.GetString();
                    mstring title2 = rValue.GetString();
                    int num3 = rValue.GetInt();
                    
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(num2)
                        .AppendM(',')
                        .AppendM(title)
                        .AppendM(',')
                        .AppendM(title2)
                        .AppendM(',')
                        .AppendM(num3);
                    
                    output.Add(sLine);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_job1_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                #region job2
                txt = @"<SourceCode>
  <Jobs>
  <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output_sp.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1.txt;oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2.txt</DFSInput>
        <DFSOutput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <FaultTolerantExecution>
        <Mode>enabled</Mode>
        <MapInputOrder>shuffle</MapInputOrder>
      </FaultTolerantExecution>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            { 
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                int num2 = sLine.NextItemToInt(',');
                mstring title2 = sLine.NextItemToString(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(num);
                
                int num3 = -1;
                
                if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input1.txt`)
                {
                    num3 = 1;
                }
                else if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input2.txt`)
                {
                    num3 = 2;
                }
                else if(StaticGlobals.DSpace_InputFileName == `oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Input3.txt`)
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
                    
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(num2)
                        .AppendM(',')
                        .AppendM(title)
                        .AppendM(',')
                        .AppendM(title2)
                        .AppendM(',')
                        .AppendM(num3);
                    
                    output.Add(sLine);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>
".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                #region job3
                txt = @"<SourceCode>
  <Jobs>
    <Job Name=`checkresults` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                string f1 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                string f2 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                
                Shell(@`qizmt get  oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output_not_sp.txt ` + f1);
                Shell(@`qizmt get  oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_Output_sp.txt ` + f2);               
                
                string[] lines1 = System.IO.File.ReadAllLines(f1);
                string[] lines2 = System.IO.File.ReadAllLines(f2);                  
                                 
                if(lines1.Length != lines2.Length)
                {
                    throw new Exception(`lines counts are different. lines1.len=` + lines1.Length.ToString() + `; lines2.len=` + lines2.Length.ToString());
                }                    
                if(!CheckGroupedKeys(lines1))
                {
                    throw new Exception(`lines1 not grouped properly`);
                }
                if(!CheckGroupedKeys(lines2))
                {
                    throw new Exception(`lines2 not grouped properly`);
                }             
                {
                    List<string> list1 = new List<string>(lines1);
                    List<string> list2 = new List<string>(lines2);
                    
                    list1.Sort();
                    list2.Sort();
                    
                    for(int i=0; i < list1.Count; i++)
                    {
                        if(list1[i] != list2[i])
                        {
                            throw new Exception(`line different at: ` + i.ToString());
                        }
                    }
                }
               
                System.IO.File.Delete(f1);
                System.IO.File.Delete(f2);
                
                Qizmt_Log(`error count=0`);
        }
        
        public bool CheckGroupedKeys(string[] lines)
        {
            //Make sure the keys are grouped.
            Dictionary<int, List<string>> dic = new Dictionary<int, List<string>>(lines.Length);
            
            int prevkey = -1;
            
            for(int i = 0; i < lines.Length; i++)
            {                
                string line = lines[i];
                string[] parts = line.Split(',');
                int key = Int32.Parse(parts[0]);
                bool keychanged = false;
                
                if(i == 0)
                {
                    keychanged = true;
                }
                else if(prevkey != key)
                {
                    keychanged = true;
                }
                
                if(keychanged)
                {                    
                    prevkey = key;
                    dic.Add(key, new List<string>());   //will error out if this key has been seen before.  thus not grouped properly.
                }
                
                dic[key].Add(line);
            }  
            
            return true;
        }
        ]]>
      </Local>
    </Job>    
  </Jobs>
</SourceCode>
".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                #region job1splitsort
                txt = @"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input*`);
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output*`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input1_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 2097152;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = 1;
                        int valuescount = 1;
                        for(int vi = 0; vi < valuescount; vi++)
                        {
                            int num2 = rnd.Next(0,9);
                            dfsoutput.WriteLine(num.ToString() + `,` + num2.ToString());        
                        }                                       
                    }        
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input2_####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max = 2097152;
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                for(int i = 0; i < max; i++)
                {
                    if(i % Qizmt_ProcessCount == Qizmt_ProcessID)
                    {
                        int num = 1;
                        int valuescount = 1;
                        for(int vi = 0; vi < valuescount; vi++)
                        {
                            int num2 = rnd.Next(0,9);
                            dfsoutput.WriteLine(num.ToString() + `,` + num2.ToString());        
                        }                                       
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
                Shell(@`Qizmt combine oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input1_*.txt +oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input1.txt`);    
                Shell(@`Qizmt combine oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input2_*.txt +oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input2.txt`);    
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input1.txt;oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input2.txt</DFSInput>
        <DFSOutput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output_not_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                    
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                int num2 = sLine.NextItemToInt(',');
                
                recordset rKey = recordset.Prepare();
                num = 1; // constant key to cause splitsort
                rKey.PutInt(num);
                                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(num2);
                
                for(int i = 0; i < 32; i++)
                {
                    output.Add(rKey, rValue);
                }                
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int num = rKey.GetInt();                
               
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int num2 = rValue.GetInt();                  
                    
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(num2);
                    
                    output.Add(sLine);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>
".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_splitsort_job1_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                #region job2splitsort
                txt = @"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output_sp.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input1.txt;oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Input2.txt</DFSInput>
        <DFSOutput>dfs://oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output_sp.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <FaultTolerantExecution>
        <Mode>enabled</Mode>
        <MapInputOrder>shuffle</MapInputOrder>
      </FaultTolerantExecution>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                    
                mstring sLine = mstring.Prepare(line);
                int num = sLine.NextItemToInt(',');
                int num2 = sLine.NextItemToInt(',');
                
                recordset rKey = recordset.Prepare();
                num = 1; // constant key to cause splitsort
                rKey.PutInt(num);
                                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(num2);
                
                for(int i = 0; i < 32; i++)
                {
                    output.Add(rKey, rValue);
                }                
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int num = rKey.GetInt();
                               
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int num2 = rValue.GetInt();                  
                    
                    mstring sLine = mstring.Prepare(num);
                    sLine = sLine.AppendM(',')
                        .AppendM(num2);
                    
                    output.Add(sLine);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    </Jobs>
</SourceCode>
".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_splitsort_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                #region job3splitsort
                txt = @"<SourceCode>
  <Jobs>
    <Job Name=`checkresults` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                string f1 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                string f2 = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                
                Shell(@`qizmt get  oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output_not_sp.txt ` + f1);
                Shell(@`qizmt get  oo_D3FA879A-5AC7-48c5-80BC-0168550B9A11_splitsort_Output_sp.txt ` + f2);               
                
                string[] lines1 = System.IO.File.ReadAllLines(f1);
                string[] lines2 = System.IO.File.ReadAllLines(f2);                  
                                 
                if(lines1.Length != lines2.Length)
                {
                    throw new Exception(`lines counts are different. lines1.len=` + lines1.Length.ToString() + `; lines2.len=` + lines2.Length.ToString());
                }
                {
                    List<string> list1 = new List<string>(lines1);
                    List<string> list2 = new List<string>(lines2);
                    
                    list1.Sort();
                    list2.Sort();
                    
                    for(int i=0; i < list1.Count; i++)
                    {
                        if(list1[i] != list2[i])
                        {
                            throw new Exception(`line different at: ` + i.ToString());
                        }
                    }
                }
               
                System.IO.File.Delete(f1);
                System.IO.File.Delete(f2);
                
                Qizmt_Log(`error count=0`);
        }
        ]]>
      </Local>
    </Job>    
  </Jobs>
</SourceCode>
".Replace('`', '"');
                System.IO.File.WriteAllText(tempdir + @"\reg_splitsort_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml", txt);
                #endregion

                Exec.Shell("qizmt del reg_*job*_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                Exec.Shell("qizmt importdir \"" + tempdir + "\"");
                System.IO.Directory.Delete(tempdir, true);
                Console.WriteLine("Done");                
            }

            string controlfile = @"\\" + MySpace.DataMining.AELight.Surrogate.MasterHost + @"\c$\temp\"
                   + MySpace.DataMining.AELight.FTTest.controlfilename;
            try
            {
                Console.WriteLine("Running job in normal mode...");
                string output = Exec.Shell("qizmt exec reg_job1_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                if (verbose)
                {
                    Console.WriteLine(output);
                }
                Console.WriteLine("Done");                               

                //Test failure at map
                {
                    string phase = "map";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                //Test failure at exchangeremote
                {
                    string phase = "exchangeremote";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                //Test failure at exchangeowned
                {
                    string phase = "exchangeowned";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                //Test failure at sort
                {
                    string phase = "sort";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                //Test failure at reduce
                {
                    string phase = "reduce";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                //Test failure at replication
                {
                    string phase = "replication";
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                    output = Exec.Shell("qizmt exec reg_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");

                    Console.WriteLine("Checking results...");
                    output = Exec.Shell("qizmt exec reg_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        throw new Exception("Test failed");
                    }
                    if (verbose)
                    {
                        Console.WriteLine(output);
                    }
                    Console.WriteLine("Done");
                }

                if (!skipsplitsort)
                {
                    System.IO.File.Delete(controlfile); //!

                    Console.WriteLine("Running job in normal mode with splitsort...");
                    Exec.Shell("qizmt exec reg_splitsort_job1_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                    Console.WriteLine("Done");

                    //Test failure at splitsort
                    {
                        string phase = "splitsort";
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                        output = Exec.Shell("qizmt exec reg_splitsort_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                        if (verbose)
                        {
                            Console.WriteLine(output);
                        }
                        Console.WriteLine("Done");

                        Console.WriteLine("Checking results...");
                        output = Exec.Shell("qizmt exec reg_splitsort_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                        if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                        {
                            throw new Exception("Test failed");
                        }
                        if (verbose)
                        {
                            Console.WriteLine(output);
                        }
                        Console.WriteLine("Done");
                    }

                    //Test failure at reducelargezblock, reduce when splitsort occurrs.
                    {
                        string phase = "reducelargezblock";
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        Console.WriteLine("Running job in FTE mode with failure at {0}...", phase);
                        output = Exec.Shell("qizmt exec reg_splitsort_job2_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                        if (verbose)
                        {
                            Console.WriteLine(output);
                        }
                        Console.WriteLine("Done");

                        Console.WriteLine("Checking results...");
                        output = Exec.Shell("qizmt exec reg_splitsort_job3_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                        if (output.IndexOf("error count=0", StringComparison.OrdinalIgnoreCase) == -1)
                        {
                            throw new Exception("Test failed");
                        }
                        if (verbose)
                        {
                            Console.WriteLine(output);
                        }
                        Console.WriteLine("Done");
                    }
                }                
            }
            finally
            {
                Exec.Shell("qizmt del reg_*job*_D3FA879A-5AC7-48c5-80BC-0168550B9A11.xml");
                Exec.Shell("qizmt del *_D3FA879A-5AC7-48c5-80BC-0168550B9A11_*.txt");
                System.IO.File.Delete(controlfile);
            }            
        }

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
    }
}
