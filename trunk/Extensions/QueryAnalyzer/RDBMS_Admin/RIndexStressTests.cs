using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void RunRIndexFilteringStressTest(string[] args)
        {
            bool verbose = false;
            int max= 5000;
            int maxasso = 10;
            int maxassoasso = 3;
            int batchsize = 46;
            string param = "";

            if (args.Length >= 5)
            {
                max = Int32.Parse(args[1]);
                maxasso = Int32.Parse(args[2]);
                maxassoasso = Int32.Parse(args[3]);
                batchsize = Int32.Parse(args[4]);
                param = max.ToString() + " " + maxasso.ToString() + " " + maxassoasso.ToString() + " " + batchsize.ToString();

                if (args.Length == 6 && args[5].ToLower() == "-v")
                {
                    verbose = true;
                }
            }

            string tempdir = CurrentDir + @"\" + Guid.NewGuid().ToString();
            System.IO.Directory.CreateDirectory(tempdir);

            List<string> alljobfiles = new List<string>();

            Console.WriteLine("Generating RIndex stress test on Int...");

            #region QizmtSQL-RIndexStressTest-Int

            string token = "97EDDBC662AB4a7c9861EC2D012B5C81";

            alljobfiles.Add(@"QizmtSQL-RIndexStressTest-Int.xml");
            Exec.Shell(@"Qizmt del " + alljobfiles[alljobfiles.Count - 1], false);
            using (System.IO.StreamWriter fs = new System.IO.StreamWriter(tempdir + @"\" + alljobfiles[alljobfiles.Count - 1]))
            {
                fs.Write((@"<SourceCode>
  <Jobs>
    <Job Name=`Cleanup_Previous_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`System.Data.dll` Type=`system`/>           
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                //Clean up previous data.
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_InputTable.bin`);
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Output.txt`);                 
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblRIndexStressTestInt`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblRIndexStressTestIntIndex`;
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }
                    catch
                    {

                    }
                }           
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Generate_Data` Custodian=`` Email=`` Description=`Generate_Data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://QizmtSQL-RIndexStressTest-Int_InputPart####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {                              
                int max =  " + max.ToString() + @";    //Max primary                
                int maxasso =  " + maxasso.ToString() + @";   //Max associations for a primary                
                int maxassoasso = " + maxassoasso.ToString() + @";    //Max shared associations
                
                if(Qizmt_ExecArgs.Length == 4)
                {
                    max = Int32.Parse(Qizmt_ExecArgs[0]);
                    maxasso = Int32.Parse(Qizmt_ExecArgs[1]);
                    maxassoasso = Int32.Parse(Qizmt_ExecArgs[2]);
                }
                
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                //Create sample data.
                List<int> assoasso = new List<int>();
                for(int i = 0; i < max; i++)
                {                   
                    if(i % StaticGlobals.Qizmt_BlocksTotalCount == StaticGlobals.Qizmt_BlockID)
                    {
                        int self = rnd.Next(Int32.MinValue, Int32.MaxValue);
                        
                        int numberofasso = rnd.Next(1, maxasso + 1);                        
                        for(int ai = 0; ai < numberofasso; ai++)
                        {
                            int asso = rnd.Next(Int32.MinValue, Int32.MaxValue);
                            dfsoutput.WriteLine(self.ToString() + `,` + asso.ToString());
                            
                            if(ai < 3)
                            {
                                if(ai == 0)
                                {
                                    assoasso.Clear();
                                    int maxaa = rnd.Next(1, maxassoasso + 1);
                                    for(int aa = 0; aa < maxaa; aa++)
                                    {
                                        assoasso.Add(rnd.Next(Int32.MinValue, Int32.MaxValue));
                                    }
                                }
                                foreach(int aa in assoasso)
                                {
                                    dfsoutput.WriteLine(asso.ToString() + `,` + aa.ToString());
                                }                                
                            }                            
                        }                         
                    }                    
                }                
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Combine_InputPart_Files_And_FGet` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {             
                string fgetdirInput = ``;  
                string fgetdirOutput = ``;
                {
                    string tempdir = IOUtils.GetTempDirectory();
                    int del = tempdir.IndexOf(@`\`, 2);
                    fgetdirInput = tempdir.Substring(del + 1) + @`\fgetinput_" + token + @"`;
                    fgetdirOutput = tempdir.Substring(del + 1) + @`\fgetoutput_" + token + @"`;
                }
                
                string[] fgetdirsInput = new string[StaticGlobals.DSpace_Hosts.Length];
                string[] fputdirsOutput = new string[StaticGlobals.DSpace_Hosts.Length];
                
                for(int i = 0; i < StaticGlobals.DSpace_Hosts.Length; i++)
                {
                    string host = StaticGlobals.DSpace_Hosts[i];
                    {
                        string thisdir = @`\\` + host + @`\` + fgetdirInput;
                        if(System.IO.Directory.Exists(thisdir))
                        {
                            System.IO.Directory.Delete(thisdir, true);
                        }
                        System.IO.Directory.CreateDirectory(thisdir);
                        fgetdirsInput[i] = thisdir;
                    }
                    {
                        string thisdir = @`\\` + host + @`\` + fgetdirOutput;
                        if(System.IO.Directory.Exists(thisdir))
                        {
                            System.IO.Directory.Delete(thisdir, true);
                        }
                        System.IO.Directory.CreateDirectory(thisdir);
                        fputdirsOutput[i] = thisdir;
                    }   
                }
                                
                string tempfngetinput = IOUtils.GetTempDirectory() + @`\fgetinput_" + token + @".txt`;
                using(System.IO.StreamWriter writer = new System.IO.StreamWriter(tempfngetinput))
                {
                    foreach(string dir in fgetdirsInput)
                    {
                        writer.WriteLine(dir);                        
                    }    
                    writer.Close();
                }                
                
                string tempfngetoutput = IOUtils.GetTempDirectory() + @`\fgetoutput_" + token + @".txt`;
                using(System.IO.StreamWriter writer = new System.IO.StreamWriter(tempfngetoutput))
                {
                    foreach(string dir in fputdirsOutput)
                    {
                        writer.WriteLine(dir);                        
                    }    
                    writer.Close();
                }            
                
                Shell(@`Qizmt combine QizmtSQL-RIndexStressTest-Int_InputPart*.txt +QizmtSQL-RIndexStressTest-Int_Input.txt`); 
                Shell(@`Qizmt fget QizmtSQL-RIndexStressTest-Int_Input.txt @` + tempfngetinput);
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Input.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`FPut_Files` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            { 
                string tempfngetinput = IOUtils.GetTempDirectory() + @`\fgetinput_" + token + @".txt`;
                
                DGlobals.Add(`starttime`, DateTime.Now.ToString());
                
                Shell(@`Qizmt fput dirs=@` + tempfngetinput);
            }
        ]]>
      </Local>
    </Job>    
    <Job Name=`Sort_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://QizmtSQL-RIndexStressTest-Int_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexStressTest-Int_InputTable.bin@nInt,nInt</DFSOutput>
        <OutputMethod>fsorted</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                mstring sLine = mstring.Prepare(line);
                int self = sLine.NextItemToInt(',');
                int asso = sLine.NextItemToInt(',');
                
                DbRecordset rKey = DbRecordset.Prepare();
                rKey.PutInt(self);
                
                DbRecordset rValue = DbRecordset.Prepare();
                rValue.PutInt(self);
                rValue.PutInt(asso);
                
                output.Add(rKey.ToByteSlice(), rValue.ToByteSlice());                
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {              
                for(int i = 0; i < values.Length; i++)
                {        
                    output.Add(values[i].Value);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Cleanup_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Input*.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Create_RIndex` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Add Reference=`System.Data.dll` Type=`system`/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                
                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create table tblRIndexStressTestInt (id int, rid int)`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblRIndexStressTestInt bind 'dfs://QizmtSQL-RIndexStressTest-Int_InputTable.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex tblRIndexStressTestIntIndex from tblRIndexStressTestInt pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
                
                TimeSpan duration = DateTime.Now - DateTime.Parse(DGlobals.Get(`starttime`));
                Qizmt_Log(`" + token + @"load=` + duration.TotalSeconds.ToString() + `=`);
                
                DGlobals.Add(`starttime`, DateTime.Now.ToString());
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Process_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblRIndexStressTestInt@nInt,nInt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexStressTest-Int_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Add Reference=`System.Data.dll` Type=`system`/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                DbRecordset rline = DbRecordset.Prepare(line);
                int id = rline.GetInt();
                int rid = rline.GetInt();
                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(id);
                
                DbRecordset rval = DbRecordset.Prepare();
                rval.PutInt(rid);                
            
                output.Add(rkey.ToByteSlice(), rval.ToByteSlice());
            }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[        
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                DbConnection conn = null;
                DbCommand cmd = null;        
                Dictionary<int, List<int>> selfToAssos= null;
                Dictionary<int, int> score =null;
                Dictionary<int, int> goodscore = null;
                Dictionary<int, int> exclude = null;
                Dictionary<int, int> unique = null;
                
                int batchsize = " + batchsize.ToString() + @";   
           
                 public void ReduceInitialize()
                 {                      
                      if(Qizmt_ExecArgs.Length == 4)
                      {
                            batchsize = Int32.Parse(Qizmt_ExecArgs[3]);                            
                      }                      
                       
                      conn = fact.CreateConnection();
                      conn.ConnectionString = `Data Source = ` + string.Join(`,`, StaticGlobals.Qizmt_Hosts) + `; rindex=pooled; retrymaxcount = 100; retrysleep = 5000`;
                      conn.Open();                       
                      cmd = conn.CreateCommand();     
                      
                      selfToAssos= new Dictionary<int, List<int>>();
                      score = new Dictionary<int, int>();
                      goodscore = new Dictionary<int, int>();
                      exclude = new Dictionary<int, int>();
                      unique = new Dictionary<int, int>();                 
                 }        
            ]]>
         </ReduceInitialize>
        <Reduce>
          <![CDATA[               
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {                
               DbRecordset rkey = DbRecordset.Prepare(key);
               int self = rkey.GetInt();
               
               selfToAssos.Add(self, new List<int>());
               List<int> assos = selfToAssos[self];               
               for(int i = 0; i < values.Length; i++)
               {
                   DbRecordset rval = DbRecordset.Prepare(values[i].Value);
                   int asso = rval.GetInt();
                   assos.Add(asso);                   
               } 
               
                if(selfToAssos.Count >= batchsize || StaticGlobals.Qizmt_Last == true)
                {     
                    CalculateScore(output);
                }         
            }
            
            StringBuilder whrsb = new StringBuilder();
            
            private void CalculateScore(ReduceOutput output)
            {           
                   //Get associations of the associations collected so far
                   whrsb.Length = 0;
                   unique.Clear();
                   foreach(int self in selfToAssos.Keys)
                   {
                       List<int> assos = selfToAssos[self];  
                       foreach(int asso in assos)
                       {
                           if(!unique.ContainsKey(asso))
                           {
                               whrsb.Append(` or key = `).Append(asso.ToString());
                               unique.Add(asso, 0);
                           }                           
                       }
                   }
                  
                   string whr = whrsb.ToString();
                   if(whr.Length > 0)
                   {
                       Dictionary<int, List<int>> assoassos = new Dictionary<int, List<int>>();    
                       cmd.CommandText = `rselect * from tblRIndexStressTestIntIndex where ` + whr.Substring(3);
                     
                       DbDataReader reader = cmd.ExecuteReader();
                       while (reader.Read())
                       {
                            int asso = reader.GetInt32(0);
                            int assoasso = reader.GetInt32(1);
                            if(!assoassos.ContainsKey(asso))
                            {
                                assoassos.Add(asso, new List<int>());
                            }
                            assoassos[asso].Add(assoasso);                            
                       }
                        reader.Close();
                      
                        //Calculate score                        
                        foreach(int self in selfToAssos.Keys)
                        {                    
                            score.Clear();
                            goodscore.Clear();
                            exclude.Clear();
                            
                            List<int> assos = selfToAssos[self];
                            
                            foreach(int asso in assos)
                            {
                                if(!exclude.ContainsKey(asso))
                                {
                                    exclude.Add(asso, 0);
                                }
                            }
                            
                            foreach(int asso in assos)
                            {
                                if(assoassos.ContainsKey(asso))
                                {
                                    List<int> aalist = assoassos[asso];
                                    foreach(int aa in aalist)
                                    {
                                        if(!score.ContainsKey(aa))
                                        {
                                            score.Add(aa, 0);
                                        }
                                        
                                        if(aa != self && !exclude.ContainsKey(aa))
                                        {
                                            int curscore = ++score[aa];
                                            
                                            if(curscore > 2)
                                            {
                                                goodscore[aa] = curscore;
                                            }        
                                        }                                                                        
                                    }   
                                }
                            } //foreach asso
                            
                          
                            foreach(int aa in goodscore.Keys)
                            {
                                
                                mstring sline = mstring.Prepare(self);
                                sline.AppendM(',');
                                sline.AppendM(aa);
                                sline.AppendM(',');
                                sline.AppendM(goodscore[aa]);
                                
                                output.WriteLine(sline);
                            
                            }   
                            
                        } //foreach self
                   }
                   
                   selfToAssos.Clear();
               } 
        ]]>
        </Reduce>        
        <ReduceFinalize>
          <![CDATA[
            public void ReduceFinalize()
            {   
                 conn.Close();                
            }             
            ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
    <Job Name=`FGet_Results` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                TimeSpan duration = DateTime.Now - DateTime.Parse(DGlobals.Get(`starttime`));
                Qizmt_Log(`" + token + @"filter=` + duration.TotalSeconds.ToString() + `=`);   

                Qizmt_Log(Shell(`Qizmt info QizmtSQL-RIndexStressTest-Int_Output.txt`));
                Qizmt_Log(Shell(`Qizmt info RDBMS_Table_tblRIndexStressTestInt`));

                string tempfngetoutput = IOUtils.GetTempDirectory() + @`\fgetoutput_" + token + @".txt`;         
                
                DateTime starttime = DateTime.Now;
                Shell(@`Qizmt fget QizmtSQL-RIndexStressTest-Int_Output.txt @` + tempfngetoutput);
                duration = DateTime.Now - starttime;
                
                Qizmt_Log(`" + token + @"write=` + duration.TotalSeconds.ToString() + `=`);   
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Cleanup_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`System.Data.dll` Type=`system`/>           
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_InputTable.bin`);
                Shell(@`Qizmt del QizmtSQL-RIndexStressTest-Int_Output.txt`); 
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblRIndexStressTestInt`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblRIndexStressTestIntIndex`;
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }
                    catch
                    {

                    }
                } 
                
                string tempfngetinput = IOUtils.GetTempDirectory() + @`\fgetinput_" + token + @".txt`;
                {
                     string[] dirs = System.IO.File.ReadAllLines(tempfngetinput);
                     foreach(string dir in dirs)
                     {
                         System.IO.Directory.Delete(dir, true);
                     }
                }
                
                string tempfngetoutput = IOUtils.GetTempDirectory() + @`\fgetoutput_" + token + @".txt`;
                {
                     string[] dirs = System.IO.File.ReadAllLines(tempfngetoutput);
                     foreach(string dir in dirs)
                     {
                         System.IO.Directory.Delete(dir, true);
                     }
                }
               
                System.IO.File.Delete(tempfngetinput);
                System.IO.File.Delete(tempfngetoutput);
            }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                fs.Close();
            }
            #endregion

            Exec.Shell("Qizmt importdir \"" + tempdir + "\"");
            System.IO.Directory.Delete(tempdir, true);
            Console.WriteLine("Stress tests generated.");

            Console.WriteLine("Running RIndex stress test on Int...Qizmt exec QizmtSQL-RIndexStressTest-Int.xml " + param);
            string output = Exec.Shell(@"Qizmt exec QizmtSQL-RIndexStressTest-Int.xml " + param);
            if (verbose)
            {
                Console.WriteLine(output);
            }

            ConsoleColor oldcolor = Console.ForegroundColor;
            double totaldur = 0;
            Console.WriteLine("");
            Console.WriteLine("---Test Results---");

            Console.Write("Load Graph Duration: ");
            Console.ForegroundColor = ConsoleColor.Green;
            double thisdur = ParseDuration(output, token + "load=");
            totaldur += thisdur;
            Console.WriteLine(DurationString((int)Math.Round(thisdur)));

            Console.ForegroundColor = oldcolor;
            Console.Write("Collaborative Filtering Duration: ");
            Console.ForegroundColor = ConsoleColor.Green;
            thisdur = ParseDuration(output, token + "filter=");
            totaldur += thisdur;
            Console.WriteLine(DurationString((int)Math.Round(thisdur)));

            Console.ForegroundColor = oldcolor;
            Console.Write("Results Write Duration: ");
            Console.ForegroundColor = ConsoleColor.Green;
            thisdur = ParseDuration(output, token + "write=");
            totaldur += thisdur;
            Console.WriteLine(DurationString((int)Math.Round(thisdur)));
            Console.ForegroundColor = oldcolor;

            Console.Write("Total Duration: ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(DurationString((int)Math.Round(totaldur)));
            Console.ForegroundColor = oldcolor;
        }

        private static void GenerateRIndexBasicStressTest(string[] args)
        {
            string tempdir = CurrentDir + @"\" + Guid.NewGuid().ToString();
            System.IO.Directory.CreateDirectory(tempdir);

            List<string> alljobfiles = new List<string>();
            Console.WriteLine("Generating RIndex stress test on Int...");

            #region RIndexBasicStressTest
            alljobfiles.Add(@"QizmtSQL-RIndexBasicStressTest.xml");
            Exec.Shell(@"Qizmt del " + alljobfiles[alljobfiles.Count - 1], false);
            using (System.IO.StreamWriter fs = new System.IO.StreamWriter(tempdir + @"\" + alljobfiles[alljobfiles.Count - 1]))
            {
                fs.Write(@"<SourceCode>
  <Jobs>   
    <Job Name=`Cleanup_Previous_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`System.Data.dll` Type=`system`/>     
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-RIndexBasicStressTest_Output.txt`);
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblRIndexBasicStressTest`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblRIndexBasicStressTestIndex`;
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }
                    catch
                    {

                    }
                }
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Generate_Data_Remote` Custodian=`` Email=`` Description=`Generate_Data_Remote`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://QizmtSQL-RIndexBasicStressTest_Input####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int max =  10000000;
                int maxasso = 10;
                int maxassoasso = 3; //*3
                
                if(Qizmt_ExecArgs.Length == 4)
                {
                    max = Int32.Parse(Qizmt_ExecArgs[0]);
                    maxasso = Int32.Parse(Qizmt_ExecArgs[1]);
                    maxassoasso = Int32.Parse(Qizmt_ExecArgs[2]);
                }
                
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                //Create sample data.
                List<int> assoasso = new List<int>();
                for(int i = 0; i < max; i++)
                {                   
                    if(i % StaticGlobals.DSpace_BlocksTotalCount == StaticGlobals.DSpace_BlockID)
                    {
                        int self = i;
                        
                        int numberofasso = rnd.Next(1, maxasso + 1);                        
                        for(int ai = 0; ai < numberofasso; ai++)
                        {
                            int asso = rnd.Next(0, max);
                            dfsoutput.WriteLine(self.ToString() + `,` + asso.ToString());                         
                        }                         
                    }                    
                }                
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Generate_Data_MR` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://QizmtSQL-RIndexBasicStressTest_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexBasicStressTest_InputTable.bin@nInt,nInt</DFSOutput>
        <OutputMethod>fsorted</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                mstring sLine = mstring.Prepare(line);
                int self = sLine.NextItemToInt(',');
                int asso = sLine.NextItemToInt(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(self);
                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(asso);
                
                output.Add(rKey, rValue);                
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int self = rKey.GetInt();
                
                for(int i = 0; i < values.Length; i++)
                {                   
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    int asso = rValue.GetInt();
                    
                    DbRecordset row = DbRecordset.Prepare();
                    row.PutInt(self); 
                    row.PutInt(asso);   
                    
                    output.Add(row.ToByteSlice());
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Cleanup_Binary_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Create_RIndex` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Add Reference=`System.Data.dll` Type=`system`/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
               
                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create table tblRIndexBasicStressTest (id int, rid int)`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblRIndexBasicStressTest bind 'dfs://QizmtSQL-RIndexBasicStressTest_InputTable.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex tblRIndexBasicStressTestIndex from tblRIndexBasicStressTest pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`rindexexample_processdata` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblRIndexBasicStressTest@nInt,nInt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexBasicStressTest_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Add Reference=`System.Data.dll` Type=`system`/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                DbRecordset rline = DbRecordset.Prepare(line);
                int id = rline.GetInt();
                int rid = rline.GetInt();
                
                recordset rkey = recordset.Prepare();
                rkey.PutInt(id);
                
                recordset rval = recordset.Prepare();
                rval.PutInt(rid);                
            
                output.Add(rkey, rval);
            }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[        
          
            StringBuilder whrsb = new StringBuilder();
            
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                DbConnection conn = null;
                DbCommand cmd = null;                

                int batchsize = 1900;                
               
                //int batchsize = 1000;                
               
                int reduceittrs = 0;
                 public void ReduceInitialize()
                 { 
                        whrsb.Append(`rselect * from tblRIndexBasicStressTestIndex where KEY = -1`);
                     
                      if(Qizmt_ExecArgs.Length == 4)
                      {
                            batchsize = Int32.Parse(Qizmt_ExecArgs[3]);                            
                      }                      
                       
                      conn = fact.CreateConnection();
                      conn.ConnectionString = `Data Source = ` + string.Join(`,`, StaticGlobals.Qizmt_Hosts) + `; rindex=pooled; retrymaxcount = 100; retrysleep = 5000`;
                      conn.Open();                       
                      cmd = conn.CreateCommand();     
                      
      
                 }        
            ]]>
         </ReduceInitialize>
        <Reduce>
          <![CDATA[          
          
            
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                {
                   recordset rkey = recordset.Prepare(key);
                   int self = rkey.GetInt();
                   
          
                   for(int i = 1; i < values.Length; i++)
                   {
                       recordset rval = recordset.Prepare(values[i].Value);
                       int asso = rval.GetInt();
                       whrsb.Append(` OR KEY = `).Append(asso.ToString());                  
                   }                 
                }               

               
               ++reduceittrs;
               if(reduceittrs >= batchsize || StaticGlobals.Qizmt_Last == true)
               {     
                   string whr = whrsb.ToString();
                   if(whr.Length > 0)
                   {
                       cmd.CommandText = whr;
                       using(DbDataReader reader = cmd.ExecuteReader())
                       {
                           while (reader.Read())
                           {
                                int asso = reader.GetInt32(0);
                                int assoasso = reader.GetInt32(1);
                           }
                            reader.Close();
                       }
                   }
                   whrsb.Length = 0;
                   whrsb.Append(`rselect * from tblRIndexBasicStressTestIndex where KEY = -1`);
                   reduceittrs = 0;
               } 
            }
        ]]>
        </Reduce>        
        <ReduceFinalize>
          <![CDATA[
            public void ReduceFinalize()
            {   
                 conn.Close();                
            }             
            ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
    <Job Name=`Cleanup_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference=`System.Data.dll` Type=`system`/>     
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-RIndexBasicStressTest_Output.txt`);
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblRIndexBasicStressTest`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblRIndexBasicStressTestIndex`;
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }
                    catch
                    {

                    }
                }
            }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                fs.Close();
            }
            #endregion

            Exec.Shell("Qizmt importdir \"" + tempdir + "\"");
            System.IO.Directory.Delete(tempdir, true);

            foreach (string job in alljobfiles)
            {
                Console.WriteLine("    Qizmt exec {0}", job);
                Console.WriteLine("    Qizmt exec {0} [maxPrimary] [maxAssociations] [maxSharedAssociations] [batchSize]", job);
            }
        }

        private static string DurationString(int secs)
        {
            int mins = secs / 60;
            int hrs = mins / 60;
            string srhrs = (mins / 60).ToString().PadLeft(2, '0');
            string srmins = (mins % 60).ToString().PadLeft(2, '0');
            string srsecs = (secs % 60).ToString().PadLeft(2, '0');
            return srhrs + ":" + srmins + ":" + srsecs;
        }

        private static double ParseDuration(string output, string subtoken)
        {
            double sec = -1;
            int del = output.IndexOf(subtoken);
            if (del > -1)
            {
                int strstart = del + subtoken.Length;
                int strlen = output.IndexOf("=", strstart) - strstart;
                sec = double.Parse(output.Substring(strstart, strlen));
            }
            return sec;
        }
    }
}
