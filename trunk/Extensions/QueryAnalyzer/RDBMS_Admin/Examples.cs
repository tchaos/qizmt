using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void GenerateExamples()
        {
            string tempdir = CurrentDir + @"\" + Guid.NewGuid().ToString();
            System.IO.Directory.CreateDirectory(tempdir);

            List<string> alljobfiles = new List<string>();

            Console.WriteLine("Generating examples...");

            #region CSVToTable
            alljobfiles.Add(@"QizmtSQL-CSVToTable.xml");
            Exec.Shell(@"Qizmt del " + alljobfiles[alljobfiles.Count - 1], false);
            using (System.IO.StreamWriter fs = new System.IO.StreamWriter(tempdir + @"\" + alljobfiles[alljobfiles.Count - 1]))
            {
                fs.Write(@"<SourceCode>
  <Jobs>
    <Job Name=`QizmtSQL-CSVToTable_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-CSVToTable_Input.txt`);
                Shell(@`Qizmt del QizmtSQL-CSVToTable_Output.bin`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`QizmtSQL-CSVToTable_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://QizmtSQL-CSVToTable_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample CSV data.
                dfsoutput.WriteLine(`11,1498,The Last Supper,100.45,374000000,200,1/1/1498 10:00:00 AM`);
                dfsoutput.WriteLine(`12,1503,Mona Lisa,4.75,600000000,200,1/1/1503 10:00:00 AM`);
                dfsoutput.WriteLine(`13,1889,The Starry Night,1.5,100000000,201,1/1/1889 10:00:00 AM`);
                dfsoutput.WriteLine(`14,1889,Irises,4.93,100000000,201,1/1/1889 10:00:00 AM`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`QizmtSQL-CSVToTable` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://QizmtSQL-CSVToTable_Input.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-CSVToTable_Output.bin@nInt,nInt,nChar(300),nDouble,nLong,nInt,NDateTime</DFSOutput>
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
                int paintingID = sLine.NextItemToInt(',');
                int year = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                double size = sLine.NextItemToDouble(',');
                long pixel = sLine.NextItemToLong(',');
                int artistID = sLine.NextItemToInt(',');
                DateTime bday = DateTime.Parse(sLine.NextItemToString(',').ToString());
                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(paintingID);
                
                DbRecordset rvalue = DbRecordset.Prepare();
                rvalue.PutInt(paintingID);
                rvalue.PutInt(year);                
                rvalue.PutString(title, 300);
                rvalue.PutDouble(size);
                rvalue.PutLong(pixel);
                rvalue.PutInt(artistID);
                rvalue.PutDateTime(bday);
                
                output.Add(rkey.ToByteSlice(), rvalue.ToByteSlice());
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
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                fs.Close();
            }             
            #endregion

            #region TableToCSV
            alljobfiles.Add(@"QizmtSQL-TableToCSV.xml");
            Exec.Shell(@"Qizmt del " + alljobfiles[alljobfiles.Count - 1], false);
            using (System.IO.StreamWriter fs = new System.IO.StreamWriter(tempdir + @"\" + alljobfiles[alljobfiles.Count - 1]))
            {
                fs.Write(@"<SourceCode>
  <Jobs>
    <Job Name=`QizmtSQL-TableToCSV_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del QizmtSQL-TableToCSV_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-TableToCSV_Output.bin`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`QizmtSQL-TableToCSV_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://QizmtSQL-TableToCSV_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample CSV data.
                dfsoutput.WriteLine(`11,1498,The Last Supper,100.45,374000000,200,1/1/1498 10:00:00 AM`);
                dfsoutput.WriteLine(`12,1503,Mona Lisa,4.75,600000000,200,1/1/1503 10:00:00 AM`);
                dfsoutput.WriteLine(`13,1889,The Starry Night,1.5,100000000,201,1/1/1889 10:00:00 AM`);
                dfsoutput.WriteLine(`14,1889,Irises,4.93,100000000,201,1/1/1889 10:00:00 AM`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`QizmtSQL-TableToCSV` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://QizmtSQL-TableToCSV_Input.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-TableToCSV_Output.bin@nInt,nInt,nChar(300),nDouble,nLong,nInt,NDateTime</DFSOutput>
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
                int paintingID = sLine.NextItemToInt(',');
                int year = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                double size = sLine.NextItemToDouble(',');
                long pixel = sLine.NextItemToLong(',');
                int artistID = sLine.NextItemToInt(',');
                DateTime bday = DateTime.Parse(sLine.NextItemToString(',').ToString());
                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(paintingID);
                
                DbRecordset rvalue = DbRecordset.Prepare();
                rvalue.PutInt(paintingID);
                rvalue.PutInt(year);                
                rvalue.PutString(title, 300);
                rvalue.PutDouble(size);
                rvalue.PutLong(pixel);
                rvalue.PutInt(artistID);
                rvalue.PutDateTime(bday);
                
                output.Add(rkey.ToByteSlice(), rvalue.ToByteSlice());
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
    <Job Name=`QizmtSQL-TableToCSV` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt,nInt,nChar(300),nDouble,nLong,nInt,NDateTime</KeyLength>
        <DFSInput>dfs://QizmtSQL-TableToCSV_Output.bin@nInt,nInt,nChar(300),nDouble,nLong,nInt,NDateTime</DFSInput>
        <DFSOutput>dfs://QizmtSQL-TableToCSV_Input2.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                output.Add(line, line);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {                
                for(int i = 0; i < values.Length; i++)
                {
                    DbRecordset rvalue = DbRecordset.Prepare(values[i].Value);
                    int paintingID = rvalue.GetInt();
                    int year = rvalue.GetInt();
                    mstring title = rvalue.GetString(300);
                    double size = rvalue.GetDouble();
                    long pixel = rvalue.GetLong();
                    int artistID = rvalue.GetInt();
                    DateTime bday = rvalue.GetDateTime();
                    
                    mstring line = mstring.Prepare(paintingID);
                    line = line.AppendM(',').AppendM(year).
                    AppendM(',').AppendM(title).
                    AppendM(',').AppendM(size).
                    AppendM(',').AppendM(pixel).
                    AppendM(',').AppendM(artistID).
                    AppendM(',').AppendM(bday.ToString());
                    
                    output.Add(line);                    
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                fs.Close();
            }
            #endregion

            #region QizmtSQL-GraphProcessing-Int
            alljobfiles.Add(@"QizmtSQL-GraphProcessing-Int.xml");
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
                //Clean up previous data.
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Int_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Int_Output.txt`); 
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblGraphProcessingInt`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblGraphProcessingIntIndex`;
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
          <DFSWriter>dfs://QizmtSQL-GraphProcessing-Int_Input####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {                              
                int max =  5000;    //Max primary                
                int maxasso = 10;   //Max associations for a primary                
                int maxassoasso = 3;    //Max shared associations
                
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
    <Job Name=`Sort_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://QizmtSQL-GraphProcessing-Int_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Int_InputTable.bin@nInt,nInt</DFSOutput>
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
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Int_Input*.txt`);
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
                    cmd.CommandText = `create table tblGraphProcessingInt (id int, rid int)`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblGraphProcessingInt bind 'dfs://QizmtSQL-GraphProcessing-Int_InputTable.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex tblGraphProcessingIntIndex from tblGraphProcessingInt pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Process_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblGraphProcessingInt@nInt,nInt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Int_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
        <Setting name=`Subprocess_TotalPrime` value=`NearPrime2XCoreCount` />
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
                
                int batchsize = 46;   
           
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
                       cmd.CommandText = `rselect * from tblGraphProcessingIntIndex where ` + whr.Substring(3);
                     
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
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                fs.Close();
            }
            #endregion

            #region QizmtSQL-GraphProcessing-Double
            alljobfiles.Add(@"QizmtSQL-GraphProcessing-Double.xml");
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
                //Clean up previous data.
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Double_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Double_Output.txt`); 
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblGraphProcessingDouble`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblGraphProcessingDoubleIndex`;
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
          <DFSWriter>dfs://QizmtSQL-GraphProcessing-Double_Input####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {                              
                int max =  5000;    //Max number of primary ID's               
                int maxasso = 10;   //Max associations for each primary ID               
                int maxassoasso = 3;    //Max shared associations
                
                if(Qizmt_ExecArgs.Length == 4)
                {
                    max = Int32.Parse(Qizmt_ExecArgs[0]);
                    maxasso = Int32.Parse(Qizmt_ExecArgs[1]);
                    maxassoasso = Int32.Parse(Qizmt_ExecArgs[2]);
                }
                
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                //Create sample data.
                List<double> assoasso = new List<double>();
                for(int i = 0; i < max; i++)
                {                   
                    if(i % StaticGlobals.Qizmt_BlocksTotalCount == StaticGlobals.Qizmt_BlockID)
                    {
                        double self = GenerateDouble(rnd);
                        
                        int numberofasso = rnd.Next(1, maxasso + 1);                        
                        for(int ai = 0; ai < numberofasso; ai++)
                        {
                            double asso = GenerateDouble(rnd);
                            dfsoutput.WriteLine(self.ToString() + `,` + asso.ToString());
                            
                            if(ai < 3)
                            {
                                if(ai == 0)
                                {
                                    assoasso.Clear();
                                    int maxaa = rnd.Next(1, maxassoasso + 1);
                                    for(int aa = 0; aa < maxaa; aa++)
                                    {
                                        assoasso.Add(GenerateDouble(rnd));
                                    }
                                }
                                foreach(double aa in assoasso)
                                {
                                    dfsoutput.WriteLine(asso.ToString() + `,` + aa.ToString());
                                }                                
                            }                            
                        }                         
                    }                    
                }                
            }
            
            private double GenerateDouble(Random rnd)
            {
                return Math.Round(rnd.NextDouble() * rnd.Next(Int32.MinValue, Int32.MaxValue), 5);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Sort_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nDouble</KeyLength>
        <DFSInput>dfs://QizmtSQL-GraphProcessing-Double_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Double_InputTable.bin@nDouble,nDouble</DFSOutput>
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
                double self = sLine.NextItemToDouble(',');
                double asso = sLine.NextItemToDouble(',');
                
                DbRecordset rKey = DbRecordset.Prepare();
                rKey.PutDouble(self);
                
                DbRecordset rValue = DbRecordset.Prepare();
                rValue.PutDouble(self);
                rValue.PutDouble(asso);
                
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
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Double_Input*.txt`);
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
                    cmd.CommandText = `create table tblGraphProcessingDouble (id double, rid double)`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblGraphProcessingDouble bind 'dfs://QizmtSQL-GraphProcessing-Double_InputTable.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex tblGraphProcessingDoubleIndex from tblGraphProcessingDouble pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Process_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nDouble</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblGraphProcessingDouble@nDouble,nDouble</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Double_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
        <Setting name=`Subprocess_TotalPrime` value=`NearPrime2XCoreCount` />
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
                double id = rline.GetDouble();
                double rid = rline.GetDouble();
                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutDouble(id);
                
                DbRecordset rval = DbRecordset.Prepare();
                rval.PutDouble(rid);                
            
                output.Add(rkey.ToByteSlice(), rval.ToByteSlice());
            }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[        
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                DbConnection conn = null;
                DbCommand cmd = null;        
                Dictionary<double, List<double>> selfToAssos= null;
                Dictionary<double, int> score =null;
                Dictionary<double, int> goodscore = null;
                Dictionary<double, int> exclude = null;
                Dictionary<double, int> unique = null;
                
                int batchsize = 46;   
           
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
                      
                      selfToAssos= new Dictionary<double, List<double>>();
                      score = new Dictionary<double, int>();
                      goodscore = new Dictionary<double, int>();
                      exclude = new Dictionary<double, int>();
                      unique = new Dictionary<double, int>();                 
                 }        
            ]]>
         </ReduceInitialize>
        <Reduce>
          <![CDATA[               
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {                
               DbRecordset rkey = DbRecordset.Prepare(key);
               double self = rkey.GetDouble();
               
               selfToAssos.Add(self, new List<double>());
               List<double> assos = selfToAssos[self];               
               for(int i = 0; i < values.Length; i++)
               {
                   DbRecordset rval = DbRecordset.Prepare(values[i].Value);
                   double asso = rval.GetDouble();
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
                   foreach(double self in selfToAssos.Keys)
                   {
                       List<double> assos = selfToAssos[self];  
                       foreach(double asso in assos)
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
                       Dictionary<double, List<double>> assoassos = new Dictionary<double, List<double>>();    
                       cmd.CommandText = `rselect * from tblGraphProcessingDoubleIndex where ` + whr.Substring(3);
                     
                       DbDataReader reader = cmd.ExecuteReader();
                       while (reader.Read())
                       {
                            double asso = reader.GetDouble(0);
                            double assoasso = reader.GetDouble(1);
                            if(!assoassos.ContainsKey(asso))
                            {
                                assoassos.Add(asso, new List<double>());
                            }
                            assoassos[asso].Add(assoasso);                            
                       }
                        reader.Close();
                      
                        //Calculate score                        
                        foreach(double self in selfToAssos.Keys)
                        {                    
                            score.Clear();
                            goodscore.Clear();
                            exclude.Clear();
                            
                            List<double> assos = selfToAssos[self];
                            
                            foreach(double asso in assos)
                            {
                                if(!exclude.ContainsKey(asso))
                                {
                                    exclude.Add(asso, 0);
                                }
                            }
                            
                            foreach(double asso in assos)
                            {
                                if(assoassos.ContainsKey(asso))
                                {
                                    List<double> aalist = assoassos[asso];
                                    foreach(double aa in aalist)
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
                            
                          
                            foreach(double aa in goodscore.Keys)
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
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                fs.Close();
            }
            #endregion

            #region QizmtSQL-GraphProcessing-Char
            alljobfiles.Add(@"QizmtSQL-GraphProcessing-Char.xml");
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
                //Clean up previous data.
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Char_Input*.txt`);
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Char_Output.txt`); 
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblGraphProcessingChar`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex tblGraphProcessingCharIndex`;
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
          <DFSWriter>dfs://QizmtSQL-GraphProcessing-Char_Input####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {                              
                int max =  5000;    //Max number of primary ID's               
                int maxasso = 10;   //Max associations for each primary ID               
                int maxassoasso = 3;    //Max shared associations
                int maxstrlen = 200;    
                
                if(Qizmt_ExecArgs.Length == 4)
                {
                    max = Int32.Parse(Qizmt_ExecArgs[0]);
                    maxasso = Int32.Parse(Qizmt_ExecArgs[1]);
                    maxassoasso = Int32.Parse(Qizmt_ExecArgs[2]);
                }
                
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                
                //Create sample data.
                List<string> assoasso = new List<string>();
                for(int i = 0; i < max; i++)
                {                   
                    if(i % StaticGlobals.Qizmt_BlocksTotalCount == StaticGlobals.Qizmt_BlockID)
                    {
                        string self = GenerateString(rnd, maxstrlen);
                        
                        int numberofasso = rnd.Next(1, maxasso + 1);                        
                        for(int ai = 0; ai < numberofasso; ai++)
                        {
                            string asso = GenerateString(rnd, maxstrlen);
                            dfsoutput.WriteLine(self + `,` + asso);
                            
                            if(ai < 3)
                            {
                                if(ai == 0)
                                {
                                    assoasso.Clear();
                                    int maxaa = rnd.Next(1, maxassoasso + 1);
                                    for(int aa = 0; aa < maxaa; aa++)
                                    {
                                        assoasso.Add(GenerateString(rnd, maxstrlen));
                                    }
                                }
                                foreach(string aa in assoasso)
                                {
                                    dfsoutput.WriteLine(asso + `,` + aa);
                                }                                
                            }                            
                        }                         
                    }                    
                }                
            }
            
            private string GenerateString(Random rnd, int maxlen)
            {
                string str = ``;
                int strlen = rnd.Next(1, maxlen + 1);
                for(int i = 0; i < strlen; i++)
                {
                    str += (char)rnd.Next(65, 127);
                }
                return str;
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Sort_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nChar(200)</KeyLength>
        <DFSInput>dfs://QizmtSQL-GraphProcessing-Char_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Char_InputTable.bin@nChar(200),nChar(200)</DFSOutput>
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
                mstring self = sLine.NextItemToString(',');
                mstring asso = sLine.NextItemToString(',');
                
                DbRecordset rKey = DbRecordset.Prepare();
                rKey.PutString(self, 200);
                
                DbRecordset rValue = DbRecordset.Prepare();
                rValue.PutString(self, 200);
                rValue.PutString(asso, 200);
                
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
                Shell(@`Qizmt del QizmtSQL-GraphProcessing-Char_Input*.txt`);
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
                    cmd.CommandText = `create table tblGraphProcessingChar (id char(200), rid char(200))`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblGraphProcessingChar bind 'dfs://QizmtSQL-GraphProcessing-Char_InputTable.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex tblGraphProcessingCharIndex from tblGraphProcessingChar pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Process_Data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nChar(200)</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblGraphProcessingChar@nChar(200),nChar(200)</DFSInput>
        <DFSOutput>dfs://QizmtSQL-GraphProcessing-Char_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
        <Setting name=`Subprocess_TotalPrime` value=`NearPrime2XCoreCount` />
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
                mstring id = rline.GetString(200);
                mstring rid = rline.GetString(200);
                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutString(id, 200);
                
                DbRecordset rval = DbRecordset.Prepare();
                rval.PutString(rid, 200);                
            
                output.Add(rkey.ToByteSlice(), rval.ToByteSlice());
            }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[        
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                DbConnection conn = null;
                DbCommand cmd = null;        
                Dictionary<string, List<string>> selfToAssos= null;
                Dictionary<string, int> score =null;
                Dictionary<string, int> goodscore = null;
                Dictionary<string, int> exclude = null;
                Dictionary<string, int> unique = null;
                
                int batchsize = 46;   
           
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
                      
                      selfToAssos= new Dictionary<string, List<string>>();
                      score = new Dictionary<string, int>();
                      goodscore = new Dictionary<string, int>();
                      exclude = new Dictionary<string, int>();
                      unique = new Dictionary<string, int>();                 
                 }        
            ]]>
         </ReduceInitialize>
        <Reduce>
          <![CDATA[               
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {                
               DbRecordset rkey = DbRecordset.Prepare(key);
               string self = rkey.GetString(200).ToString();
               
               selfToAssos.Add(self, new List<string>());
               List<string> assos = selfToAssos[self];               
               for(int i = 0; i < values.Length; i++)
               {
                   DbRecordset rval = DbRecordset.Prepare(values[i].Value);
                   string asso = rval.GetString(200).ToString();
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
                   foreach(string self in selfToAssos.Keys)
                   {
                       List<string> assos = selfToAssos[self];  
                       foreach(string asso in assos)
                       {
                           if(!unique.ContainsKey(asso))
                           {
                               whrsb.Append(` or key = '`).Append(asso).Append(`'`);
                               unique.Add(asso, 0);
                           }                           
                       }
                   }
                  
                   string whr = whrsb.ToString();
                   if(whr.Length > 0)
                   {
                       Dictionary<string, List<string>> assoassos = new Dictionary<string, List<string>>();    
                       cmd.CommandText = `rselect * from tblGraphProcessingCharIndex where ` + whr.Substring(3);
                     
                       DbDataReader reader = cmd.ExecuteReader();
                       while (reader.Read())
                       {
                            string asso = reader.GetString(0);
                            string assoasso = reader.GetString(1);
                            if(!assoassos.ContainsKey(asso))
                            {
                                assoassos.Add(asso, new List<string>());
                            }
                            assoassos[asso].Add(assoasso);                            
                       }
                        reader.Close();
                      
                        //Calculate score                        
                        foreach(string self in selfToAssos.Keys)
                        {                    
                            score.Clear();
                            goodscore.Clear();
                            exclude.Clear();
                            
                            List<string> assos = selfToAssos[self];
                            
                            foreach(string asso in assos)
                            {
                                if(!exclude.ContainsKey(asso))
                                {
                                    exclude.Add(asso, 0);
                                }
                            }
                            
                            foreach(string asso in assos)
                            {
                                if(assoassos.ContainsKey(asso))
                                {
                                    List<string> aalist = assoassos[asso];
                                    foreach(string aa in aalist)
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
                            
                          
                            foreach(string aa in goodscore.Keys)
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
            }
        }
    }
}
