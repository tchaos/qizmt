<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Qizmt SQL INDEXs/Distributed Memory](MySpaceQizmtSQLQuickStartGuideDistributedMemory.md)


```
<SourceCode>
  <Jobs>   
    <Job Name="Cleanup_Previous_Data" Custodian="" Email="">
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference="System.Data.dll" Type="system"/>     
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                //Clean up data from previous run.
                Shell(@"Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt");
                Shell(@"Qizmt del QizmtSQL-RIndexBasicStressTest_Output.txt");

                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = "Data Source = localhost";
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "drop rindex tblRIndexBasicStressTestIndex";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "drop table tblRIndexBasicStressTest";
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
    <Job Name="Generate_Data_Remote" Custodian="" Email="" Description="Generate_Data_Remote">
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
                //Create random graph.

                int max = 10000000;
                int maxasso = 10;
                int maxassoasso = 3; //*3

                if (Qizmt_ExecArgs.Length == 4)
                {
                    max = Int32.Parse(Qizmt_ExecArgs[0]);
                    maxasso = Int32.Parse(Qizmt_ExecArgs[1]);
                    maxassoasso = Int32.Parse(Qizmt_ExecArgs[2]);
                }

                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);

                List<int> assoasso = new List<int>();
                for (int i = 0; i < max; i++)
                {
                    if (i % StaticGlobals.Qizmt_BlocksTotalCount == StaticGlobals.Qizmt_BlockID)
                    {
                        int self = i;

                        int numberofasso = rnd.Next(1, maxasso + 1);
                        for (int ai = 0; ai < numberofasso; ai++)
                        {
                            int asso = rnd.Next(0, max);
                            dfsoutput.WriteLine(self.ToString() + "," + asso.ToString());
                        }
                    }
                }
            }
        ]]>
      </Remote>
    </Job>
    <Job Name="Generate_Data_MR" Custodian="" Email="">
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://QizmtSQL-RIndexBasicStressTest_Input*.txt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexBasicStressTest_InputTable.bin@nInt,nInt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <Add Reference="RDBMS_DBCORE.dll" Type="dfs"/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                //Sort graph by left key.  This prepares the underlying binary table used for creating the table.
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

                for (int i = 0; i < values.Length; i++)
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
    <Job Name="Cleanup_Binary_Data" Custodian="" Email="">
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@"Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt");
            }
        ]]>
      </Local>
    </Job>
    <Job Name="Create_RIndex" Custodian="" Email="">
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference="RDBMS_DBCORE.dll" Type="dfs"/>
      <Add Reference="System.Data.dll" Type="system"/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
            public virtual void Local()
            {

                //If highly skewed patterns in data, e.g. user generated data, 
                //shuffle the physical location of data chunks while keeping the data sorted:
	   //Shell(@”Qizmt shuffle <source> <target>”);

                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

                using (DbConnection conn = fact.CreateConnection())
                {
                    //Batch SQL: Cast sorted rectangular binary data as a SQL table (cheap operation)
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE TABLE tblRIndexBasicStressTest (id INT, rid INT)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "INSERT INTO tblRIndexBasicStressTest bind 'dfs://QizmtSQL-RIndexBasicStressTest_InputTable.bin'";
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                   //Batch SQL: Create rindex on already sorted SQL table  (cheap operation)
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "CREATE RINDEX tblRIndexBasicStressTestIndex FROM tblRIndexBasicStressTest PINMEMORYHASH ON id";
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        ]]>
      </Local>
    </Job>
    <Job Name="rindexexample_processdata" Custodian="" Email="">
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://RDBMS_Table_tblRIndexBasicStressTest@nInt,nInt</DFSInput>
        <DFSOutput>dfs://QizmtSQL-RIndexBasicStressTest_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference="RDBMS_DBCORE.dll" Type="dfs"/>
      <Add Reference="System.Data.dll" Type="system"/>
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

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");
            DbConnection conn = null;
            DbCommand cmd = null;

            int batchsize = 1900; 

            int reduceittrs = 0;
            public void ReduceInitialize()
            {
                whrsb.Append("RSELECT * FROM tblRIndexBasicStressTestIndex WHERE KEY = -1");

                if (Qizmt_ExecArgs.Length == 4)
                {
                    batchsize = Int32.Parse(Qizmt_ExecArgs[3]);
                }

                conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = " + string.Join(",", StaticGlobals.Qizmt_Hosts) + "; rindex=pooled; retrymaxcount = 100; retrysleep = 5000";
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

                    for (int i = 1; i < values.Length; i++)
                    {
                        recordset rval = recordset.Prepare(values[i].Value);
                        int asso = rval.GetInt();
                        //Append another OR statement to SQL string
                        whrsb.Append(" OR KEY = ").Append(asso.ToString());
                    }
                }

                ++reduceittrs;
                if (reduceittrs >= batchsize || StaticGlobals.Qizmt_Last == true)
                {
                    string whr = whrsb.ToString();
                    if (whr.Length > 0)
                    {
                        cmd.CommandText = whr;
                        //Download batch of tuples from distributed memory into local memory for this reducer
                        using (DbDataReader reader = cmd.ExecuteReader())
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
                    //Start new SQL string for selecting the next batch into local memory from distributed memory
                    whrsb.Append("RSELECT * FROM tblRIndexBasicStressTestIndex WHERE KEY = -1");
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
    <Job Name="Cleanup_Data" Custodian="" Email="">
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Add Reference="System.Data.dll" Type="system"/>     
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Local>
        <![CDATA[
             public virtual void Local()
            {
                Shell(@"Qizmt del QizmtSQL-RIndexBasicStressTest_Input*.txt");
                Shell(@"Qizmt del QizmtSQL-RIndexBasicStressTest_Output.txt");

                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = "Data Source = localhost";
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "drop table tblRIndexBasicStressTest";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "drop rindex tblRIndexBasicStressTestIndex";
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


```