using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        public static void RIndexReplication()
        {
            {
                int replicationFactor = 0;
                string[] lines = Exec.Shell("Qizmt replicationview").Split('\n');
                for (int i = 0; i < lines.Length; i++)
                {
                    string line = lines[i].Trim();
                    if (line.Length > 0)
                    {
                        int del = line.LastIndexOf(' ');
                        replicationFactor = Int32.Parse(line.Substring(del + 1));
                        if (replicationFactor > 0)
                        {
                            break;
                        }
                    }
                }
                if (replicationFactor < 2)
                {
                    throw new Exception("Cannot run RIndexReplication regression test when replication factor is less than 2.");
                }
            }

            string guid = Guid.NewGuid().ToString().Replace("-", "");
            string tablename = "rselect_test_" + guid;
            string indexname = guid + "apple";
            string indexname_order = guid + "order";

            string[] allhosts = null;
            string qizmtrootdir = "";
            {
                string[] installs = Exec.Shell("Qizmt slaveinstalls").Trim()
                    .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                allhosts = new string[installs.Length];

                for (int ip = 0; ip < installs.Length; ip++)
                {
                    string[] parts = installs[ip].Split(' ');
                    string installpath = parts[1];
                    int del = installpath.IndexOf(@"\", 2);
                    qizmtrootdir = installpath.Substring(del + 1);
                    allhosts[ip] = parts[0];
                }
            }

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            {
                Console.WriteLine("Creating data, table, and index...");

                string job = (@"<SourceCode>
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
               Shell(@`Qizmt del data_Input" + guid + @"*`);
                Shell(@`Qizmt del data_Output" + guid + @"*`);   
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);

                using (DbConnection conn = fact.CreateConnection())
                {
                    try
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table " + tablename + @"`;
                        cmd.ExecuteNonQuery();  
                        cmd.CommandText = `drop rindex " + indexname + @"`;
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
    <Job Name=`data_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://data_Input" + guid + @".txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`1`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://data_Input" + guid + @".txt</DFSInput>
        <DFSOutput>dfs://data_Output" + guid + @"1.bin@nInt,nInt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(4);                
                output.Add(rkey.ToByteSlice(), ByteSlice.Prepare());
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(5);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
               {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(5);
                    rkey.PutInt(2);                    
                    output.Add(rkey.ToByteSlice());      
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://data_Input" + guid + @".txt</DFSInput>
        <DFSOutput>dfs://data_Output" + guid + @"2.bin@nInt,nInt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(4);                
                output.Add(rkey.ToByteSlice(), ByteSlice.Prepare());
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(10);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
               {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(10);
                    rkey.PutInt(2);                    
                    output.Add(rkey.ToByteSlice());      
                }
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(14);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://data_Input" + guid + @".txt</DFSInput>
        <DFSOutput>dfs://data_Output" + guid + @"3.bin@nInt,nInt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(4);                
                output.Add(rkey.ToByteSlice(), ByteSlice.Prepare());
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(21);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
               {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(25);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(80);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(80);
                    rkey.PutInt(2);                    
                    output.Add(rkey.ToByteSlice());      
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`data` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nInt</KeyLength>
        <DFSInput>dfs://data_Input" + guid + @".txt</DFSInput>
        <DFSOutput>dfs://data_Output" + guid + @"4.bin@nInt,nInt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {                
                DbRecordset rkey = DbRecordset.Prepare();
                rkey.PutInt(4);                
                output.Add(rkey.ToByteSlice(), ByteSlice.Prepare());
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(80);
                    rkey.PutInt(3);                    
                    output.Add(rkey.ToByteSlice());      
                }
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(80);
                    rkey.PutInt(4);                    
                    output.Add(rkey.ToByteSlice());      
                }
                {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(90);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
               {
                    DbRecordset rkey = DbRecordset.Prepare();
                    rkey.PutInt(102);
                    rkey.PutInt(1);                    
                    output.Add(rkey.ToByteSlice());      
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Prepare_tblFriends_Table_Create_RIndex` Custodian=`` Email=``>
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
                Shell(@`qizmt combine data_Output" + guid + @"1.bin data_Output" + guid + @"2.bin data_Output" + guid + @"3.bin data_Output" + guid + @"4.bin +data_Output" + guid + @".bin`);
                
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);
                
                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create table " + tablename + @" (id int, rid int)`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into " + tablename + @" bind 'dfs://data_Output" + guid + @".bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex " + indexname + @" from " + tablename + @" pinmemoryHASH ON id`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"');
                string tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\" + Guid.NewGuid().ToString().Replace("-", "");
                if (System.IO.Directory.Exists(tempdir))
                {
                    System.IO.Directory.Delete(tempdir, true);
                }
                System.IO.Directory.CreateDirectory(tempdir);
                string tempjobname = Guid.NewGuid().ToString();
                System.IO.File.WriteAllText(tempdir + @"\" + tempjobname, job);

                Exec.Shell("Qizmt importdir \"" + tempdir + "\"");

                Exec.Shell("dspace exec " + tempjobname);

                //Clean up
                Exec.Shell(@"Qizmt del " + tempjobname);
                System.IO.Directory.Delete(tempdir, true);
            }

            {
                Console.WriteLine("RInsert/RDeleting rindex...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = " + string.Join(",", allhosts) + "; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText =
                    "rdelete from " + indexname + " where key = 80 and rid=1\0" +
                    "rdelete from " + indexname + " where key = 80 and rid=2\0" +
                    "rdelete from " + indexname + " where key = 80 and rid=3\0" +
                    "rdelete from " + indexname + " where key = 80 and rid=4\0" +
                    "rinsert into " + indexname + " values(10, 3) where key = 10\0" +
                    "rinsert into " + indexname + " values(10, 19) where key = 10\0" +
                    "rdelete from " + indexname + " where key = 10 and rid=19\0" +
                    "rinsert into " + indexname + " values(20, 1) where key = 20\0" +
                    "rinsert into " + indexname + " values(30, 1) where key = 30\0" +
                    "rinsert into " + indexname + " values(30, 2) where key = 30\0" +
                     "rinsert into " + indexname + " values(9, 1) where key = 9\0" +
                    "rinsert into " + indexname + " values(9, 2) where key = 9\0" +
                    "rinsert into " + indexname + " values(99, 1) where key = 99\0" +
                    "rdelete from " + indexname + " where key = 99 and rid=4444444\0" +   //deleting existing key but secondary column value doesn't match
                    "rdelete from " + indexname + " where key = 100000 and rid=4444444\0" +   //deleting non-existing key.
                     "rdelete from " + indexname + " where key = 5 and rid=2\0" +
                    "rinsert into " + indexname + " values(2, 1) where key = 2\0" +
                    "rinsert into " + indexname + " values(2, 19) where key = 2\0" +
                    "rinsert into " + indexname + " values(2, 2) where key = 2\0" +
                    "rdelete from " + indexname + " where key = 2 and rid=19\0";
                cmd.ExecuteNonQuery();
                conn.Close();
            }

            Dictionary<string, string> renamed = new Dictionary<string, string>();
            {
                Console.WriteLine("Invalidating chunks...");

                string bulkget = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\" + Guid.NewGuid().ToString().Replace("-", "");
                Exec.Shell("Qizmt bulkget " + bulkget + " RDBMS_Table_" + tablename);
                string[] lines = System.IO.File.ReadAllLines(bulkget);
                System.IO.File.Delete(bulkget);

                foreach (string line in lines)
                {
                    string[] parts = line.Split(' ');
                    string firsthost = parts[0].Split(';')[0];
                    string chunkname = parts[1];
                    string oldpath = @"\\" + firsthost + @"\" + qizmtrootdir + @"\" + chunkname;
                    string newpath = oldpath + "_regtest";
                    System.IO.File.Move(oldpath, newpath);
                    renamed.Add(oldpath, newpath);
                }
            }

            {
                Console.WriteLine("RSelecting from rindex after invalidating chunks, should fail over...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = " + string.Join(",", allhosts) + "; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "rselect * from " + indexname + " where key=80 or key = 2 or key = 5 or key=9 or key=10 or key=14 or key=20 or key=21 or key=25 or key=30 or key=90 or key=99 or key=102";
                DbDataReader reader = cmd.ExecuteReader();
                int cnt = 0;
                List<KeyValuePair<int, int>> results = new List<KeyValuePair<int, int>>();
                while (reader.Read())
                {
                    int x = reader.GetInt32(0);
                    int y = reader.GetInt32(1);
                    KeyValuePair<int, int> row = new KeyValuePair<int, int>(x, y);
                    results.Add(row);
                }
                reader.Close();
                conn.Close();

                results.Sort(delegate(KeyValuePair<int, int> x, KeyValuePair<int, int> y)
                {
                    if (x.Key != y.Key)
                    {
                        return x.Key.CompareTo(y.Key);
                    }
                    else
                    {
                        return x.Value.CompareTo(y.Value);
                    }
                });

                List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>();
                expected.Add(new KeyValuePair<int, int>(2, 1));
                expected.Add(new KeyValuePair<int, int>(2, 2));
                expected.Add(new KeyValuePair<int, int>(5, 1));
                expected.Add(new KeyValuePair<int, int>(9, 1));
                expected.Add(new KeyValuePair<int, int>(9, 2));
                expected.Add(new KeyValuePair<int, int>(10, 1));
                expected.Add(new KeyValuePair<int, int>(10, 2));
                expected.Add(new KeyValuePair<int, int>(10, 3));
                expected.Add(new KeyValuePair<int, int>(14, 1));
                expected.Add(new KeyValuePair<int, int>(20, 1));
                expected.Add(new KeyValuePair<int, int>(21, 1));
                expected.Add(new KeyValuePair<int, int>(25, 1));
                expected.Add(new KeyValuePair<int, int>(30, 1));
                expected.Add(new KeyValuePair<int, int>(30, 2));
                expected.Add(new KeyValuePair<int, int>(90, 1));
                expected.Add(new KeyValuePair<int, int>(99, 1));
                expected.Add(new KeyValuePair<int, int>(102, 1));

                if (results.Count != expected.Count)
                {
                    throw new Exception("Expected " + expected.Count.ToString() + " rows to be returned.  But received " + results.Count.ToString() + " rows instead.");
                }

                for (int i = 0; i < results.Count; i++)
                {
                    if (results[i].Key != expected[i].Key || results[i].Value != expected[i].Value)
                    {
                        throw new Exception("Row returned is different from expected results.  Received row:(" + results[i].Key.ToString() + "," + results[i].Value.ToString() + ").  Expected row:(" + expected[i].Key.ToString() + "," + expected[i].Value.ToString() + ")");
                    }
                }
            }

            {
                Console.WriteLine("Fixing chunks...");
                foreach (KeyValuePair<string, string> pair in renamed)
                {
                    System.IO.File.Move(pair.Value, pair.Key);
                }
            }

            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablename;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname;
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
