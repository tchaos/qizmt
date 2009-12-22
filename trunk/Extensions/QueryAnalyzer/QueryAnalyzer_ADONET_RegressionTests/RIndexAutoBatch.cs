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
        public static void RIndexAutoBatch()
        {
            string tablenameSorted = "tblTest9763995E4A51419eA8DFC7FCA06FCC71";
            string indexname_keeporder = "indOrder9763995E4A51419eA8DFC7FCA06FCC71";
            string indexname = "ind9763995E4A51419eA8DFC7FCA06FCC71";      
            List<long> testkeys = new List<long>();
            testkeys.Add(0); //lower bound
            testkeys.Add(9); //upper bound
            for (long i = 1; i < 5; i++)  //in between
            {
                testkeys.Add(i);
            }

            {
                Console.WriteLine("Creating data...");

                string job = @"
<SourceCode>
  <Jobs>
    <Job Name=`Cleanup` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
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
                Shell(@`Qizmt del RegressionTest_RIndexAutoBatch_Input.txt`);
                Shell(@`Qizmt del RegressionTest_RIndexAutoBatch_Input.bin`);
                try
                {
                    System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);                
                    using (DbConnection conn = fact.CreateConnection())
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblTest9763995E4A51419eA8DFC7FCA06FCC71`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex ind9763995E4A51419eA8DFC7FCA06FCC71`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex indOrder9763995E4A51419eA8DFC7FCA06FCC71`;
                        cmd.ExecuteNonQuery();
                        conn.Close();
                    }
                }
                catch
                {
                    
                }                
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`createdata` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://RegressionTest_RIndexAutoBatch_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Add Reference=`RDBMS_DBCORE.dll` Type=`dfs`/>
      <Add Reference=`System.Data.dll` Type=`system`/>
      <Using>RDBMS_DBCORE</Using>      
      <Using>System.Data</Using>
      <Using>System.Data.Common</Using>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`x`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`createdata` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://RegressionTest_RIndexAutoBatch_Input.txt</DFSInput>
        <DFSOutput>dfs://RegressionTest_RIndexAutoBatch_Input.bin@nInt,nLong,nchar(400)</DFSOutput>
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
                recordset rkey = recordset.Prepare();
                rkey.PutInt(1);
                output.Add(rkey, recordset.Prepare());
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                for(int i = 0; i < 10; i++)
                {
                    for(int s = 0; s < 64330; s++)
                    {
                        DbRecordset rout = DbRecordset.Prepare();
                        rout.PutInt(s);
                        rout.PutLong((long)i);
                        rout.PutString(`x`, 400);
                        output.Add(rout.ToByteSlice());
                    }       
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`createrindex` Custodian=`` Email=``>
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
                    cmd.CommandText = `create table tblTest9763995E4A51419eA8DFC7FCA06FCC71 (num1 int, num2 long, str char(400))`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblTest9763995E4A51419eA8DFC7FCA06FCC71 bind 'dfs://RegressionTest_RIndexAutoBatch_Input.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex indOrder9763995E4A51419eA8DFC7FCA06FCC71 from tblTest9763995E4A51419eA8DFC7FCA06FCC71 keepvalueorder ON num2`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `create rindex ind9763995E4A51419eA8DFC7FCA06FCC71 from tblTest9763995E4A51419eA8DFC7FCA06FCC71 ON num2`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }         
        }
        ]]>
      </Local>
    </Job>   
  </Jobs>
</SourceCode>
".Replace('`', '"');
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


            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");

            {
                Console.WriteLine("Querying data using RSELECT NOPOOL KEEPVALUEORDER...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();

                foreach (long key in testkeys)
                {
                    cmd.CommandText = "rselect * from " + indexname_keeporder.ToUpper() + " where key = " + key.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    int order = 0;
                    while (reader.Read())
                    {
                        int num1 = reader.GetInt32(0);
                        long num2 = reader.GetInt64(1);
                        string str = reader.GetString(2);
                        if (order++ != num1)
                        {
                            throw new Exception("Expected num1 to be: " + order.ToString() + ", but " + num1.ToString() + " is returned instead.");
                        }
                        if (num2 != key)
                        {
                            throw new Exception("Expected key to be: " + key.ToString() + ", but " + num2.ToString() + " is returned instead.");
                        }
                    }
                    reader.Close();

                    if (order != 64330)
                    {
                        throw new Exception("Expected 64330 rows to be returned for key: " + key.ToString() + ", but only " + order.ToString() + " is returned.");
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect NOPOOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT pooled and OR statements KEEPVALUEORDER...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                for (int ki = 0; ki < testkeys.Count; ki += 2)
                {
                    long key1 = testkeys[ki];
                    long key2 = testkeys[ki + 1];
                    cmd.CommandText = "rselect * from " + indexname_keeporder.ToUpper() + " where key = " + key1.ToString() + " OR key = " + key2.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    int order1 = 0;
                    int order2 = 0;
                    while (reader.Read())
                    {
                        int num1 = reader.GetInt32(0);
                        long num2 = reader.GetInt64(1);
                        string str = reader.GetString(2);
                        if (num2 == key1)
                        {
                            if (order1++ != num1)
                            {
                                throw new Exception("Expected num1 to be: " + order1.ToString() + ", but " + num1.ToString() + " is returned instead.");
                            }
                        }
                        else if (num2 == key2)
                        {
                            if (order2++ != num1)
                            {
                                throw new Exception("Expected num1 to be: " + order2.ToString() + ", but " + num1.ToString() + " is returned instead.");
                            }
                        }
                        else
                        {
                            throw new Exception("Unexpected key is returned: " + num2.ToString());
                        }
                    }
                    reader.Close();

                    if (order1 != 64330)
                    {
                        throw new Exception("Expected 64330 rows to be returned for key: " + key1.ToString() + ", but only " + order1.ToString() + " is returned.");
                    }
                    if (order2 != 64330)
                    {
                        throw new Exception("Expected 64330 rows to be returned for key: " + key2.ToString() + ", but only " + order2.ToString() + " is returned.");
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT pooled and OR statements...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                for (int ki = 0; ki < testkeys.Count; ki += 2)
                {
                    long key1 = testkeys[ki];
                    long key2 = testkeys[ki + 1];
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key1.ToString() + " OR key = " + key2.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    int count1 = 0;
                    int count2 = 0;
                    while (reader.Read())
                    {
                        int num1 = reader.GetInt32(0);
                        long num2 = reader.GetInt64(1);
                        string str = reader.GetString(2);
                        if (num2 == key1)
                        {
                            count1++;
                        }
                        else if (num2 == key2)
                        {
                            count2++;
                        }
                        else
                        {
                            throw new Exception("Unexpected key is returned: " + num2.ToString());
                        }
                    }
                    reader.Close();

                    if (count1 != 64330)
                    {
                        throw new Exception("Expected 64330 rows to be returned for key: " + key1.ToString() + ", but only " + count1.ToString() + " is returned.");
                    }
                    if (count2 != 64330)
                    {
                        throw new Exception("Expected 64330 rows to be returned for key: " + key2.ToString() + ", but only " + count2.ToString() + " is returned.");
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL completed.");
            }

            {
                testkeys.Clear();
                testkeys.Add(-1);
                testkeys.Add(90);

                Console.WriteLine("Testing out of bound keys KEEPVALUEORDER...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                for (int ki = 0; ki < testkeys.Count; ki += 2)
                {
                    long key1 = testkeys[ki];
                    long key2 = testkeys[ki + 1];
                    cmd.CommandText = "rselect * from " + indexname_keeporder.ToUpper() + " where key = " + key1.ToString() + " OR key = " + key2.ToString();
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                    }
                    reader.Close();

                    if (count > 0)
                    {
                        throw new Exception("Expected 0 rows in return.  But received " + count.ToString() + " rows.");
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect POOL completed.");
            }           

            {
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "drop table " + tablenameSorted;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname_keeporder.ToUpper();
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop rindex " + indexname.ToUpper();
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}