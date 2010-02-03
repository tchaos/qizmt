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
        public static void RIndexKeepValueOrder()
        {
            string tablenameSorted = "tblTestB026B487BC3D451eA055A4699EE5D36A";
            string indexname = "indB026B487BC3D451eA055A4699EE5D36A";           
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
                Shell(@`Qizmt del RegressionTest_RIndexKeepValueOrder_Input.txt`);
                Shell(@`Qizmt del RegressionTest_RIndexKeepValueOrder_Input.bin`);
                try
                {
                    System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory(`Qizmt_DataProvider`);                
                    using (DbConnection conn = fact.CreateConnection())
                    {
                        conn.ConnectionString = `Data Source = localhost`;
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = `drop table tblTestB026B487BC3D451eA055A4699EE5D36A`;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = `drop rindex indB026B487BC3D451eA055A4699EE5D36A`;
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
          <DFSWriter>dfs://RegressionTest_RIndexKeepValueOrder_Input.txt</DFSWriter>
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
        <DFSInput>dfs://RegressionTest_RIndexKeepValueOrder_Input.txt</DFSInput>
        <DFSOutput>dfs://RegressionTest_RIndexKeepValueOrder_Input.bin@nInt,nLong,nchar(400)</DFSOutput>
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
                    for(int s = 0; s < 50000; s++)
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
                    cmd.CommandText = `create table tblTestB026B487BC3D451eA055A4699EE5D36A (num1 int, num2 long, str char(400))`;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = `insert into tblTestB026B487BC3D451eA055A4699EE5D36A bind 'dfs://RegressionTest_RIndexKeepValueOrder_Input.bin'`;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }

                using (DbConnection conn = fact.CreateConnection())
                {
                    conn.ConnectionString = `Data Source = localhost`;
                    conn.Open();
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = `create rindex indB026B487BC3D451eA055A4699EE5D36A from tblTestB026B487BC3D451eA055A4699EE5D36A keepvalueorder ON num2`;
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
                Console.WriteLine("Querying data using RSELECT NOPOOL...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=nopool";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();

                foreach (long key in testkeys)
                {
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key.ToString();                  
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

                    if (order != 50000)
                    {
                        throw new Exception("Expected 50000 rows to be returned for key: " + key.ToString() + ", but only " + order.ToString() + " is returned.");
                    }
                }
                conn.Close();
                Console.WriteLine("RSelect NOPOOL completed.");
            }

            {
                Console.WriteLine("Querying data using RSELECT pooled and OR statements...");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = "Data Source = localhost; rindex=pooled";
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                for(int ki = 0; ki < testkeys.Count; ki+=2)
                {
                    long key1 = testkeys[ki];
                    long key2 = testkeys[ki + 1];
                    cmd.CommandText = "rselect * from " + indexname.ToUpper() + " where key = " + key1.ToString() + " OR key = " + key2.ToString();
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

                    if (order1 != 50000)
                    {
                        throw new Exception("Expected 50000 rows to be returned for key: " + key1.ToString() + ", but only " + order1.ToString() + " is returned.");
                    }
                    if (order2 != 50000)
                    {
                        throw new Exception("Expected 50000 rows to be returned for key: " + key2.ToString() + ", but only " + order2.ToString() + " is returned.");
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
                cmd.CommandText = "drop rindex " + indexname.ToUpper();
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
