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
        public static void FaultTolerantExecution()
        {
            if (!MySpace.DataMining.AELight.FTTest.enabled)
            {
                throw new Exception("TESTFAULTTOLERANT is not #defined.  Need Qizmt build with all #define TESTFAULTTOLERANT uncommented.");
            }

            string guid = "A275169D14B34df48229FC3F43A0AA31";
            string tblUsers = "users_" + guid;
            string tblPageviews = "pageviews_" + guid;
            string tblPageToNames = "pagetonames_" + guid;
            string controlfile = @"\\" + MySpace.DataMining.AELight.Surrogate.MasterHost + @"\c$\temp\"
                   + MySpace.DataMining.AELight.FTTest.controlfilename;

            string tempdir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) +
                @"\" + Guid.NewGuid().ToString();            

            System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");
            DbConnection conn = fact.CreateConnection();
            conn.ConnectionString = "Data Source = localhost; mr bypass=10; fault tolerant execution=enabled";
            DbCommand cmd = conn.CreateCommand();
                        
            try
            {                
                {
                    System.IO.Directory.CreateDirectory(tempdir);

                    #region genPageViews
                    System.IO.File.WriteAllText(tempdir + @"\genPageViews_873200A2-EFD4-4136-9209-A807CF8BA3C2.xml", 
@"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del tblPageViews_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.bin`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://tblPageViews_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.bin@8</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                List<byte> buf = new List<byte>(24);
                
                {
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(100);
                    rs.PutInt(1);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
                {
                    buf.Clear();
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(101);
                    rs.PutInt(1);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
                {
                    buf.Clear();
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(300);
                    rs.PutInt(3);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
                {
                    buf.Clear();
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(301);
                    rs.PutInt(3);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
                {
                    buf.Clear();
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(302);
                    rs.PutInt(3);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
                {
                    buf.Clear();
                    recordset rs  = recordset.Prepare();
                    rs.PutInt(400);
                    rs.PutInt(4);
                    rs.ToByteSlice().AppendTo(buf);
                    dfsoutput.WriteRecord(buf);
                }
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                    #endregion

                    #region genPageToNames
                    System.IO.File.WriteAllText(tempdir + @"\genPageToNames_873200A2-EFD4-4136-9209-A807CF8BA3C2.xml",
@"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del tblPageToNames_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://tblPageToNames_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`90,a`);
                dfsoutput.WriteLine(`91,b`);
                dfsoutput.WriteLine(`92,c`);
                dfsoutput.WriteLine(`93,d`);
                dfsoutput.WriteLine(`94,e`);
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                    #endregion

                    Exec.Shell("qizmt importdir \"" + tempdir + "\"");
                    Exec.Shell("qizmt exec genPageViews_873200A2-EFD4-4136-9209-A807CF8BA3C2.xml");
                    Exec.Shell("qizmt exec genPageToNames_873200A2-EFD4-4136-9209-A807CF8BA3C2.xml");
                }                

                conn.Open();

                #region createtables
                {
                    Console.WriteLine("Creating tables...");

                    try
                    {
                        cmd.CommandText = "drop table " + tblUsers;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "drop table " + tblPageviews;
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "drop table " + tblPageToNames;
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush                        
                    }
                    catch
                    {
                    }

                    conn.Open();
                    cmd.CommandText = "create table " + tblUsers + " (userid int, username char(50))";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "insert into " + tblUsers + " values (1, 'john')";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "insert into " + tblUsers + " values (2, 'mary')";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "insert into " + tblUsers + " values (3, 'joe')";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "create table " + tblPageviews + " (pageid int, uid int)";
                    cmd.ExecuteNonQuery();                   

                    cmd.CommandText = "create table " + tblPageToNames + " (pageid int, username char(50))";
                    cmd.ExecuteNonQuery();                    

                    conn.Close(); //flush      
                    conn.Open();
                }
                #endregion

                #region testIMPORT
                {
                    string phase = "exchangeowned";
                    Console.WriteLine("Testing IMPORT in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "INSERT INTO " + tblPageviews + " IMPORT 'tblPageViews_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.bin'";
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    conn.Open();

                    System.IO.File.WriteAllText(controlfile, "");
                    cmd.CommandText = "select * from " + tblPageviews;
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int pageid = reader.GetInt32(0);
                        int userid = reader.GetInt32(1);
                        if (!(pageid == 100 && userid == 1) && !(pageid== 101 && userid == 1) &&
                            !(pageid == 300 && userid == 3) && !(pageid == 301 && userid == 3) && !(pageid == 302 && userid == 3) &&
                            !(pageid == 400 && userid == 4))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 6)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testIMPORTLINES
                {
                    string phase = "reduce";
                    Console.WriteLine("Testing IMPORTLINES in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "INSERT INTO " + tblPageToNames + " IMPORTLINES 'tblPageToNames_873200A2-EFD4-4136-9209-A807CF8BA3C2_Output.txt'";
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    conn.Open();

                    System.IO.File.WriteAllText(controlfile, "");
                    cmd.CommandText = "select * from " + tblPageToNames;
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int pageid = reader.GetInt32(0);
                        string name = reader.GetString(1);
                        if (!(pageid == 90 && name == "a") && !(pageid == 91 && name == "b") &&
                            !(pageid == 92 && name == "c") && !(pageid == 93 && name == "d") && 
                            !(pageid == 94 && name == "e"))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 5)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testSELECT
                {
                    string phase = "map";
                    Console.WriteLine("Testing SELECT in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "select * from " + tblUsers + " where userid = 2";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        if (reader.GetString(1) != "mary")
                        {
                            throw new Exception("Unexpected value for field username");
                        }
                    }
                    reader.Close();
                    if (count != 1)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testSELECT
                {
                    string phase = "exchangeremote";
                    Console.WriteLine("Testing SELECT in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "select * from " + tblUsers + " where userid > 0";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        string username = reader.GetString(1);
                        if (username != "mary" && username != "john" && username != "joe")
                        {
                            throw new Exception("Unexpected value for field username");
                        }
                    }
                    reader.Close();
                    if (count != 3)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testSELECT
                {
                    string phase = "exchangeowned";
                    Console.WriteLine("Testing SELECT in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "select username from " + tblUsers + " where userid=3";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        if (reader.GetString(0) != "joe")
                        {
                            throw new Exception("Unexpected value for field username");
                        }
                    }
                    reader.Close();
                    if (count != 1)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testSELECT
                {
                    string phase = "sort";
                    Console.WriteLine("Testing SELECT in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "select username from " + tblUsers + " where abs(userid)=3";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        if (reader.GetString(0) != "joe")
                        {
                            throw new Exception("Unexpected value for field username");
                        }
                    }
                    reader.Close();
                    if (count != 1)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testSELECT
                {
                    Console.WriteLine("Testing SELECT in FTE mode with no failure...");
                    System.IO.File.WriteAllText(controlfile, ""); //Do not simulate failure since FTE is not enabled for SELECT func(column)

                    cmd.CommandText = "select max(pageid), uid from " + tblPageviews + " group by uid";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int pageid = reader.GetInt32(0);
                        int uid = reader.GetInt32(1);
                        if (!(pageid == 101 && uid == 1) && !(pageid == 302 && uid == 3) && !(pageid == 400 && uid == 4))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 3)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testINNERJOIN
                {
                    string phase = "reduce";
                    Console.WriteLine("Testing INNER JOIN in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "SELECT * FROM " + tblPageviews + " inner join " + tblUsers + " on uid=userid";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int pageid = reader.GetInt32(0);
                        string username = reader.GetString(3);
                        if (!(pageid == 100 && username == "john") && !(pageid == 101 && username == "john") &&
                            !(pageid == 300 && username == "joe") && !(pageid == 301 && username == "joe") &&
                            !(pageid == 302 && username == "joe"))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 5)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testINNERJOIN
                {
                    string phase = "sort";
                    Console.WriteLine("Testing INNER JOIN in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    long jid = GetJobID("RDBMS_JoinOn.DBCORE");
                    if (jid != -1)
                    {
                        throw new Exception("Expected jobid = -1");
                    }

                    List<string> errors = new List<string>();

                    System.Threading.Thread tm = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            while (jid == -1)
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                jid = GetJobID("RDBMS_JoinOn.DBCORE");
                            }
                            Console.WriteLine("jid={0}", jid);

                            for (; ; )
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                if (GetJobOutputIndexOf(jid, "Replicating") > -1)
                                {
                                    //Can clear control file for the next job run.
                                    ClearControlFile(controlfile);
                                    Console.WriteLine("control file cleared");
                                    break;
                                }
                            }
                        }));

                    tm.IsBackground = true;
                    tm.Start();

                    System.Threading.Thread ts = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            cmd.CommandText = "select max(pageid), username from " + tblPageviews +
                                                    " inner join " + tblUsers + " on uid = userid group by username";
                            DbDataReader reader = cmd.ExecuteReader();
                            int count = 0;
                            while (reader.Read())
                            {
                                count++;
                                int pageid = reader.GetInt32(0);
                                string username = reader.GetString(1);
                                if (!(pageid == 101 && username == "john") && !(pageid == 302 && username == "joe"))
                                {
                                    errors.Add("Unexpected value");
                                    throw new Exception("Unexpected value");
                                }
                            }
                            reader.Close();
                            if (count != 2)
                            {
                                errors.Add("Unexpected rows returned");
                                throw new Exception("Unexpected rows returned");
                            }
                        }));

                    ts.IsBackground = true;
                    ts.Start();

                    tm.Join();
                    ts.Join();

                    if (errors.Count > 0)
                    {
                        StringBuilder sb = new StringBuilder();
                        foreach (string err in errors)
                        {
                            sb.Append(err);
                            sb.Append(Environment.NewLine);
                        }
                        throw new Exception("Error during job run:" + sb.ToString());
                    }

                    Console.WriteLine("Done");
                }
                #endregion

                #region testLEFTJOIN
                {
                    string phase = "replication";
                    Console.WriteLine("Testing LEFT OUTER JOIN in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "SELECT * FROM " + tblPageviews + " left outer join " + tblUsers + " on uid=userid";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int pageid = (int)reader[0];
                        string username = DBNull.Value.Equals(reader[3]) ? null : (string)reader[3];
                        if (!(pageid == 100 && username == "john") && !(pageid == 101 && username == "john") &&
                            !(pageid == 300 && username == "joe") && !(pageid == 301 && username == "joe") &&
                            !(pageid == 302 && username == "joe") && !(pageid == 400 && username == null))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 6)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testRIGHTJOIN
                {
                    string phase = "map";
                    Console.WriteLine("Testing RIGHT OUTER JOIN in FTE mode with failure at {0}...", phase);
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                    {
                        w.WriteLine("{1}:" + phase);
                    }

                    cmd.CommandText = "SELECT * FROM " + tblPageviews + " right outer join " + tblUsers + " on uid=userid";
                    DbDataReader reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        count++;
                        int userid = (int)reader[2];
                        int pageid = DBNull.Value.Equals(reader[0]) ? -1 : (int)reader[0];
                        if (!(userid == 1 && pageid == 100) && !(userid == 1 && pageid == 101) &&
                            !(userid == 3 && pageid == 300) && !(userid == 3 && pageid == 301) &&
                            !(userid == 3 && pageid == 302) && !(userid == 2 && pageid == -1))
                        {
                            throw new Exception("Unexpected value");
                        }
                    }
                    reader.Close();
                    if (count != 6)
                    {
                        throw new Exception("Unexpected rows returned");
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testDELETE
                {
                    {
                        string phase = "exchangeremote";
                        Console.WriteLine("Testing DELETE in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        cmd.CommandText = "DELETE FROM " + tblPageToNames + " where username='b'";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        string phase = "exchangeowned";
                        Console.WriteLine("Testing DELETE in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        cmd.CommandText = "DELETE FROM " + tblPageToNames + " where abs(pageid)= 93";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        string phase = "sort";
                        Console.WriteLine("Testing DELETE in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        cmd.CommandText = "delete from " + tblPageToNames + " where mod(pageid, 2) = 0";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        System.IO.File.WriteAllText(controlfile, "");
                        cmd.CommandText = "select * from " + tblPageToNames;
                        DbDataReader reader = cmd.ExecuteReader();
                        int count = 0;
                        while (reader.Read())
                        {
                            count++;
                        }
                        reader.Close();
                        if (count != 0)
                        {
                            throw new Exception("Unexpected number of rows returned");
                        }
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testINSERT
                {
                    {
                        string phase = "reduce";
                        Console.WriteLine("Testing INSERT in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        cmd.CommandText = "insert into " + tblPageToNames +
                            " select userid, username from " + tblUsers + " where userid > 2";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        Console.WriteLine("Testing INSERT in FTE mode with no failure");
                        System.IO.File.WriteAllText(controlfile, "");

                        cmd.CommandText = "insert into " + tblPageToNames +
                            " select abs(userid), username from " + tblUsers + " where userid = 2";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        Console.WriteLine("Testing INSERT in FTE mode with no failure");

                        cmd.CommandText = "insert into " + tblPageToNames +
                            " select userid, username from " + tblUsers +
                            " where userid = 1 order by userid";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                        Console.WriteLine("+");
                    }
                    {
                        System.IO.File.WriteAllText(controlfile, "");
                        cmd.CommandText = "select * from " + tblPageToNames;
                        DbDataReader reader = cmd.ExecuteReader();
                        int count = 0;
                        while (reader.Read())
                        {
                            count++;
                            int pageid = reader.GetInt32(0);
                            string username = reader.GetString(1);
                            if (!(pageid == 1 && username == "john") && !(pageid == 2 && username == "mary") &&
                                !(pageid == 3 && username == "joe"))
                            {
                                throw new Exception("Unexpected value");
                            }
                        }
                        reader.Close();
                        if (count != 3)
                        {
                            throw new Exception("Unexpected number of rows returned");
                        }
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region testINSERT
                {
                    //prepare
                    {
                        System.IO.File.WriteAllText(controlfile, "");
                        cmd.CommandText = "delete from " + tblPageToNames + " where pageid > 0";
                        cmd.ExecuteNonQuery();
                        conn.Close(); //flush
                        conn.Open();
                    }

                    {
                        string phase = "exchangeowned";
                        Console.WriteLine("Testing INSERT in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        long jid = GetJobID("RDBMS_JoinOn.DBCORE");
                        if (jid != -1)
                        {
                            throw new Exception("Expected jobid = -1");
                        }

                        System.Threading.Thread tm = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            while (jid == -1)
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                jid = GetJobID("RDBMS_JoinOn.DBCORE");
                            }
                            Console.WriteLine("jid={0}", jid);

                            for (; ; )
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                if (GetJobOutputIndexOf(jid, "Replicating") > -1)
                                {
                                    //Can clear control file for the next job run.
                                    ClearControlFile(controlfile);
                                    Console.WriteLine("control file cleared");
                                    break;
                                }
                            }
                        }));

                        tm.IsBackground = true;
                        tm.Start();

                        System.Threading.Thread ts = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            cmd.CommandText = "insert into " + tblPageToNames +
                                " select abs(pageid), username from " + tblPageviews +
                                " inner join " + tblUsers + " on uid = userid where abs(userid) = 1";
                            cmd.ExecuteNonQuery();
                            conn.Close(); //flush
                            conn.Open();
                        }));

                        ts.IsBackground = true;
                        ts.Start();

                        tm.Join();
                        ts.Join();
                        Console.WriteLine("+");
                    }
                    {
                        string phase = "exchangeremote";
                        Console.WriteLine("Testing INSERT in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        long jid = GetJobID("RDBMS_JoinOn.DBCORE");
                        if (jid != -1)
                        {
                            throw new Exception("Expected jobid = -1");
                        }

                        System.Threading.Thread tm = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            while (jid == -1)
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                jid = GetJobID("RDBMS_JoinOn.DBCORE");
                            }
                            Console.WriteLine("jid={0}", jid);

                            for (; ; )
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                if (GetJobOutputIndexOf(jid, "Replicating") > -1)
                                {
                                    //Can clear control file for the next job run.
                                    ClearControlFile(controlfile);
                                    Console.WriteLine("control file cleared");
                                    break;
                                }
                            }
                        }));

                        tm.IsBackground = true;
                        tm.Start();

                        System.Threading.Thread ts = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            cmd.CommandText = "insert into " + tblPageToNames +
                                " select abs(pageid), username from " + tblPageviews +
                                " inner join " + tblUsers +
                                " on uid = userid where abs(userid) = 3 order by pageid";
                            cmd.ExecuteNonQuery();
                            conn.Close(); //flush
                            conn.Open();
                        }));

                        ts.IsBackground = true;
                        ts.Start();

                        tm.Join();
                        ts.Join();
                        Console.WriteLine("+");
                    }
                    {
                        string phase = "sort";
                        Console.WriteLine("Testing INSERT in FTE mode with failure at {0}...", phase);
                        using (System.IO.StreamWriter w = new System.IO.StreamWriter(controlfile))
                        {
                            w.WriteLine("{1}:" + phase);
                        }

                        long jid = GetJobID("RDBMS_JoinOn.DBCORE");
                        if (jid != -1)
                        {
                            throw new Exception("Expected jobid = -1");
                        }

                        System.Threading.Thread tm = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            while (jid == -1)
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                jid = GetJobID("RDBMS_JoinOn.DBCORE");
                            }
                            Console.WriteLine("jid={0}", jid);

                            for (; ; )
                            {
                                Console.Write(".");
                                System.Threading.Thread.Sleep(1000);
                                if (GetJobOutputIndexOf(jid, "Replicating") > -1)
                                {
                                    //Can clear control file for the next job run.
                                    ClearControlFile(controlfile);
                                    Console.WriteLine("control file cleared");
                                    break;
                                }
                            }
                        }));

                        tm.IsBackground = true;
                        tm.Start();

                        System.Threading.Thread ts = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                        {
                            cmd.CommandText = "insert into " + tblPageToNames +
                                " select max(userid), username from " + tblPageviews +
                                " inner join " + tblUsers +
                                " on uid = userid group by username";
                            cmd.ExecuteNonQuery();
                            conn.Close(); //flush
                            conn.Open();
                        }));

                        ts.IsBackground = true;
                        ts.Start();

                        tm.Join();
                        ts.Join();
                        Console.WriteLine("+");
                    }

                    {
                        System.IO.File.WriteAllText(controlfile, "");
                        cmd.CommandText = "select * from " + tblPageToNames;
                        DbDataReader reader = cmd.ExecuteReader();
                        int count = 0;
                        while (reader.Read())
                        {
                            count++;
                            int pageid = reader.GetInt32(0);
                            string username = reader.GetString(1);
                            if (!(pageid == 100 && username == "john") && !(pageid == 101 && username == "john") &&
                                !(pageid == 300 && username == "joe") && !(pageid == 301 && username == "joe") &&
                                !(pageid == 302 && username == "joe") && !(pageid == 1 && username == "john") &&
                                !(pageid == 3 && username == "joe"))
                            {
                                throw new Exception("Unexpected value");
                            }
                        }
                        reader.Close();

                        if (count != 7)
                        {
                            throw new Exception("Unexpected number of rows returned");
                        }
                    }
                    Console.WriteLine("Done");
                }
                #endregion

                #region cleanup
                try
                {
                    cmd.CommandText = "drop table " + tblUsers;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "drop table " + tblPageviews;
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "drop table " + tblPageToNames;
                    cmd.ExecuteNonQuery();
                }
                catch
                {
                }
                #endregion
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    try
                    {
                        conn.Close();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Closing error: " + e.ToString());
                    }    
                }

                System.IO.Directory.Delete(tempdir, true);
                Exec.Shell("qizmt del *873200A2-EFD4-4136-9209-A807CF8BA3C2*");
                System.IO.File.Delete(controlfile);                
            }
        }

        static long GetJobID(string jobname)
        {
            string[] lines = Exec.Shell("qizmt ps").Split('\r');
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i].Trim();
                if (line.IndexOf(jobname, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    int del = line.IndexOf(' ');
                    long jobid = long.Parse(line.Substring(0, del));
                    return jobid;
                }
            }
            return -1;
        }

        static int GetJobOutputIndexOf(long jobid, string token)
        {
            string output = Exec.Shell("qizmt viewjob " + jobid.ToString());
            return output.IndexOf(token, StringComparison.OrdinalIgnoreCase);
        }

        static void ClearControlFile(string fn)
        {
            int triesremain = 30;
            for (; ; )
            {
                try
                {
                    System.IO.File.WriteAllText(fn, "");
                    break;
                }
                catch(Exception e)
                {
                    if (triesremain-- <= 0)
                    {
                        throw new Exception("Cannot clear control file.  Error=" + e.ToString());
                    }
                    System.Threading.Thread.Sleep(3000);
                }
            }
        }
    }
}
