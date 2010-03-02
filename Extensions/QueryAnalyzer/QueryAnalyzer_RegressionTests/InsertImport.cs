using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void InsertImport(string TableName, bool nocheck)
        {

            try
            {

                {
                    string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\dspace\regression_test_QueryAnalyzer_Import-" + Guid.NewGuid().ToString();
                    if (!System.IO.Directory.Exists(exectempdir))
                    {
                        System.IO.Directory.CreateDirectory(exectempdir);
                    }
                    Exec.Shell("dspace del RegressionTest_exec{5929813D-A928-4758-8EDE-F0130E142AEF}");
                    string execfp = exectempdir + @"\RegressionTest_exec{5929813D-A928-4758-8EDE-F0130E142AEF}";
                    System.IO.File.WriteAllText(execfp, (@"<SourceCode>
  <Jobs>
    <Job Name=`RegressionTest_GenQaData_Preprocessing`>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del RegressionTest_GenQaData{DF1D615D-A6DC-40d1-A1F6-39EF39E9C464}_Output`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`RegressionTest_GenQaData` Custodian=`Christopher Miller` Email=`cmiller@myspace-inc.com` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://RegressionTest_GenQaData{DF1D615D-A6DC-40d1-A1F6-39EF39E9C464}_Output@105</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {
              
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(-9999);
                  rs.PutString(`{48C7D626-9790-40eb-A8CA-073F514ED7D6}`);
                  rs.PutInt(42);
                  rs.PutLong(-0x9999999999);
                  rs.PutDouble(-0x9999999999);
                  List<byte> foo = new List<byte>();
                  rs.ToByteSlice().AppendTo(foo);
                  while(foo.Count < DSpace_OutputRecordLength)
                  {
                      foo.Add(0);
                  }
                  dfsoutput.WriteRecord(foo);
              }
              
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(33);
                  rs.PutString(`hello world`);
                  rs.PutInt(42);
                  rs.PutLong(0x9999999999);
                  rs.PutDouble(1.2);
                  List<byte> foo = new List<byte>();
                  rs.ToByteSlice().AppendTo(foo);
                  while(foo.Count < DSpace_OutputRecordLength)
                  {
                      foo.Add(0);
                  }
                  dfsoutput.WriteRecord(foo);
              }
              
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(234);
                  rs.PutString(`hi`);
                  rs.PutInt(42);
                  rs.PutLong(0x9999999999);
                  rs.PutDouble(1.3);
                  List<byte> foo = new List<byte>();
                  rs.ToByteSlice().AppendTo(foo);
                  while(foo.Count < DSpace_OutputRecordLength)
                  {
                      foo.Add(0);
                  }
                  dfsoutput.WriteRecord(foo);
              }
              
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(1000);
                  rs.PutString(`foo`);
                  rs.PutInt(42);
                  rs.PutLong(0x9999999999);
                  rs.PutDouble(1.4);
                  List<byte> foo = new List<byte>();
                  rs.ToByteSlice().AppendTo(foo);
                  while(foo.Count < DSpace_OutputRecordLength)
                  {
                      foo.Add(0);
                  }
                  dfsoutput.WriteRecord(foo);
              }
              
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(2);
                  rs.PutString(`Y`);
                  rs.PutInt(42);
                  rs.PutLong(0x9999999999);
                  rs.PutDouble(1.5);
                  List<byte> foo = new List<byte>();
                  rs.ToByteSlice().AppendTo(foo);
                  while(foo.Count < DSpace_OutputRecordLength)
                  {
                      foo.Add(0);
                  }
                  dfsoutput.WriteRecord(foo);
              }
              
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                    Exec.Shell("dspace importdir \"" + exectempdir + "\"");
                    try
                    {
                        System.IO.File.Delete(execfp);
                        System.IO.Directory.Delete(exectempdir);
                    }
                    catch
                    {
                    }
                }

                Console.WriteLine("Generating input data file...");
                Exec.Shell("dspace exec RegressionTest_exec{5929813D-A928-4758-8EDE-F0130E142AEF}");

                Console.WriteLine("Importing data...");
                Console.WriteLine(QaExec("INSERT INTO " + TableName + " IMPORT 'RegressionTest_GenQaData{DF1D615D-A6DC-40d1-A1F6-39EF39E9C464}_Output'").Trim());

                if (nocheck)
                {
                    return;
                }

                Console.WriteLine("Ensuring data imported into table...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE x = 42 ORDER BY NUM;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find expected inserted values; SELECT output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected inserted values");
                }

            }
            finally
            {
                Exec.Shell("dspace del RegressionTest_exec{5929813D-A928-4758-8EDE-F0130E142AEF}");
                Exec.Shell("dspace del RegressionTest_GenQaData{DF1D615D-A6DC-40d1-A1F6-39EF39E9C464}_Output");
            }

        }

    }
}
