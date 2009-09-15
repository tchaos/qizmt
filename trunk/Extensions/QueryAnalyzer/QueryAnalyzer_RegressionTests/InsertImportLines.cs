using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void InsertImportLines(string TableName, bool nocheck)
        {

            try
            {

                {
                    string exectempdir = @"\\" + System.Net.Dns.GetHostName() + @"\C$\temp\dspace\regression_test_QueryAnalyzer_ImportLines-" + Guid.NewGuid().ToString();
                    if (!System.IO.Directory.Exists(exectempdir))
                    {
                        System.IO.Directory.CreateDirectory(exectempdir);
                    }
                    Exec.Shell("dspace del RegressionTest_exec{9BA3ED2D-8CDA-46d5-8CBB-E79729454693}");
                    string execfp = exectempdir + @"\RegressionTest_exec{9BA3ED2D-8CDA-46d5-8CBB-E79729454693}";
                    System.IO.File.WriteAllText(execfp, (@"<SourceCode>
  <Jobs>
    <Job Name=`RegressionTest_GenQaLineData_Preprocessing`>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del RegressionTest_GenQaLineData{82952C0F-797C-4bdc-8FAF-EDBD188C5E2F}_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`RegressionTest_GenQaLineData` Custodian=`Christopher Miller` Email=`cmiller@myspace-inc.com` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://RegressionTest_GenQaLineData{82952C0F-797C-4bdc-8FAF-EDBD188C5E2F}_Output.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {
              dfsoutput.WriteLine(`101,{55D73453-316A-4c37-A89F-74434732C3A9},203,111,111.111`);
              dfsoutput.WriteLine(`101,{2DFAF613-9CA6-4895-B8EB-84F221815DB9},201,222,222.222`);
              dfsoutput.WriteLine(`101,{D12ABBFF-3E36-4340-87AB-3A7A05D03D6D},202,333,333.333`);
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                    Exec.Shell("dspace importdir " + exectempdir);
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
                Exec.Shell("dspace exec RegressionTest_exec{9BA3ED2D-8CDA-46d5-8CBB-E79729454693}");

                Console.WriteLine("Importing data...");
                Console.WriteLine(QaExec("INSERT INTO " + TableName + " IMPORTLINES 'RegressionTest_GenQaLineData{82952C0F-797C-4bdc-8FAF-EDBD188C5E2F}_Output.txt'").Trim());

                if (nocheck)
                {
                    return;
                }

                Console.WriteLine("Ensuring data imported into table...");
                string outputSelect1 = QaExec("SELECT TOP 3 * FROM " + TableName + " WHERE num = 101 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{2DFAF613-9CA6-4895-B8EB-84F221815DB9}"))
                {
                    Console.Error.WriteLine("Did not find expected inserted values; SELECT output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected inserted values");
                }

            }
            finally
            {
                Exec.Shell("dspace del RegressionTest_exec{9BA3ED2D-8CDA-46d5-8CBB-E79729454693}");
                Exec.Shell("dspace del RegressionTest_GenQaLineData{82952C0F-797C-4bdc-8FAF-EDBD188C5E2F}_Output.txt");
            }

        }

    }
}
