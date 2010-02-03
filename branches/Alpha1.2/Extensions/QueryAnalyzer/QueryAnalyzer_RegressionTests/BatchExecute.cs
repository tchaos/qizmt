using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void BatchExecute(string TableName)
        {

            const string QUERY_TERM = "\r\n\0\r\n";

            string[] queries;

            Console.WriteLine("Inserting one row...");
            queries = new string[]
                {
                    "insert into " + TableName + " values(881,'{C3014915-DC5F-4475-94D7-609F9F6A86F2}',121255,121255,121255);"
                };
            {
                if (!System.IO.Directory.Exists(@"c:\temp"))
                {
                    System.IO.Directory.CreateDirectory(@"c:\temp");
                }
                string befp = @"\\" + System.Net.Dns.GetHostName() + @"\c$\temp\batchexecute-test-" + Guid.NewGuid().ToString() + ".txt";
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(befp))
                {
                    foreach (string q in queries)
                    {
                        sw.Write(q);
                        sw.Write(QUERY_TERM);
                    }
                }
                try
                {
                    Console.WriteLine(QaExec(" BATCHEXECUTE   '" + befp + "'; ").Trim());

                    string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 121255 ORDER BY NUM;");
                    if (-1 == outputSelect1.IndexOf("{C3014915-DC5F-4475-94D7-609F9F6A86F2}"))
                    {
                        Console.Error.WriteLine("Did not find expected batch- inserted values; SELECT output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected batch- inserted values");
                    }

                }
                finally
                {
                    System.IO.File.Delete(befp);
                }
            }

            Console.WriteLine("Inserting multiple rows with an intermixed select...");
            queries = new string[]
                {
                    "insert into " + TableName + " values(882,'{2F010B06-144F-490b-BD59-8BE4A6E16E76}',121255,121255,121255);",
                    "insert into " + TableName + " values(883,'{777225D8-5E48-4eec-9A39-D39757E3638A}',121255,121255,121255);",
                    "select top 50 * from " + TableName + " where x = 121255 ORDER BY NUM;", // Note: this gets sent to stdout.
                    "insert into " + TableName + " values(880,'{39104784-23CA-4929-BB4D-227E1309FEA5}',121255,121255,121255);"
                };
            {
                if (!System.IO.Directory.Exists(@"c:\temp"))
                {
                    System.IO.Directory.CreateDirectory(@"c:\temp");
                }
                string befp = @"\\" + System.Net.Dns.GetHostName() + @"\c$\temp\batchexecute-test-" + Guid.NewGuid().ToString() + ".txt";
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(befp))
                {
                    foreach (string q in queries)
                    {
                        sw.Write(q);
                        sw.Write(QUERY_TERM);
                    }
                }
                try
                {
                    Console.WriteLine(QaExec(" BATCHEXECUTE   '" + befp + "'; ").Trim());

                    string outputSelect1 = QaExec("SELECT TOP 50 * FROM " + TableName + " WHERE x = 121255 ORDER BY NUM;");
                    if (-1 == outputSelect1.IndexOf("{39104784-23CA-4929-BB4D-227E1309FEA5}") // After!
                        || -1 == outputSelect1.IndexOf("{777225D8-5E48-4eec-9A39-D39757E3638A}") // Before!
                        )
                    {
                        Console.Error.WriteLine("Did not find expected batch- inserted values; SELECT output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected batch- inserted values");
                    }

                }
                finally
                {
                    System.IO.File.Delete(befp);
                }
            }

        }

    }
}
