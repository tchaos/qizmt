using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void ShellTest(string TableName)
        {

            string outputShell1 = QaExec("SHELL 'dspace.exe ls RDBMS_Q*.DBCORE'; ");
            if (-1 == outputShell1.IndexOf("RDBMS_QueryAnalyzer.DBCORE", StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine("Did not find RDBMS_QueryAnalyzer.DBCORE in ls output: {0}", outputShell1);
                throw new Exception("Did not find RDBMS_QueryAnalyzer.DBCORE in ls output");
            }

        }

    }
}
