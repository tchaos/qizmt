using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace mrdebug_regressiontests
{
    class TestClient
    {
        static void Main(string[] args)
        {
            string sDir = new DirectoryInfo(".").FullName + @"\";
            ShellCtrl pShellCtrl = new ShellCtrl(sDir + @"mrdebug.exe " + sDir + @"mrdebug_testproc.exe");
            pShellCtrl.WriteLine("run");
            System.Console.ReadKey();
        }
    }
}
