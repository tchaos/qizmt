using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class Exec
    {
        public static string GetQizmtExe()
        {
            string exe = "qizmt.exe";

#if DEBUG
            exe = @"C:\SimpleSolutions\Applications\DataMining\API\MySpace.Datamining.DistributedObjects\bin\Debug\qizmt.exe";
#endif
            return exe;
        }

        public static string Shell(string line, bool suppresserrors)
        {
            System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("cmd.exe", @"/C " + line);
            psi.CreateNoWindow = true;
            //psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
            psi.UseShellExecute = false;
            psi.RedirectStandardOutput = true;
            if (!suppresserrors)
            {
                psi.RedirectStandardError = true;
            }
            string result;
            using (System.Diagnostics.Process process = System.Diagnostics.Process.Start(psi))
            {
                ShellErrInfo sei = null;
                System.Threading.Thread errthd = null;
                if (!suppresserrors)
                {
                    sei = new ShellErrInfo();
                    sei.reader = process.StandardError;
                    errthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(shellerrthreadproc));
                    errthd.Start(sei);
                }
                result = process.StandardOutput.ReadToEnd();
                process.WaitForExit();
                if (!suppresserrors)
                {
                    errthd.Join();
                    if (!string.IsNullOrEmpty(sei.err))
                    {
                        sei.err = sei.err.Trim();
                        if (sei.err.Length != 0)
                        {
                            throw new ShellException(line, sei.err);
                        }
                    }
                }
            }
            return result;
        }

        class ShellErrInfo
        {
            public string err;
            public System.IO.StreamReader reader;
        }

        static void shellerrthreadproc(object obj)
        {
            ShellErrInfo sei = (ShellErrInfo)obj;
            sei.err = sei.reader.ReadToEnd();
        }


        public static string Shell(string line)
        {
            return Shell(line, false);
        }


        public class ShellException : Exception
        {
            public ShellException(string cmd, string msg)
                : base("Shell(\"" + cmd + "\") error: " + msg)
            {
            }
        }
    }
}
