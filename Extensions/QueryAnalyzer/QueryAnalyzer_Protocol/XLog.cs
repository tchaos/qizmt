using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace QueryAnalyzer_Protocol
{
    public static class XLog
    {
        public static bool logging = false;

        internal static Mutex logmutex = new Mutex(false, "qaprotocollog");
        
        public static void errorlog(string line)
        {
            logmutex.WaitOne();
            try
            {
                using (System.IO.StreamWriter fstm = System.IO.File.AppendText("errors.txt"))
                {
                    string build = "";
                    try
                    {
                        System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                        System.Reflection.AssemblyName an = asm.GetName();
                        int bn = an.Version.Build;
                        int rv = an.Version.Revision;
                        build = "(build:" + bn.ToString() + "." + rv.ToString() + ") ";
                    }
                    catch
                    {
                    }
                    fstm.WriteLine("[{0} {1}ms] QueryAnalyzer_Protocol error: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                    fstm.WriteLine();
                }
            }
            catch
            {
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }


        public static void log(string name, string line)
        {
            logmutex.WaitOne();
            try
            {
                StreamWriter fstm = File.AppendText(name + ".txt");
                fstm.WriteLine("{0}", line);
                fstm.Close();
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }
    }
}
