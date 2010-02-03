using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    public partial class Program
    {

        public static void Callstack(string[] args)
        {
            if (args.Length <= 1)
            {
                Console.Error.WriteLine("Expected host");
                return;
            }

            string hostcs = args[1];
            string netpath = @"\\" + hostcs + @"\" + CurrentDir.Replace(':', '$');

            {
                int protopid;
                string sprotopid;
                string protofp = netpath + @"\protocol.pid.rdbms";
                for (; ; System.Threading.Thread.Sleep(1000 * 1))
                {
                    try
                    {
                        string protofcontent;
                        using (System.IO.FileStream f = new System.IO.FileStream(protofp,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read,
                            System.IO.FileShare.Read | System.IO.FileShare.Write | System.IO.FileShare.Delete))
                        {
                            System.IO.StreamReader sr = new System.IO.StreamReader(f);
                            protofcontent = sr.ReadToEnd();
                            sr.Close();
                        }
                        {
                            int inl = protofcontent.IndexOf('\n'); // Continue if no \n.
                            if (-1 != inl)
                            {
                                sprotopid = protofcontent.Substring(0, inl).Trim();
                                break;
                            }
                        }
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        Console.Error.WriteLine("No protocol process for host {0}", hostcs);
                        return;
                    }
                }
                protopid = int.Parse(sprotopid);
                sprotopid = protopid.ToString(); // Normalize.

                Console.WriteLine("Waiting on protocol callstack...");

                {
                    string path = netpath + @"\" + sprotopid + ".trace.rdbms";
                    for (; System.IO.File.Exists(path); System.Threading.Thread.Sleep(1000 * 1))
                    {
                    }
                    string tpath = "pid" + System.Diagnostics.Process.GetCurrentProcess().Id + "tracing.proto" + sprotopid + ".tof.rdbms";
                    System.IO.File.WriteAllText(path, tpath + Environment.NewLine + ".");
                }

                for (int tries = 0; ; tries++)
                {
                    if (0 != tries)
                    {
                        System.Threading.Thread.Sleep(1000 * 3);
                    }
                    {
                        string tpath = netpath + @"\" + "pid" + System.Diagnostics.Process.GetCurrentProcess().Id + "tracing.proto" + sprotopid + ".tof.rdbms";
                        {
                            string toutput = ReadTraceFromFile(tpath);
                            if (null == toutput)
                            {
                                if(!System.IO.File.Exists(protofp))
                                {
                                    Console.WriteLine();
                                    Console.WriteLine("Protocol no longer running");
                                    try
                                    {
                                        System.IO.File.Delete(netpath + @"\" + sprotopid + ".trace.rdbms");
                                    }
                                    catch
                                    {
                                    }
                                    break;
                                }
                            }
                            else
                            {
                                Console.WriteLine();
                                Console.WriteLine(toutput);
                                try
                                {
                                    System.IO.File.Delete(tpath);
                                }
                                catch
                                {
                                }
                                break;
                            }
                        }
                    }
                }
                Console.WriteLine();

            }

        }


        // Important: returns null if it's not ready to be read yet!
        static string ReadTraceFromFile(string fp)
        {
            string result = null;
            try
            {
                const string tracefiledelim = "{C8683F6C-0655-42e7-ACD9-0DDED6509A7C}";
                using (System.IO.FileStream f = new System.IO.FileStream(fp, System.IO.FileMode.Open, System.IO.FileAccess.Read,
                    System.IO.FileShare.Read | System.IO.FileShare.Write | System.IO.FileShare.Delete))
                {
                    System.IO.StreamReader sr = new System.IO.StreamReader(f);
                    string s = sr.ReadToEnd();
                    sr.Close();
                    int i = s.IndexOf(tracefiledelim);
                    if (-1 != i)
                    {
                        s = s.Substring(i + tracefiledelim.Length);
                        i = s.IndexOf(tracefiledelim);
                        if (-1 != i)
                        {
                            result = s.Substring(0, i).Trim('\r', '\n');
                        }
                    }
                }
            }
            catch
            {
            }
            return result;
        }


    }
}
