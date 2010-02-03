using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using MySpace.DataMining.AELight;

namespace RegressionTest2
{
    public partial class Program
    {

        static void Mrdebug(string[] args)
        {
            string justtest = null;
            if (args.Length > 1)
            {
                justtest = args[1];
            }

            {
                string testname = "Run";
                if (_mrdebug_runtest(testname, justtest))
                {
                    Console.WriteLine("    Running test {0}    ", testname);
                    Console.Write("        ");
                    try
                    {
                        _Mrdebug_Run();
                        Console.WriteLine("[PASSED] - {0}", testname);
                    }
                    catch
                    {
                        Console.WriteLine("[FAILED] - {0}", testname);
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "Breakpoint";
                if (_mrdebug_runtest(testname, justtest))
                {
                    Console.WriteLine("    Running test {0}    ", testname);
                    Console.Write("        ");
                    try
                    {
                        _Mrdebug_Breakpoint(false);
                        Console.WriteLine("[PASSED] - {0}", testname);
                    }
                    catch
                    {
                        Console.WriteLine("[FAILED] - {0}", testname);
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "ManyBreakpoints";
                if (_mrdebug_runtest(testname, justtest))
                {
                    Console.WriteLine("    Running test {0}    ", testname);
                    Console.Write("        ");
                    try
                    {
                        _Mrdebug_Breakpoint(true);
                        Console.WriteLine("[PASSED] - {0}", testname);
                    }
                    catch
                    {
                        Console.WriteLine("[FAILED] - {0}", testname);
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "StepOver";
                if (_mrdebug_runtest(testname, justtest))
                {
                    Console.WriteLine("    Running test {0}    ", testname);
                    Console.Write("        ");
                    try
                    {
                        _Mrdebug_StepOver();
                        Console.WriteLine("[PASSED] - {0}", testname);
                    }
                    catch
                    {
                        Console.WriteLine("[FAILED] - {0}", testname);
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "StepInto";
                if (_mrdebug_runtest(testname, justtest))
                {
                    Console.WriteLine("    Running test {0}    ", testname);
                    Console.Write("        ");
                    try
                    {
                        _Mrdebug_StepInto();
                        Console.WriteLine("[PASSED] - {0}", testname);
                    }
                    catch
                    {
                        Console.WriteLine("[FAILED] - {0}", testname);
                    }
                    Console.WriteLine();
                }
            }

            Console.WriteLine("Done");

        }


        static void _Mrdebug_Run()
        {
            ShellCtrl sh = new ShellCtrl("mrdebug mrdebugee.exe -xdebug", true);
            sh.WriteLine("cont");
            int GotProcessStart = 0, GotFirst = 0, GotProcessExit = 0;
            int LineNumber = 0;
            StringBuilder sboutput = new StringBuilder();
            Exception exception = null;
            try
            {
                for (; ; )
                {
                    string line = _timely_ReadLine(sh);
                    if (null == line)
                    {
                        break;
                    }
                    sboutput.AppendLine(line);
                    LineNumber++;
                    if (0 == GotProcessStart
                        && "STOP: Breakpoint Hit" == line)
                    {
                        GotProcessStart = LineNumber;
                        Console.Write('.');
                    }
                    if (-1 != line.IndexOf("*FIRST*"))
                    {
                        GotFirst = LineNumber;
                        Console.Write('.');
                    }
                    if (0 == GotProcessExit
                        && "Process exited." == line)
                    {
                        GotProcessExit = LineNumber;
                        Console.Write('.');
                        sh.WriteLine("quit");
                    }
                }
                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine("Exception: {0}", e.ToString());
            }
            if (null != exception
                || (!(GotProcessStart > 0
                && GotFirst > GotProcessStart
                && GotProcessExit > GotFirst)))
            {
                Console.WriteLine("*** Failure. Debugger output: {0}", sboutput.ToString());
                throw new Exception("Debugee process start and end did not occur correctly");
            }
        }


        static void _Mrdebug_Breakpoint(bool ManyBreakpoints)
        {
            ShellCtrl sh = new ShellCtrl("mrdebug mrdebugee.exe -xdebug", true);
            sh.WriteLine("b Program.cs:83"); // Always the first one, even if ManyBreakpoints.
            if (ManyBreakpoints)
            {
                sh.WriteLine("b Program.cs:95");
                sh.WriteLine("b Program.cs:141");
                sh.WriteLine("b Program.cs:147");
                sh.WriteLine("b Program.cs:153");
                sh.WriteLine("b Program.cs:178");
                sh.WriteLine("b Program.cs:184");
            }
            sh.WriteLine("cont");
            int GotProcessStart = 0, GotFirst = 0, GotBeforeBp = 0, GotBp = 0, GotAfterBp = 0, GotProcessExit = 0;
            int LineNumber = 0;
            StringBuilder sboutput = new StringBuilder();
            Exception exception = null;
            try
            {
                for (; ; )
                {
                    string line = _timely_ReadLine(sh);
                    if (null == line)
                    {
                        break;
                    }
                    sboutput.AppendLine(line);
                    LineNumber++;
                    if (0 == GotProcessStart
                        && "STOP: Breakpoint Hit" == line)
                    {
                        GotProcessStart = LineNumber;
                        Console.Write('.');
                    }
                    if (-1 != line.IndexOf("*FIRST*"))
                    {
                        GotFirst = LineNumber;
                        Console.Write('.');
                    }
                    if (0 == GotBeforeBp
                        && -1 != line.IndexOf("{8339B895-EFA5-476b-865B-6FE4B77268E4}before"))
                    {
                        GotBeforeBp = LineNumber;
                        Console.Write('.');
                    }
                    if (-1 != line.IndexOf("break at #"))
                    {
                        Console.Write('.');
                        if (!ManyBreakpoints || 0 == GotBp)
                        {
                            if (0 != GotBp)
                            {
                                throw new Exception("Got too many breakpoints");
                            }
                            GotBp = LineNumber;
                            if (0 == GotBeforeBp)
                            {
                                throw new Exception("Expected magic before- GUID before breakpoint");
                            }
                            if (0 != GotAfterBp)
                            {
                                throw new Exception("Did not expect magic after- GUID before breakpoint");
                            }
                        }
                        if (ManyBreakpoints)
                        {
                            // Keep adding this one on a breakpoint.
                            sh.WriteLine("b Program.cs:190");
                        }
                        sh.WriteLine("cont");
                    }
                    if (0 == GotAfterBp
                        && -1 != line.IndexOf("{8339B895-EFA5-476b-865B-6FE4B77268E4}after"))
                    {
                        GotAfterBp = LineNumber;
                        Console.Write('.');
                    }
                    if (0 == GotProcessExit
                        && "Process exited." == line)
                    {
                        GotProcessExit = LineNumber;
                        Console.Write('.');
                        sh.WriteLine("quit");
                    }
                }
                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine("Exception: {0}", e.ToString());
            }
            if (null != exception
                || (!(GotProcessStart > 0
                && GotFirst > GotProcessStart
                && GotBeforeBp > GotProcessStart
                && GotBp > GotBeforeBp
                && GotAfterBp > GotBp
                && GotProcessExit > GotAfterBp)))
            {
                Console.WriteLine("*** Failure. Debugger output: {0}", sboutput.ToString());
                throw new Exception("Debugee process events did not occur correctly");
            }
        }


        static void _Mrdebug_StepInto()
        {
            ShellCtrl sh = new ShellCtrl("mrdebug mrdebugee.exe -xdebug", true);
            //sh.WriteLine("cont");
            int GotProcessStart = 0, GotFirst = 0, GotProcessExit = 0;
            int LineNumber = 0;
            bool StillStepping = true;
            StringBuilder sboutput = new StringBuilder();
            Exception exception = null;
            try
            {
                for (; ; )
                {
                    string line = _timely_ReadLine(sh);
                    if (null == line)
                    {
                        break;
                    }
                    sboutput.AppendLine(line);
                    LineNumber++;
                    if (0 == GotProcessStart
                        && "STOP: Breakpoint Hit" == line)
                    {
                        GotProcessStart = LineNumber;
                        Console.Write('.');
                    }
                    if (-1 != line.IndexOf("*FIRST*"))
                    {
                        GotFirst = LineNumber;
                        Console.Write('.');
                    }
                    if (0 == GotProcessExit
                        && "Process exited." == line)
                    {
                        GotProcessExit = LineNumber;
                        Console.Write('.');
                        StillStepping = false;
                        sh.WriteLine("quit");
                    }
                    if (StillStepping)
                    {
                        int codeline = _mrdebug_codeline(line);
                        if (codeline >= 0)
                        {
                            Console.Write("{0},", codeline);
                            sh.WriteLine("si"); // Step in.
                        }
                    }
                }
                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine("Exception: {0}", e.ToString());
            }
            if (null != exception
                || (!(GotProcessStart > 0
                && GotFirst > GotProcessStart
                && GotProcessExit > GotFirst)))
            {
                Console.WriteLine("*** Failure. Debugger output: {0}", sboutput.ToString());
                throw new Exception("Debugee process start and end did not occur correctly");
            }
        }


        static void _Mrdebug_StepOver()
        {
            ShellCtrl sh = new ShellCtrl("mrdebug mrdebugee.exe -xdebug", true);
            sh.WriteLine("b Program.cs:41");
            sh.WriteLine("cont");
            int GotProcessStart = 0, GotFirst = 0, GotProcessExit = 0;
            int LineNumber = 0;
            bool StillStepping = true;
            StringBuilder sboutput = new StringBuilder();
            Exception exception = null;
            try
            {
                for (; ; )
                {
                    string line = _timely_ReadLine(sh);
                    if (null == line)
                    {
                        break;
                    }
                    sboutput.AppendLine(line);
                    LineNumber++;
                    if (0 == GotProcessStart
                        && "STOP: Breakpoint Hit" == line)
                    {
                        GotProcessStart = LineNumber;
                        Console.Write('.');
                        StillStepping = true;
                    }
                    if (-1 != line.IndexOf("*FIRST*"))
                    {
                        GotFirst = LineNumber;
                        Console.Write('.');
                    }
                    if (0 == GotProcessExit
                        && "Process exited." == line)
                    {
                        GotProcessExit = LineNumber;
                        Console.Write('.');
                        StillStepping = false;
                        sh.WriteLine("quit");
                    }
                    if (StillStepping)
                    {
                        int codeline = _mrdebug_codeline(line);
                        if (codeline >= 0)
                        {
                            Console.Write("{0},", codeline);
                            sh.WriteLine("so"); // Step over.
                        }
                    }
                }
                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine("Exception: {0}", e.ToString());
            }
            if (null != exception
                || (!(GotProcessStart > 0
                && GotFirst > GotProcessStart
                && GotProcessExit > GotFirst)))
            {
                Console.WriteLine("*** Failure. Debugger output: {0}", sboutput.ToString());
                throw new Exception("Debugee process start and end did not occur correctly");
            }
        }


        static string _timely_ReadLine(System.IO.TextReader tr, int timelySecs)
        {
            string result = null;
            System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(
                delegate()
                {
                    result = tr.ReadLine();
                }));
            thd.Start();
            if (!thd.Join(1000 * timelySecs))
            {
                throw new Exception("ReadLine did not complete within " + timelySecs.ToString() + " seconds");
            }
            return result;
        }

        static string _timely_ReadLine(System.IO.TextReader tr)
        {
            return _timely_ReadLine(tr, 60);
        }


        static bool _mrdebug_runtest(string testname, string justtest)
        {
            return null == justtest
                || 0 == string.Compare(testname, justtest, true);
        }

        
        static int _mrdebug_codeline(string line)
        {
            line = line.Trim();
            if (line.StartsWith("(mrdebug)"))
            {
                line = line.Substring(9);
                line = line.Trim();
            }
            if (line.Length > 0 && char.IsDigit(line[0]))
            {
                for (int i = 1; i < line.Length; i++)
                {
                    if (char.IsDigit(line[i]))
                    {
                        continue;
                    }
                    if (':' == line[i])
                    {
                        return int.Parse(line.Substring(0, i));
                    }
                    break;
                }
            }
            else if ("(source line information unavailable) <no IL>" == line)
            {
                return 0;
            }
            else if ("Hidden location" == line)
            {
                return 0;
            }
            else if (line.StartsWith("Unable to get current line: "))
            {
                return 0;
            }
            return -1;
        }

    }
}
