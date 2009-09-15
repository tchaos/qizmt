/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Runtime.InteropServices;

using mrdbgcom;


namespace mrdebug
{
    public class MRDebug
    {

        public class DebugProcess
        {

            public DebugProcess()
            {
            }


            public void AddCommand(string line)
            {
                if (!FinishedInitializing)
                {
                    throw new Exception("Cannot run command before FinishedInitializing: " + line);
                }

                /*
                // Interferes with interfacing clients.
#if DEBUG
                if (IsXDebug)
                {
                    Console.WriteLine("  * Adding command: {0}", line);
                }
#endif
                 * */

                lock (cmdq)
                {
                    cmdq.Enqueue(line);
                }

                string origcmd;
                string cmd, args;
                {
                    int isp = line.IndexOf(' ');
                    if (-1 == isp)
                    {
                        origcmd = line;
                        args = "";
                    }
                    else
                    {
                        origcmd = line.Substring(0, isp);
                        args = line.Substring(isp + 1);
                    }
                    cmd = origcmd.ToLower();
                }

                switch (cmd)
                {
                    case "quit":
                    case "exit":
                        lock (cmdq)
                        {
                            cmdq.Clear();
                            cmdq.Enqueue(line);
                        }
                        try
                        {
                            lock (this)
                            {
                                idbgproc.Stop(~(uint)0);
                                idbgproc.Terminate(0);
                            }
                            Until("ExitProcess").WaitOne();
                        }
                        catch
                        {
                        }
                        finally
                        {
                            ProcessExit = true;
                        }
                        break;
                }

                lock (cmdq)
                {
                    cmde.Set();
                }
            }

            Queue<string> cmdq = new Queue<string>();
            System.Threading.AutoResetEvent cmde = new System.Threading.AutoResetEvent(false);

            public void Run()
            {
                if (null == idbg)
                {
                    throw new ArgumentException("Critical error:  Run: idbg is null");
                }
                if (null == idbgproc)
                {
                    throw new ArgumentException("Critical error:  Run: idbgproc is null");
                }

                //this.idbg = idbg;
                //this.idbgproc = idbgproc;

                this.din = Console.In;
                this.dout = Console.Out;

                CmdSyncWait = new System.Threading.AutoResetEvent(false);

                imodules = new List<ICorDebugModule>();
                iappdomains = new List<ICorDebugAppDomain>();
                iassemblies = new List<ICorDebugAssembly>();
                ibreakpoints = new List<ICorDebugFunctionBreakpoint>();

#if DEBUG
                if (IsXDebug)
                {
                    Console.WriteLine("  * Waiting for initial breakpoint and finish initializing");
                }
#endif

                //Until("LoadModule:this").WaitOne();
                Until("Breakpoint").WaitOne();
                FinishedInitializing = true;

                if (IsVerbose)
                {
                    Console.WriteLine("  Finished initializing debug process");
                }

                //System.Threading.Thread.Sleep(3000);
                lock (this)
                {
                    ShowCurrentLine_unlocked();
                }

                while(!ProcessExit)
                {
                    System.GC.Collect();

                    try
                    {
                        dout.Write("(mrdebug) ");

                        string origcmd;
                        string cmd, args;
                        {
                            string line = null;
                            lock (cmdq)
                            {
                                if (cmdq.Count > 0)
                                {
                                    line = cmdq.Dequeue();
                                }
                            }
                            if (null == line)
                            {
                                for (; ; )
                                {
                                    //bool waited = cmde.WaitOne(1000, false);
                                    bool waited = cmde.WaitOne();
                                    lock (cmdq)
                                    {
                                        if (cmdq.Count < 1)
                                        {
                                            continue;
                                        }
#if DEBUG
                                        if (!waited)
                                        {
                                            Console.WriteLine("DEBUG:  (!waited)");
                                        }
#endif
                                        line = cmdq.Dequeue();
                                        break;
                                    }
                                }
                            }
                            if (line.Length == 0)
                            {
                                line = lastcmdline;
                            }
                            else
                            {
                                lastcmdline = line;
                            }
                            int isp = line.IndexOf(' ');
                            if (-1 == isp)
                            {
                                origcmd = line;
                                args = "";
                            }
                            else
                            {
                                origcmd = line.Substring(0, isp);
                                args = line.Substring(isp + 1);
                            }
                            cmd = origcmd.ToLower();
                        }

                        if (ProcessExit)
                        {
                            switch (cmd)
                            {
                                case "quit":
                                case "exit":
                                    break;
                                default:
                                    dout.WriteLine("Error: Command '{0}' cannot be run at this time", origcmd);
                                    break;
                            }
                            continue; // Exits loop.
                        }

                        {
                            switch (cmd)
                            {
                                case "go":
                                case "cont": // continue
                                    {
                                        ContinueUntil("Breakpoint");
                                    }
                                    break;

                                case "delete":
                                case "d":
                                    lock (this)
                                    {
                                        if (0 != args.Length)
                                        {
                                            dout.WriteLine("Error: delete only works with no parameters");
                                        }
                                        else
                                        {
                                            for (int ibp = 0; ibp < ibreakpoints.Count; ibp++)
                                            {
                                                ibreakpoints[ibp].Activate(0); // Deactivate.
                                            }
                                        }
                                    }
                                    break;

                                case "b":
                                case "break":
                                    lock (this)
                                    {
                                        string BkptFile = "";
                                        string sBkptLine;
                                        {
                                            int ilc = args.LastIndexOf(':');
                                            if (ilc > 1)
                                            {
                                                BkptFile = args.Substring(0, ilc);
                                                sBkptLine = args.Substring(ilc + 1);
                                            }
                                            else
                                            {
                                                sBkptLine = args;
                                            }
                                        }

                                        int BkptLine;
                                        if (!int.TryParse(sBkptLine, out BkptLine) || BkptLine < 0)
                                        {
                                            dout.WriteLine("Error: breakpoint line number expected");
                                            continue;
                                        }

                                        Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                                        ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));

                                        for (int im = 0; im < imodules.Count; im++)
                                        {
                                            ICorDebugModule imod = imodules[im];

                                            ICorDebugAssembly iasm;
                                            imod.GetAssembly(out iasm);
                                            string assemblyname;
                                            {
                                                IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                                                uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                                                imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                                                if (asmnamelen > PATH_BUFFER_LENGTH)
                                                {
                                                    throw new Exception("Assembly path too long");
                                                }
                                                asmnamelen--; // Remove nul.
                                                assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                                                Marshal.FreeHGlobal(pasmnamebuf);
                                            }
                                            if (!assemblyname.EndsWith(".exe", true, null))
                                            {
                                                continue;
                                            }

                                            /*IMetaDataImport importer;
                                            {
                                                object oimdi;
                                                Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                                imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                                importer = (IMetaDataImport)oimdi;
                                            }*/

                                            Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                                            IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                                            Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                                            object oimporter;
                                            disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                                            IntPtr pimporter = IntPtr.Zero;
                                            try
                                            {
                                                pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                                                ISymUnmanagedReader reader;
                                                int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                                                if (0 == hrreader)
                                                {
                                                    int cDocs = 0;
                                                    reader.GetDocuments(0, out cDocs, null);
                                                    ISymUnmanagedDocument[] unmDocs = new ISymUnmanagedDocument[cDocs];
                                                    reader.GetDocuments(cDocs, out cDocs, unmDocs);

                                                    for (int idoc = 0; idoc < cDocs; idoc++)
                                                    {
                                                        // See if current doc is the one I'm adding to.. via GetURL
                                                        ISymUnmanagedDocument unmDoc = unmDocs[idoc];

                                                        //BkptFile
                                                        if (null != BkptFile)
                                                        {
                                                            string docurl;
                                                            {
                                                                int urllen = 0;
                                                                unmDoc.GetURL(0, out urllen, null);
                                                                StringBuilder sburl = new StringBuilder(urllen);
                                                                unmDoc.GetURL(urllen, out urllen, sburl);
                                                                sburl.Length = urllen - 1; // Remove nul.
                                                                docurl = sburl.ToString();
                                                            }
                                                            if (!FileNamesMatch(docurl, BkptFile))
                                                            {
                                                                continue;
                                                            }
                                                        }

                                                        {
                                                            int RealBkptLine = -1;
                                                            try
                                                            {
                                                                unmDoc.FindClosestLine(BkptLine, out RealBkptLine);
                                                            }
                                                            catch
                                                            {
                                                            }
                                                            if (RealBkptLine < 0)
                                                            {
                                                                continue;
                                                            }
                                                            BkptLine = RealBkptLine;
                                                        }

                                                        ISymUnmanagedMethod unmMethod;
                                                        reader.GetMethodFromDocumentPosition(unmDoc, BkptLine, 0, out unmMethod);
                                                        System.Diagnostics.SymbolStore.SymbolToken methodToken;
                                                        unmMethod.GetToken(out methodToken);
                                                        int seqptscount;
                                                        unmMethod.GetSequencePointCount(out seqptscount);
                                                        int[] spoffsets = new int[seqptscount];
                                                        ISymUnmanagedDocument[] spdocs = new ISymUnmanagedDocument[seqptscount];
                                                        int[] spstartlines = new int[seqptscount];
                                                        int[] spendlines = new int[seqptscount];
                                                        int[] spstartcols = new int[seqptscount];
                                                        int[] spendcols = new int[seqptscount];
                                                        int cPoints;
                                                        unmMethod.GetSequencePoints(seqptscount, out cPoints, spoffsets, spdocs, spstartlines, spstartcols, spendlines, spendcols);

                                                        int spoffset = -1;
                                                        for (int isp = 0; isp < cPoints; isp++)
                                                        {
                                                            if (BkptLine == spstartlines[isp])
                                                            {
                                                                spoffset = spoffsets[isp];
                                                                break;
                                                            }
                                                        }

                                                        ICorDebugFunction idbgFunc;
                                                        imod.GetFunctionFromToken((uint)methodToken.GetToken(), out idbgFunc);

                                                        ICorDebugFunctionBreakpoint pBkpt;
                                                        if (-1 != spoffset)
                                                        {
                                                            ICorDebugCode pCode;
                                                            idbgFunc.GetILCode(out pCode);
                                                            pCode.CreateBreakpoint((uint)spoffset, out pBkpt);
                                                        }
                                                        else
                                                        {
                                                            idbgFunc.CreateBreakpoint(out pBkpt);
                                                        }
                                                        pBkpt.Activate(1);
                                                        ibreakpoints.Add(pBkpt);

                                                    }

                                                }

                                            }
                                            finally
                                            {
                                                if (IntPtr.Zero != pimporter)
                                                {
                                                    Marshal.Release(pimporter);
                                                    pimporter = IntPtr.Zero;
                                                }
                                            }
                                        }
                                    }
                                    break;

                                case "n":
                                case "next":
                                case "so": // Step over.
                                    Step(false);
                                    break;

                                case "s":
                                case "step":
                                case "si":
                                    Step(true);
                                    break;

                                case "out":
                                case "o":
                                    StepOut();
                                    break;

                                case "where":
                                case "w":
                                    lock (this)
                                    {
                                        List<WhereInfo> wheres = new List<WhereInfo>();
                                        int CurrentFrameIndex = GetWhere(wheres, 50);
                                        for (int fi = 0; fi < wheres.Count; fi++)
                                        {
                                            WhereInfo wi = wheres[fi];
                                            dout.Write("{0}){1} {2}", fi, (fi == CurrentFrameIndex) ? "*" : "", wi.Source);
                                            if (-1 != wi.FileLineNumber)
                                            {
                                                string slinenum;
                                                if (HIDDEN_LINE_NUMBER == wi.FileLineNumber)
                                                {
                                                    slinenum = "?";
                                                }
                                                else
                                                {
                                                    slinenum = wi.FileLineNumber.ToString();
                                                }
                                                dout.WriteLine(" in {0}:{1}", wi.FileName, slinenum);
                                            }
                                            else
                                            {
                                                dout.WriteLine();
                                            }
                                        }

                                    }
                                    break;

                                case "print":
                                    lock (this)
                                    {
                                        ICorDebugThread ithd;
                                        idbgproc.GetThread(ActiveThreadID, out ithd);
                                        ICorDebugFrame iframe;
                                        ithd.GetActiveFrame(out iframe);
                                        if (null == iframe)
                                        {
                                            dout.WriteLine("Error: No frame available");
                                        }
                                        else
                                        {
                                            ICorDebugFunction ifunc;
                                            iframe.GetFunction(out ifunc);
                                            ICorDebugILFrame ilframe = iframe as ICorDebugILFrame;
                                            if (null == ilframe)
                                            {
                                                dout.WriteLine("Error: No IL frame");
                                            }
                                            else
                                            {
                                                uint ftoken;
                                                ifunc.GetToken(out ftoken);

                                                ICorDebugModule imod;
                                                ifunc.GetModule(out imod);

                                                ICorDebugAssembly iasm;
                                                imod.GetAssembly(out iasm);
                                                string assemblyname;
                                                {
                                                    IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                                                    uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                                                    imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                                                    if (asmnamelen > PATH_BUFFER_LENGTH)
                                                    {
                                                        throw new Exception("Assembly path too long");
                                                    }
                                                    asmnamelen--; // Remove nul.
                                                    assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                                                    Marshal.FreeHGlobal(pasmnamebuf);
                                                }

                                                Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                                                IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                                                Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                                                object oimporter;
                                                disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                                                IntPtr pimporter = IntPtr.Zero;
                                                try
                                                {
                                                    pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                                                    Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                                                    ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));

                                                    ISymUnmanagedReader reader;
                                                    int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                                                    if (0 == hrreader)
                                                    {
                                                        ISymUnmanagedMethod unmMethod;
                                                        reader.GetMethod(new System.Diagnostics.SymbolStore.SymbolToken((int)ftoken), out unmMethod);
                                                        if (null != unmMethod)
                                                        {

                                                            IMetaDataImport importer;
                                                            {
                                                                object oimdi;
                                                                Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                                                imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                                                importer = (IMetaDataImport)oimdi;
                                                            }

                                                            uint ip;
                                                            CorDebugMappingResult mappingresults;
                                                            ilframe.GetIP(out ip, out mappingresults);

                                                            ISymUnmanagedScope unmScope;
                                                            unmMethod.GetRootScope(out unmScope);

                                                            _PrintLocals(ilframe, unmScope, ip, dout);

                                                            _PrintArgs(ilframe, importer, dout);

                                                        }

                                                    }
                                                    else
                                                    {
                                                    }
                                                }
                                                finally
                                                {
                                                    if (IntPtr.Zero != pimporter)
                                                    {
                                                        Marshal.Release(pimporter);
                                                        pimporter = IntPtr.Zero;
                                                    }
                                                }

                                            }
                                        }
                                    }
                                    break;

                                case "funceval":
                                case "f":
                                    {
                                        bool needwait = false;
                                        ICorDebugEval peval;
                                        ICorDebugFunction ifunc;
                                        ICorDebugValue pvalue;
                                        lock (this)
                                        {
                                            string xxstart = "System.Object::ToString ";
                                            if (!args.StartsWith(xxstart))
                                            {
                                                dout.WriteLine("funceval only supports " + xxstart);
                                            }
                                            string TypeName = "System.Object";
                                            string FuncName = "ToString";
                                            string fargs = args.Substring(xxstart.Length);

                                            ICorDebugValue fargvalue0 = MyFindLocalVarOrArg_unlocked(fargs);
                                            if (null == fargvalue0)
                                            {
                                                dout.WriteLine("Could not resolve argument {0} - funceval only supports one parameter that is a local", fargvalue0);
                                            }
                                            else
                                            {
                                                IMetaDataImport importerMscorlib;
                                                {
                                                    object oimdi;
                                                    Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                                    imoduleMscorlib.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                                    importerMscorlib = (IMetaDataImport)oimdi;
                                                }

                                                uint ctoken;
                                                if (0 != importerMscorlib.FindTypeDefByName(TypeName, 0, out ctoken))
                                                {
                                                    dout.WriteLine("Could not resolve {0}", TypeName);
                                                }
                                                else
                                                {
                                                    List<uint> MethodTokens = new List<uint>();
                                                    {
                                                        uint hEnum = 0;
                                                        uint[] aMethodToken = new uint[1];
                                                        for (; ; )
                                                        {
                                                            uint cTokens;
                                                            importerMscorlib.EnumMethods(ref hEnum, ctoken, aMethodToken, 1, out cTokens);
                                                            if (0 == cTokens)
                                                            {
                                                                break;
                                                            }
                                                            MethodTokens.Add(aMethodToken[0]);
                                                        }
                                                    }

                                                    IMetaDataImport importer = importerMscorlib;
                                                    uint ftoken = 0;
                                                    for (int imt = 0; imt < MethodTokens.Count; imt++)
                                                    {
                                                        uint cmftoken = MethodTokens[imt];
                                                        uint cmftypedeftoken;
                                                        string cmMethodName;
                                                        {
                                                            uint chMethod;
                                                            uint dwAttr;
                                                            IntPtr pvSigBlob;
                                                            uint cbSigBlob;
                                                            uint ulCodeRVA;
                                                            uint dwImplFlags;
                                                            int hrmethodprops = (int)importer.GetMethodProps(cmftoken, out cmftypedeftoken,
                                                                null, 0, out chMethod,
                                                                out dwAttr,
                                                                out pvSigBlob, out cbSigBlob,
                                                                out ulCodeRVA, out dwImplFlags);
                                                            Marshal.ThrowExceptionForHR(hrmethodprops);
                                                            char[] methodnamechars = new char[chMethod];
                                                            hrmethodprops = (int)importer.GetMethodProps(cmftoken, out cmftypedeftoken,
                                                                methodnamechars, (uint)methodnamechars.Length, out chMethod,
                                                                out dwAttr,
                                                                out pvSigBlob, out cbSigBlob,
                                                                out ulCodeRVA, out dwImplFlags);
                                                            Marshal.ThrowExceptionForHR(hrmethodprops);
                                                            chMethod--; // Remove nul.
                                                            cmMethodName = new string(methodnamechars, 0, (int)chMethod);
                                                        }
                                                        if (cmMethodName == FuncName)
                                                        {
                                                            ftoken = cmftoken;
                                                            break;
                                                        }

                                                    }
                                                    if (0 == ftoken)
                                                    {
                                                        dout.WriteLine("Could not resolve {0}.{1}", TypeName, FuncName);
                                                    }
                                                    else
                                                    {
                                                        //ICorDebugEval peval;
                                                        idbgthd.CreateEval(out peval);
                                                        if (null == peval)
                                                        {
                                                            dout.WriteLine("Cannot evaluate at this time");
                                                        }
                                                        else
                                                        {
                                                            //ICorDebugFunction ifunc;
                                                            imoduleMscorlib.GetFunctionFromToken(ftoken, out ifunc);
                                                            //ICorDebugValue pvalue;
                                                            pvalue = fargvalue0;
                                                            peval.CallFunction(ifunc, 1, ref pvalue);
                                                            needwait = true;
                                                        }
                                                    }

                                                }
                                            }
                                        }
                                        if (needwait)
                                        {
                                            ContinueUntil("EvalComplete");
                                        }
                                    }
                                    break;

                                case "quit":
                                case "exit":
                                    if (!ProcessExit)
                                    {
                                        lock (this)
                                        {
                                            idbgproc.Terminate(0);
                                        }
                                        Until("ExitProcess").WaitOne();
                                    }
                                    else
                                    {
                                        System.Threading.Thread.Sleep(1000);
                                    }
                                    ProcessExit = true;
                                    break;

                                default:
                                    dout.WriteLine("Error: Command '{0}' not found.", origcmd);
                                    break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
#if DEBUG
                        System.IO.File.WriteAllText(@"c:\mrdebug-errors.txt", DateTime.Now.ToString() + " - " + e.ToString());
#endif
                        dout.WriteLine("Command exception: " + e.ToString());
                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }


            void OutputWriteColor(string s, ConsoleColor c)
            {
                if (object.ReferenceEquals(Console.Out, dout))
                {
                    ConsoleColor oldc = Console.ForegroundColor;
                    //try
                    {
                        Console.ForegroundColor = c;
                        dout.Write(s);
                    }
                    //finally
                    {
                        Console.ForegroundColor = oldc;
                    }
                }
                else
                {
                    dout.Write(s);
                }
            }


            string LastCurrentLineFileName = "";
            string[] LastCurrentLines;

            public void ShowCurrentLine_unlocked()
            {
                WhereInfo wi;
                {
                    List<WhereInfo> wis = new List<WhereInfo>(1);
                    GetWhere(wis, 1);
                    if (wis.Count < 1)
                    {
                        OutputWriteColor("    Unable to find current line", ConsoleColor.Red);
                        dout.WriteLine();
                        return;
                    }
                    wi = wis[0];
                }
                if (-1 == wi.FileLineNumber)
                {
                    OutputWriteColor("    " + wi.Source, ConsoleColor.Red);
                    dout.WriteLine();
                    return;
                }
                if (wi.FileName != LastCurrentLineFileName)
                {
                    try
                    {
                        LastCurrentLines = System.IO.File.ReadAllLines(wi.FileName);
                        LastCurrentLineFileName = wi.FileName;
                    }
                    catch(Exception fe)
                    {
                        OutputWriteColor("    Unable to get current line: " + fe.Message, ConsoleColor.Red);
                        dout.WriteLine();
                        return;
                    }
                }
                if (HIDDEN_LINE_NUMBER == wi.FileLineNumber)
                {
                    OutputWriteColor("    Hidden location", ConsoleColor.Red);
                    dout.WriteLine();
                }
                else
                {
                    dout.Write("{0}: ", wi.FileLineNumber);
                    int ln = wi.FileLineNumber - 1;
                    if (ln < 0)
                    {
                        OutputWriteColor("    Invalid line number", ConsoleColor.Red);
                    }
                    else if (ln >= LastCurrentLines.Length)
                    {
                        OutputWriteColor("    Past end of file", ConsoleColor.Red);
                    }
                    else
                    {
                        OutputWriteColor("    " + LastCurrentLines[ln], ConsoleColor.Green);
                    }
                    dout.WriteLine();
                }
            }


            public struct WhereInfo
            {
                public string Source; // Problem message if problem.
                public string FileName; // Null if problem.
                public int FileLineNumber; // -1 if problem.
            }

            const int HIDDEN_LINE_NUMBER = 0xFEEFEE;

            // Returns index in appendto of current, or -1 if current isn't contained in the list.
            int GetWhere(List<WhereInfo> appendto, int max)
            {
                if (max < 1)
                {
                    return -1;
                }
                WhereInfo wi;
                ICorDebugThread ithd = idbgthd;
                ICorDebugChain ichain = null;
                try
                {
                    ithd.GetActiveChain(out ichain);
                }
                catch (System.Runtime.InteropServices.COMException ce)
                {
                }
                if (null == ichain)
                {
                    wi.Source = "Call stack not available at this time";
                    wi.FileName = null;
                    wi.FileLineNumber = -1;
                    appendto.Add(wi);
                    return -1;
                }
                else
                {
                    int CurrentFrameIndex = 0;
                    ICorDebugFrame iframe;
                    //ichain.GetActiveFrame(out iframe);
                    idbgthd.GetActiveFrame(out iframe);
                    for (int fi = 0; fi < max; fi++, iframe.GetCaller(out iframe))
                    {
                        if (null == iframe)
                        {
                            break;
                        }
                        for (; ; )
                        {
                            iframe.GetChain(out ichain);
                            int xmanaged = 0;
                            if (null != ichain)
                            {
                                ichain.IsManaged(out xmanaged);
                            }
                            if (0 == xmanaged)
                            {
                                iframe.GetCaller(out iframe);
                                if (null == iframe)
                                {
                                    break;
                                }
                                iframe.GetChain(out ichain);
                                if (null == ichain)
                                {
                                    continue;
                                }
                            }
                            break;
                        }
                        if (null == iframe)
                        {
                            break;
                        }
                        {
                            {
                                
                                {
                                    ICorDebugFunction ifunc;
                                    iframe.GetFunction(out ifunc);
                                    uint ftoken;
                                    ifunc.GetToken(out ftoken);

                                    ICorDebugModule imod;
                                    ifunc.GetModule(out imod);

                                    ICorDebugAssembly iasm;
                                    imod.GetAssembly(out iasm);
                                    string assemblyname;
                                    {
                                        IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                                        uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                                        imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                                        if (asmnamelen > PATH_BUFFER_LENGTH)
                                        {
                                            throw new Exception("Assembly path too long");
                                        }
                                        asmnamelen--; // Remove nul.
                                        assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                                        Marshal.FreeHGlobal(pasmnamebuf);
                                    }

                                    ICorDebugILFrame ilframe = iframe as ICorDebugILFrame;
                                    if (null != ilframe)
                                    {
                                        uint noffset;
                                        CorDebugMappingResult mapresult;
                                        ilframe.GetIP(out noffset, out mapresult);
                                        if (mapresult == CorDebugMappingResult.MAPPING_NO_INFO
                                            || mapresult == CorDebugMappingResult.MAPPING_UNMAPPED_ADDRESS)
                                        {
                                            wi.Source = "(source line information unavailable)";
                                            wi.FileName = null;
                                            wi.FileLineNumber = -1;
                                            appendto.Add(wi);
                                        }
                                        else
                                        {
                                            Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                                            IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                                            Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                                            object oimporter;
                                            disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                                            IntPtr pimporter = IntPtr.Zero;
                                            try
                                            {
                                                pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                                                Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                                                ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));

                                                ISymUnmanagedReader reader;
                                                int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                                                if (0 == hrreader)
                                                {
                                                    ISymUnmanagedMethod unmMethod;
                                                    reader.GetMethod(new System.Diagnostics.SymbolStore.SymbolToken((int)ftoken), out unmMethod);
                                                    if (null != unmMethod)
                                                    {
                                                        int seqptscount;
                                                        unmMethod.GetSequencePointCount(out seqptscount);
                                                        int[] spoffsets = new int[seqptscount];
                                                        ISymUnmanagedDocument[] spdocs = new ISymUnmanagedDocument[seqptscount];
                                                        int[] spstartlines = new int[seqptscount];
                                                        int[] spendlines = new int[seqptscount];
                                                        int[] spstartcols = new int[seqptscount];
                                                        int[] spendcols = new int[seqptscount];
                                                        int cPoints;
                                                        unmMethod.GetSequencePoints(seqptscount, out cPoints, spoffsets, spdocs, spstartlines, spstartcols, spendlines, spendcols);

                                                        if (cPoints > 0 && spoffsets[0] <= noffset)
                                                        {
                                                            int ix;
                                                            for (ix = 0; ix < cPoints; ix++)
                                                            {
                                                                if (spoffsets[ix] >= noffset)
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                            if (ix == cPoints || spoffsets[ix] != noffset)
                                                            {
                                                                ix--;
                                                            }
                                                            int linnum = spstartlines[ix];
                                                            ISymUnmanagedDocument idoc = spdocs[ix];

                                                            string url;
                                                            {
                                                                int urllen = 0;
                                                                idoc.GetURL(0, out urllen, null);
                                                                StringBuilder sburl = new StringBuilder(urllen);
                                                                idoc.GetURL(urllen, out urllen, sburl);
                                                                sburl.Length = urllen - 1; // Remove nul.
                                                                url = sburl.ToString();
                                                            }

                                                            uint ftypedeftoken;
                                                            string MethodName;
                                                            {
                                                                IMetaDataImport importer;
                                                                {
                                                                    object oimdi;
                                                                    Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                                                    imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                                                    importer = (IMetaDataImport)oimdi;
                                                                }
                                                                uint chMethod;
                                                                uint dwAttr;
                                                                IntPtr pvSigBlob;
                                                                uint cbSigBlob;
                                                                uint ulCodeRVA;
                                                                uint dwImplFlags;
                                                                int hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                                                                    null, 0, out chMethod,
                                                                    out dwAttr,
                                                                    out pvSigBlob, out cbSigBlob,
                                                                    out ulCodeRVA, out dwImplFlags);
                                                                Marshal.ThrowExceptionForHR(hrmethodprops);
                                                                char[] methodnamechars = new char[chMethod];
                                                                hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                                                                    methodnamechars, (uint)methodnamechars.Length, out chMethod,
                                                                    out dwAttr,
                                                                    out pvSigBlob, out cbSigBlob,
                                                                    out ulCodeRVA, out dwImplFlags);
                                                                Marshal.ThrowExceptionForHR(hrmethodprops);
                                                                chMethod--; // Remove nul.
                                                                MethodName = new string(methodnamechars, 0, (int)chMethod);
                                                            }

                                                            wi.Source = MethodName;
                                                            wi.FileName = url;
                                                            wi.FileLineNumber = linnum;
                                                            appendto.Add(wi);
                                                        }
                                                        else
                                                        {
                                                            wi.Source = "(source line information unavailable) <cannot find in source>";
                                                            wi.FileName = null;
                                                            wi.FileLineNumber = -1;
                                                            appendto.Add(wi);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        wi.Source = "(source line information unavailable) <no information>";
                                                        wi.FileName = null;
                                                        wi.FileLineNumber = -1;
                                                        appendto.Add(wi);
                                                    }

                                                }
                                                else
                                                {
                                                    wi.Source = "(source line information unavailable) <no IL>";
                                                    wi.FileName = null;
                                                    wi.FileLineNumber = -1;
                                                    appendto.Add(wi);
                                                }
                                            }
                                            finally
                                            {
                                                if (IntPtr.Zero != pimporter)
                                                {
                                                    Marshal.Release(pimporter);
                                                    pimporter = IntPtr.Zero;
                                                }
                                            }

                                        }
                                    }
                                    else
                                    {
                                        wi.Source = "(source line information unavailable) <no IL frame>";
                                        wi.FileName = null;
                                        wi.FileLineNumber = -1;
                                        appendto.Add(wi);
                                    }
                                }

                            }
                        }
                    }
                    return CurrentFrameIndex;
                }
                return -1;
            }


            ICorDebugValue MyFindLocalVarOrArg_unlocked(string name)
            {
                {
                    ICorDebugThread ithd;
                    idbgproc.GetThread(ActiveThreadID, out ithd);
                    ICorDebugFrame iframe;
                    ithd.GetActiveFrame(out iframe);
                    if (null == iframe)
                    {
                        //dout.WriteLine("Error: No frame available");
                        return null;
                    }
                    else
                    {
                        ICorDebugFunction ifunc;
                        iframe.GetFunction(out ifunc);
                        ICorDebugILFrame ilframe = iframe as ICorDebugILFrame;
                        if (null == ilframe)
                        {
                            //dout.WriteLine("Error: No IL frame");
                            return null;
                        }
                        else
                        {
                            uint ftoken;
                            ifunc.GetToken(out ftoken);

                            ICorDebugModule imod;
                            ifunc.GetModule(out imod);

                            ICorDebugAssembly iasm;
                            imod.GetAssembly(out iasm);
                            string assemblyname;
                            {
                                IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                                uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                                imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                                if (asmnamelen > PATH_BUFFER_LENGTH)
                                {
                                    throw new Exception("Assembly path too long");
                                }
                                asmnamelen--; // Remove nul.
                                assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                                Marshal.FreeHGlobal(pasmnamebuf);
                            }

                            Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                            IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                            Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                            object oimporter;
                            disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                            IntPtr pimporter = IntPtr.Zero;
                            try
                            {
                                pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                                Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                                ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));

                                ISymUnmanagedReader reader;
                                int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                                if (0 == hrreader)
                                {
                                    ISymUnmanagedMethod unmMethod;
                                    reader.GetMethod(new System.Diagnostics.SymbolStore.SymbolToken((int)ftoken), out unmMethod);
                                    if (null != unmMethod)
                                    {

                                        IMetaDataImport importer;
                                        {
                                            object oimdi;
                                            Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                            imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                            importer = (IMetaDataImport)oimdi;
                                        }

                                        uint ip;
                                        CorDebugMappingResult mappingresults;
                                        ilframe.GetIP(out ip, out mappingresults);

                                        ISymUnmanagedScope unmScope;
                                        unmMethod.GetRootScope(out unmScope);

                                        ICorDebugValue result;
                                        result = _FindLocal(name, ilframe, unmScope, ip);
                                        if (null != result)
                                        {
                                            return result;
                                        }
                                        result = _FindArg(name, ilframe, importer);
                                        return result;
                                    }

                                }
                                else
                                {
                                }
                            }
                            finally
                            {
                                if (IntPtr.Zero != pimporter)
                                {
                                    Marshal.Release(pimporter);
                                    pimporter = IntPtr.Zero;
                                }
                            }

                        }
                    }
                }
                return null;
            }


            static ICorDebugValue _FindLocal(string name, ICorDebugILFrame ilframe, ISymUnmanagedScope unmScope, uint ip)
            {
                int varcount;
                unmScope.GetLocalCount(out varcount);
                ISymUnmanagedVariable[] vars = new ISymUnmanagedVariable[varcount];
                unmScope.GetLocals(varcount, out varcount, vars);

                for (int iv = 0; iv < varcount; iv++)
                {
                    ISymUnmanagedVariable var = vars[iv];
                    string varname;
                    {
                        int namelen;
                        var.GetName(0, out namelen, null);
                        StringBuilder sbName = new StringBuilder(namelen);
                        var.GetName(sbName.Capacity, out namelen, sbName);
                        namelen--; // Remove nul.
                        sbName.Length = namelen;
                        varname = sbName.ToString();
                    }
                    if (name == varname)
                    {
                        int field1;
                        var.GetAddressField1(out field1);
                        ICorDebugValue pvalue;
                        ilframe.GetLocalVariable((uint)field1, out pvalue);
                        return pvalue;
                    }
                }

                int cChildren;
                unmScope.GetChildren(0, out cChildren, null);
                ISymUnmanagedScope[] children = new ISymUnmanagedScope[cChildren];
                unmScope.GetChildren(children.Length, out cChildren, children);
                for (int ic = 0; ic < cChildren; ic++)
                {
                    ICorDebugValue pvalue = _FindLocal(name, ilframe, children[ic], ip);
                    if (null != pvalue)
                    {
                        return pvalue;
                    }
                }

                return null;
            }


            static ICorDebugValue _FindArg(string name, ICorDebugILFrame ilframe, IMetaDataImport importer)
            {
                uint argcount;
                {
                    ICorDebugValueEnum ven;
                    ilframe.EnumerateArguments(out ven);
                    ven.GetCount(out argcount);
                }
                if (argcount < 1)
                {
                    return null;
                }

                uint ftoken;
                ilframe.GetFunctionToken(out ftoken);

                System.Reflection.MethodAttributes fattribs;
                {
                    uint chMethod;
                    uint dwAttr;
                    IntPtr pvSigBlob;
                    uint cbSigBlob;
                    uint ulCodeRVA;
                    uint dwImplFlags;
                    uint ftypedeftoken;
                    int hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                        null, 0, out chMethod,
                        out dwAttr,
                        out pvSigBlob, out cbSigBlob,
                        out ulCodeRVA, out dwImplFlags);
                    Marshal.ThrowExceptionForHR(hrmethodprops);
                    char[] methodnamechars = new char[chMethod];
                    hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                        methodnamechars, (uint)methodnamechars.Length, out chMethod,
                        out dwAttr,
                        out pvSigBlob, out cbSigBlob,
                        out ulCodeRVA, out dwImplFlags);
                    Marshal.ThrowExceptionForHR(hrmethodprops);
                    chMethod--; // Remove nul.
                    //MethodName = new string(methodnamechars, 0, (int)chMethod);
                    fattribs = (System.Reflection.MethodAttributes)dwAttr;
                }

                List<uint> paramtoks = new List<uint>();
                {
                    uint henum = 0;
                    try
                    {
                        uint[] aoneparamtok = new uint[1];
                        for (; ; )
                        {
                            uint count;
                            importer.EnumParams(ref henum, ftoken, aoneparamtok, 1, out count);
                            if (1 != count)
                            {
                                break;
                            }
                            paramtoks.Add(aoneparamtok[0]);
                        }
                    }
                    finally
                    {
                        importer.CloseEnum(henum);
                    }
                }

                uint ia = 0;
                int ip = 0; //paramtoks.Count - (int)argcount;
                for (; ia < argcount; ia++)
                {
                    string argname;
                    if (0 == ia
                            && System.Reflection.MethodAttributes.Static != (fattribs & System.Reflection.MethodAttributes.Static))
                    {
                        argname = "this";
                    }
                    else
                    {
                        if (ip >= paramtoks.Count)
                        {
                            argname = "unnamed_param_" + (1 + ia).ToString();
                        }
                        else
                        {
                            {
                                uint ptok = paramtoks[ip++];
                                uint parenttok;
                                uint pulSequence;
                                uint argnamelen = 0;
                                uint dwAttr, dwCPlusTypeFlag, cchValue;
                                IntPtr pValue;
                                importer.GetParamProps(ptok, out parenttok, out pulSequence, null, argnamelen, out argnamelen,
                                    out dwAttr, out dwCPlusTypeFlag, out pValue, out cchValue);
                                char[] argnamebuf = new char[argnamelen];
                                importer.GetParamProps(ptok, out parenttok, out pulSequence, argnamebuf, argnamelen, out argnamelen,
                                    out dwAttr, out dwCPlusTypeFlag, out pValue, out cchValue);
                                argnamelen--; // Remove nul.
                                argname = new string(argnamebuf, 0, (int)argnamelen);
                            }
                        }
                    }
                    if (name == argname)
                    {
                        ICorDebugValue pvalue;
                        ilframe.GetArgument(ia, out pvalue);
                        return pvalue;
                    }
                }
                return null;
            }


            static void _PrintLocals(ICorDebugILFrame ilframe, ISymUnmanagedScope unmScope, uint ip, System.IO.TextWriter writer)
            {
                int varcount;
                unmScope.GetLocalCount(out varcount);
                ISymUnmanagedVariable[] vars = new ISymUnmanagedVariable[varcount];
                unmScope.GetLocals(varcount, out varcount, vars);

                for (int iv = 0; iv < varcount; iv++)
                {
                    ISymUnmanagedVariable var = vars[iv];
                    string varname;
                    {
                        int namelen;
                        var.GetName(0, out namelen, null);
                        StringBuilder sbName = new StringBuilder(namelen);
                        var.GetName(sbName.Capacity, out namelen, sbName);
                        namelen--; // Remove nul.
                        sbName.Length = namelen;
                        varname = sbName.ToString();
                    }
                    string valstr;
                    {
                        int field1;
                        var.GetAddressField1(out field1);
                        ICorDebugValue pvalue;
                        ilframe.GetLocalVariable((uint)field1, out pvalue);
                        valstr = ToString(pvalue);
                    }
                    writer.WriteLine("{0}={1}", varname, valstr);
                }

                int cChildren;
                unmScope.GetChildren(0, out cChildren, null);
                ISymUnmanagedScope[] children = new ISymUnmanagedScope[cChildren];
                unmScope.GetChildren(children.Length, out cChildren, children);
                for (int ic = 0; ic < cChildren; ic++)
                {
                    _PrintLocals(ilframe, children[ic], ip, writer);
                }

            }


            static void _PrintArgs(ICorDebugILFrame ilframe, IMetaDataImport importer, System.IO.TextWriter writer)
            {
                uint argcount;
                {
                    ICorDebugValueEnum ven;
                    ilframe.EnumerateArguments(out ven);
                    ven.GetCount(out argcount);
                }
                if (argcount < 1)
                {
                    return;
                }

                uint ftoken;
                ilframe.GetFunctionToken(out ftoken);

                System.Reflection.MethodAttributes fattribs;
                {
                    uint chMethod;
                    uint dwAttr;
                    IntPtr pvSigBlob;
                    uint cbSigBlob;
                    uint ulCodeRVA;
                    uint dwImplFlags;
                    uint ftypedeftoken;
                    int hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                        null, 0, out chMethod,
                        out dwAttr,
                        out pvSigBlob, out cbSigBlob,
                        out ulCodeRVA, out dwImplFlags);
                    Marshal.ThrowExceptionForHR(hrmethodprops);
                    char[] methodnamechars = new char[chMethod];
                    hrmethodprops = (int)importer.GetMethodProps(ftoken, out ftypedeftoken,
                        methodnamechars, (uint)methodnamechars.Length, out chMethod,
                        out dwAttr,
                        out pvSigBlob, out cbSigBlob,
                        out ulCodeRVA, out dwImplFlags);
                    Marshal.ThrowExceptionForHR(hrmethodprops);
                    chMethod--; // Remove nul.
                    //MethodName = new string(methodnamechars, 0, (int)chMethod);
                    fattribs = (System.Reflection.MethodAttributes)dwAttr;
                }

                List<uint> paramtoks = new List<uint>();
                {
                    uint henum = 0;
                    try
                    {
                        uint[] aoneparamtok = new uint[1];
                        for (; ; )
                        {
                            uint count;
                            importer.EnumParams(ref henum, ftoken, aoneparamtok, 1, out count);
                            if (1 != count)
                            {
                                break;
                            }
                            paramtoks.Add(aoneparamtok[0]);
                        }
                    }
                    finally
                    {
                        importer.CloseEnum(henum);
                    }
                }

                uint ia = 0;
                int ip = 0; //paramtoks.Count - (int)argcount;
                for (; ia < argcount; ia++)
                {
                    string argname;
                    if (0 == ia
                            && System.Reflection.MethodAttributes.Static != (fattribs & System.Reflection.MethodAttributes.Static))
                    {
                        argname = "this";
                    }
                    else
                    {
                        if (ip >= paramtoks.Count)
                        {
                            argname = "unnamed_param_" + (1 + ia).ToString();
                        }
                        else
                        {
                            {
                                uint ptok = paramtoks[ip++];
                                uint parenttok;
                                uint pulSequence;
                                uint argnamelen = 0;
                                uint dwAttr, dwCPlusTypeFlag, cchValue;
                                IntPtr pValue;
                                importer.GetParamProps(ptok, out parenttok, out pulSequence, null, argnamelen, out argnamelen,
                                    out dwAttr, out dwCPlusTypeFlag, out pValue, out cchValue);
                                char[] argnamebuf = new char[argnamelen];
                                importer.GetParamProps(ptok, out parenttok, out pulSequence, argnamebuf, argnamelen, out argnamelen,
                                    out dwAttr, out dwCPlusTypeFlag, out pValue, out cchValue);
                                argnamelen--; // Remove nul.
                                argname = new string(argnamebuf, 0, (int)argnamelen);
                            }
                        }
                    }
                    string argstr;
                    {
                        ICorDebugValue pvalue;
                        ilframe.GetArgument(ia, out pvalue);
                        argstr = ToString(pvalue);
                    }
                    writer.WriteLine("{0}={1}", argname, argstr);
                }
            }


            internal static char[] _PathSlashChars = new char[] { '\\', '/' };

            // Considers if a full path or just name is provided for either/both.
            public static bool FileNamesMatch(string fn1, string fn2)
            {
                int ilslash1 = fn1.LastIndexOfAny(_PathSlashChars);
                int ilslash2 = fn2.LastIndexOfAny(_PathSlashChars);
                if (-1 != ilslash1)
                {
                    if (-1 != ilslash2)
                    {
                    }
                    else
                    {
                        fn1 = fn1.Substring(ilslash1 + 1);
                    }
                }
                else
                {
                    if (-1 != ilslash2)
                    {
                        fn2 = fn2.Substring(ilslash2 + 1);
                    }
                }
                return 0 == string.Compare(fn1, fn2, true);
            }


            public static ICorDebugValue Dereference(ICorDebugValue pvalue)
            {
                for (; ; )
                {
                    ICorDebugReferenceValue pref = pvalue as ICorDebugReferenceValue;
                    if (null != pref)
                    {
                        int isnull;
                        pref.IsNull(out isnull);
                        if (0 != isnull)
                        {
                            return null;
                        }
                        pref.Dereference(out pvalue);
                        continue;
                    }
                    break;
                }
                return pvalue;
            }


            public static string ToString(ICorDebugValue pvalue)
            {
                if (null == pvalue)
                {
                    return "<N/A>";
                }
                pvalue = Dereference(pvalue);
                if (null == pvalue)
                {
                    return "<null>";
                }
                uint type;
                pvalue.GetType(out type);
                CorElementType cortype = (CorElementType)type;
                switch (cortype)
                {
                    case CorElementType.ELEMENT_TYPE_ARRAY:
                    case CorElementType.ELEMENT_TYPE_SZARRAY:
                        return "array";

                    case CorElementType.ELEMENT_TYPE_I1:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            sbyte x = default(sbyte);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_U1:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            byte x = default(byte);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_I2:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            Int16 x = default(Int16);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_U2:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            UInt16 x = default(UInt16);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_I4:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            Int32 x = default(Int32);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_U4:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            UInt32 x = default(UInt32);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_I:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            IntPtr x = default(IntPtr);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_U:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            UIntPtr x = default(UIntPtr);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_I8:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            Int64 x = default(Int64);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_U8:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            UInt64 x = default(UInt64);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_R4:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            Single x = default(Single);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }
                    case CorElementType.ELEMENT_TYPE_R8:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            Double x = default(Double);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return x.ToString();
                        }

                    case CorElementType.ELEMENT_TYPE_BOOLEAN:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            byte x = default(byte);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return (0 != x) ? "true" : "false";
                        }

                    case CorElementType.ELEMENT_TYPE_CHAR:
                        unsafe
                        {
                            ICorDebugGenericValue pgen = (ICorDebugGenericValue)pvalue;
                            char x = default(char);
                            IntPtr px = new IntPtr(&x);
                            pgen.GetValue(px);
                            return "'" + x.ToString() + "'";
                        }

                    case CorElementType.ELEMENT_TYPE_PTR:
                        return "<non-null pointer>";

                    case CorElementType.ELEMENT_TYPE_BYREF:
                    case CorElementType.ELEMENT_TYPE_TYPEDBYREF:
                    case CorElementType.ELEMENT_TYPE_OBJECT:
                        return "<printing value of type: not implemented>";

                    case CorElementType.ELEMENT_TYPE_CLASS:
                    case CorElementType.ELEMENT_TYPE_VALUETYPE:
                        {
                            ICorDebugObjectValue pobj = pvalue as ICorDebugObjectValue;
                            if (null == pobj)
                            {
                                ICorDebugBoxValue pbox = pvalue as ICorDebugBoxValue;
                                if (null == pbox)
                                {
                                    goto default;
                                }
                                pbox.GetObject(out pobj);
                                if (null == pobj)
                                {
                                    goto default;
                                }
                            }
                            ICorDebugClass pclass;
                            pobj.GetClass(out pclass);
                            uint ctoken;
                            pclass.GetToken(out ctoken);
                            if (ctoken == 0) // Class of globals.
                            {
                                return "";
                            }
                            ICorDebugValue2 pvalue2 = pvalue as ICorDebugValue2;
                            if (null != pvalue2)
                            {
                                ICorDebugType ptype;
                                pvalue2.GetExactType(out ptype);
                                ICorDebugModule imod;
                                pclass.GetModule(out imod);


                                IMetaDataImport importer;
                                {
                                    object oimdi;
                                    Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                    imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                    importer = (IMetaDataImport)oimdi;
                                }

                                string TypeDef;
                                {
                                    uint chTypeDef;
                                    uint dwTypeDefFlags;
                                    uint tkExtends;
                                    importer.GetTypeDefProps(ctoken, null, 0, out chTypeDef, out dwTypeDefFlags, out tkExtends);
                                    char[] typedefchars = new char[chTypeDef];
                                    importer.GetTypeDefProps(ctoken, typedefchars, (uint)typedefchars.Length, out chTypeDef, out dwTypeDefFlags, out tkExtends);
                                    chTypeDef--; // Remove nul.
                                    TypeDef = new string(typedefchars, 0, (int)chTypeDef);
                                }

                                //return TypeDef;
                                return "<" + TypeDef + ">";

                            }
                            return "";
                        }

                    case CorElementType.ELEMENT_TYPE_STRING:
                        {
                            ICorDebugStringValue pstr = (ICorDebugStringValue)pvalue;
                            uint cchStrx;
                            pstr.GetLength(out cchStrx);
                            IntPtr pStrx = Marshal.AllocHGlobal((int)cchStrx * 2);
                            pstr.GetString(cchStrx, out cchStrx, pStrx);
                            string Strx = Marshal.PtrToStringUni(pStrx, (int)cchStrx);
                            Marshal.FreeHGlobal(pStrx);
                            return "\"" + Strx.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
                        }

                    default:
                        return "<" + cortype.ToString() + ">";
                }
            }


            public void StepOut()
            {
                if (IsStepping)
                {
#if DEBUG
                    dout.WriteLine("DEBUG:  Cannot StepOut() (IsStepping)");
#endif
                    return;
                }

                ICorDebugStepper stepper;
                idbgfrm.CreateStepper(out stepper);
                stepper.StepOut();
                IsStepping = true;
                ContinueUntil("StepComplete");
                lock (this)
                {
#if DEBUG
                    if (IsStepping)
                    {
                        dout.WriteLine("DEBUG:  StepOut: whoops, should be still stepping (ignore at exit)");
                    }
#endif
                    IsStepping = false;
                }
            }

            public void _StepDirect(bool stepInto, bool User)
            {
                bool stepped = false;
                lock (this)
                {
                    if (User)
                    {
                        if (IsStepping)
                        {
#if DEBUG
                            dout.WriteLine("DEBUG:  Cannot Step(stepInto={0}) (IsStepping)", (stepInto ? "true" : "false"));
#endif
                            return;
                        }
                    }

                    //idbgthd.GetActiveFrame(out idbgfrm);
                    ICorDebugStepper stepper;
                    idbgfrm.CreateStepper(out stepper);
                    stepper.SetUnmappedStopMask(0); //STOP_NONE

                    ICorDebugILFrame ilframe = idbgfrm as ICorDebugILFrame;
                    uint ip;
                    CorDebugMappingResult mappingresults;
                    ilframe.GetIP(out ip, out mappingresults);

                    ICorDebugFunction f;
                    idbgfrm.GetFunction(out f);
                    ICorDebugModule imod;
                    f.GetModule(out imod);
                    UInt32 ftoken;
                    f.GetToken(out ftoken);

                    Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                    ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));
                    {
                        ICorDebugAssembly iasm;
                        imod.GetAssembly(out iasm);
                        string assemblyname;
                        {
                            IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                            uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                            imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                            if (asmnamelen > PATH_BUFFER_LENGTH)
                            {
                                throw new Exception("Assembly path too long");
                            }
                            asmnamelen--; // Remove nul.
                            assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                            Marshal.FreeHGlobal(pasmnamebuf);
                        }

                        Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                        IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                        Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                        object oimporter;
                        disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                        IntPtr pimporter = IntPtr.Zero;
                        try
                        {
                            pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                            ISymUnmanagedReader reader;
                            int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                            if (0 == hrreader)
                            {
                                ISymUnmanagedMethod unmMethod;
                                if (0 == reader.GetMethod(new System.Diagnostics.SymbolStore.SymbolToken((int)ftoken), out unmMethod))
                                {
                                    int seqptscount;
                                    unmMethod.GetSequencePointCount(out seqptscount);
                                    int[] spoffsets = new int[seqptscount];
                                    ISymUnmanagedDocument[] spdocs = new ISymUnmanagedDocument[seqptscount];
                                    int[] spstartlines = new int[seqptscount];
                                    int[] spendlines = new int[seqptscount];
                                    int[] spstartcols = new int[seqptscount];
                                    int[] spendcols = new int[seqptscount];
                                    int cPoints;
                                    unmMethod.GetSequencePoints(seqptscount, out cPoints, spoffsets, spdocs, spstartlines, spstartcols, spendlines, spendcols);

                                    COR_DEBUG_STEP_RANGE[] ranges = null;
                                    for (int j = 0; j < seqptscount; j++)
                                    {
                                        if (spoffsets[j] > ip)
                                        {
                                            ranges = new COR_DEBUG_STEP_RANGE[1];
                                            ranges[0].endOffset = (uint)spoffsets[j];
                                            ranges[0].startOffset = (uint)spoffsets[j - 1];
                                            break;
                                        }
                                    }
                                    if (ranges == null && seqptscount > 0)
                                    {
                                        ranges = new COR_DEBUG_STEP_RANGE[1];
                                        ranges[0].startOffset = (uint)spoffsets[seqptscount - 1];
                                        ICorDebugCode icode;
                                        f.GetILCode(out icode);
                                        uint codesize;
                                        icode.GetSize(out codesize);
                                        ranges[0].endOffset = codesize;
                                    }

                                    if (ranges != null)
                                    {
                                        IsStepping = true;
                                        stepper.StepRange(stepInto ? 1 : 0, ref ranges[0], (uint)ranges.Length);
                                        stepped = true;
                                    }
                                }
                            }
                        }
                        finally
                        {
                            if (IntPtr.Zero != pimporter)
                            {
                                Marshal.Release(pimporter);
                                pimporter = IntPtr.Zero;
                            }
                        }
                    }
                    if(!stepped)
                    {
                        IsStepping = true;
                        stepper.Step(0); // Step over non-IL.
                        stepped = true;
                    }
                }
                if (stepped)
                {
                    if (User)
                    {
                        ContinueUntil("StepComplete");
                        lock (this)
                        {
#if DEBUG
                            if (IsStepping)
                            {
                                dout.WriteLine("DEBUG:  Step: whoops, should be still stepping (ignore at exit)");
                            }
#endif
                            IsStepping = false;
                        }
                    }
                    else
                    {
                        idbgproc.Continue(0);
                    }
                }
            }

            public void Step(bool stepInto)
            {
                _StepDirect(stepInto, true);
            }


            internal uint[] _GetFieldTokens(IMetaDataImport importer, ICorDebugClass iclass)
            {
                uint ctoken;
                iclass.GetToken(out ctoken);
                uint henum = 0;
                List<uint> fieldtoks = new List<uint>();
                try
                {
                    uint[] aonefieldtok = new uint[1];
                    for (; ; )
                    {
                        uint count;
                        importer.EnumFields(ref henum, ctoken, aonefieldtok, 1, out count);
                        if (1 != count)
                        {
                            break;
                        }
                        fieldtoks.Add(aonefieldtok[0]);
                    }
                }
                finally
                {
                    importer.CloseEnum(henum);
                }
                return fieldtoks.ToArray();
            }

            internal string _FieldNameFromToken(IMetaDataImport importer, uint fieldtok)
            {
                uint fieldnamelen = 0;
                uint dwAttr;
                IntPtr pvSigBlob;
                uint cbSigBlob, dwCPlusTypeFlag;
                IntPtr pValue;
                uint cchValue;
                uint classtypetok;
                // 33554432/0x02000000 class type refers to the base type.
                importer.GetFieldProps(fieldtok, out classtypetok, null, fieldnamelen, out fieldnamelen,
                    out dwAttr, out pvSigBlob, out cbSigBlob, out dwCPlusTypeFlag, out pValue, out cchValue);
                char[] fieldnamebuf = new char[fieldnamelen];
                importer.GetFieldProps(fieldtok, out classtypetok, fieldnamebuf, fieldnamelen, out fieldnamelen,
                    out dwAttr, out pvSigBlob, out cbSigBlob, out dwCPlusTypeFlag, out pValue, out cchValue);
                if (0 == fieldnamelen)
                {
                    return null;
                }
                fieldnamelen--; // Remove nul.
                string fieldname = new string(fieldnamebuf, 0, (int)fieldnamelen);
                return fieldname;
            }

            internal uint _FindFieldTokenByName(IMetaDataImport importer, uint classtok, uint[] fieldtoks, string name)
            {
                for (int i = 0; i < fieldtoks.Length; i++)
                {
                    uint fieldtok = fieldtoks[i];
                    string fn = _FieldNameFromToken(importer, fieldtok);
                    if (fn == name)
                    {
                        return fieldtok;
                    }
                }
                return 0;
            }

            internal uint _FindFieldTokenByName(IMetaDataImport importer, ICorDebugClass iclass, uint[] fieldtoks, string name)
            {
                uint classtok;
                iclass.GetToken(out classtok);
                return _FindFieldTokenByName(importer, classtok, fieldtoks, name);
            }

            internal ICorDebugValue _GetExceptionInfo(out string emsg, out ulong eaddr)
            {
                ICorDebugValue ie = null;
                try
                {
                    idbgthd.GetCurrentException(out ie);
                }
                catch (COMException)
                {
                }
                ie = Dereference(ie);
                string _emsg = "N/A";
                ulong _eaddr = 0;
                if (null != ie)
                {
                    ie.GetAddress(out _eaddr);
                    //_emsg = DebugProcess.ToString(ie); // "<System.Exception>"
                    {
                        ICorDebugObjectValue ieobj = ie as ICorDebugObjectValue;
                        if (null != ieobj)
                        {
                            ICorDebugModule imod = imoduleMscorlib;

                            IMetaDataImport importer;
                            {
                                object oimdi;
                                Guid IMetaDataImportGUID = typeof(IMetaDataImport).GUID;
                                imod.GetMetaDataInterface(ref IMetaDataImportGUID, out oimdi);
                                importer = (IMetaDataImport)oimdi;
                            }

                            ICorDebugClass iclass;
                            {
                                uint SystemExceptionToken;
                                if (0 != importer.FindTypeDefByName("System.Exception", 0, out SystemExceptionToken))
                                {
                                    dout.WriteLine("Could not resolve {0}", "System.Exception");
                                }
                                //importerMscorlib.
                                imoduleMscorlib.GetClassFromToken(SystemExceptionToken, out iclass);
                            }

                            uint[] fieldtoks = _GetFieldTokens(importer, iclass);
                            uint fieldtok = _FindFieldTokenByName(importer, iclass, fieldtoks, "_message");
                            if (0 != fieldtok)
                            {
                                ICorDebugValue iemessage;
                                ieobj.GetFieldValue(iclass, fieldtok, out iemessage);
                                _emsg = ToString(iemessage);
                            }

                        }
                    }
                }
                emsg = _emsg;
                eaddr = _eaddr;
                return ie;
            }


            System.Threading.WaitHandle Until(string syncname)
            {
#if DEBUG
                if (null != this.CmdSyncName)
                {
                    Console.WriteLine("DEBUG:  Until: cannot wait on \"" + syncname + "\", already waiting on \"" + CmdSyncName + "\"");
                }
#endif
                //this.CmdSyncWait.Reset();
                this.CmdSyncName = syncname;
                return this.CmdSyncWait;
            }


            void ContinueUntil(string syncname)
            {
                System.Threading.WaitHandle wh = Until(syncname);
                lock (this)
                {
                    idbgproc.Continue(0);
                }
                wh.WaitOne();
            }


            public ICorDebug idbg;
            public ICorDebugProcess idbgproc;
            internal uint ActiveThreadID = uint.MaxValue;
            internal bool IsStepping = false;
            internal bool ProcessStarted = false;
            internal bool ProcessExit = false;
            internal bool EntryPointBreakpointSet = false;

            internal void _reseteventcache()
            {
                _idbgthd = null;
                _idbgfrm = null;
            }

            ICorDebugThread _idbgthd;
            internal ICorDebugThread idbgthd
            {
                get
                {
                    if (null != _idbgthd)
                    {
                        return _idbgthd;
                    }
                    if (ActiveThreadID == uint.MaxValue)
                    {
                        _idbgthd = null;
                    }
                    else
                    {
                        idbgproc.GetThread(ActiveThreadID, out _idbgthd);
                    }
                    if (null == _idbgthd)
                    {
                        throw new Exception("Active thread is null");
                    }
                    return _idbgthd;
                }
            }

            ICorDebugFrame _idbgfrm;
            internal ICorDebugFrame idbgfrm
            {
                get
                {
                    if (null != _idbgfrm)
                    {
                        return _idbgfrm;
                    }
                    idbgthd.GetActiveFrame(out _idbgfrm);
                    if (null == _idbgfrm)
                    {
                        throw new Exception("Active frame is null");
                    }
                    return _idbgfrm;
                }
            }

            internal List<ICorDebugModule> imodules;
            internal List<ICorDebugAppDomain> iappdomains;
            internal List<ICorDebugAssembly> iassemblies;
            internal ICorDebugModule imoduleMscorlib;
            internal List<ICorDebugFunctionBreakpoint> ibreakpoints;

            internal System.IO.TextReader din;
            internal System.IO.TextWriter dout;

            internal System.Threading.AutoResetEvent CmdSyncWait;
            internal string CmdSyncName = null;
            internal string lastcmdline = "";

            public bool FinishedInitializing = false;

        }


        // Note: callbacks run in another thread; lock on dbgproc.
        class DebugCallback : ICorDebugManagedCallback, ICorDebugManagedCallback2
        {
            DebugProcess dbgproc;

            public DebugCallback(DebugProcess dbgproc)
            {
                this.dbgproc = dbgproc;
            }


            int _resumes = 0;

            void _ResumeCallback()
            {
                //Console.WriteLine("_resumes={0}", _resumes++);

                if (null == dbgproc)
                {
                    throw new Exception("Critical error:  _ResumeCallback: DebugProcess is null");
                }
                lock (dbgproc)
                {
                    if (null == dbgproc.idbgproc)
                    {
                        throw new Exception("Critical error:  _ResumeCallback: ICorDebugProcess is null");
                    }
                    dbgproc.idbgproc.Continue(0);
                }
            }

            void _EnterCallback()
            {
                int i33 = 33;
            }

            void _CallbackEvent(string name, bool force)
            {
#if DEBUG
                Console.Out.Write("DEBUG:  Callback event: " + name);
                Console.Out.Flush();
#endif
                lock (dbgproc)
                {
                    bool samename = null != dbgproc.CmdSyncName && name == dbgproc.CmdSyncName;
                    if (force || samename)
                    {
#if DEBUG
                        if (force && !samename)
                        {
                            Console.WriteLine("DEBUG:  Broke though " + dbgproc.CmdSyncName + " with: " + name);
                        }
#endif
                        dbgproc.CmdSyncName = null;
                        dbgproc.CmdSyncWait.Set();
#if DEBUG
                        Console.Out.WriteLine(" signaled");
#endif
                    }
                    else
                    {
#if DEBUG
                        Console.Out.WriteLine(" not signaled");
#endif
                    }

                    dbgproc._reseteventcache();
                }
            }

            void _CallbackEvent(string name)
            {
                _CallbackEvent(name, false);
            }

            void _CallbackNotImplemented()
            {
                lock (dbgproc)
                {
                    _ResumeCallback();
                }
            }

            void _CallbackException(Exception e)
            {
                Console.WriteLine("<!> CRITICAL ERROR: " + e.ToString());
            }

            void _UpdateActiveThread(ICorDebugThread thread)
            {
                if (null == thread)
                {
                    int i33 = 33;
                }
                else
                {
                    lock (dbgproc)
                    {
                        //dbgproc.idbgthd = thread;
                        thread.GetID(out dbgproc.ActiveThreadID);
                        //thread.GetActiveFrame(out dbgproc.idbgfrm);
                    }
                }
            }

            bool _IsSourceAtSpecialPosition_unlocked()
            {
                bool isspecial = false;

                if (dbgproc.idbgfrm is ICorDebugILFrame)
                {
                    ICorDebugILFrame ilframe = dbgproc.idbgfrm as ICorDebugILFrame;
                    uint ip;
                    CorDebugMappingResult mapresult;
                    ilframe.GetIP(out ip, out mapresult);
                    if (mapresult == CorDebugMappingResult.MAPPING_NO_INFO || mapresult == CorDebugMappingResult.MAPPING_UNMAPPED_ADDRESS)
                    {
                        return false;
                    }                       

                    ICorDebugFunction f;
                    dbgproc.idbgfrm.GetFunction(out f);
                    ICorDebugModule imod;
                    f.GetModule(out imod);
                    UInt32 ftoken;
                    f.GetToken(out ftoken);

                    Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                    ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));
                    {
                        ICorDebugAssembly iasm;
                        imod.GetAssembly(out iasm);
                        string assemblyname;
                        {
                            IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                            uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                            imod.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                            if (asmnamelen > PATH_BUFFER_LENGTH)
                            {
                                throw new Exception("Assembly path too long");
                            }
                            asmnamelen--; // Remove nul.
                            assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                            Marshal.FreeHGlobal(pasmnamebuf);
                        }

                        Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                        IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                        Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                        object oimporter;
                        disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                        IntPtr pimporter = IntPtr.Zero;
                        try
                        {
                            pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                            ISymUnmanagedReader reader;
                            int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                            if (0 == hrreader)
                            {
                                ISymUnmanagedMethod unmMethod;
                                if (0 == reader.GetMethod(new System.Diagnostics.SymbolStore.SymbolToken((int)ftoken), out unmMethod))
                                {
                                    int seqptscount;
                                    unmMethod.GetSequencePointCount(out seqptscount);
                                    int[] spoffsets = new int[seqptscount];
                                    ISymUnmanagedDocument[] spdocs = new ISymUnmanagedDocument[seqptscount];
                                    int[] spstartlines = new int[seqptscount];
                                    int[] spendlines = new int[seqptscount];
                                    int[] spstartcols = new int[seqptscount];
                                    int[] spendcols = new int[seqptscount];
                                    int cPoints;
                                    unmMethod.GetSequencePoints(seqptscount, out cPoints, spoffsets, spdocs, spstartlines, spstartcols, spendlines, spendcols);

                                    {
                                        if ((seqptscount > 0) && (spoffsets[0] <= ip))
                                        {
                                            int i;
                                            for (i = 0; i < seqptscount; ++i)
                                            {
                                                if (spoffsets[i] >= ip)
                                                {
                                                    break;
                                                }
                                            } 

                                            if (i == seqptscount || spoffsets[i] != ip)
                                            {
                                                --i;
                                            }

                                            int specialseqpt =0xfeefee;
                                            if (spstartlines[i] == specialseqpt)
                                            {
                                                int j = i;
                                                while (j > 0)
                                                {
                                                    --j;
                                                    if (spstartlines[j] != specialseqpt)
                                                    {
                                                        isspecial = true;
                                                        break;
                                                    }
                                                }

                                                if (!isspecial)
                                                {
                                                    j = i;
                                                    while (++j < seqptscount)
                                                    {
                                                        if (spstartlines[j] != specialseqpt)
                                                        {
                                                            isspecial = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        finally
                        {
                            if (IntPtr.Zero != pimporter)
                            {
                                Marshal.Release(pimporter);
                                pimporter = IntPtr.Zero;
                            }
                        }
                    }
                }

                return isspecial;
            }

            public void Break(ICorDebugAppDomain pAppDomain, ICorDebugThread thread)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(thread);

                    lock (dbgproc)
                    {
                        dbgproc.dout.WriteLine("STOP UserBreak");

                        if (dbgproc.IsStepping)
                        {
                            _ResumeCallback();
                            return; // Skip _CallbackEvent!
                        }

                        dbgproc.ShowCurrentLine_unlocked();
                    }

                    _CallbackEvent("Break", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void Breakpoint(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugBreakpoint pBreakpoint)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(pThread);
                    lock (dbgproc)
                    {
                        ICorDebugFunctionBreakpoint iFuncBkpt = pBreakpoint as ICorDebugFunctionBreakpoint;
                        int BkptNum = 0;
                        if (null != iFuncBkpt)
                        {
                            BkptNum = 1 + dbgproc.ibreakpoints.IndexOf(iFuncBkpt);
                        }
                        if (BkptNum < 1)
                        {
                            dbgproc.dout.WriteLine("STOP: Breakpoint Hit");
                        }
                        else
                        {
                            dbgproc.dout.WriteLine("break at #{0}\t\t", BkptNum);
                        }

                        if (dbgproc.IsStepping)
                        {
                            _ResumeCallback();
                            return; // Skip _CallbackEvent!
                        }

                        /*ICorDebugFunctionBreakpoint pFuncBreakpoint = pBreakpoint as ICorDebugFunctionBreakpoint;
                        if (null == pFuncBreakpoint)
                        {
                        }
                        else
                        {
                            ICorDebugFunction pFunc;
                            pFuncBreakpoint.GetFunction(out pFunc);
                            ICorDebugCode pCode;
                            pFunc.GetILCode(out pCode);
                        }*/

                        if (dbgproc.FinishedInitializing)
                        {
                            dbgproc.ShowCurrentLine_unlocked();
                        }

                    }

                    _CallbackEvent("Breakpoint");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void BreakpointSetError(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugBreakpoint pBreakpoint, uint dwError)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(pThread);

                    _CallbackNotImplemented();

                    _CallbackEvent("BreakpointSetError", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ControlCTrap(ICorDebugProcess pProcess)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("ControlCTrap");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void CreateAppDomain(ICorDebugProcess pProcess, ICorDebugAppDomain pAppDomain)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        pAppDomain.Attach();

                        dbgproc.iappdomains.Add(pAppDomain);
                    }

                    _ResumeCallback();

                    _CallbackEvent("CreateAppDomain");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void CreateProcess(ICorDebugProcess pProcess)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        ICorDebugProcess2 iproc2 = pProcess as ICorDebugProcess2;
                        if (null != iproc2)
                        {
                            iproc2.SetDesiredNGENCompilerFlags(0x3); // CORDEBUG_JIT_DISABLE_OPTIMIZATION
                        }
                    }

                    _ResumeCallback();

                    _CallbackEvent("CreateProcess");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void CreateThread(ICorDebugAppDomain pAppDomain, ICorDebugThread thread)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(thread);

                    _ResumeCallback();

                    _CallbackEvent("CreateThread");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void DebuggerError(ICorDebugProcess pProcess, int errorHR, uint errorCode)
            {
                try
                {
                    _EnterCallback();

                    try
                    {
                        Marshal.ThrowExceptionForHR(errorHR);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }

                    _CallbackEvent("DebugError", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void EditAndContinueRemap(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugFunction pFunction, int fAccurate)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("EditAndContinueRemap");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void EvalComplete(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugEval pEval)
            {
                try
                {
                    _EnterCallback();

                    bool eventforce = false;

                    lock (dbgproc)
                    {
                        try
                        {
                            ICorDebugValue presult;
                            pEval.GetResult(out presult);
                            presult = DebugProcess.Dereference(presult);
                            ulong addr;
                            presult.GetAddress(out addr);
                            dbgproc.dout.WriteLine("$result= (0x{0:x}) {1}", addr, DebugProcess.ToString(presult));
                        }
                        catch
                        {
                            eventforce = true;
                            string emsg;
                            ulong eaddr;
                            dbgproc._GetExceptionInfo(out emsg, out eaddr);
                            dbgproc.dout.WriteLine("Function evaluation completed with an exception.");
                            dbgproc.dout.WriteLine("$result= (0x{0:x}) <System.Exception>", eaddr);
                            dbgproc.dout.WriteLine("_message=(0x{0:x}) {1}", eaddr, emsg);
                            dbgproc.dout.WriteLine(); // Needed.
                        }
                    }

                    _CallbackEvent("EvalComplete", eventforce);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void EvalException(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugEval pEval)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(pThread);

                    lock (dbgproc)
                    {
                        string emsg;
                        ulong eaddr;
                        dbgproc._GetExceptionInfo(out emsg, out eaddr);
                        dbgproc.dout.WriteLine("Function evaluation completed with an exception.");
                        dbgproc.dout.WriteLine("$result= (0x{0:x}) <System.Exception>", eaddr);
                        dbgproc.dout.WriteLine("_message=(0x{0:x}) {1}", eaddr, emsg);
                        dbgproc.dout.WriteLine(); // Needed.
                    }

                    _CallbackEvent("EvalException", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void Exception(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, int unhandled)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(pThread);

                    if (0 == unhandled)
                    {
                        // Handled, so keep going.
                        _ResumeCallback();

                        _CallbackEvent("Exception");
                    }
                    else
                    {
                        // Unhandled...
                        lock (dbgproc)
                        {
                            string emsg;
                            ulong eaddr;
                            dbgproc._GetExceptionInfo(out emsg, out eaddr);
                            dbgproc.dout.WriteLine("Unhandled exception generated: (0x{0:x})", eaddr);
                            dbgproc.dout.WriteLine("_message=(0x0) {0}", emsg);
                            dbgproc.dout.WriteLine("Exception is called: System.Exception");
                            dbgproc.dout.WriteLine();
                        }

                        _CallbackEvent("Exception", true);
                    }
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ExitAppDomain(ICorDebugProcess pProcess, ICorDebugAppDomain pAppDomain)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("ExitAppDomain");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ExitProcess(ICorDebugProcess pProcess)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        dbgproc.dout.WriteLine("Process exited.");
                        dbgproc.ProcessExit = true;
                    }

                    _CallbackEvent("ExitProcess", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ExitThread(ICorDebugAppDomain pAppDomain, ICorDebugThread thread)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("ExitThread");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void LoadAssembly(ICorDebugAppDomain pAppDomain, ICorDebugAssembly pAssembly)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        dbgproc.iassemblies.Add(pAssembly);
                    }

                    _ResumeCallback();

                    _CallbackEvent("LoadAssembly");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void LoadClass(ICorDebugAppDomain pAppDomain, ICorDebugClass c)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("LoadClass");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void LoadModule(ICorDebugAppDomain pAppDomain, ICorDebugModule pModule)
            {
                try
                {
                    _EnterCallback();
                    
                    string assemblyname;
                    bool thismodule = false;
                    lock (dbgproc)
                    {
                        dbgproc.imodules.Add(pModule);

                        ICorDebugModule2 imod2 = pModule as ICorDebugModule2;
                        if (null != imod2)
                        {
                            imod2.SetJITCompilerFlags(0x3); // CORDEBUG_JIT_DISABLE_OPTIMIZATION
                        }

                        //lock (dbgproc)
                        {
                            IntPtr pasmnamebuf = Marshal.AllocHGlobal(PATH_BUFFER_LENGTH * 2);
                            uint asmnamelen = (uint)PATH_BUFFER_LENGTH;
                            pModule.GetName(asmnamelen, out asmnamelen, pasmnamebuf);
                            if (asmnamelen > PATH_BUFFER_LENGTH)
                            {
                                throw new Exception("Assembly path too long");
                            }
                            asmnamelen--; // Remove nul.
                            assemblyname = Marshal.PtrToStringUni(pasmnamebuf, (int)asmnamelen);
                            Marshal.FreeHGlobal(pasmnamebuf);
                        }

                        if (assemblyname.EndsWith(".exe", true, null))
                        {
                            thismodule = true;
                            //lock (dbgproc) // ...
                            {
                                Guid CLSID_CorSymBinder = new Guid("0A29FF9E-7F9C-4437-8B11-F424491E3931");
                                ISymUnmanagedBinder binder = (ISymUnmanagedBinder)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_CorSymBinder));

                                ICorDebugModule imod = pModule;
                                //ICorDebugAssembly iasm;
                                //imod.GetAssembly(out iasm);

                                Guid CLSID_IMetaDataDispenser = new Guid(0xe5cb7a31, 0x7512, 0x11d2, 0x89, 0xce, 0x00, 0x80, 0xc7, 0x92, 0xe5, 0xd8);
                                IMetaDataDispenser disp = (IMetaDataDispenser)Activator.CreateInstance(Type.GetTypeFromCLSID(CLSID_IMetaDataDispenser));

                                Guid CLSID_IMetaDataImport = new Guid(0x7dac8207, 0xd3ae, 0x4c75, 0x9b, 0x67, 0x92, 0x80, 0x1a, 0x49, 0x7d, 0x44);
                                object oimporter;
                                disp.OpenScope(assemblyname, 0 /* OPEN_READ */, ref CLSID_IMetaDataImport, out oimporter);

                                IntPtr pimporter = IntPtr.Zero;
                                try
                                {
                                    pimporter = Marshal.GetComInterfaceForObject(oimporter, typeof(IMMImport));

                                    ISymUnmanagedReader reader;
                                    int hrreader = binder.GetReaderForFile(pimporter, assemblyname, null, out reader);
                                    if (0 == hrreader)
                                    {
                                        {
                                            System.Diagnostics.SymbolStore.SymbolToken symtok;
                                            int hruep = reader.GetUserEntryPoint(out symtok);
                                            if (0 == hruep)
                                            {
                                                //dbgproc.dout.WriteLine("Got entry point for " + assemblyname);

                                                ICorDebugFunction idbgFunc;
                                                imod.GetFunctionFromToken((uint)symtok.GetToken(), out idbgFunc);

                                                ICorDebugClass pclass;
                                                idbgFunc.GetClass(out pclass);

                                                ICorDebugFunctionBreakpoint pBkpt;
                                                idbgFunc.CreateBreakpoint(out pBkpt);
                                                pBkpt.Activate(1);

                                                dbgproc.EntryPointBreakpointSet = true;
                                                dbgproc.ProcessStarted = true;
                                            }

                                        }

                                    }
                                    else
                                    {
                                        dbgproc.EntryPointBreakpointSet = false;
                                        dbgproc.dout.WriteLine("Unable to find entry point (no debug information reader) for " + assemblyname);
                                    }

                                }
                                finally
                                {
                                    if (IntPtr.Zero != pimporter)
                                    {
                                        Marshal.Release(pimporter);
                                        pimporter = IntPtr.Zero;
                                    }
                                }

                            }
                        }
                        //else
                        {
                            if (assemblyname.EndsWith(@"\mscorlib.dll", true, null))
                            {
                                //lock (dbgproc)
                                {
                                    dbgproc.imoduleMscorlib = pModule;
                                }
                            }
                        }

                    }

                    _ResumeCallback();

                    _CallbackEvent("LoadModule");
                    /*{
                        string asmfn;
                        int ilslash = assemblyname.LastIndexOf('\\');
                        if (-1 == ilslash)
                        {
                            asmfn = assemblyname;
                        }
                        else
                        {
                            asmfn = assemblyname.Substring(ilslash + 1);
                        }
                        _CallbackEvent("LoadModule:" + asmfn);
                    }*/
                    if (thismodule)
                    {
                        _CallbackEvent("LoadModule:this");
                    }
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void LogMessage(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, int lLevel, ref ushort pLogSwitchName, ref ushort pMessage)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("LogMessage");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void LogSwitch(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, int lLevel, uint ulReason, ref ushort pLogSwitchName, ref ushort pParentName)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("LogSwitch");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void NameChange(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread)
            {
                try
                {
                    _EnterCallback();

                    // Domain or thread name changed.

                    _UpdateActiveThread(pThread);

                    _ResumeCallback();

                    _CallbackEvent("NameChange");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void StepComplete(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugStepper pStepper, CorDebugStepReason reason)
            {
                try
                {
                    _EnterCallback();

                    _UpdateActiveThread(pThread);

                    bool _isspecial;
                    lock (dbgproc)
                    {
                        _isspecial = _IsSourceAtSpecialPosition_unlocked();
                    }
                    if (_isspecial)
                    {
                        dbgproc._StepDirect(false, false);
                        return; // Don't fire this callback event; fire the next one.
                    }

                    lock (dbgproc)
                    {
#if DEBUG
                        if (!dbgproc.IsStepping)
                        {
                            dbgproc.dout.WriteLine("DEBUG:  StepComplete: (!dbgproc.IsStepping)");
                        }
#endif
                        dbgproc.IsStepping = false;

                        {
                            //dbgproc.dout.WriteLine("0: -");
                            dbgproc.ShowCurrentLine_unlocked();
                        }

                    }

                    _CallbackEvent("StepComplete");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void UnloadAssembly(ICorDebugAppDomain pAppDomain, ICorDebugAssembly pAssembly)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("UnloadAssembly");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void UnloadClass(ICorDebugAppDomain pAppDomain, ICorDebugClass c)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("UnloadClass");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void UnloadModule(ICorDebugAppDomain pAppDomain, ICorDebugModule pModule)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        dbgproc.imodules.Remove(pModule);
                    }

                    _ResumeCallback();

                    _CallbackEvent("UnloadModule");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void UpdateModuleSymbols(ICorDebugAppDomain pAppDomain, ICorDebugModule pModule, IStream pSymbolStream)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("UpdateModuleSymbols");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ChangeConnection(ICorDebugProcess pProcess, uint dwConnectionId)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("ChangeConnection");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void CreateConnection(ICorDebugProcess pProcess, uint dwConnectionId, ref ushort pConnName)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("CreateConnection");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void DestroyConnection(ICorDebugProcess pProcess, uint dwConnectionId)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("DestroyConnection");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void Exception(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugFrame pFrame, uint nOffset, CorDebugExceptionCallbackType dwEventType, uint dwFlags)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("Exception");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void ExceptionUnwind(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, CorDebugExceptionUnwindCallbackType dwEventType, uint dwFlags)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("ExceptionUnwind");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void FunctionRemapComplete(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugFunction pFunction)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("FunctionRemapComplete");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void FunctionRemapOpportunity(ICorDebugAppDomain pAppDomain, ICorDebugThread pThread, ICorDebugFunction pOldFunction, ICorDebugFunction pNewFunction, uint oldILOffset)
            {
                try
                {
                    _EnterCallback();

                    _CallbackNotImplemented();

                    _CallbackEvent("FunctionRemapOpportunity");
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

            public void MDANotification(ICorDebugController pController, ICorDebugThread pThread, ICorDebugMDA pMDA)
            {
                try
                {
                    _EnterCallback();

                    lock (dbgproc)
                    {
                        string desc;
                        {
                            uint len = 0;
                            pMDA.GetDescription(len, out len, IntPtr.Zero);
                            IntPtr pdesc = Marshal.AllocHGlobal((int)len * 2);
                            pMDA.GetDescription(len, out len, pdesc);
                            desc = Marshal.PtrToStringUni(pdesc, (int)len);
                            if (desc.EndsWith("\0"))
                            {
                                desc = desc.Substring(0, desc.Length - 1);
                            }
                            Marshal.FreeHGlobal(pdesc);
                        }
                        dbgproc.dout.WriteLine("MDA notification: {0}", desc);
                    }

                    _CallbackEvent("MDANotification", true);
                }
                catch (Exception e)
                {
                    _CallbackException(e);
                }
            }

        }


        static bool IsXDebug = false;
        static bool IsVerbose = false;

        [MTAThread]
        static void Main(string[] args)
        {

            if(4 != IntPtr.Size)
            {
                string bitserr = "Expected to be 32-bit process, not " + (IntPtr.Size * 8).ToString() + "-bit";
                Console.WriteLine("Error:  {0}", bitserr);
                Console.Error.WriteLine("{0}", bitserr);
                Environment.Exit(110);
            }

            Console.WriteLine("MR.Debug");

            int iarg = 0;

            if (args.Length > iarg && "-xdebug" == args[iarg])
            {
                iarg++;
                IsXDebug = true;
                IsVerbose = true;
            }
            if (args.Length > iarg && "-verbose" == args[iarg])
            {
                iarg++;
                IsVerbose = true;
            }

            string ClientProcessName;
            if (args.Length > iarg)
            {
                ClientProcessName = args[iarg++];
            }
            else
            {
                throw new ArgumentException("Expected client process name");
            }
            string ClientProcessArgs = "";
            if (args.Length > iarg)
            {
                ClientProcessArgs = args[iarg++];
            }

            if (IsVerbose)
            {
                Console.WriteLine("  Debugging: \"{0}\" \"{1}\"", ClientProcessName, ClientProcessArgs);
            }

            DebugProcess dbgproc = new DebugProcess();

            dbgproc.idbg = CreateDebuggingInterfaceFromVersion(3 /* 2.0 */, ClientProcessVersion);
            if (null == dbgproc.idbg)
            {
                throw new NotSupportedException("null debugger");
            }
            dbgproc.idbg.Initialize();
            dbgproc.idbg.SetManagedHandler(new DebugCallback(dbgproc));

            if (IsVerbose)
            {
                Console.WriteLine("  Initialized debugging interface");
            }

            _SECURITY_ATTRIBUTES nosecattribs = new _SECURITY_ATTRIBUTES();
            nosecattribs.bInheritHandle = 0;
            nosecattribs.lpSecurityDescriptor = IntPtr.Zero;
            nosecattribs.nLength = (uint)Marshal.SizeOf(typeof(_SECURITY_ATTRIBUTES));

            //PROCESS_INFORMATION pi = new PROCESS_INFORMATION();
            IntPtr ppi = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(PROCESS_INFORMATION)));

            STARTUPINFO si = new STARTUPINFO();
            si.cb = Marshal.SizeOf(typeof(STARTUPINFO));
            si.dwFlags = 0x00000100; // STARTF_USESTDHANDLES
            si.hStdInput = new IntPtr(0);
            si.hStdOutput = GetStdHandle(-11); // STD_OUTPUT_HANDLE
            si.hStdError = GetStdHandle(-12); // STD_ERROR_HANDLE
            IntPtr psi = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(STARTUPINFO)));
            Marshal.StructureToPtr(si, psi, false);

            lock (dbgproc)
            {
                dbgproc.idbg.CreateProcess(ClientProcessName, "\"" + ClientProcessName + "\" " + ClientProcessArgs,
                    ref nosecattribs, ref nosecattribs, 1, 0,
                    IntPtr.Zero, Environment.CurrentDirectory, (uint)psi.ToInt32(), (uint)ppi.ToInt32(),
                    CorDebugCreateProcessFlags.DEBUG_NO_SPECIAL_OPTIONS, out dbgproc.idbgproc);
            }

            if (IsVerbose)
            {
                Console.WriteLine("  Started debug process");
            }

            System.Threading.Thread runthread = new System.Threading.Thread(
                new System.Threading.ThreadStart(
                delegate()
                {
                    try
                    {
                        dbgproc.Run();
                    }
                    catch (Exception e)
                    {
                        string runthderr = "Run thread error: " + e.Message;
                        Console.WriteLine("Error:  {0}", runthderr);
                        Console.Error.WriteLine("{0}", runthderr);
                        Environment.Exit(111);
                    }
                }));
            runthread.Start();

            while (!dbgproc.FinishedInitializing)
            {
                System.Threading.Thread.Sleep(100);
            }
            for (; ; )
            {
                string line = Console.ReadLine();
                if (null == line)
                {
                    line = "quit";
                }
                dbgproc.AddCommand(line);
                if (dbgproc.ProcessExit)
                {
                    break;
                }
            }

            dbgproc.idbg.Terminate();

#if DEBUG
            Console.WriteLine("DEBUG:  exit");
#endif

        }


        public static string ClientProcessVersion
        {
            get
            {
                if (null == _clientver)
                {
                    int len;
                    GetCORVersion(null, 0, out len);
                    StringBuilder sb = new StringBuilder(len);
                    int hr = GetCORVersion(sb, sb.Capacity, out len);
                    Marshal.ThrowExceptionForHR(hr);
                    _clientver = sb.ToString();
                }
                return _clientver;
            }
        }

        static string _clientver = null;


        const int PATH_BUFFER_LENGTH = 384;


        #region External

        [StructLayout(LayoutKind.Sequential)]
        internal struct PROCESS_INFORMATION
        {
            public IntPtr hProcess;
            public IntPtr hThread;
            public int dwProcessId;
            public int dwThreadId;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal struct STARTUPINFO
        {
            public Int32 cb;
            public string lpReserved;
            public string lpDesktop;
            public string lpTitle;
            public Int32 dwX;
            public Int32 dwY;
            public Int32 dwXSize;
            public Int32 dwYSize;
            public Int32 dwXCountChars;
            public Int32 dwYCountChars;
            public Int32 dwFillAttribute;
            public Int32 dwFlags;
            public Int16 wShowWindow;
            public Int16 cbReserved2;
            public IntPtr lpReserved2;
            public IntPtr hStdInput;
            public IntPtr hStdOutput;
            public IntPtr hStdError;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("ole32.dll", ExactSpelling = true, PreserveSig = false)]
        [return: MarshalAs(UnmanagedType.Interface)]
        static extern object CoCreateInstance(
           [In, MarshalAs(UnmanagedType.LPStruct)] Guid rclsid,
           [MarshalAs(UnmanagedType.IUnknown)] object pUnkOuter,
           CLSCTX dwClsContext,
           [In, MarshalAs(UnmanagedType.LPStruct)] Guid riid);


        [Flags]
        enum CLSCTX : uint
        {
            CLSCTX_INPROC_SERVER = 0x1,
            CLSCTX_INPROC_HANDLER = 0x2,
            CLSCTX_LOCAL_SERVER = 0x4,
            CLSCTX_INPROC_SERVER16 = 0x8,
            CLSCTX_REMOTE_SERVER = 0x10,
            CLSCTX_INPROC_HANDLER16 = 0x20,
            CLSCTX_RESERVED1 = 0x40,
            CLSCTX_RESERVED2 = 0x80,
            CLSCTX_RESERVED3 = 0x100,
            CLSCTX_RESERVED4 = 0x200,
            CLSCTX_NO_CODE_DOWNLOAD = 0x400,
            CLSCTX_RESERVED5 = 0x800,
            CLSCTX_NO_CUSTOM_MARSHAL = 0x1000,
            CLSCTX_ENABLE_CODE_DOWNLOAD = 0x2000,
            CLSCTX_NO_FAILURE_LOG = 0x4000,
            CLSCTX_DISABLE_AAA = 0x8000,
            CLSCTX_ENABLE_AAA = 0x10000,
            CLSCTX_FROM_DEFAULT_CONTEXT = 0x20000,
            CLSCTX_INPROC = CLSCTX_INPROC_SERVER | CLSCTX_INPROC_HANDLER,
            CLSCTX_SERVER = CLSCTX_INPROC_SERVER | CLSCTX_LOCAL_SERVER | CLSCTX_REMOTE_SERVER,
            CLSCTX_ALL = CLSCTX_SERVER | CLSCTX_INPROC_HANDLER
        }


        [DllImport("mscoree.dll", CharSet = CharSet.Unicode, PreserveSig = false)]
        static extern ICorDebug CreateDebuggingInterfaceFromVersion(int iDebuggerVersion, string szDebuggeeVersion);

        [DllImport("mscoree.dll", CharSet = CharSet.Unicode)]
        static extern int GetCORVersion([Out, MarshalAs(UnmanagedType.LPWStr)] StringBuilder szName, Int32 cchBuffer, out Int32 dwLength);


        [ComImport,
            Guid("AA544d42-28CB-11d3-bd22-0000f80849bd"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        interface ISymUnmanagedBinder
        {
            [PreserveSig]
            int GetReaderForFile(IntPtr importer,
                [MarshalAs(UnmanagedType.LPWStr)] String filename,
                [MarshalAs(UnmanagedType.LPWStr)] String SearchPath,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedReader retVal);

            [PreserveSig]
            int GetReaderFromStream(IntPtr importer,
                IStream stream,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedReader retVal);
        }


        [ComImport,
            Guid("9F60EEBE-2D9A-3F7C-BF58-80BC991C60BB"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        internal interface ISymUnmanagedVariable
        {
            void GetName(int cchName,
                out int pcchName,
                [MarshalAs(UnmanagedType.LPWStr)] StringBuilder szName);

            void GetAttributes(out int pRetVal);

            void GetSignature(int cSig,
                out int pcSig,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] byte[] sig);

            void GetAddressKind(out int pRetVal);

            void GetAddressField1(out int pRetVal);

            void GetAddressField2(out int pRetVal);

            void GetAddressField3(out int pRetVal);

            void GetStartOffset(out int pRetVal);

            void GetEndOffset(out int pRetVal);
        }


        [ComImport,
            Guid("B4CE6286-2A6B-3712-A3B7-1EE1DAD467B5"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        interface ISymUnmanagedReader
        {
            void GetDocument([MarshalAs(UnmanagedType.LPWStr)] String url,
                Guid language,
                Guid languageVendor,
                Guid documentType,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedDocument retVal);

            void GetDocuments(int cDocs,
                out int pcDocs,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedDocument[] pDocs);

            [PreserveSig]
            int GetUserEntryPoint(out System.Diagnostics.SymbolStore.SymbolToken EntryPoint);

            [PreserveSig]
            int GetMethod(System.Diagnostics.SymbolStore.SymbolToken methodToken,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedMethod retVal);

            [PreserveSig]
            int GetMethodByVersion(System.Diagnostics.SymbolStore.SymbolToken methodToken,
                int version,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedMethod retVal);

            void GetVariables(System.Diagnostics.SymbolStore.SymbolToken parent,
                int cVars,
                out int pcVars,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] ISymUnmanagedVariable[] vars);

            void GetGlobalVariables(int cVars,
                out int pcVars,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedVariable[] vars);


            void GetMethodFromDocumentPosition(ISymUnmanagedDocument document,
                int line,
                int column,
                [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedMethod retVal);

            void GetSymAttribute(System.Diagnostics.SymbolStore.SymbolToken parent,
                [MarshalAs(UnmanagedType.LPWStr)] String name,
                int sizeBuffer,
                out int lengthBuffer,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)] byte[] buffer);

            void GetNamespaces(int cNameSpaces,
                out int pcNameSpaces,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedNamespace[] namespaces);

            void Initialize(IntPtr importer,
                [MarshalAs(UnmanagedType.LPWStr)] String filename,
                [MarshalAs(UnmanagedType.LPWStr)] String searchPath,
                IStream stream);

            void UpdateSymbolStore([MarshalAs(UnmanagedType.LPWStr)] String filename,
                IStream stream);

            void ReplaceSymbolStore([MarshalAs(UnmanagedType.LPWStr)] String filename,
                IStream stream);

            void GetSymbolStoreFileName(int cchName,
                out int pcchName,
                [MarshalAs(UnmanagedType.LPWStr)] StringBuilder szName);

            void GetMethodsFromDocumentPosition(ISymUnmanagedDocument document,
                int line,
                int column,
                int cMethod,
                out int pcMethod,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)] ISymUnmanagedMethod[] pRetVal);

            void GetDocumentVersion(ISymUnmanagedDocument pDoc,
                out int version,
                out Boolean pbCurrent);

            void GetMethodVersion(ISymUnmanagedMethod pMethod,
                out int version);
        }

        [ComImport,
            Guid("0DFF7289-54F8-11d3-BD28-0000F80849BD"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        interface ISymUnmanagedNamespace
        {
            void GetName(int cchName,
                out int pcchName,
                [MarshalAs(UnmanagedType.LPWStr)] StringBuilder szName);

            void GetNamespaces(int cNameSpaces,
                out int pcNameSpaces,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedNamespace[] namespaces);

            void GetVariables(int cVars,
                out int pcVars,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedVariable[] pVars);
        }


        [ComImport,
            Guid("68005D0F-B8E0-3B01-84D5-A11A94154942"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        interface ISymUnmanagedScope
        {
            void GetMethod([MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedMethod pRetVal);

            void GetParent([MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedScope pRetVal);

            void GetChildren(int cChildren,
                out int pcChildren,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedScope[] children);

            void GetStartOffset(out int pRetVal);

            void GetEndOffset(out int pRetVal);

            void GetLocalCount(out int pRetVal);

            void GetLocals(int cLocals,
                out int pcLocals,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedVariable[] locals);

            void GetNamespaces(int cNameSpaces,
                out int pcNameSpaces,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedNamespace[] namespaces);
        }


        [ComImport,
            Guid("B62B923C-B500-3158-A543-24F307A8B7E1"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        interface ISymUnmanagedMethod
        {
            void GetToken(out System.Diagnostics.SymbolStore.SymbolToken pToken);
            void GetSequencePointCount(out int retVal);
            void GetRootScope([MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedScope retVal);
            void GetScopeFromOffset(int offset, [MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedScope retVal);
            void GetOffset(ISymUnmanagedDocument document,
                int line,
                int column,
                out int retVal);
            void GetRanges(ISymUnmanagedDocument document,
                int line,
                int column,
                int cRanges,
                out int pcRanges,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)] int[] ranges);
            void GetParameters(int cParams,
                out int pcParams,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedVariable[] parms);
            void GetNamespace([MarshalAs(UnmanagedType.Interface)] out ISymUnmanagedNamespace retVal);
            void GetSourceStartEnd(ISymUnmanagedDocument[] docs,
                [In, Out, MarshalAs(UnmanagedType.LPArray)] int[] lines,
                [In, Out, MarshalAs(UnmanagedType.LPArray)] int[] columns,
                out Boolean retVal);
            void GetSequencePoints(int cPoints,
                out int pcPoints,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] int[] offsets,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] ISymUnmanagedDocument[] documents,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] int[] lines,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] int[] columns,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] int[] endLines,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] int[] endColumns);
        }


        [ComImport,
            Guid("40DE4037-7C81-3E1E-B022-AE1ABFF2CA08"),
            InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
            ComVisible(false)]
        internal interface ISymUnmanagedDocument
        {
            void GetURL(int cchUrl,
                out int pcchUrl,
                [MarshalAs(UnmanagedType.LPWStr)] StringBuilder szUrl);

            void GetDocumentType(ref Guid pRetVal);

            void GetLanguage(ref Guid pRetVal);

            void GetLanguageVendor(ref Guid pRetVal);

            void GetCheckSumAlgorithmId(ref Guid pRetVal);

            void GetCheckSum(int cData,
                out int pcData,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] byte[] data);

            void FindClosestLine(int line,
                out int pRetVal);

            void HasEmbeddedSource(out Boolean pRetVal);

            void GetSourceLength(out int pRetVal);

            void GetSourceRange(int startLine,
                int startColumn,
                int endLine,
                int endColumn,
                int cSourceBytes,
                out int pcSourceBytes,
                [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] byte[] source);

        }


        [Guid("809c652e-7396-11d2-9771-00a0c9b4d50c"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        [ComVisible(true)]
        private interface IMetaDataDispenser
        {
            void _EE2C198C776C4a9eADEEC68598C3F775();
            void OpenScope([In, MarshalAs(UnmanagedType.LPWStr)] String szScope, [In] Int32 dwOpenFlags, [In] ref Guid riid, [Out, MarshalAs(UnmanagedType.IUnknown)] out Object punk);
            // ...
        }


        [Guid("7DAC8207-D3AE-4c75-9B67-92801A497D44"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        [ComVisible(true)]
        public interface IMMImport
        {
            void ManagedMethod(ref int val);
        }


        [ComImport, Guid("7DAC8207-D3AE-4c75-9B67-92801A497D44"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        [ComVisible(true)]
        public interface IMetaDataImport
        {
            void CloseEnum(uint hEnum);

            uint CountEnum(uint hEnum, out uint count);

            uint ResetEnum(uint hEnum, uint ulPos);

            uint EnumTypeDefs(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rTypeDefs, uint cMax, out uint pcTypeDefs);

            uint EnumInterfaceImpls(ref uint phEnum, uint td, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rImpls, uint cMax, out uint pcImpls);

            uint EnumTypeRefs(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rTypeDefs, uint cMax, out uint pcTypeRefs);

            uint FindTypeDefByName([MarshalAs(UnmanagedType.LPWStr)]string szTypeDef, uint tkEnclosingClass, out uint ptd);

            uint GetScopeProps([MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)]char[] szName, uint cchName, out uint pchName, ref Guid pmvid);

            uint GetModuleFromScope(out uint pmd);

            uint GetTypeDefProps(uint td, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szTypeDef, uint cchTypeDef, out uint pchTypeDef, out uint pdwTypeDefFlags, out uint ptkExtends);

            uint GetInterfaceImplProps(uint iiImpl, out uint pClass, out uint ptkIface);

            uint GetTypeRefProps(uint tr, out uint ptkResolutionScope, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]char[] szName, uint cchName, out uint pchName);

            uint ResolveTypeRef(uint tr, ref Guid riid, [MarshalAs(UnmanagedType.Interface)]out object ppIScope, out uint ptd);

            uint EnumMembers(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rMembers, uint cMax, out uint pcTokens);

            uint EnumMembersWithName(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)]uint[] rMembers, uint cMax, out uint pcTokens);

            uint EnumMethods(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rMethods, uint cMax, out uint pcTokens);

            uint EnumMethodsWithName(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)]uint[] rMethods, uint cMax, out uint pcTokens);

            uint EnumFields(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rFields, uint cMax, out uint pcTokens);

            uint EnumFieldsWithName(ref uint phEnum, uint cl, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)]uint[] rFields, uint cMax, out uint pcTokens);

            uint EnumParams(ref uint phEnum, uint mb, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rParams, uint cMax, out uint pcTokens);

            uint EnumMemberRefs(ref uint phEnum, uint tkParent, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rMemberRefs, uint cMax, out uint pcTokens);

            uint EnumMethodImpls(ref uint phEnum, uint td, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rMethodBody, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rMethodDecl, uint cMax, out uint pcTokens);

            uint EnumPermissionSets(ref uint phEnum, uint tk, uint dwActions, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rPermission, uint cMax, out uint pcTokens);

            uint FindMember(uint td, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]byte[] pvSigBlob, uint cbSigBlob, out uint pmb);

            uint FindMethod(uint td, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]byte[] pvSigBlob, uint cbSigBlob, out uint pmb);

            uint FindField(uint td, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]byte[] pvSigBlob, uint cbSigBlob, out uint pmb);

            uint FindMemberRef(uint td, [MarshalAs(UnmanagedType.LPWStr)]string szName, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]byte[] pvSigBlob, int cbSigBlob, out uint pmr);

            uint GetMethodProps(uint mb, out uint pClass, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szMethod, uint cchMethod, out uint pchMethod, out uint pdwAttr, out IntPtr ppvSigBlob, out uint pcbSigBlob, out uint pulCodeRVA, out uint pdwImplFlags);

            uint GetMemberRefProps(uint mr, out uint ptk, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szMember, uint cchMember, out uint pchMember, out IntPtr ppvSigBlob, out uint pbSigBlob);

            uint EnumProperties(ref uint phEnum, uint td, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rProperties, uint cMax, out uint pcProperties);

            uint EnumEvents(ref uint phEnum, uint td, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rEvents, uint cMax, out uint pcEvents);

            uint GetEventProps(uint ev, out uint pClass, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szEvent, uint cchEvent, out uint pchEvent, out uint pdwEventFlags, out uint ptkEventType, out uint pmdAddOn, out uint pmdRemoveOn, out uint pmdFire, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 10)]uint[] rmdOtherMethod, uint cMax, out uint pcOtherMethod);

            uint EnumMethodSemantics(ref uint phEnum, uint mb, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]uint[] rEventProp, uint cMax, out uint pcEventProp);

            uint GetMethodSemantics(uint mb, uint tkEventProp, out uint pdwSemanticsFlags);

            uint GetClassLayout(uint td, out uint pdwPackSize, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]long[] rFieldOffset, uint cMax, out uint pcFieldOffset, out uint pulClassSize);

            uint GetFieldMarshal(uint tk, out IntPtr ppvNativeType, out uint pcbNativeType);

            uint GetRVA(uint tk, out uint pulCodeRVA, out uint pdwImplFlags);

            uint GetPermissionSetProps(uint pm, out uint pdwAction, out IntPtr ppvPermission, out uint pcbPermission);

            uint GetSigFromToken(uint mdSig, out IntPtr ppvSig, out uint pcbSig);

            uint GetModuleRefProps(uint mur, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]char[] szName, uint cchName, out uint pchName);

            uint EnumModuleRefs(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]uint[] rModuleRefs, uint cmax, out uint pcModuleRefs);

            uint GetTypeSpecFromToken(uint typespec, out IntPtr ppvSig, out uint pcbSig);

            uint GetNameFromToken(uint tk, out IntPtr pszUtf8NamePtr);

            uint EnumUnresolvedMethods(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]uint[] rMethods, uint cMax, out uint pcTokens);

            uint GetUserString(uint stk, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] char[] szString, uint cchString, out uint pchString);

            uint GetPinvokeMap(uint tk, out uint pdwMappingFlags, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szImportName, uint cchImportName, out uint pchImportName, out uint pmrImportDLL);

            uint EnumSignatures(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]uint[] rSignatures, uint cmax, out uint pcSignatures);

            uint EnumTypeSpecs(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]uint[] rTypeSpecs, uint cmax, out uint pcTypeSpecs);

            uint EnumUserStrings(ref uint phEnum, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)]uint[] rStrings, uint cmax, out uint pcStrings);

            uint GetParamForMethodIndex(uint md, uint ulParamSeq, out uint ppd);

            uint EnumCustomAttributes(ref uint phEnum, uint tk, uint tkType, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]uint[] rCustomAttributes, uint cMax, out uint pcCustomAttributes);

            uint GetCustomAttributeProps(uint cv, out uint ptkObj, out uint ptkType, out IntPtr ppBlob, out uint pcbSize);

            uint FindTypeRef(uint tkResolutionScope, [MarshalAs(UnmanagedType.LPWStr)]string szName, out uint ptr);

            uint GetMemberProps(uint mb, out uint pClass, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szMember, uint cchMember, out uint pchMember, out uint pdwAttr, out IntPtr ppvSigBlob, out uint pcbSigBlob, out uint pulCodeRVA, out uint pdwImplFlags, out uint pdwCPlusTypeFlag, out IntPtr ppValue, out uint pcchValue);

            uint GetFieldProps(uint mb, out uint pClass, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szField, uint cchField, out uint pchField, out uint pdwAttr, out IntPtr ppvSigBlob, out uint pcbSigBlob, out uint pdwCPlusTypeFlag, out IntPtr ppValue, out uint pcchValue);

            uint GetPropertyProps(uint prop, out uint pClass, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]char[] szProperty, uint cchProperty, out uint pchProperty, out uint pdwPropFlags, out IntPtr ppvSig, out uint pbSig, out uint pdwCPlusTypeFlag, out IntPtr ppDefaultValue, out uint pcchDefaultValue, out uint pmdSetter, out uint pmdGetter, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 13)]uint[] rmdOtherMethod, uint cMax, out uint pcOtherMethod);

            uint GetParamProps(uint tk, out uint pmd, out uint pulSequence, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)]char[] szName, uint cchName, out uint pchName, out uint pdwAttr, out uint pdwCPlusTypeFlag, out IntPtr ppValue, out uint pcchValue);

            uint GetCustomAttributeByName(uint tkObj, [MarshalAs(UnmanagedType.LPWStr)]string szName, out IntPtr ppData, out uint pcbData);

            bool IsValidToken(uint tk);

            uint GetNestedClassProps(uint tdNestedClass, out uint ptdEnclosingClass);

            uint GetNativeCallConvFromSig(IntPtr pvSig, uint cbSig, out uint pCallConv);

            uint IsGlobal(uint pd, out uint pbGlobal);
        }


        enum CorElementType
        {
            ELEMENT_TYPE_END = 0x0,
            ELEMENT_TYPE_VOID = 0x1,
            ELEMENT_TYPE_BOOLEAN = 0x2,
            ELEMENT_TYPE_CHAR = 0x3,
            ELEMENT_TYPE_I1 = 0x4,
            ELEMENT_TYPE_U1 = 0x5,
            ELEMENT_TYPE_I2 = 0x6,
            ELEMENT_TYPE_U2 = 0x7,
            ELEMENT_TYPE_I4 = 0x8,
            ELEMENT_TYPE_U4 = 0x9,
            ELEMENT_TYPE_I8 = 0xa,
            ELEMENT_TYPE_U8 = 0xb,
            ELEMENT_TYPE_R4 = 0xc,
            ELEMENT_TYPE_R8 = 0xd,
            ELEMENT_TYPE_STRING = 0xe,

            ELEMENT_TYPE_PTR = 0xf,
            ELEMENT_TYPE_BYREF = 0x10,

            ELEMENT_TYPE_VALUETYPE = 0x11,
            ELEMENT_TYPE_CLASS = 0x12,
            ELEMENT_TYPE_VAR = 0x13,
            ELEMENT_TYPE_ARRAY = 0x14,
            ELEMENT_TYPE_GENERICINST = 0x15,
            ELEMENT_TYPE_TYPEDBYREF = 0x16,

            ELEMENT_TYPE_I = 0x18,
            ELEMENT_TYPE_U = 0x19,
            ELEMENT_TYPE_FNPTR = 0x1B,
            ELEMENT_TYPE_OBJECT = 0x1C,
            ELEMENT_TYPE_SZARRAY = 0x1D,
            ELEMENT_TYPE_MVAR = 0x1e,

            ELEMENT_TYPE_CMOD_REQD = 0x1F,
            ELEMENT_TYPE_CMOD_OPT = 0x20,

            ELEMENT_TYPE_INTERNAL = 0x21,
            ELEMENT_TYPE_MAX = 0x22,

            ELEMENT_TYPE_MODIFIER = 0x40,
            ELEMENT_TYPE_SENTINEL = 0x01 | ELEMENT_TYPE_MODIFIER,
            ELEMENT_TYPE_PINNED = 0x05 | ELEMENT_TYPE_MODIFIER,
            ELEMENT_TYPE_R4_HFA = 0x06 | ELEMENT_TYPE_MODIFIER,
            ELEMENT_TYPE_R8_HFA = 0x07 | ELEMENT_TYPE_MODIFIER

        }

        #endregion


    }




}
