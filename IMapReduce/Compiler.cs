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

namespace MySpace.DataMining.DistributedObjects
{
    public class Compiler
    {


#if DEBUG
        bool _compilerdbg = true;
#else
        bool _compilerdbg = false;
#endif

        public bool DebugMode
        {
            get
            {
                return _compilerdbg;
            }

            set
            {
                _compilerdbg = value;
            }
        }


        public List<string> AssemblyReferences = new List<string>();

        string compileopts = "";
        string compilerversion = "v3.5";

        string getcompileropts()
        {
            string result = compileopts;

            if (_compilerdbg)
            {
                result = "/D:DEBUG /debug+ " + result;
            }

            /* // error CS1617: Invalid option 'ISO-2' for /langversion; must be ISO-1 or Default
            if (-1 == compileopts.IndexOf("/langversion:"))
            {
                result = "/langversion:ISO-2 " + result;
            }
             * */

            result = "/utf8output " + result;

            return result;
        }


        public string CompilerOptions
        {
            get
            {
                return compileopts;
            }

            set
            {
                compileopts = value;
            }
        }

        public string CompilerVersion
        {
            get
            {
                return compilerversion;
            }

            set
            {
                compilerversion = value;
            }
        }


        public void CleanOutput(string outputname)
        {
            try
            {
                System.IO.File.Delete(outputname);
                if (_compilerdbg)
                {
                    System.IO.File.Delete(outputname.Replace(".dll", ".pdb"));
                }
            }
            catch (Exception eee)
            {
                int i23zzz = 23 + 23;
                //LogLine("CleanCompilerOutput: Failed to delete compiler output: " + eee.ToString());
            }
        }


        public System.Reflection.Assembly CompileSource(string code, bool exe, string outputname)
        {
            return _CompileSource(code, false, exe, outputname);
        }

        public System.Reflection.Assembly CompileSourceFile(string path, bool exe, string outputname)
        {
            return _CompileSource(path, true, exe, outputname);
        }

        System.Reflection.Assembly _CompileSource(string Input, bool InputIsFile, bool exe, string outputname)
        {
            string[] asmrefs = new string[AssemblyReferences.Count];
            List<string> assemblydirs = new List<string>();
            char[] slashes = new char[] { '/', '\\' };
            for (int i = 0; i < asmrefs.Length; i++)
            {
                string ar = AssemblyReferences[i];
                int ils = ar.LastIndexOfAny(slashes);
                if (-1 != ils)
                {
                    assemblydirs.Add(ar.Substring(0, ils));
                    ar = ar.Substring(ils + 1);
                }
                asmrefs[i] = ar;
            }
            StringBuilder localcompileropts = new StringBuilder();
            {
                for (int i = 0; i < assemblydirs.Count; i++)
                {
                    localcompileropts.Append(" /lib:\"" + assemblydirs[i] + "\"");
                }
            }
            System.CodeDom.Compiler.CompilerParameters cp = new System.CodeDom.Compiler.CompilerParameters(asmrefs);
            cp.IncludeDebugInformation = _compilerdbg;
            System.CodeDom.Compiler.CompilerResults cr = null;
            bool alreadylogged = false;
            string reason = "";
            for (int rotor = 1; ; rotor++)
            {
#if DEBUG
                if (rotor > 3)
                {
                    throw new System.IO.FileNotFoundException("ArrayComboList.CompileSource dynamic C# compilation: Unable to create DLL" + reason);
                }
#endif
                try
                {
                    cp.OutputAssembly = outputname;
                    cp.GenerateExecutable = exe;
                    cp.GenerateInMemory = false;
                    cp.CompilerOptions = getcompileropts() + localcompileropts.ToString();

                    {
                        System.Threading.Mutex mdc = new System.Threading.Mutex(false, "DynCmp");
                        try
                        {
                            mdc.WaitOne();
                        }
                        catch (System.Threading.AbandonedMutexException)
                        {
                        }
                        try
                        {
                            Dictionary<string, string> providerOptions = new Dictionary<string, string>();
                            providerOptions["CompilerVersion"] = "v3.5";
                            using (Microsoft.CSharp.CSharpCodeProvider cscp = new Microsoft.CSharp.CSharpCodeProvider(providerOptions))
                            {
                                if (InputIsFile)
                                {
                                    cr = cscp.CompileAssemblyFromFile(cp, Input);
                                }
                                else
                                {
                                    cr = cscp.CompileAssemblyFromSource(cp, Input);
                                }
                            }
                        }
                        finally
                        {
                            mdc.ReleaseMutex();
                            mdc.Close();
                        }
                    }
                    if (cr.Errors.HasErrors)
                    {
                        try
                        {
                            lock (typeof(Compiler))
                            {
                                if (InputIsFile)
                                {
                                    System.IO.File.Copy(Input, "error.cs", true); // overwrite=true
                                }
                                else
                                {
                                    System.IO.File.WriteAllText("error.cs", Input);
                                }
                            }
                        }
                        catch
                        {
                        }
                        for (int i = 0; i < cr.Errors.Count; i++)
                        {
                            if (!cr.Errors[i].IsWarning)
                            {
                                throw new Exception("CompileSource code compile error: " + cr.Errors[i].ToString());
                            }
                        }
                        throw new Exception("CompileSource code compile error: " + cr.Errors[0].ToString());
                    }
                    if (0 != cr.NativeCompilerReturnValue)
                    {
                        //LogLine("CompileSource code compile did not return 0 (returned " + cr.NativeCompilerReturnValue.ToString() + "): ");
                        for (int i = 0; i < cr.Output.Count; i++)
                        {
                            string ss = cr.Output[i].Trim();
                            if (0 != ss.Length)
                            {
                                //LogLine("  C" + rotor.ToString() + "- " + cr.Output[i]);
                            }
                        }
                    }
#if DEBUG
                    if (rotor > 1)
                    {
                        System.Threading.Thread.Sleep(1000 * (rotor - 1));
                    }
#else
                    System.Threading.Thread.Sleep(1000 * rotor);
#endif

                    break; // Good.
                }
                catch (System.IO.IOException e)
                {
                    if (!alreadylogged)
                    {
                        alreadylogged = true;
                        //LogLine("Rotor retry: " + e.ToString());
                    }
                    reason = ": " + e.ToString();
                    continue;
                }
            }

            return cr.CompiledAssembly;
        }


    }

}

