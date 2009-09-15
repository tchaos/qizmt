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

//#define SLAVE_GET_LAST_ERROR


using System;
using System.Collections.Generic;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{
    public class ArrayComboList : DistObject5
    { 
        int keylen;
        int largestvaluelen = 0;
        protected byte[] buf;
        int addbufinitsize = 0;
        int _zmaps = 0;
        string sBlockID;
        ulong btreeCapSize = 0;


        public bool LocalCompile = true;


        public ArrayComboList(string objectname, int keylength)
            : base(objectname)
        {
            {
                int ilbid = objectname.LastIndexOf("_BlockID");
                if (-1 != ilbid)
                {
                    sBlockID = objectname.Substring(ilbid + 8);
                }
            }

            this.keylen = keylength;
            this.modbytes = keylength;

            this.buf = new byte[BUF_SIZE];

            EnableAddBuffer(); // ..!

            CommonCs.AddCommonAssemblyReferences(CompilerAssemblyReferences);

        }


        public override void Open()
        {
            base.Open();

            // Force all the slaves to cache their network path.
            foreach (SlaveInfo _slave in dslaves)
            {
                BufSlaveInfo slave = (BufSlaveInfo)_slave;
                slave.CacheNetworkPath();

                if (modbytes != keylen)
                {
                    slave.nstm.WriteByte((byte)'k');
                    Entry.ToBytes(modbytes, buf, 0);
                    XContent.SendXContent(slave.nstm, buf, 4);
                }

                if (jobfailover)
                {
                    slave.nstm.WriteByte((byte)'j');
                    Entry.ToBytes(1, buf, 0); // Enable job failover! (1)
                    XContent.SendXContent(slave.nstm, buf, 4);
                }

                // Send assembly references if not local compile.
                if (!LocalCompile)
                {
                    StringBuilder sb = new StringBuilder(16 + 20 * CompilerAssemblyReferences.Count);
                    for (int i = 0; i < CompilerAssemblyReferences.Count; i++)
                    {
                        if (CompilerAssemblyReferences[i].Length > 0)
                        {
                            if (sb.Length != 0)
                            {
                                sb.Append(';');
                            }
                            sb.Append(CompilerAssemblyReferences[i]);
                        }
                    }
                    sb.Append(";;");
                    sb.Append(CompilerOptions);

                    slave.nstm.WriteByte((byte)'W'); // Overwrites compiler assembly references and options on remote side.
                    XContent.SendXContent(slave.nstm, sb.ToString());
                }

            }
        }


        // Assumes capacity is "foo;bar" and appends ";keylen" for slave.
        public override void AddBlock(string capacity, string sUserBlockInfo)
        {
            ensurenotopen("AddBlock");
            ensurenotenumd("AddBlock");

            base.AddBlock(capacity + ";" + this.keylen.ToString(), sUserBlockInfo);
        }

        public virtual void AddBlock(string count_capacity, string estimated_row_capacity, string sUserBlockInfo)
        {
            AddBlock(count_capacity + ";" + estimated_row_capacity, sUserBlockInfo);
        }


        public void EnableAddBuffer(int buffersize)
        {
            if (buffersize <= 0)
            {
                throw new Exception("Invalid buffer size");
            }
            addbufinitsize = buffersize;
        }

        public void EnableAddBuffer()
        {
            EnableAddBuffer(FILE_BUFFER_SIZE);
        }


        public int GetKeyBufferLength()
        {
            return keylen;
        }

        public int GetValueBufferLength()
        {
            return largestvaluelen;
        }


        public override char GetDistObjectTypeChar()
        {
            return 'A';
        }


        /*
        // 1 is gzip, 0 is no compression.
        // If used, must be called before mapping!
        public byte CompressZMapBlocks
        {
            set
            {
                ensureopen("ZBlockExchange");
                ensurenotsortd("ZBlockExchange");
                ensurenotenumd("ZBlockExchange");

                foreach (SlaveInfo slave in dslaves)
                {
                    slave.nstm.WriteByte((byte)'');
                    buf[0] = value;
                    XContent.SendXContent(slave.nstm, buf, 1);
                }
            }
        }
         * */


        public void SortBlocks()
        {
            ensureopen("SortBlocks");
            ensurenotenumd("SortBlocks");
            sortd = true;

            // Enter all the locks...
            foreach (SlaveInfo slave in dslaves)
            {
                System.Threading.Monitor.Enter(slave); // !
            }
            try
            {
                // Sort all concurrently...
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;

                    //if (addbufinitsize > 0)
                    {
                        bool flushed = slave.FlushAddBuf_unlocked(); // !
                        if (flushed)
                        {
                            slave.SlaveErrorCheck();
                        }
                    }

                    slave.nstm.WriteByte((byte)'s'); // Sort.
                }
                // Wait for acks... (join!)
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;

                    if ((byte)'+' != slave.nstm.ReadByte())
                    {
                        slave.SlaveErrorCheck();
                        throw new Exception("SortBlocks error: Sub process " + slave.slaveID.ToString() + " did not return a valid response");
                    }
                    slave.SlaveErrorCheck();
                }
            }
            finally
            {
                // Release all locks...
                foreach (SlaveInfo slave in dslaves)
                {
                    System.Threading.Monitor.Exit(slave); // !
                }
            }
        }


        public void Add(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset, int valuelength)
        {
            ensureopen("Add");
            ensurenotenumd("Add");
            ensurenotsortd("Add");

            /* // key.Length is not the actual length.
            if (key.Length != this.keylen)
            {
                throw new Exception("Key length mismatch");
            }
             * */

            if (valuelength > largestvaluelen)
            {
                largestvaluelen = valuelength;
            }

            int slaveID = DetermineSlave(key, keyoffset, modbytes);

            /*if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Slave missing: slaveID needed: " + slaveID.ToString());
            }*/
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                BufSlaveInfo bslave = (BufSlaveInfo)slave;
                bslave.EnsureAddBuf_unlocked(addbufinitsize);
                bool flushed = bslave.AddBuf_unlocked(key, keyoffset, this.keylen, value, valueoffset, valuelength);
                if (flushed)
                {
                    bslave.SlaveErrorCheck();
                }
            }
        }

        public void Add(IList<byte> key, IList<byte> value, int valuelength)
        {
            if (key.Count != this.keylen)
            {
                throw new Exception("Key length mismatch; expected " + this.keylen.ToString() + " bytes, got " + key.Count.ToString());
            }

            Add(key, 0, value, 0, valuelength);
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            Add(key, value, value.Count);
        }

        public void Add(IList<byte> key, byte[] value)
        {
            /*
            if (key.Length != this.keylen)
            {
                throw new Exception("Key length mismatch");
            }
             * */

            Add(key, 0, value, 0, value.Length);
        }


        bool jobfailover = false;

        public bool JobFailover
        {
            get
            {
                return jobfailover;
            }

            set
            {
                jobfailover = value;
            }
        }


        int modbytes;

        // Similar KeyMajor but no negatives.
        public int KeyModByteCount
        {
            get
            {
                return modbytes;
            }

            set
            {
                modbytes = value;
            }
        }


        public ulong BTreeCapSize
        {
            get
            {
                return btreeCapSize;
            }
            set
            {
                btreeCapSize = value;
            }
        }


        protected Compiler compiler = new Compiler();


        public string CompilerOptions
        {
            get
            {
                return compiler.CompilerOptions;
            }

            set
            {
                compiler.CompilerOptions = value;
            }
        }

        public string CompilerVersion
        {
            get
            {
                return compiler.CompilerVersion;
            }

            set
            {
                compiler.CompilerVersion = value;
            }
        }


        public bool CompilerDebugMode
        {
            get
            {
                return compiler.DebugMode;
            }

            set
            {
                compiler.DebugMode = value;
            }
        }


        public List<string> CompilerAssemblyReferences
        {
            get
            {
                return compiler.AssemblyReferences;
            }
        }


        public void AddOpenCVExtension(int bits)
        {           
            CommonCs.AddOpenCVAssemblyReference(CompilerAssemblyReferences, bits);
        }

        public void AddOpenCVExtension()
        {
            AddOpenCVExtension(0);
        }





        public void AddUnsafe()
        {
            CompilerOptions += " /unsafe";
        }

        internal static string ExpandListCode(IList<string> args)
        {
            StringBuilder result = new StringBuilder();
            foreach (string earg in args)
            {
                if (0 != result.Length)
                {
                    result.Append(',');
                }
                result.Append("@`");
                result.Append(earg);
                result.Append('`');
            }
            return result.ToString();
        }


        // Returns the DLL's file contents.
        public byte[] CompilePluginSource(string code, bool wantassembly, ref System.Reflection.Assembly asm)
        {
            string outputname = Environment.CurrentDirectory + @"\temp_" + Guid.NewGuid() + ".dll";
            System.Reflection.Assembly a = compiler.CompileSource(code, false, outputname);
            byte[] dlldata = System.IO.File.ReadAllBytes(outputname);
            if (wantassembly)
            {
                asm = a;
            }
            else
            {
                compiler.CleanOutput(outputname);
            }
            return dlldata;
        }

        public byte[] CompilePluginSource(string code)
        {
            System.Reflection.Assembly asm = null;
            return CompilePluginSource(code, false, ref asm);
        }
        

        public System.Reflection.Assembly CompileSource(string code, bool exe, string outputname)
        {
            return compiler.CompileSource(code, exe, outputname);
        }

        public System.Reflection.Assembly CompileSourceFile(string path, bool exe, string outputname)
        {
            return compiler.CompileSource(path, exe, outputname);
        }


        public System.Reflection.Assembly CompileSourceRemote(string code, bool exe, string outputname)
        {
            {
                System.IO.FileInfo fioutput = new System.IO.FileInfo(outputname);
                outputname = fioutput.FullName;
            }

            ensureopen("CompileSourceRemote");

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'C'); // 'C' for Compile.

                XContent.SendXContent(slave.nstm, code);

                buf[0] = exe ? (byte)1 : (byte)0;
                buf[1] = compiler.DebugMode ? (byte)1 : (byte)0;
                XContent.SendXContent(slave.nstm, buf, 2);

                XContent.SendXContent(slave.nstm, outputname);

                switch (slave.nstm.ReadByte())
                {
                    case '+':
                        break; // Good!

                    case '-':
                        {
                            string ename = XContent.ReceiveXString(slave.nstm, buf);
                            string emsg = XContent.ReceiveXString(slave.nstm, buf);
                            throw new Exception(emsg);
                        }
                        break;

                    case -1:
                        throw new Exception("Socket closed while compiling output file " + outputname);

                    default:
                        throw new Exception("Unknown response from CompileSourceRemote for output file " + outputname);
                }

                break;
            }

            return System.Reflection.Assembly.LoadFile(outputname);
        }


        public void CleanCompilerOutput(string outputname)
        {
            compiler.CleanOutput(outputname);
        }


        public int CookTimeout = 1000 * 60;
        public int CookRetries = 64;


        void _BeforeLoad(byte[] dlldata, string classname, string pluginsource)
        {
            ensureopen("BeforeLoad");
            ensurenotsortd("BeforeLoad");
            ensurenotenumd("BeforeLoad");

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'F'); // 'F' for BeforeLoad (first)
                if (null == classname)
                {
                    XContent.SendXContent(slave.nstm, "");
                }
                else
                {
                    XContent.SendXContent(slave.nstm, classname);
                }
                XContent.SendXContent(slave.nstm, pluginsource);
                XContent.SendXContent(slave.nstm, dlldata, null == dlldata ? 0 : dlldata.Length);
            }

            // "Join"...
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'.'); // Ping.
                if ((int)',' != slave.nstm.ReadByte())
                {
                    throw new Exception("BeforeLoad sync error (pong)");
                }
            }
        }


        public virtual void BeforeLoadFullSource(string code, string classname)
        {
            if (CompilerInvoked != null)
            {
                CompilerInvoked("BeforeLoad", false);
            }
            byte[] dlldata = CompilePluginSource(code);
            //byte[] dlldata = null;
            if (CompilerInvoked != null)
            {
                CompilerInvoked("BeforeLoad", true);
            }
            _BeforeLoad(dlldata, classname, code);
        }

        public virtual void BeforeLoad(string code)
        {
            BeforeLoadFullSource(@"using System;
using System.Collections.Generic;
using System.Text;

using MySpace.DataMining.DistributedObjects;

namespace UserLoader
{
    public class BeforeLoader : MySpace.DataMining.DistributedObjects.IBeforeLoad
    {
        " + code + @"    }

}", "BeforeLoader");
        }


        public int ZMapBlockCount
        {
            get
            {
                return _zmaps;
            }

            set
            {
                ensurenotopen("ZMapBlockCount");
                _zmaps = value;
            }
        }


        public void ZBlockExchange(IList<ArrayComboList> all, IList<int> ownedzmapblockIDs)
        {          
            ensureopen("ZBlockExchange");
            ensurenotsortd("ZBlockExchange");
            ensurenotenumd("ZBlockExchange");

            byte[] ranges = new byte[ownedzmapblockIDs.Count * 4];
            for (int i = 0; i < ownedzmapblockIDs.Count; i++)
            {
                Entry.ToBytes(ownedzmapblockIDs[i], ranges, i * 4);
            }

            // Note: doesn't manage mutliple slaves for one ACL: won't exchange zmapblocks for other slaves in same ACL.
            string basepaths = "";
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < all.Count; i++)
                {
                    if (this != all[i]) // ...
                    {
                        if (all[i].dslaves.Count != 1)
                        {
                            throw new Exception("Can only have one sub process per ArrayComboList for ZBlockExchange");
                        }
                        {
                            BufSlaveInfo slave = (BufSlaveInfo)all[i].dslaves[0];
                            if (0 != sb.Length)
                            {
                                sb.Append(';');
                            }
                            /*
                            sb.Append(slave.Host);
                            sb.Append(':');
                            sb.Append(slave.zmapblockserverport);
                             * */
                            sb.Append(slave.NetworkPath + @"\" + slave.zmapblockbasename);
                        }
                    }
                }
                basepaths = sb.ToString();
            }

            // Even if hostsandports is 0-length, need to tell the slave to exchange so it moves from zmapblocks to zblocks...

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'y'); // 'y' for exchange.
                XContent.SendXContent(slave.nstm, ranges);
                XContent.SendXContent(slave.nstm, basepaths);
            }

            foreach (SlaveInfo slave in dslaves)
            {
                if ((int)'+' != slave.nstm.ReadByte())
                {
                    throw new Exception("ZBlockExchange failure; sub process did not report success");
                }
            }
        }


        // allinputdfsnodes are all the samples (of input nodes) for all the ACLs in this ACL-Group; DOES include .zsa.
        // sampleblockcount is number of ACLs in this ACL-Group.
        public void LoadSamples(IList<string> allinputsamples, int sampleblockcount, string sortclass)
        {
            ensureopen("LoadSamples");
            ensurenotsortd("LoadSamples");
            ensurenotenumd("LoadSamples");

            string fns = "";
            if(null != allinputsamples)
            {
                StringBuilder sb = new StringBuilder(64 * allinputsamples.Count);
                for (int i = 0; i < allinputsamples.Count; i++)
                {
                    if (0 != i)
                    {
                        sb.Append(';');
                    }
                    sb.Append(allinputsamples[i]);
                }
                fns = sb.ToString();
            }

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'K');
                Entry.ToBytes(sampleblockcount, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 4);
                XContent.SendXContent(slave.nstm, fns);
                Entry.ULongToBytes(BTreeCapSize, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 8);
                XContent.SendXContent(slave.nstm, sortclass);
            }

            PlusValidate("LoadSamples");
        }

        public void LoadSamples(IList<string> allinputsamples, int sampleblockcount)
        {
            LoadSamples(allinputsamples, sampleblockcount, "MdoDistro");
        }


        public int InputRecordLength = int.MinValue;
        public int OutputRecordLength = int.MinValue;


        void _DoMap(IList<string> inputdfsnodes, byte[] dlldata, string classname, string pluginsource)
        {
            ensureopen("DoMap");
            ensurenotsortd("DoMap");
            ensurenotenumd("DoMap");

            string fns = "";
            for (int i = 0; i < inputdfsnodes.Count; i++)
            {
                fns += inputdfsnodes[i] + ";";
            }
            fns = fns.Trim(';');

            foreach (SlaveInfo _slave in dslaves)
            {
                BufSlaveInfo slave = (BufSlaveInfo)_slave;
                slave.nstm.WriteByte((byte)'M'); // 'M' for Map
                if (null == classname)
                {
                    XContent.SendXContent(slave.nstm, "");
                }
                else
                {
                    XContent.SendXContent(slave.nstm, classname);
                }
                XContent.SendXContent(slave.nstm, pluginsource);
                XContent.SendXContent(slave.nstm, dlldata, null == dlldata ? 0 : dlldata.Length);

                slave.zmapblockbasename = "zmap_%n_" + Guid.NewGuid().ToString() + ".zm";
                XContent.SendXContent(slave.nstm, slave.zmapblockbasename);

                XContent.SendXContent(slave.nstm, fns); // All slaves of same DistObj get same input files.
                Entry.ToBytes(_zmaps, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 4);
            }

            // "Join"...
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'.'); // Ping.
                if ((int)',' != slave.nstm.ReadByte())
                {
                    throw new Exception("Map sync error (pong)");
                }
            }
        }


        public virtual void DoMapFullSource(IList<string> inputdfsnodes, string code, string classname)
        {
            byte[] dlldata = null;
            if (LocalCompile)
            {
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("Map", false);
                }
                dlldata = CompilePluginSource(code);
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("Map", true);
                }
            }
            _DoMap(inputdfsnodes, dlldata, classname, code);
        }


        public virtual string GetMapSource(string code, string[] usings, string classname)
        {
            string susings = "";
            if (usings != null)
            {
                foreach (string nm in usings)
                {
                    susings += "using " + nm + ";" + Environment.NewLine;
                }
            }
            return (@"using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;

" + susings + @"

namespace UserMapper
{
    public class " + classname + @"_Sample : " + classname + @"
    {
        public " + classname + @"_Sample()
        {
            _issample = true;
        }
    }

    public class " + classname + @" : MySpace.DataMining.DistributedObjects.IMap
    {
        public bool MapSampling
        {
            get
            {
                return _issample;
            }
        }
        protected bool _issample = false;

        const int InputRecordLength = " + InputRecordLength.ToString() + @";

        List<byte> _mapbuf = new List<byte>();
        bool _prevCr = false;
        bool _atype = " + (atype ? "true" : "false") + @";
        const int _CookTimeout = " + CookTimeout.ToString() + @";
        const int _CookRetries = " + CookRetries.ToString() + @";
        public void OnMap(MapInput input, MapOutput output)
        {
                StaticGlobals.MapIteration = -1;
                StaticGlobals.ReduceIteration = -1;
                StaticGlobals.DSpace_KeyLength = DSpace_KeyLength;
                StaticGlobals.DSpace_SlaveIP = DSpace_SlaveIP;
                StaticGlobals.DSpace_SlaveHost = DSpace_SlaveHost;
                StaticGlobals.DSpace_BlocksTotalCount = DSpace_BlocksTotalCount;
                StaticGlobals.DSpace_BlockID = DSpace_BlockID;
                StaticGlobals.ExecutionContext = ExecutionContextType.MAP;
                StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts) + @"};
                StaticGlobals.DSpace_OutputDirection = `" + StaticGlobals.DSpace_OutputDirection + @"`;
                StaticGlobals.DSpace_OutputDirection_ascending = " + (StaticGlobals.DSpace_OutputDirection_ascending ? "true" : "false") + @";
                StaticGlobals.DSpace_InputRecordLength = " + InputRecordLength.ToString() + @";
                StaticGlobals.DSpace_OutputRecordLength = " + OutputRecordLength.ToString() + @";
                StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";
                ").Replace('`', '"') + DGlobalsM.ToCode() + (@"
            
            //----------------------------COOKING--------------------------------
            bool cooking_is_cooked = false;
            int cooking_cooksremain = _CookRetries;
            bool cooking_is_read = false;
            long cooking_pos = 0; // Last known good position between records/lines.
            //----------------------------COOKING--------------------------------
            
            for(;;)//while cooked
            {
                try
                {
                    for(;;)//foreach record
                    {
                        _mapbuf.Clear();
                        long recordlen = 0;
                        int ib;
                        for(;;)//foreach byte
                        {
                            //----------------------------COOKING--------------------------------
                            cooking_is_read = true;
                            if(cooking_is_cooked)
                            {
                                cooking_is_cooked = false;
                                System.Threading.Thread.Sleep(_CookTimeout);
                                input.Reopen(cooking_pos);
                            }
                            //----------------------------COOKING--------------------------------
                            ib = input.Stream.ReadByte();
                            recordlen++;                            
                            if(--StaticGlobals.DSpace_InputBytesRemain <= 1)
                            {
                                StaticGlobals.DSpace_Last = true;
                            }
                            //----------------------------COOKING--------------------------------
                            cooking_is_read = false;
                            //----------------------------COOKING--------------------------------
                            if(-1 == ib)
                            {
                                if(0 != _mapbuf.Count)
                                {
                                    #if DEBUG
                                    if(InputRecordLength > 0)
                                    {
                                        throw new Exception(`DEBUG: EOF && (0 != _mapbuf.Count) && (InputRecordLength > 0)`);
                                    }
                                    #endif
                                    Stack.ResetStack();
                                    recordset.ResetBuffers();
                                    ++StaticGlobals.MapIteration;
                                    Map(ByteSlice.Create(_mapbuf), output);                            
                                }
                                return;
                            }
                            if(InputRecordLength > 0)
                            {
                                _mapbuf.Add((byte)ib);
                                #if DEBUG
                                if(recordlen != _mapbuf.Count)
                                {
                                    throw new Exception(`DEBUG: (recordlen != _mapbuf.Count)`);
                                }
                                if(recordlen > InputRecordLength)
                                {
                                    throw new Exception(`DEBUG: (recordlen > InputRecordLength)`);
                                }
                                #endif
                                if(recordlen == InputRecordLength)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                if('\r' == ib)
                                {
                                    _prevCr = true;
                                    break;
                                }
                                if('\n' == ib)
                                {
                                    if(_prevCr)
                                    {
                                        _prevCr = false;
                                        continue;
                                    }
                                    break;
                                }
                                _prevCr = false;
                                _mapbuf.Add((byte)ib);
                            }
                        }
                        if(!_atype)
                        {
                            break;
                        }
                        cooking_pos += recordlen;
                        Stack.ResetStack();
                        recordset.ResetBuffers();
                        ++StaticGlobals.MapIteration;
                        Map(ByteSlice.Create(_mapbuf), output);
                        
                        /*if(_issample)
                        {
                            break;
                        }*/
                    }
                }
                catch(Exception e)
                {
                    //----------------------------COOKING--------------------------------
                    if(!cooking_is_read)
                    {
                        throw;
                    }
                    if(cooking_cooksremain-- <= 0)
                    {
                        throw new System.IO.IOException(`cooked too many times`, e);
                    }
                    cooking_is_cooked = true;
                    continue;
                    //----------------------------COOKING--------------------------------
                }
                break;
            }

        }
        
        ").Replace('`', '"') + code + (@"
    }
}");
        }


        // inputdfsnodes must be accessible by the remote machine. Can be star-delimited failover names (starnames).
        public virtual void DoMap(IList<string> inputdfsnodes, string code, string[] usings)
        {
            const string classname = "DfsMapper";
            DoMapFullSource(inputdfsnodes, GetMapSource(code, usings, classname), classname);
        }


        public int FoilKeySkipFactor = 5000;

        public string FoilCacheName = null;


        void _BeginDoFoilMapSample(IList<string> inputdfsnodes, byte[] dlldata, string classname, string pluginsource)
        {
            ensureopen("DoFoilMapSample");
            ensurenotsortd("DoFoilMapSample");
            ensurenotenumd("DoFoilMapSample");

            string fns = "";
            for (int i = 0; i < inputdfsnodes.Count; i++)
            {
                fns += inputdfsnodes[i] + ";";
            }
            fns = fns.Trim(';');

            if (1 != dslaves.Count)
            {
                throw new Exception("DoFoilMapSample: Must be exactly 1 slave/AddBlock");
            }
            //foreach (SlaveInfo _slave in dslaves)
            {
                SlaveInfo _slave = dslaves[0];
                BufSlaveInfo slave = (BufSlaveInfo)_slave;
                slave.nstm.WriteByte((byte)'f');
                if (null == classname)
                {
                    XContent.SendXContent(slave.nstm, "");
                }
                else
                {
                    XContent.SendXContent(slave.nstm, classname);
                }
                XContent.SendXContent(slave.nstm, pluginsource);
                XContent.SendXContent(slave.nstm, dlldata, null == dlldata ? 0 : dlldata.Length);

                string zfin;
                if (null != FoilCacheName)
                {
                    if (string.IsNullOrEmpty(sBlockID))
                    {
                        throw new Exception("BlockID required for FoilMapSample");
                    }
                    zfin = FoilCacheName + "." + sBlockID; // Not using SafeTextPath here; cache name is always direct to fs.
                }
                else
                {
                    zfin = Guid.NewGuid().ToString();
                }
                slave.zfoilbasename = "zfoil_" + zfin + ".zf";
                XContent.SendXContent(slave.nstm, slave.zfoilbasename);

                XContent.SendXContent(slave.nstm, fns);

                Entry.ToBytes(FoilKeySkipFactor, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 4);

            }
        }

        public virtual void BeginDoFoilMapSample(IList<string> inputdfsnodes, string code, string[] usings)
        {
            const string classname = "DfsMapper";
            string source = GetMapSource(code, usings, classname);
            byte[] dlldata = null;
            if (LocalCompile)
            {
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("FoilMapSample", false);
                }
                dlldata = CompilePluginSource(source);
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("FoilMapSample", true);
                }
            }
            _BeginDoFoilMapSample(inputdfsnodes, dlldata, classname, source);
        }

        public void EndDoFoilMapSample()
        {
            Ping("DoFoilMapSample");
        }

        public void DoFoilMapSample(IList<string> inputdfsnodes, string code, string[] usings)
        {
            BeginDoFoilMapSample(inputdfsnodes, code, usings);
            EndDoFoilMapSample();
        }


        public virtual void Flush()
        {
            ensureopen("Flush");
            ensurenotenumd("Flush");

            foreach (SlaveInfo _slave in dslaves)
            {
                BufSlaveInfo slave = (BufSlaveInfo)_slave;
                lock (slave)
                {
                    bool flushed = slave.FlushAddBuf_unlocked();
                    if (flushed)
                    {
                        slave.SlaveErrorCheck();
                    }
                }
            }
        }


        public override void Close()
        {
            closed = true;

            base.Close();
        }


        public void CommitZBalls(string cachename, bool keepzblocks)
        {
            ensureopen("CommitZBalls");
            ensurewassortd("CommitZBalls");
            ensurenotenumd("CommitZBalls");

            if (null == sBlockID || "" == sBlockID)
            {
                throw new Exception("BlockID required");
            }

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'z');
                XContent.SendXContent(slave.nstm, cachename);
                XContent.SendXContent(slave.nstm, sBlockID);
                buf[0] = keepzblocks ? (byte)1 : (byte)0;
                XContent.SendXContent(slave.nstm, buf, 1);
            }

            foreach (SlaveInfo slave in dslaves)
            {
                if ((int)'+' != slave.nstm.ReadByte())
                {
                    throw new Exception("CommitZBalls: sub process did not report a success");
                }
            }
        }


        public bool IntegrateZBalls(string cachename)
        {
            ensureopen("IntegrateZBalls");
            ensurewassortd("IntegrateZBalls");
            ensurenotenumd("IntegrateZBalls");

            if (null == sBlockID || "" == sBlockID)
            {
                throw new Exception("BlockID required");
            }

            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'Z');
                XContent.SendXContent(slave.nstm, cachename);
                XContent.SendXContent(slave.nstm, sBlockID);
            }

            bool ok = true;
            foreach (SlaveInfo slave in dslaves)
            {
                if ((int)'+' != slave.nstm.ReadByte())
                {
                    //throw new Exception("IntegrateZBalls: slave did not report a success");
                    ok = false;
                }
            }
            return ok;
        }


        public string zfoilbasename
        {
            get
            {
                if (1 == dslaves.Count)
                {
                    BufSlaveInfo slave = dslaves[0] as BufSlaveInfo;
                    if (null != slave)
                    {
                        return slave.zfoilbasename;
                    }
                }
                return null;
            }
        }


        protected class BufSlaveInfo : SlaveInfo
        {
            byte[] addbuf = null;
            int addbuflen = 0; // Number of valid bytes in addbuf.

            internal int zmapblockserverport = 0;

            internal string zmapblockbasename;

            internal string zfoilbasename;

            private string _netpath = null;


            protected internal void CacheNetworkPath()
            {
                if (null == _netpath)
                {
                    _netpath = GetNetworkPath(nstm, Host);
                }
            }


            public string NetworkPath
            {
                get
                {
                    CacheNetworkPath();
                    return _netpath;
                }
            }


            internal void EnsureAddBuf_unlocked(int initsize)
            {
                if (null == addbuf)
                {
                    addbuf = new byte[initsize];
                }
            }


            // Returns true if flushed, false if nothing to flush.
            internal bool FlushAddBuf_unlocked()
            {
                if (addbuflen > 0)
                {
                    int xlen = addbuflen;
                    addbuflen = 0;
                    nstm.WriteByte((byte)'p'); // Put/publish batch.
                    XContent.SendXContent(nstm, addbuf, xlen);
                    return true;
                }
                return false;
            }


            // Returns true if flushed, false if buffered.
            internal bool AddBuf_unlocked(
                IList<byte> key, int keyoffset, int keylength,
                IList<byte> value, int valueoffset, int valuelength)
            {
                bool flushed = false;
                int xlen = keylength + 4 + valuelength;

                if (addbuflen + xlen > addbuf.Length)
                {
                    flushed = FlushAddBuf_unlocked();
                    addbuflen = 0;
                    if (xlen > addbuf.Length)
                    {
                        addbuf = new byte[xlen];
                    }
                }

                for (int i = 0; i != keylength; i++)
                {
                    addbuf[addbuflen + i] = key[keyoffset + i];
                }
                Entry.ToBytes(valuelength, addbuf, addbuflen + keylength);
                for (int i = 0; i != valuelength; i++)
                {
                    addbuf[addbuflen + keylength + 4 + i] = value[valueoffset + i];
                }
                addbuflen += xlen;

                return flushed;
            }


            byte[] errbuf = new byte[64];

            protected string GetLastErrorReset()
            {
#if SLAVE_GET_LAST_ERROR
                nstm.WriteByte((byte)'?');
                int len;
                errbuf = XContent.ReceiveXBytes(nstm, out len, errbuf);
                if (0 != len)
                {
                    return Encoding.UTF8.GetString(errbuf, 0, len);
                }
#endif
                return null; // No error.
            }

            public override void SlaveErrorCheck()
            {
                string msg = GetLastErrorReset();
                if (null != msg)
                {
                    throw new SlaveException(msg);
                }
            }

        }


        public override SlaveInfo createSlaveInfo()
        {
            return new BufSlaveInfo();
        }


        public void StartZMapBlockServer()
        {
            ensureopen("StartZMapBlockServer");
            ensurenotenumd("StartZMapBlockServer");

            lock (dslaves)
            {
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;
                    lock (slave)
                    {
                        slave.nstm.WriteByte((byte)'x');
                        int len;
                        buf = XContent.ReceiveXBytes(slave.nstm, out len, buf);
                        if (len >= 4)
                        {
                            slave.zmapblockserverport = Entry.BytesToInt(buf);
                            /*if (0 == slave.zmapblockserverport)
                            {
                                throw new Exception("StartZMapBlockServer failed; block info: " + slave.sblockinfo);
                            }*/
                        }
                    }
                }
            }
        }

        public void StopZMapBlockServer()
        {
            ensureopen("StopZMapBlockServer");

            lock (dslaves)
            {
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;
                    lock (slave)
                    {
                        slave.zmapblockserverport = 0;
                        slave.nstm.WriteByte((byte)'X');
                    }
                }
            }
        }


        public ArrayComboListEnumerator[] GetEnumerators(byte[] dlldata, string classname, string pluginsource)
        {
            ensureopen("GetEnumerators");
            ensurenotenumd("GetEnumerators");
            ensurewassortd("GetEnumerators");
            enumd = true;

            ArrayComboListEnumerator[] results;
            lock (dslaves)
            {
                int i;
                results = new ArrayComboListEnumerator[dslaves.Count];

                i = 0;
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;
                    lock (slave)
                    {
                        bool flushed = slave.FlushAddBuf_unlocked();
                        if (flushed)
                        {
                            slave.SlaveErrorCheck();
                        }
                    }
                    results[i] = new ArrayComboListEnumerator(this, slave, dlldata, classname, pluginsource);
                    results[i].buf = this.buf; // Not needed, but fine.
                    i++;
                }
            }
            return results;
        }

        public ArrayComboListEnumerator[] GetEnumerators(string dllfilepath, string classname)
        {
            byte[] dlldata = null;
            if (null != dllfilepath)
            {
                dlldata = System.IO.File.ReadAllBytes(dllfilepath);
            }
            return GetEnumerators(dlldata, classname, null);
        }

        public ArrayComboListEnumerator[] GetEnumerators(string dllfilepath)
        {
            return GetEnumerators(dllfilepath, null);
        }

        public ArrayComboListEnumerator[] GetEnumerators()
        {
            return GetEnumerators(null, null);
        }


        public virtual ArrayComboListEnumerator[] GetEnumeratorsWithFullSource(string code, string classname)
        {
            byte[] dlldata = null;
            if (LocalCompile)
            {
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("Reduce", false);
                }
                dlldata = CompilePluginSource(code);
                if (CompilerInvoked != null)
                {
                    CompilerInvoked("Reduce", true);
                }
            }
            return GetEnumerators(dlldata, classname, code);
        }


        ArrayComboListEnumerator[] GetEnumeratorsReducerOutput(string code, string reduceroutputclasscode, string[] usings)
        {
            string susings = "";
            if (usings != null)
            {
                foreach (string nm in usings)
                {
                    susings += "using " + nm + ";" + Environment.NewLine;
                }
            }
            return GetEnumeratorsWithFullSource(
                @"using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;
using ByteSliceList = MySpace.DataMining.EasyReducer.RandomAccessReducer.RandomAccessEntries;
using ReduceOutput = MySpace.DataMining.EasyReducer.RandomAccessReducer.RandomAccessOutput;

" + susings + @"

namespace MySpace.DataMining.EasyReducer
{
    public class RandomAccessReducer : IBeforeReduce, IAfterReduce, IReducedToFile, IBeforeReduceOrd
    {
        int i = 0;
        RandomAccessEntries raentries = null;


        public RandomAccessEntries Values
        {
            get { return raentries; }
        }
        

        int _rord = -1;

        public void SetReduceOrd(int ord)
        {
            _rord = ord;
        }

        public int ReduceOrd
        {
            get { return _rord; }
        }


        public void OnEnumeratorFinished()
        {
            _done();
            Type thistype = this.GetType();
            System.Reflection.MethodInfo finalmethod = thistype.GetMethod(" + "\"" + @"ReduceFinalize" + "\"" + @");
            if(null != finalmethod)
            {" + 
               @"StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";" +
                DGlobalsM.ToCode() +
                @"
                if(StaticGlobals.DSpace_Hosts == null)
                {
                    StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts).Replace('`', '"') + @"};
                }                
                finalmethod.Invoke(this, null);
            }
        }

        public bool OnGetEnumerator(EntriesInput input, EntriesOutput output)
        {
            if(null == raentries)
            {
                raentries = new RandomAccessEntries();
            }
            raentries.SetInput(input);
            byte[] firstkeybuf, xkeybuf;
            int firstkeyoffset, xkeyoffset;
            int firstkeylen, xkeylen;
            if(StaticGlobals.DSpace_InputBytesRemain == 0 && i == input.entries.Count - 1)
            {
                StaticGlobals.DSpace_Last = true;
            }
            for (; i < input.entries.Count; )
            {
                input.entries[i].LocateKey(input, out firstkeybuf, out firstkeyoffset, out firstkeylen);
                int len = 1;
                for (int j = i + 1; j < input.entries.Count; j++)
                {
                    bool nomatch = false;
                    input.entries[j].LocateKey(input, out xkeybuf, out xkeyoffset, out xkeylen);
                    if (firstkeylen != xkeylen)
                    {
                        break;
                    }
                    for (int ki = 0; ki != xkeylen; ki++)
                    {
                        if (xkeybuf[xkeyoffset + ki] != firstkeybuf[firstkeyoffset + ki])
                        {
                            nomatch = true;
                            break;
                        }
                    }
                    if (nomatch)
                    {
                        break;
                    }
                    len++;
                }
                raentries.set(i, len);
                OReduce(this, output);
                i += len;
                return true; // Continue.
            }
            i = 0;
            return false; // At end; stop.
        }


        public class RandomAccessEntries: IEnumerator<ByteSlice>,IEnumerable<ByteSlice>
        {
            public RandomAccessEntries()
            {
            }            
            
            internal void SetInput(EntriesInput input)
            {
                this.input = input;
                this.items.parent = this;
            }

            internal void set(int offset, int length)
            {
                this.offset = offset;
                this.length = length;
                this.curPos = -1;
            }

            public int Length
            {
                get
                {
                    return length;
                }
            }

            public ByteSlice Current
            {
                get
                {
                     return this[curPos].Value;
                }           
            }

            object System.Collections.IEnumerator.Current
            {
                get
                {
                     return this[curPos].Value;
                }  
            }            

            public void Reset()
            {
                curPos = -1;
            }

            public bool MoveNext()
            {
                return (++curPos < length);
            }

            public RandomAccessEntriesItems Items
            {
                get
                {
                    return items;
                }
            }

            public ReduceEntry this[int index]
            {
                get
                {
                    if (index < 0 || index > length)
                    {
                        throw new ArgumentOutOfRangeException();
                    }
                    return ReduceEntry.Create(input, input.entries[offset + index]);
                }
            }

            public void Dispose()
            {                
            }

            public IEnumerator<ByteSlice> GetEnumerator()
            {
                return this;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return this;
            }

            int offset, length;
            EntriesInput input;
            RandomAccessEntriesItems items;
            int curPos;
        }

        public struct RandomAccessEntriesItems
        {
            internal RandomAccessEntries parent;

            public ByteSlice this[int index]
            {
                get
                {
                   return parent[index].Value;
                }
            }
        }


        public class ExplicitCacheAttribute: Attribute
        {
        }


        " + reduceroutputclasscode + @"


        public virtual void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
        {
            /*
            for (int i = 0; i < values.Length; i++)
            {
                ByteSlice value = values[i].Value;
                //output.Add(key, value);
            }
            */
        }
    }


    class EnumWithCodeReducer : RandomAccessReducer
    {
        public EnumWithCodeReducer()
        {
                Type thistype = this.GetType();
                System.Reflection.MethodInfo initmethod = thistype.GetMethod(" + "\"" + @"ReduceInitialize" + "\"" + @");
                if(null != initmethod)
                {" + 
                    @"StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";" +
                    DGlobalsM.ToCode() + @";
                    if(StaticGlobals.DSpace_Hosts == null)
                    {
                        StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts).Replace('`', '"') + @"};
                    }     
                    initmethod.Invoke(this, null);
                }
        }
"
                + code // !
                + @"    }

}
", "EnumWithCodeReducer");
        }


        string enumcachename = "";

        public string EnumeratorCacheName
        {
            get
            {
                return enumcachename;
            }

            set
            {
                enumcachename = value;
            }
        }


        public long DfsSampleDistance = 67108864 / 16;


        public int EnumerateToFile(string filepath, long basefilesize, string code, string[] usings, IList<long> appendsizes)
        {
            List<string> filepaths = new List<string>(1);
            filepaths.Add(filepath);
            EnumerateToFiles(filepaths, basefilesize, code, usings);
            return GetNumberOfEnumerationFilesCreated(0, appendsizes);
        }


        // Returns number of files written.
        // if basefilesize>0 writes to multiple files (-1 for no max)
        // filepath contains %n to be replaced with current file number, starting at 0.
        // If multiple slaves, returns total files of all slaves.
        // appendsizes can be null.
        public void EnumerateToFiles(IList<string> filepaths, long basefilesize, string code, string[] usings)
        {
            if (basefilesize <= 0)
            {
                basefilesize = long.MaxValue;
            }

            StringBuilder csvfilepaths = new StringBuilder();
            for (int i = 0; i < filepaths.Count; i++)
            {
                if (0 != i)
                {
                    csvfilepaths.Append(", ");
                }
                csvfilepaths.Append("@`");
                csvfilepaths.Append(filepaths[i]);
                csvfilepaths.Append('`');
            }

            ArrayComboListEnumerator[] enums = GetEnumeratorsReducerOutput(code, (@"

        const int NRAOUTS = " + filepaths.Count.ToString() + @";
        static readonly string[] filepaths = new string[] { " + csvfilepaths.ToString() + @" };
        static byte compressenumout = " + CompressFileOutput.ToString() + @"; // Can't be const or the compiler 'detects' dead code.
        static byte[] flipbuf = null;
        static bool flip = " + (string.Compare(StaticGlobals.DSpace_OutputDirection, "descending", true) == 0 ? "true" : "false") + @";
        const long DfsSampleDistance = " + DfsSampleDistance.ToString() + @";

        RandomAccessOutput[] raouts = null;
        void OReduce(RandomAccessReducer rar, EntriesOutput output)
        {
            StaticGlobals.ExecutionContext = ExecutionContextType.REDUCE;
            StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts) + @"};
            StaticGlobals.DSpace_OutputDirection = `" + StaticGlobals.DSpace_OutputDirection + @"`;
            StaticGlobals.DSpace_OutputDirection_ascending = " + (StaticGlobals.DSpace_OutputDirection_ascending ? "true" : "false") + @";
            StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";
            ").Replace('`', '"') + DGlobalsM.ToCode() + (@"
            if(NRAOUTS > 0)
            {
                StaticGlobals.DSpace_KeyLength = rar.Values[0].Key.Length;            
            }

            if(null == raouts)
            {
                raouts = new RandomAccessOutput[NRAOUTS];
                for(int i = 0; i < NRAOUTS; i++)
                {
                    raouts[i] = new RandomAccessOutput(output, filepaths[i]);
                    raouts[i].rar = rar;
                }
                
                {
                    System.Reflection.MethodInfo reducemi = this.GetType().GetMethod(`Reduce`);
                    object[] reduceattribs = reducemi.GetCustomAttributes(false);
                    bool expcache = false;
                    for(int i = 0; i < reduceattribs.Length; i++)
                    {
                        if(null != reduceattribs[i] as ExplicitCacheAttribute)
                        {
                            expcache = true;
                        }
                    }
                    if(expcache)
                    {
                        //System.IO.File.WriteAllText(@`c:\eee.txt`, `expcache!`);
                        if(NRAOUTS > 0)
                        {
                            raouts[0]._FirstCache();
                        }
                    }
                }
            }

            ByteSlice key = rar.Values[0].Key;
            if(flip)
            {
                if(flipbuf == null)
                {
                    flipbuf = new byte[key.Length];
                }
                key = rar.Values[0].Key.FlipAllBits(flipbuf);    
            }

            for(int i = 0; i < NRAOUTS; i++)
            {
                    Stack.ResetStack();
                    recordset.ResetBuffers();
                    ++StaticGlobals.ReduceIteration;
                    Reduce(key, rar.Values, raouts[i]);                     
            }
        }


        public int GetReducedFileCount(int n, IList<long> appendsizes)
        {
            if(null != raouts && n < raouts.Length && n >= 0)
            {
                return raouts[n]._getreducedfilecount(appendsizes);
            }
            return 0;
        }


        void _done()
        {
            if(null != raouts) // Can be null if OReduce was never called.
            {
                for(int i = 0; i < NRAOUTS; i++)
                {
                    raouts[i]._done();
                }
            }
        }


        public class RandomAccessOutput
        {
            string _basefilename;
            const long _basefilesize = " + basefilesize.ToString() + @";
            List<long> _filesizes = new List<long>();
            
            int _filenum = -1;
            System.IO.Stream _outstm = null;
            System.IO.Stream _outsamps = null;
            long _cursize = 0;
            long _totalsize = 0;
            byte[] _smallbuf = new byte[4 + 8];
            List<byte> _bytebuf = new List<byte>();

            internal RandomAccessReducer rar;

            long _nextsamplepos = -1;

            const int HEADERSIZE = 4 + 8;

            const int OutputRecordLength = " + OutputRecordLength.ToString() + @";
            const bool _WriteSamples = OutputRecordLength < 1;

            public RandomAccessOutput(EntriesOutput eoutput, string basefilename)
            {
                this._basefilename = basefilename;

                StaticGlobals.DSpace_OutputRecordLength = " + OutputRecordLength.ToString() + @";
            }

            const string sBlockID = @`" + sBlockID + @"`; // Slave ID.
            
            const string cachename = @`" + enumcachename + @"`;
            System.IO.FileStream fzkeyball = null;
            System.IO.FileStream fzvalueball = null;
            int lastzballhash = -42;

            // Pre-set with defaults. Updated on first cache.
            int numzblocks = 139;
            int zblockkeyfilebufsize = " + FILE_BUFFER_SIZE.ToString() + @";
            int zblockvaluefilebufsize = " + FILE_BUFFER_SIZE.ToString() + @";
            int zballvaluesize = 0;
            byte[] zkeybuf; // + 4 for value offset.
            byte[] zvaluebuf; // + 4 for value length.


            static string CreateZBallFileName(int zblockID, string cachename, string otherinfo)
            {
                if(string.IsNullOrEmpty(cachename))
                {
                    throw new Exception(`RandomAccessOutput.CreateZBallFileName: no cache name`);
                }
                return `zsball_` + cachename + `_` + zblockID.ToString() + `_` + otherinfo + `.zsb`;
            }

            public static int _ParseCapacity(string capacity)
            {
                try
                {
                    if (null == capacity || 0 == capacity.Length)
                    {
                        throw new Exception(`Invalid capacity: capacity not specified`);
                    }
                    switch (capacity[capacity.Length - 1])
                    {
                        case 'B':
                            if (1 == capacity.Length)
                            {
                                throw new Exception(`Invalid capacity: ` + capacity);
                            }
                            switch (capacity[capacity.Length - 2])
                            {
                                case 'K': // KB
                                    return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                                case 'M': // MB
                                    return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                                case 'G': // GB
                                    return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                                default: // Just bytes with B suffix.
                                    return int.Parse(capacity.Substring(0, capacity.Length - 1));
                            }
                        //break;

                        default: // Assume just bytes without a suffix.
                            return int.Parse(capacity);
                    }
                }
                catch (FormatException e)
                {
                    throw new FormatException(`Invalid capacity: bad format: '` + capacity + `' problem: ` + e.ToString());
                }
                catch (OverflowException e)
                {
                    throw new OverflowException(`Invalid capacity: overflow: '` + capacity + `' problem: ` + e.ToString());
                }
            }

            
            internal void _FirstCache()
            {
                lastzballhash = -1;
                if(string.IsNullOrEmpty(cachename))
                {
                    throw new Exception(`RandomAccessOutput.Cache: no cache name`);
                }
                
                // Get information...
                {
                    System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                    xd.Load(`slaveconfig.xml`);
                    System.Xml.XmlNode xslave = xd[`slave`];

                    if (null != xslave)
                    {
                        System.Xml.XmlNode xzblocks = xslave[`zblocks`];
                        if (null != xzblocks)
                        {
                            {
                                System.Xml.XmlAttribute xnzb = xzblocks.Attributes[`count`];
                                if (null != xnzb)
                                {
                                    numzblocks = int.Parse(xnzb.Value);
                                    /*{
                                        string computer_name = System.Environment.GetEnvironmentVariable(`COMPUTERNAME`);
                                        if (computer_name == `MAPDCMILLER`)
                                        {
                                            numzblocks = 3;
                                        }
                                    }*/
                                }
                            }
                            /*{
                                System.Xml.XmlAttribute xzbs = xzblocks.Attributes[`addbuffersize`];
                                if (null != xzbs)
                                {
                                    int x = _ParseCapacity(xzbs.Value);
                                    zblockkeybufsize = x;
                                    zblockvaluebufsize = x;
                                }
                            }*/
                            /*{
                                System.Xml.XmlAttribute xzbs = xzblocks.Attributes[`addkeybuffersize`];
                                if (null != xzbs)
                                {
                                    zblockkeybufsize = _ParseCapacity(xzbs.Value);
                                }
                            }*/
                            /*{
                                System.Xml.XmlAttribute xzbs = xzblocks.Attributes[`addvaluebuffersize`];
                                if (null != xzbs)
                                {
                                    zblockvaluebufsize = _ParseCapacity(xzbs.Value);
                                }
                            }*/
                        }
                    }

                }

                // Delete any existing cache files.
                for(int iz = 0; iz < numzblocks; iz++)
                {
                    try
                    {
                        string zkeyballname = CreateZBallFileName(iz, cachename, sBlockID.PadLeft(4, '0') + `key`);
                        string zvalueballname = CreateZBallFileName(iz, cachename, sBlockID.PadLeft(4, '0') + `value`);
                        System.IO.File.Delete(zkeyballname);
                        System.IO.File.Delete(zvalueballname);
                    }
                    catch
                    {
                    }
                }
            }

            void _closecache()
            {
                if(null != fzkeyball)
                {
                    fzkeyball.Close();
                    fzkeyball = null;
                }
                if(null != fzvalueball)
                {
                    fzvalueball.Close();
                    fzvalueball = null;
                }
            }

            public void Cache(ByteSlice key, ByteSlice value)
            {
                if(-42 == lastzballhash)
                {
                    _FirstCache();
                }

                int iz = rar.ReduceOrd;
                if(-1 == iz)
                {
                    throw new Exception(`Invalid Cache ReduceOrd`);
                }
                if(iz < lastzballhash)
                {
                    throw new Exception(`RandomAccessOutput.Cache: explicit cache keys must preserve order`);
                }
                if(iz != lastzballhash)
                {
                    _closecache();
                    zballvaluesize = 0;
                    string zkeyballname = CreateZBallFileName(iz, cachename, sBlockID.PadLeft(4, '0') + `key`);
                    string zvalueballname = CreateZBallFileName(iz, cachename, sBlockID.PadLeft(4, '0') + `value`);
                    lastzballhash = iz;
                    fzkeyball = new System.IO.FileStream(zkeyballname, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None, zblockkeyfilebufsize);
                    fzvalueball = new System.IO.FileStream(zvalueballname, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None, zblockvaluefilebufsize);

                    if(null == zkeybuf)
                    {
                        zkeybuf = new byte[key.Length + 4];
                        zvaluebuf = new byte[(128 >= value.Length) ? 128 + 4 : Entry.Round2Power(value.Length + 4)];                        
                    }
                }

                if(key.Length > zkeybuf.Length - 4)
                {
                    throw new Exception(`Key length mismatch for Reducer Cache(key, value)`);
                }
                bool flip = !StaticGlobals.DSpace_OutputDirection_ascending;
                for(int i = 0; i < key.Length; i++)
                {
                    if(!flip)
                    {
                        zkeybuf[i] = key[i];
                    }
                    else
                    {
                        zkeybuf[i] = (byte)(~key[i]);
                    }                    
                }
                Entry.ToBytes(zballvaluesize, zkeybuf, key.Length);
                fzkeyball.Write(zkeybuf, 0, key.Length + 4);
                if(value.Length + 4 > zvaluebuf.Length)
                {
                    zvaluebuf = new byte[Entry.Round2Power(value.Length + 32)];
                }
                Entry.ToBytes(value.Length, zvaluebuf, 0);
                for(int i = 0; i < value.Length; i++)
                {
                    zvaluebuf[4 + i] = value[i];
                }
                fzvalueball.Write(zvaluebuf, 0, 4 + value.Length);
                zballvaluesize += 4 + value.Length; // !
                
            }

            public void Cache(string key, string value)
            {
                Cache(ByteSlice.PreparePaddedStringUTF8(key, StaticGlobals.DSpace_KeyLength), ByteSlice.Prepare(value));
            }

            public void Cache(recordset key, recordset value)
            {
                if (key.ContainsString)
                {
                    throw new Exception(`Key recordset cannot contain string.`);
                }

                Cache(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
            }

            public void Cache(mstring key, mstring value)
            {               
                Cache(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
            }

            public void Cache(mstring key, recordset value)
            {                 
                Cache(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
            }           
    
            /*
            public void Add(ByteSlice key, ByteSlice value)
            {
            }
            */
            
            void _nextfile()
            {
                _filenum++;
                string fn = _basefilename.Replace(`%n`, _filenum.ToString());
                if(null != _outstm)
                {
                    _filesizes.Add(HEADERSIZE + _cursize);
                    _outstm.Close();
                    if(_WriteSamples)
                    {
                        _outsamps.Close();
                    }
                }
                _outstm = new System.IO.FileStream(fn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, " + FILE_BUFFER_SIZE.ToString() + @");
                if(_WriteSamples)
                {
                    _outsamps = new System.IO.FileStream(fn + `.zsa`, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, " + FILE_BUFFER_SIZE.ToString() + @");
                    _nextsamplepos = 0;
                }

                if(1 == compressenumout)
                {
                    _outstm = new System.IO.Compression.GZipStream(_outstm, System.IO.Compression.CompressionMode.Compress);
                }
                
                // Write header...
                Entry.ToBytes(HEADERSIZE, _smallbuf, 0);
                _bytebuf.Clear();
                Entry.ToBytesAppend64(_totalsize, _bytebuf);
                _smallbuf[4] = _bytebuf[0];
                _smallbuf[5] = _bytebuf[1];
                _smallbuf[6] = _bytebuf[2];
                _smallbuf[7] = _bytebuf[3];
                _smallbuf[8] = _bytebuf[4];
                _smallbuf[9] = _bytebuf[5];
                _smallbuf[10] = _bytebuf[6];
                _smallbuf[11] = _bytebuf[7];
                _outstm.Write(_smallbuf, 0, HEADERSIZE);

                _cursize = 0;
            }

            public void Add(ByteSlice entry, bool addnewline)
            {
                if(-1 == _filenum || _cursize >= _basefilesize)
                {
                    _nextfile();
                }
                
                {
                    for(int i = 0; i < entry.Length; i++)
                    {
                        _outstm.WriteByte(entry[i]);
                    }
                    int len = entry.Length;
                    if(addnewline)
                    {
                        string nl = Environment.NewLine;
                        for(int i = 0; i < nl.Length; i++)
                        {
                            _outstm.WriteByte((byte)nl[i]);
                            len++;
                        }
                    }
                    
                    _cursize += len;
                    _totalsize += len;
                }

                if(_WriteSamples)
                {
                    if(_cursize >= _nextsamplepos)
                    {
                        {
                            for(int i = 0; i < entry.Length; i++)
                            {
                                _outsamps.WriteByte(entry[i]);
                            }
                            int len = entry.Length;
                            if(addnewline)
                            {
                                string nl = Environment.NewLine;
                                for(int i = 0; i < nl.Length; i++)
                                {
                                    _outsamps.WriteByte((byte)nl[i]);
                                    len++;
                                }
                            }
                        }
                        _nextsamplepos += DfsSampleDistance;
                    }
                }

            }
            
            void AddCheck(ByteSlice entry, bool addnewline)
            {
                if(OutputRecordLength > 0)
                {
                    if(addnewline)
                    {
                        throw new Exception(`Cannot write-line in fixed-length record rectangular binary mode`);
                    }
                }
                else
                {
                    if(!addnewline)
                    {
                        throw new Exception(`Need write-line`);
                    }
                }
                Add(entry, addnewline);
            }
            
            void AddCheck(ByteSlice entry)
            {
                AddCheck(entry, true);
            }

            public void Add(ByteSlice entry)
            {
                bool addnewline = OutputRecordLength < 1;
                Add(entry, addnewline);
            }

            public void WriteLine(ByteSlice entry)
            {
                AddCheck(entry);
            }

            public void Add(mstring ms)
            {
                Add(ms.ToByteSlice());
            }

            public void AddBinary(Blob b)
            {
                string s = b.ToDfsLine();
                ByteSlice bs = ByteSlice.Prepare(s);
                Add(bs);
            }

            public void Add(recordset rs)
            {
                int rsLength = rs.Length;
                if(rsLength > OutputRecordLength)
                {
                    throw new Exception(`recordset is larger than record length; got length of ` + rsLength.ToString() + ` when expecting length of ` + OutputRecordLength.ToString());
                }
                else if(rsLength < OutputRecordLength)
                {
                    for(; rsLength < OutputRecordLength; rsLength++)
                    {
                        rs.PutByte(0);
                    }
                    #if DEBUG
                    if(rs.Length != rsLength)
                    {
                        throw new Exception(`DEBUG: (rs.Length != rsLength)`);
                    }
                    #endif
                }
                #if DEBUG
                if(rs.Length != OutputRecordLength)
                {
                    throw new Exception(`DEBUG: (rs.Length != OutputRecordLength)`);
                }
                #endif
                AddCheck(rs.ToRow(), false); // WriteRecord(rs.ToRow());
            }

            public void WriteLine(mstring ms)
            {
                AddCheck(ms.ToByteSlice());
            }

            public void WriteRecord(ByteSlice entry)
            {
                if(OutputRecordLength < 1)
                {
                    throw new Exception(`Cannot write records; not in fixed-length record rectangular binary mode`);
                }
                if(entry.Length != OutputRecordLength) // && OutputRecordLength > 0)
                {
                    throw new Exception(`Record length mismatch; got length of ` + entry.Length.ToString() + ` when expecting length of ` + OutputRecordLength.ToString());
                }
                AddCheck(entry, false);
            }
            
            internal void _done()
            {
                _closecache(); // !

                if(null != _outstm)
                {
                    _outstm.Close();
                    //_outstm = null;
                    _filesizes.Add(HEADERSIZE + _cursize);
                    if(_WriteSamples)
                    {
                        _outsamps.Close();
                    }
                }
            }

            internal int _getreducedfilecount(IList<long> appendsizes)
            {
                if(null != appendsizes)
                {
                    for(int i = 0; i < _filenum + 1; i++)
                    {
                        //string fn = _basefilename.Replace(`%n`, i.ToString());  
                        long x = 0;
                        if(i < _filesizes.Count)
                        {
                            x =_filesizes[i];
                        }   
                        if(x >= HEADERSIZE)
                        {
                            x -= HEADERSIZE;
                        }
                        appendsizes.Add(x);
                    }
                }
                return _filenum + 1;
            }

        }").Replace('`', '"'), usings);

            foreach (ArrayComboListEnumerator en in enums)
            {
                while (en.MoveNext())
                {
                }
            }
        }


        public delegate void CompilerInvokedEvent(string action, bool complete);

        public CompilerInvokedEvent CompilerInvoked;


        byte compressenumout = 1;

        // If used, must be called before EnumerateToFile[s].
        public byte CompressFileOutput
        {
            set
            {
                compressenumout = value;
            }

            get
            {
                return compressenumout;
            }
        }


        public int EnumerateToFile(string filepath, long basefilesize, string code, string[] usings)
        {
            return EnumerateToFile(filepath, basefilesize, code, usings, null);
        }


        public int GetNumberOfEnumerationFilesCreated(int n, IList<long> appendsizes)
        {
            int result = 0;
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'c');
                Entry.ToBytes(n, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 4);
                int len;               
                buf = XContent.ReceiveXBytes(slave.nstm, out len, buf);
                if (len >= 4)
                {
                    result += Entry.BytesToInt(buf);

                    if (null != appendsizes)
                    {
                        for (int offset = 4; offset + 8 <= len; offset += 8)
                        {
                            appendsizes.Add(Entry.BytesToLong(buf, offset));
                        }
                    }
                }
            }
            return result;
        }


        public ArrayComboListEnumerator[] GetEnumeratorsWithCode(string code, string[] usings)
        {
            return GetEnumeratorsReducerOutput(code, @"


        public int GetReducedFileCount(int n, IList<long> appendsizes)
        {
            if(null != raout)
            {
                return raout._getreducedfilecount(n, appendsizes);
            }
            return 0;
        }


        RandomAccessOutput raout = null;
        void OReduce(RandomAccessReducer rar, EntriesOutput output)
        {
            if(null == raout)
            {
                raout = new RandomAccessOutput(output);
            }
            Reduce(rar.Values[0].Key, rar.Values, raout);
        }


        void _done()
        {
            if(null != raout) // Can be null if OReduce was never called.
            {
                raout._done();
            }
        }


        public class RandomAccessOutput
        {
            public RandomAccessOutput(EntriesOutput eoutput)
            {
                this.eoutput = eoutput;
            }


            public void Add(ByteSlice key, ByteSlice value)
            {
                eoutput.Add(key, value);
            }


            internal void _done()
            {
            }

            internal int _getreducedfilecount(IList<long> appendsizes)
            {
                return 0;
            }


            EntriesOutput eoutput;
        }", usings);

        }


        public struct ACEntry
        {
            internal IList<byte> keybuf;
            internal int keyoffset;
            internal int keylen;
            internal IList<byte> valuebuf;
            internal int valueoffset;
            internal int valuelen;


            public int GetKeyLength()
            {
                return keylen;
            }


            public void CopyKey(byte[] buf, int offset)
            {
                for (int i = 0; i != keylen; i++)
                {
                    buf[offset + i] = keybuf[keyoffset + i];
                }
            }

            public void CopyKey(byte[] buf)
            {
                CopyKey(buf, 0);
            }

            public void CopyKeyAppend(List<byte> list)
            {
                for (int i = 0; i != keylen; i++)
                {
                    list.Add(keybuf[keyoffset + i]);
                }
            }


            public int GetValueLength()
            {
                return valuelen;
            }


            public void CopyValue(byte[] buf, int offset)
            {
                for (int i = 0; i != valuelen; i++)
                {
                    buf[offset + i] = valuebuf[valueoffset + i];
                }
            }

            public void CopyValue(byte[] buf)
            {
                CopyValue(buf, 0);
            }

            public void CopyValueAppend(List<byte> list)
            {
                for (int i = 0; i != valuelen; i++)
                {
                    list.Add(valuebuf[valueoffset + i]);
                }
            }
        }


        bool closed = false;
        bool enumd = false;
        bool sortd = false;
        public bool atype = false;


        void ensureopen(string forwhat)
        {
            if (closed)
            {
                throw new Exception("ArrayComboList is closed; must be open for " + forwhat);
            }
            if (!didopen)
            {
                throw new Exception("ArrayComboList was never opened; must be open for " + forwhat);
            }
        }

        void ensurenotopen(string forwhat)
        {
            if (closed)
            {
                throw new Exception("ArrayComboList is closed for " + forwhat);
            }
            if (didopen)
            {
                throw new Exception("ArrayComboList was opened; must not be open for " + forwhat);
            }
        }

        void ensurenotenumd(string forwhat)
        {
            if (enumd)
            {
                throw new Exception("ArrayComboList was already enumerated; must not be enumerated for " + forwhat);
            }
        }

        void ensurenotsortd(string forwhat)
        {
            if (sortd)
            {
                throw new Exception("ArrayComboList was already sorted; must not be sorted for " + forwhat);
            }
        }

        void ensurewassortd(string forwhat)
        {
            if (!sortd)
            {
                throw new Exception("ArrayComboList not sorted; must be sorted for " + forwhat);
            }
        }

    }


    public class ArrayComboListEnumerator
    {
        internal ArrayComboList acl;
        internal SlaveInfo slave;
        internal bool first = true;
        byte[] dlldata; // null if using default enumerator.
        string plugincode; // only valid if compiled with source.
        string classname; // null if using first class found in dllfilepath with IBeforeReduce.


        public void SendDllNow()
        {
            if (first)
            {
                first = false;
                internalsendreset();
            }
        }


        internal ArrayComboListEnumerator(ArrayComboList acl, SlaveInfo slave, byte[] dlldata, string classname, string plugincode)
        {
            this.acl = acl;
            this.slave = slave;
            this.dlldata = dlldata;
            this.classname = classname;
            if (null == plugincode)
            {
                plugincode = "";
            }
            this.plugincode = plugincode;
        }


        public virtual ArrayComboList.ACEntry Current
        {
            get
            {
                return cur;
            }
        }


        public virtual bool MoveNext()
        {
            return cangetnext();
        }


        private ArrayComboList.ACEntry cur;
        public byte[] buf;

        public byte[] nextbuf; // Buffering the 'next' elements; null at first!
        public int nextbufpos = 0; // Where in nextbuf the valid elements start.
        public int nextbufend = 0; // Where in nextbuf the valid elements end.

        internal bool cangetnext()
        {
            lock (slave)
            {
                if (first)
                {
                    first = false;
                    internalsendreset();
                }

                if (nextbufpos < nextbufend)
                {
                    fillcurfromnextbuf();
                    return true;
                }
                else
                {
                    // 'e for batch next..
                    slave.nstm.WriteByte((byte)'e');

                    // Replies with tag '+' and xcontent values; or tag '-' if not exists.
                    //int len;
                    int x = slave.nstm.ReadByte();
                    if ('+' == x)
                    {
                        nextbufpos = 0;
                        nextbufend = 0;
                        nextbuf = XContent.ReceiveXBytes(slave.nstm, out nextbufend, nextbuf);
                        slave.SlaveErrorCheck();
                        fillcurfromnextbuf();
                        return true;
                    }
                    else if ('-' == x)
                    {
                        return false;
                    }
                    else
                    {
                        throw new Exception("Server returned invalid response for enumeration");
                    }
                }
            }
        }

        internal void getnext()
        {
            if (!cangetnext())
            {
                throw new InvalidOperationException("Past end of enumeration");
            }
        }


        internal void fillcurfromnextbuf()
        {
            if (nextbufpos + 8 > nextbufend)
            {
                throw new Exception("Unaligned values (enumeration buffer)");
            }

            cur.keybuf = nextbuf;
            cur.keyoffset = nextbufpos;
            cur.keylen = acl.GetKeyBufferLength();

            cur.valuebuf = nextbuf;
            cur.valueoffset = nextbufpos + cur.keylen + 4;
            cur.valuelen = Entry.BytesToInt(nextbuf, nextbufpos + cur.keylen);

            nextbufpos += cur.keylen + 4 + cur.valuelen;
        }


        internal void internalsendreset()
        {
            if (null != this.plugincode)
            {
                // 'D' to send DLL containing IBeforeReduce plugin..
                slave.nstm.WriteByte((byte)'D');
                if (null == classname)
                {
                    XContent.SendXContent(slave.nstm, "");
                }
                else
                {
                    XContent.SendXContent(slave.nstm, classname);
                }
                XContent.SendXContent(slave.nstm, this.plugincode);
                XContent.SendXContent(slave.nstm, dlldata, null == dlldata ? 0 : dlldata.Length);
                slave.SlaveErrorCheck();
            }
        }
    }
}
