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
    public class FixedArrayComboList : DistObject2
    {
        int keylen, valuelen;
        byte[] buf;
        int addbufinitsize = 0;


        public FixedArrayComboList(string objectname, int keylength, int valuelength)
            : base(objectname)
        {
            this.keylen = keylength;
            this.valuelen = valuelength;

            this.buf = new byte[BUF_SIZE];

            EnableAddBuffer(); // ..!
        }


        private new void AddBlock(string capacity, string sUserBlockInfo)
        {
            throw new Exception("AddBlock capacity");
        }

        public virtual void AddBlock(string sUserBlockInfo)
        {
            base.AddBlock("1;1;" + this.keylen.ToString() + ";" + this.valuelen.ToString(), sUserBlockInfo);
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
            return valuelen;
        }


        public override char GetDistObjectTypeChar()
        {
            return 'F';
        }


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
                        throw new Exception("SortBlocks error: sub process " + slave.slaveID.ToString() + " did not return a valid response");
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


        public void Add(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset)
        {
            ensureopen("Add");
            ensurenotenumd("Add");
            ensurenotsortd("Add");

            int slaveID = DetermineSlave(key, keyoffset, this.keylen);

            /*if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Slave missing: slaveID needed: " + slaveID.ToString());
            }*/
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                BufSlaveInfo bslave = (BufSlaveInfo)slave;
                bslave.EnsureAddBuf_unlocked(addbufinitsize);
                bool flushed = bslave.AddBuf_unlocked(key, keyoffset, this.keylen, value, valueoffset, this.valuelen);
                if (flushed)
                {
                    bslave.SlaveErrorCheck();
                }
            }
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            if (key.Count != this.keylen)
            {
                throw new Exception("Key length mismatch; expected " + this.keylen.ToString() + " bytes, got " + key.Count.ToString());
            }
            if (value.Count != this.valuelen)
            {
                throw new Exception("Value length mismatch; expected " + this.valuelen.ToString() + " bytes, got " + value.Count.ToString());
            }

            Add(key, 0, value, 0);
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

        string compileopts = "";

        string getcompileropts()
        {
            string result = compileopts;

            /* // error CS1617: Invalid option 'ISO-2' for /langversion; must be ISO-1 or Default
            if (-1 == compileopts.IndexOf("/langversion:"))
            {
                result = "/langversion:ISO-2 " + result;
            }
             * */

            return result;
        }


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
                XContent.SendXContent(slave.nstm, dlldata);
            }

            // "Join"...
            foreach (SlaveInfo slave in dslaves)
            {
                slave.SlaveErrorCheck();
            }
        }


        public virtual void BeforeLoadFullSource(string code, string classname)
        {
            System.CodeDom.Compiler.CompilerParameters cp = new System.CodeDom.Compiler.CompilerParameters(new string[] { "IMapReduce.dll" });
#if DEBUG
            cp.IncludeDebugInformation = true;
#endif
            System.CodeDom.Compiler.CompilerResults cr = null;
            string reason = "";
            for (int rotor = 1; ; rotor++)
            {
                /*if (rotor > 10)
                {
                    throw new System.IO.FileNotFoundException("FixedArrayComboList.BeforeLoad dynamic C# compilation: Unable to create DLL" + reason);
                }*/
                try
                {
                    cp.OutputAssembly = ".\\temp_" + Guid.NewGuid() + ".dll";
                    cp.GenerateExecutable = false; // Not exe, but dll.
                    cp.GenerateInMemory = false;
                    cp.CompilerOptions = getcompileropts();

                    using (new System.Threading.Mutex(true, "DynCmp"))
                    {
                        using (Microsoft.CSharp.CSharpCodeProvider cscp = new Microsoft.CSharp.CSharpCodeProvider())
                        {
                            cr = cscp.CompileAssemblyFromSource(cp, code);
                        }
                        CleanCompilerFiles();
                    }
                    if (cr.Errors.HasErrors)
                    {
                        try
                        {
                            lock (typeof(DistObject))
                            {
                                System.IO.File.WriteAllText("error.cs", code);
                            }
                        }
                        catch
                        {
                        }
                        throw new Exception("BeforeLoad code compile error: " + cr.Errors[0].ToString());
                    }
                    if (0 != cr.NativeCompilerReturnValue)
                    {
                        LogLine("BeforeLoad code compile did not return 0 (returned " + cr.NativeCompilerReturnValue.ToString() + "): ");
                        for (int i = 0; i < cr.Output.Count; i++)
                        {
                            string ss = cr.Output[i].Trim();
                            if (0 != ss.Length)
                            {
                                LogLine("  C" + rotor.ToString() + "- " + cr.Output[i]);
                            }
                        }
                    }
                    System.Threading.Thread.Sleep(1000 * rotor);

                    //System.IO.File.WriteAllText("xlib_" + classname + ".xlib", code); // Conflicts with multiple running inline mappers.

                    byte[] dlldata = System.IO.File.ReadAllBytes(cr.PathToAssembly);
                    _BeforeLoad(dlldata, classname, code);
                    break; // Good.
                }
                catch (System.IO.IOException e)
                {
                    reason = ": " + e.ToString();
                    continue;
                }
            }

            try
            {
                System.IO.File.Delete(cr.PathToAssembly);
#if DEBUG
                System.IO.File.Delete(cr.PathToAssembly.Replace(".dll", ".pdb"));
#endif
            }
            catch (Exception eee)
            {
                int i23zzz = 23 + 23;
            }
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


        class BufSlaveInfo : SlaveInfo
        {
            byte[] addbuf = null;
            int addbuflen = 0; // Number of valid bytes in addbuf.


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
                int xlen = keylength + valuelength;

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
                for (int i = 0; i != valuelength; i++)
                {
                    addbuf[addbuflen + keylength + i] = value[valueoffset + i];
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


        public FixedArrayComboListEnumerator[] GetEnumerators(byte[] dlldata, string classname, string pluginsource)
        {
            ensureopen("GetEnumerators");
            ensurenotenumd("GetEnumerators");
            ensurewassortd("GetEnumerators");
            enumd = true;

            FixedArrayComboListEnumerator[] results;
            lock (dslaves)
            {
                int i;
                results = new FixedArrayComboListEnumerator[dslaves.Count];

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
                    results[i] = new FixedArrayComboListEnumerator(this, slave, dlldata, classname, pluginsource);
                    results[i].buf = this.buf; // Not needed, but fine.
                    i++;
                }
            }
            return results;
        }

        public FixedArrayComboListEnumerator[] GetEnumerators(string dllfilepath, string classname)
        {
            byte[] dlldata = null;
            if (null != dllfilepath)
            {
                dlldata = System.IO.File.ReadAllBytes(dllfilepath);
            }
            return GetEnumerators(dlldata, classname, null);
        }

        public FixedArrayComboListEnumerator[] GetEnumerators(string dllfilepath)
        {
            return GetEnumerators(dllfilepath, null);
        }

        public FixedArrayComboListEnumerator[] GetEnumerators()
        {
            return GetEnumerators(null, null);
        }


        protected internal FixedArrayComboListEnumerator[] GetEnumeratorsWithFullSource(string code, string classname)
        {
            System.CodeDom.Compiler.CompilerParameters cp = new System.CodeDom.Compiler.CompilerParameters(new string[] { "IMapReduce.dll" });
#if DEBUG
            cp.IncludeDebugInformation = true;
#endif
            System.CodeDom.Compiler.CompilerResults cr = null;
            FixedArrayComboListEnumerator[] result;
            string reason = "";
            for (int rotor = 1; ; rotor++)
            {
                /*if (rotor > 10)
                {
                    throw new System.IO.FileNotFoundException("FixedArrayComboList.GetEnumerators dynamic C# compilation: Unable to create DLL" + reason);
                }*/
                try
                {
                    cp.OutputAssembly = ".\\temp_" + Guid.NewGuid() + ".dll";
                    cp.GenerateExecutable = false; // Not exe, but dll.
                    cp.GenerateInMemory = false;
                    cp.CompilerOptions = getcompileropts();

                    using (new System.Threading.Mutex(true, "DynCmp"))
                    {
                        using (Microsoft.CSharp.CSharpCodeProvider cscp = new Microsoft.CSharp.CSharpCodeProvider())
                        {
                            cr = cscp.CompileAssemblyFromSource(cp, code);
                        }
                        CleanCompilerFiles();
                    }
                    if (cr.Errors.HasErrors)
                    {
                        try
                        {
                            lock (typeof(DistObject))
                            {
                                System.IO.File.WriteAllText("error.cs", code);
                            }
                        }
                        catch
                        {
                        }
                        throw new Exception("GetEnumerators code compile error: " + cr.Errors[0].ToString());
                    }
                    if (0 != cr.NativeCompilerReturnValue)
                    {
                        LogLine("GetEnumerators code compile did not return 0 (returned " + cr.NativeCompilerReturnValue.ToString() + "): ");
                        for (int i = 0; i < cr.Output.Count; i++)
                        {
                            string ss = cr.Output[i].Trim();
                            if (0 != ss.Length)
                            {
                                LogLine("  C" + rotor.ToString() + "- " + cr.Output[i]);
                            }
                        }
                    }
                    System.Threading.Thread.Sleep(1000 * rotor);

                    //System.IO.File.WriteAllText("xlib_" + classname + ".xlib", code); // Conflicts with multiple running inline reducers.

                    byte[] dlldata = System.IO.File.ReadAllBytes(cr.PathToAssembly);
                    result = GetEnumerators(dlldata, classname, code);

                    //ArrayComboListEnumerator.SendDllNow() on all enumerators so it can be deleted.
                    for (int i = 0; i != result.Length; i++)
                    {
                        result[i].SendDllNow();
                    }
                    break; // Good.
                }
                catch (System.IO.IOException e)
                {
                    LogLine("Rotor retry: " + e.ToString());
                    reason = ": " + e.ToString();
                    continue;
                }
            }

            try
            {
                System.IO.File.Delete(cr.PathToAssembly);
#if DEBUG
                System.IO.File.Delete(cr.PathToAssembly.Replace(".dll", ".pdb"));
#endif
            }
            catch (Exception eee)
            {
                int i23zzz = 23 + 23;
            }

            return result;
        }


        public FixedArrayComboListEnumerator[] GetEnumeratorsWithCode(string code)
        {
            return GetEnumeratorsWithFullSource(
                @"using System;
using System.Collections.Generic;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.EasyReducer
{
    public class RandomAccessReducer : IBeforeReduceFixed, IAfterReduce
    {
        int i = 0;
        RandomAccessOutput raout = null;
        RandomAccessEntries raentries = null;
        
        public void OnEnumeratorFinished()
        {
            Type thistype = this.GetType();
            System.Reflection.MethodInfo finalmethod = thistype.GetMethod(" + "\"" + @"ReduceFinalize" + "\"" + @");
            if(null != finalmethod)
            {
                finalmethod.Invoke(this, null);
            }
        }

        public bool OnGetEnumerator(EntriesInput input, FixedEntriesOutput output)
        {
            if(null == raout)
            {
                raout = new RandomAccessOutput(output);
            }
            if(null == raentries)
            {
                raentries = new RandomAccessEntries();
            }
            raentries.SetInput(input);
            byte[] firstkeybuf, xkeybuf;
            int firstkeyoffset, xkeyoffset;
            int firstkeylen, xkeylen;
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
                Reduce(raentries[0].Key, raentries, raout);
                i += len;
                return true; // Continue.
            }
            i = 0;
            return false; // At end; stop.
        }


        public class RandomAccessEntries
        {
            public RandomAccessEntries()
            {
            }
            
            
            internal void SetInput(EntriesInput input)
            {
                this.input = input;
            }


            internal void set(int offset, int length)
            {
                this.offset = offset;
                this.length = length;
            }


            public int Length
            {
                get
                {
                    return length;
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


            int offset, length;
            EntriesInput input;
        }


        public class RandomAccessOutput
        {
            public RandomAccessOutput(FixedEntriesOutput eoutput)
            {
                this.eoutput = eoutput;
            }


            public void Add(ByteSlice key, ByteSlice value)
            {
                eoutput.Add(key, value);
            }


            FixedEntriesOutput eoutput;
        }


        public virtual void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
        {
            for (int i = 0; i < values.Length; i++)
            {
                ByteSlice value = values[i].Value;
                output.Add(key, value);
            }
        }
    }


    class EnumWithCodeReducer : RandomAccessReducer
    {
        public EnumWithCodeReducer()
        {
                Type thistype = this.GetType();
                System.Reflection.MethodInfo initmethod = thistype.GetMethod(" + "\"" + @"ReduceInitialize" + "\"" + @");
                if(null != initmethod)
                {
                    initmethod.Invoke(this, null);
                }
        }
"
                + code // !
                + @"    }

}
", "EnumWithCodeReducer");
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


    public class FixedArrayComboListEnumerator
    {
        internal FixedArrayComboList acl;
        internal SlaveInfo slave;
        internal bool first = true;
        byte[] dlldata; // null if using default enumerator.
        string plugincode; // only valid if compiled with source.
        string classname; // null if using first class found in dllfilepath with IBeforeReduceFixed.


        public void SendDllNow()
        {
            if (first)
            {
                first = false;
                internalsendreset();
            }
        }


        internal FixedArrayComboListEnumerator(FixedArrayComboList acl, SlaveInfo slave, byte[] dlldata, string classname, string plugincode)
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


        public virtual FixedArrayComboList.ACEntry Current
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


        private FixedArrayComboList.ACEntry cur;
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
            cur.keybuf = nextbuf;
            cur.keyoffset = nextbufpos;
            cur.keylen = acl.GetKeyBufferLength();

            cur.valuebuf = nextbuf;
            cur.valueoffset = nextbufpos + cur.keylen;
            cur.valuelen = acl.GetValueBufferLength();

            nextbufpos += cur.keylen + cur.valuelen;
        }


        internal void internalsendreset()
        {
            if (null != dlldata)
            {
                // 'D' to send DLL containing IBeforeReduceFixed plugin..
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
                XContent.SendXContent(slave.nstm, dlldata);
                slave.SlaveErrorCheck();
            }
        }
    }
}
