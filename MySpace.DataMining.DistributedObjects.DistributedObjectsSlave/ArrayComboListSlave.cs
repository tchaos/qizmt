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

// If extra logging is enabled, enable timings.
//#define ENABLE_TIMING

//#define COOK_TEST_ZMapStreamToZBlocks

#if DEBUG
//#define DEBUG_SPLIT_SIZE
#endif

//#define REDUCE_SPLIT_CALLS_GC


using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{ 
    public class ArrayComboListPart : DistObjectBase
    {
        public class ZBlock
        {

#if DEBUG_SPLIT_SIZE
            public const int ZFILE_SPLIT_SIZE = 0x400 * 0x400 * 32; // Testing.
            public const int ZFILE_SPLITBY_SIZE = ZFILE_SPLIT_SIZE;

            // Need extra space to account for bytes encoding lengths.
            public const int MAXKVBUFSIZE = 0x400 * 0x400 * 40; // Testing.
#else
            public const int ZFILE_SPLIT_SIZE = 0x400 * 0x400 * 0x400; // 1 GB
            public const int ZFILE_SPLITBY_SIZE = 0x400 * 0x400 * 200;

            // Need extra space to account for bytes encoding lengths.
            public const int MAXKVBUFSIZE = 0x400 * 0x400 * 200;
#endif

            public const long ZVALUEBLOCK32_LIMIT = int.MaxValue;

            ArrayComboListPart parent;
            internal int zblockID; // ZBlock ID (0-based n)

            int keyblockbuflen, valueblockbuflen;
            System.IO.FileStream fzkeyblock;
            System.IO.FileStream fzvalueblock;
            string fzkeyblockfilename;
            string fzvalueblockfilename;
            long zvalueblocksize = 0;
            long zkeyblocksize = 0;
            long numadded = 0;
            bool addmode = true;

            internal bool splitzkeyfile = false; // Key files need to be split (e.g. for sort).
            internal bool splitzblocks = false; // Key+values need to be split if both loaded (e.g. for reduce).

            internal BlockInfo<KeyBlockEntry32v> block32;
            internal BlockInfo<KeyBlockEntry64v> block64;

            internal int TValueOffset_Size;


            public bool ZBlockSplit
            {
                get
                {
                    return splitzkeyfile || splitzblocks;
                }
            }


            public void Close()
            {
                _justclose();

                if (null != fzkeyblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzkeyblockfilename);
                        fzkeyblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
                if (null != fzvalueblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzvalueblockfilename);
                        fzvalueblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            private void _justclose()
            {
                if (null != fzkeyblock)
                {
                    fzkeyblock.Close();
                    fzkeyblock = null;
                }
                if (null != fzvalueblock)
                {
                    fzvalueblock.Close();
                    fzvalueblock = null;
                }
            }


            public void LeaveAddMode(int readkeybuflen, int readvaluebuflen)
            {
                addmode = false;
                keyblockbuflen = readkeybuflen;
                valueblockbuflen = readvaluebuflen;
                _justclose();
            }


            internal static long jid
            {
                get
                {
                    return DistributedObjectsSlave.jid;
                }
            }

            internal static string sjid
            {
                get
                {
                    return DistributedObjectsSlave.sjid;
                }
            }


            internal static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            internal static string CreateZBlockFileName(int zblockID, string otherinfo)
            {
                return "zblock_" + _spid + "_" + zblockID.ToString() + "_" + otherinfo + ".j" + sjid + ".zb";
            }

            internal static string CreateZBallFileName(int zblockID, string cachename, string otherinfo)
            {
                if(string.IsNullOrEmpty(cachename))
                {
                    throw new Exception("ZBlock.CreateZBallFileName: no cache name");
                }
                return "zsball_" + cachename + "_" + zblockID.ToString() + "_" + otherinfo + ".zsb";
            }


            public static void CleanZBlockFiles(params string[] otherinfos)
            {
                // Clean any potential old zblock files...
                bool found = true;
                for (int i = 0; found; i++)
                {
                    found = false;
                    foreach (string otherinfo in otherinfos)
                    {
                        string fn = ZBlock.CreateZBlockFileName(i, otherinfo);
                        if (System.IO.File.Exists(fn))
                        {
                            System.IO.File.Delete(fn);
                            found = true;
                        }
                    }
                }
            }


            internal void ensurefzblock(bool sorting, bool zballing)
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = parent.CookRetries;
                for (; ; )
                {
                    try
                    {
                //----------------------------COOKING--------------------------------
                        {
                            System.IO.FileAccess access = System.IO.FileAccess.Read;
                            System.IO.FileMode mode = System.IO.FileMode.Open;
                            if (addmode || sorting || zballing)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Create;
                            }

                            if (null == fzkeyblock)
                            {
                                fzkeyblock = new System.IO.FileStream(fzkeyblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                            }
                        }

                        if (null == fzvalueblock)
                        {
                            System.IO.FileAccess access = System.IO.FileAccess.Read;
                            System.IO.FileMode mode = System.IO.FileMode.Open;
                            if (addmode)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Create;
                            }
                            if (zballing)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Open;
                            }
                            fzvalueblock = new System.IO.FileStream(fzvalueblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                        }
                //----------------------------COOKING--------------------------------
                    }
                    catch (Exception e)
                    {
                        bool firstcook = cooking_cooksremain == parent.CookRetries;
                        if (cooking_cooksremain-- <= 0)
                        {
                            string ns = " (unable to get connection count)";
                            try
                            {
                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                    + " total connections on this machine)";
                            }
                            catch
                            {
                            }
                            throw new System.IO.IOException("cooked too many times (retries="
                                + parent.CookRetries.ToString()
                                + "; timeout=" + parent.CookTimeout.ToString()
                                + ") on " + System.Net.Dns.GetHostName() + ns, e);
                        }
                        System.Threading.Thread.Sleep(parent.CookTimeout);
                        if (firstcook)
                        {
                            try
                            {
                                XLog.errorlog("cooking started (retries=" + parent.CookRetries.ToString()
                                    + "; timeout=" + parent.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName()
                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                            }
                            catch
                            {
                            }
                        }
                        continue; // !
                    }
                    break;
                }
                //----------------------------COOKING--------------------------------
            }

            internal void ensurefzblock(bool sorting)
            {
                ensurefzblock(sorting, false);
            }


            internal ZBlock(ArrayComboListPart parent, int zblockID, int addkeybuflen, int addvaluebuflen)
            {
                this.parent = parent;
                this.zblockID = zblockID;
                this.TValueOffset_Size = parent.TValueOffset_Size;

                this.keyblockbuflen = addkeybuflen;
                this.valueblockbuflen = addvaluebuflen;

                fzkeyblockfilename = CreateZBlockFileName(zblockID, "key_unsorted");
                fzvalueblockfilename = CreateZBlockFileName(zblockID, "value");
                ensurefzblock(false);
            }


            public void Flush()
            {
                if (null != fzkeyblock)
                {
                    fzkeyblock.Flush();
                }
                if (null != fzvalueblock)
                {
                    fzvalueblock.Flush();
                }
            }

            internal long getzkeyblocksize()
            {
                return zkeyblocksize;
            }

            public bool Add(byte[] keybuf, int keyoffset, byte[] valuebuf, int valueoffset, int valuelength)
            {
                long x = (long)(parent.keylen + TValueOffset_Size) + (long)valuelength;
                if (x >= ZFILE_SPLITBY_SIZE)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //parent.SetError("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }
                if (zvalueblocksize + valuelength > ZVALUEBLOCK32_LIMIT
                    && 4 == TValueOffset_Size)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (values)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock value file size > ZVALUEBLOCK_LIMIT) (consider increasing sub process count)");
                    //return false;
                }

                if (zkeyblocksize + parent.keylen + TValueOffset_Size >= ZFILE_SPLIT_SIZE)
                {
                    splitzkeyfile = true;
                }

                fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                if (8 == TValueOffset_Size)
                {
                    Entry.LongToBytes(zvalueblocksize, parent._smallbuf, 0);
                }
                else
                {
                    Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                }
                fzkeyblock.Write(parent._smallbuf, 0, TValueOffset_Size);

                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzvalueblock.Write(parent._smallbuf, 0, 4);
                fzvalueblock.Write(valuebuf, valueoffset, valuelength);

                //valueaddedbytes += valuelength;
                numadded++;

                zvalueblocksize += 4 + valuelength;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + TValueOffset_Size;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                if (zvalueblocksize + zkeyblocksize >= ZFILE_SPLIT_SIZE)
                {
                    splitzblocks = true;
                }

                return true;
            }

            public bool Add(IList<byte> keybuf, int keyoffset, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                long x = (long)(parent.keylen + TValueOffset_Size) + (long)valuelength;
                if (x >= ZFILE_SPLITBY_SIZE)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //parent.SetError("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }
                if (zvalueblocksize + valuelength > ZVALUEBLOCK32_LIMIT
                    && 4 == TValueOffset_Size)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (values)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock value file size > ZVALUEBLOCK_LIMIT) (consider increasing sub process count)");
                    //return false;
                }

                if (zkeyblocksize + parent.keylen + TValueOffset_Size >= ZFILE_SPLIT_SIZE)
                {
                    splitzkeyfile = true;
                }

                //fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                parent.StreamWrite(fzkeyblock, keybuf, keyoffset, parent.keylen);
                if (8 == TValueOffset_Size)
                {
                    Entry.LongToBytes(zvalueblocksize, parent._smallbuf, 0);
                }
                else
                {
                    Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                }
                fzkeyblock.Write(parent._smallbuf, 0, TValueOffset_Size);

                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzvalueblock.Write(parent._smallbuf, 0, 4);
                //fzvalueblock.Write(valuebuf, valueoffset, valuelength);
                parent.StreamWrite(fzvalueblock, valuebuf, valueoffset, valuelength);

                //valueaddedbytes += valuelength;
                numadded++;

                zvalueblocksize += 4 + valuelength;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + TValueOffset_Size;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                if (zvalueblocksize + zkeyblocksize >= ZFILE_SPLIT_SIZE)
                {
                    splitzblocks = true;
                }

                return true;
            }


            //--A-------------ADDCOOK------------------
            public void CopyInto(List<byte[]> net, List<Entry> entries, ref byte[] ebuf, ref byte[] evaluesbuf)
            {
#if DEBUG
                if (splitzblocks)
                {
                    throw new Exception("DEBUG:  CopyInto: cannot load all if splitzblocks");
                }
#endif
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = parent.CookRetries;
                //----------------------------COOKING--------------------------------
                for (; ; )
                {
                    try
                    {
                        ensurefzblock(false);

                        net.Clear();
                        entries.Clear();

                        long kflen = fzkeyblock.Length;
                        long keycount = numadded;

                        long vflen = fzvalueblock.Length;

                        //try
                        {
                            if (evaluesbuf.LongLength < vflen)
                            {
                                evaluesbuf = null;
                                long lnew = Entry.Round2PowerLong(vflen);
                                evaluesbuf = new byte[lnew];
                            }
                            byte[] valuesbuf = evaluesbuf;

                            if (ebuf.LongLength < kflen + vflen)
                            {
                                try
                                {
                                    ebuf = null;
                                    long lnew = Entry.Round2PowerLong(kflen + vflen);
                                    if (lnew > Int32.MaxValue)
                                    {
                                        lnew = Int32.MaxValue;
                                    }
                                    ebuf = new byte[lnew];
                                }
                                catch (Exception e)
                                {
                                    unchecked
                                    {
                                        string better_error = e.ToString();
                                        better_error += System.Environment.NewLine;
                                        better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                        better_error += "zblock combined length: " + (kflen + vflen).ToString() + System.Environment.NewLine;
                                        better_error += "ebuf length: " + ebuf.LongLength.ToString() + System.Environment.NewLine;
                                        better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                        throw new Exception(better_error);
                                    }
                                }
                            }
                            byte[] buf = ebuf;

                            net.Add(buf);

                            int newcap = (int)keycount;
                            /*if (isfirstlist)
                            {
                                isfirstlist = false;
                                newcap *= 2;
                            }*/
                            if (newcap > entries.Capacity)
                            {
                                entries.Capacity = newcap;
                            }

                            fzvalueblock.Seek(0, System.IO.SeekOrigin.Begin);
                            int rd883 = fzvalueblock.Read(valuesbuf, 0, (int)vflen);
#if DEBUG
                            if (rd883 != vflen)
                            {
                                throw new Exception("DEBUG:  (rd883 != vflen)");
                            }
#endif

                            // Read from existing unsorted zblock file into buffer.
                            fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                            long sofar = 0;
                            Entry ent;
                            ent.NetEntryOffset = 0;
                            ent.NetIndex = 0;
                            int offset = 0;
                            unchecked
                            {
                                for (long i = 0; i != keycount; i++)
                                {
                                    int morespace = offset + parent.keylen + TValueOffset_Size;
                                    if (morespace < offset || morespace > buf.Length)
                                    {
                                        offset = 0;
                                        buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                        net.Add(buf);
                                        ent.NetEntryOffset++;
                                    }

                                    ent.NetEntryOffset = offset;
                                    int read8383 = fzkeyblock.Read(buf, offset, parent.keylen);
#if DEBUG
                                    if (read8383 != parent.keylen)
                                    {
                                        throw new Exception("DEBUG:  (read8383 != parent.keylen)");
                                    }
#endif
                                    offset += parent.keylen;

                                    int valueoffset;
                                    int rd424 = fzkeyblock.Read(parent._smallbuf, 0, TValueOffset_Size);
#if DEBUG
                                    if (rd424 != TValueOffset_Size)
                                    {
                                        System.Diagnostics.Debugger.Launch();
                                        throw new Exception("DEBUG:  (rd424 != TValueOffset_Size)");
                                    }
#endif
                                    if (8 == TValueOffset_Size)
                                    {
                                        long long_valueoffset = Entry.BytesToLong(parent._smallbuf);
#if DEBUG
                                        if (long_valueoffset > int.MaxValue)
                                        {
                                            throw new Exception("DEBUG:  (long_valueoffset > int.MaxValue) should split");
                                        }
                                        if (long_valueoffset < 0)
                                        {
                                            throw new Exception("DEBUG:  (long_valueoffset < 0) invalid offset");
                                        }
#endif
                                        valueoffset = (int)long_valueoffset;
                                    }
                                    else
                                    {
                                        valueoffset = Entry.BytesToInt(parent._smallbuf);
                                    }

#if DEBUG
                                    if (valueoffset > valuesbuf.Length)
                                    {
                                        System.Diagnostics.Debugger.Launch();
                                        throw new Exception("DEBUG:  (valueoffset{" + valueoffset.ToString()
                                            + "} > valuesbuf.Length{" + valuesbuf.Length.ToString() + "})");
                                    }
#endif
                                    Buffer.BlockCopy(valuesbuf, valueoffset, buf, offset, 4);
                                    int valuelen = Entry.BytesToInt(buf, offset);
                                    offset += 4;

                                    // Handle truncated valuesbuf...
                                    if ((long)valueoffset + (long)valuelen > valuesbuf.LongLength)
                                    {
                                        offset -= parent.keylen + 4; // Undo.
                                        continue;
                                    }

                                    morespace = offset + valuelen;
                                    if (morespace < offset || morespace > buf.Length)
                                    {
                                        offset = 0;
                                        buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                        net.Add(buf);
                                        ent.NetEntryOffset++;
                                        i--; // So next iteration does this index again.
                                        continue;
                                    }

                                    Buffer.BlockCopy(valuesbuf, valueoffset + 4, buf, offset, valuelen);
                                    offset += valuelen;

                                    sofar += parent.keylen + 4 + valuelen;

                                    entries.Add(ent);
                                }
                            }
                        }
                        /*catch (Exception e)
                        {
                            net.Clear();
                            entries.Clear();
                            parent.SetError("Enumeration failure; zblock skipped: " + e.ToString());
                        }*/

                        _justclose();
                    }
                    catch(Exception e)
                    {
                        //----------------------------COOKING--------------------------------
                        bool firstcook = cooking_cooksremain == parent.CookRetries;
                        if (cooking_cooksremain-- <= 0)
                        {
                            string ns = " (unable to get connection count)";
                            try
                            {
                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                    + " total connections on this machine)";
                            }
                            catch
                            {
                            }
                            throw new System.IO.IOException("cooked too many times (retries="
                                + parent.CookRetries.ToString()
                                + "; timeout=" + parent.CookTimeout.ToString()
                                + ") on " + System.Net.Dns.GetHostName() + ns, e);
                        }
                        try
                        {
                            _justclose();
                        }
                        catch
                        {
                        }
                        System.Threading.Thread.Sleep(parent.CookTimeout);
                        if (firstcook)
                        {
                            try
                            {
                                XLog.errorlog("cooking started (retries=" + parent.CookRetries.ToString()
                                    + "; timeout=" + parent.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName()
                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                            }
                            catch
                            {
                            }
                        }
                        continue;
                        //----------------------------COOKING--------------------------------
                    }
                    break;
                }
            }


            internal struct KvSplit
            {
                internal ByteSlice Key;

                const IList<byte> vbuf_novalue = null;
                IList<byte> vbuf;
                int v1;
                int v2;

                internal bool IsValueSet
                {
                    get
                    {
                        return !object.ReferenceEquals(vbuf_novalue, this.vbuf);
                    }
                }

                internal long ValueOffset
                {
                    get
                    {
                        return (long)((ulong)(uint)v1 | ((ulong)(uint)v2 << 32));
                    }

                    set
                    {
                        this.vbuf = vbuf_novalue;
                        this.v1 = (int)(ulong)value;
                        this.v2 = (int)((ulong)value >> 32);
                    }
                }

                internal ByteSlice Value
                {
                    get
                    {
                        return ByteSlice.Prepare(this.vbuf, (int)this.v1, this.v2);
                    }

                    set
                    {
                        value.GetComponents(out this.vbuf, out this.v1, out this.v2);
                        if (this.vbuf == null)
                        {
                            throw new Exception("DEBUG:  KvSplit.Value: (this.vbuf == null)");
                        }
                    }
                }

                internal static KvSplit Prepare(ByteSlice key)
                {
                    KvSplit result;
                    result.Key = key;
                    result.vbuf = vbuf_novalue;
                    result.v1 = 0;
                    result.v2 = 0;
                    return result;
                }

            }


            internal void _CreateLargeResultFile(string createfilename, List<KvSplit> kvsbuf,
                ref byte[] ebuf, ref byte[] evaluesbuf)
            {
                using (System.IO.Stream result = new System.IO.FileStream(createfilename,
                    System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
                {
                    int avgvaluespace = checked((int)(zvalueblocksize / numadded)); // Includes value header.
                    int xvaluespace = avgvaluespace + avgvaluespace / 8 + 4; // Adjust a bit for larger values.
                    int maxkeyspersplit = checked((int)((ZFILE_SPLITBY_SIZE / 2) / (parent.keylen + TValueOffset_Size)));
                    int maxvaluespersplit = checked((int)((ZFILE_SPLITBY_SIZE / 2) / (xvaluespace)));
                    
                    int numkeyspersplit = Math.Min(maxkeyspersplit, maxvaluespersplit);

                    ensurefzblock(false);

                    if (evaluesbuf.LongLength < ZFILE_SPLITBY_SIZE)
                    {
                        evaluesbuf = null;
                        //long lnew = Entry.Round2PowerLong(ZFILE_SPLITBY_SIZE);
                        long lnew = ZFILE_SPLITBY_SIZE;
                        evaluesbuf = new byte[lnew];
                    }
                    byte[] valuesbuf = evaluesbuf;

                    if (ebuf.LongLength < ZFILE_SPLITBY_SIZE)
                    {
                        try
                        {
                            ebuf = null;
                            long lnew = Entry.Round2PowerLong(ZFILE_SPLITBY_SIZE);
                            if (lnew > Int32.MaxValue)
                            {
                                lnew = Int32.MaxValue;
                            }
                            ebuf = new byte[lnew];
                        }
                        catch (Exception e)
                        {
                            unchecked
                            {
                                string better_error = e.ToString();
                                better_error += System.Environment.NewLine;
                                better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                better_error += "ebuf length: " + ebuf.LongLength.ToString() + System.Environment.NewLine;
                                better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                throw new Exception(better_error);
                            }
                        }
                    }
                    byte[] buf = ebuf;

                    int newcap = numkeyspersplit;
                    /*if (isfirstlist)
                    {
                        isfirstlist = false;
                        newcap *= 2;
                    }*/
                    if (newcap > kvsbuf.Capacity)
                    {
                        kvsbuf.Capacity = newcap;
                    }

                    //----------------------------COOKING--------------------------------
                    int cooking_cooksremain = parent.CookRetries;
                    bool cooking_inIO = false;
                    long cooking_seekpos = 0;
                    //----------------------------COOKING--------------------------------

                    long keyblocksofar = 0;
                    byte[] xbuf = buf;
                    int xoffset = 0;
                    int keylen = parent.keylen;

                    for (; ; )
                    {
                        try
                        {

                            // Read from existing unsorted zblock file into buffer.
                            cooking_inIO = true;
                            fzkeyblock.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                            cooking_inIO = false;

                            // If cooking, this loop should continue where it left off.
                            for (;
                                keyblocksofar < zkeyblocksize;
                                kvsbuf.Clear(), xoffset = 0, xbuf = buf)
                            {

                                for (int ik = 0; ik < numkeyspersplit; ik++)
                                {
                                    {
                                        int morespace = xoffset + keylen + TValueOffset_Size;
                                        if (morespace < xoffset || morespace > xbuf.Length)
                                        {
                                            throw new Exception("DEBUG:  _CreateLargeResultFile: buffer not large enough for keys");
                                            //xoffset = 0;
                                            //xbuf = new byte[ZFILE_SPLITBY_SIZE];
                                        }
                                    }

                                    cooking_inIO = true;
                                    int read8291 = fzkeyblock.Read(xbuf, xoffset, keylen + TValueOffset_Size);
                                    if (0 == read8291)
                                    {
                                        cooking_inIO = false;
                                        break;
                                    }
#if DEBUG
                                    if (read8291 != keylen + TValueOffset_Size)
                                    {
                                        throw new Exception("DEBUG:  (read8291 != keylen + TValueOffset_Size)");
                                    }
#endif
                                    cooking_inIO = false;
                                    cooking_seekpos += keylen + TValueOffset_Size;

                                    KvSplit kvs = KvSplit.Prepare(ByteSlice.Prepare(xbuf, xoffset, keylen));
                                    if (8 == TValueOffset_Size)
                                    {
                                        kvs.ValueOffset = Entry.BytesToLong(xbuf, xoffset + keylen);
                                    }
                                    else
                                    {
                                        kvs.ValueOffset = Entry.BytesToUInt32(xbuf, xoffset + keylen);
                                    }
#if DEBUG
                                    if (kvs.ValueOffset >= zvalueblocksize)
                                    {
                                        throw new Exception("DEBUG:  _CreateLargeResultFile: (kvs.ValueOffset >= zvalueblocksize)");
                                    }
#endif
                                    xoffset += keylen; // Excluding the +N for value offset.
                                    keyblocksofar += keylen + TValueOffset_Size;
                                    kvsbuf.Add(kvs);
                                }
                                int kvsbufCount = kvsbuf.Count;

                                // If cooking, this whole loop can be started over.
                                for (long valueblockoffset = 0; valueblockoffset < zvalueblocksize; )
                                {
                                    cooking_inIO = true;
                                    fzvalueblock.Seek(valueblockoffset, System.IO.SeekOrigin.Begin);
                                    int read = fzvalueblock.Read(valuesbuf, 0, ZFILE_SPLITBY_SIZE);
                                    cooking_inIO = false;
                                    long vstart = valueblockoffset;
                                    long vstop = vstart + read;
                                    for (int ik = 0; ik < kvsbufCount; ik++)
                                    {
                                        KvSplit kvs = kvsbuf[ik];
                                        if (!kvs.IsValueSet)
                                        {
                                            long kvsValueOffset = kvs.ValueOffset;
                                            if (kvsValueOffset >= vstart
                                                && kvsValueOffset < vstop)
                                            {
                                                int valuesbufoffset = (int)(kvsValueOffset - vstart);
#if DEBUG
                                                if (valuesbufoffset < 0)
                                                {
                                                    throw new Exception("DEBUG:  _CreateLargeResultFile: (valuebufoffset < 0) (long->int overflow)");
                                                }
#endif
                                                if (valuesbufoffset + 4 > read)
                                                {
                                                    // Wasn't even enough in the valuesbuf to get the length.
                                                    read -= ((valuesbufoffset + 4) - read); // Read it next time.
                                                }
                                                else
                                                {
                                                    int valuelen = Entry.BytesToInt(valuesbuf, valuesbufoffset);
                                                    if (valuesbufoffset + 4 + valuelen > read)
                                                    {
                                                        // Entire value not in valuesbuf.
                                                        read = valuesbufoffset; // Read it next time.
                                                    }
                                                    else
                                                    {
                                                        {
                                                            int morespace = xoffset + valuelen;
                                                            if (morespace < xoffset || morespace > xbuf.Length)
                                                            {
                                                                xoffset = 0;
                                                                //xbuf = new byte[Math.Min(ZFILE_SPLITBY_SIZE, (valuelen + 8) * 4)];
                                                                {
                                                                    try
                                                                    {
                                                                        int xoldlen = xbuf.Length;
                                                                        ebuf = null;
                                                                        buf = null;
                                                                        xbuf = null;
                                                                        long lnew = Math.Max(xoldlen + valuelen * 2 + 16, xoldlen + xoldlen / 3);
                                                                        lnew = Entry.Round2PowerLong(lnew);
                                                                        if (lnew > Int32.MaxValue)
                                                                        {
                                                                            lnew = Int32.MaxValue;
                                                                        }
                                                                        ebuf = new byte[lnew];
                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        unchecked
                                                                        {
                                                                            string better_error = e.ToString();
                                                                            better_error += System.Environment.NewLine;
                                                                            better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                                                            better_error += "ebuf length: " + ebuf.LongLength.ToString() + System.Environment.NewLine;
                                                                            better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                                                            throw new Exception(better_error);
                                                                        }
                                                                    }
                                                                }
                                                                buf = ebuf;
                                                                xbuf = buf;
                                                                XLog.log("Warning: _CreateLargeResultFile: created new value buffer of length " + xbuf.Length
                                                                    + " (for value of length " + valuelen + ")");
                                                            }
                                                        }
                                                        Buffer.BlockCopy(valuesbuf, valuesbufoffset + 4, xbuf, xoffset, valuelen);
                                                        kvs.Value = ByteSlice.Prepare(xbuf, xoffset, valuelen);
                                                        kvsbuf[ik] = kvs;
                                                        xoffset += valuelen;

                                                    }
                                                }
                                            }
                                        }
                                    }
                                    valueblockoffset += read;
                                }

                                byte[] rbuf = null;
                                for (int ik = 0; ik < kvsbufCount; ik++)
                                {
                                    KvSplit kvs = kvsbuf[ik];
                                    if (!kvs.IsValueSet)
                                    {
                                        throw new Exception("DEBUG:  _CreateLargeResultFile: (!kvs.IsValueSet) (not all values loaded)");
                                    }
                                    IList<byte> rlist;
                                    int roffset;
                                    int rlength;
                                    {
                                        ByteSlice bs = kvs.Key;
                                        bs.GetComponents(out rlist, out roffset, out rlength);
                                        if (!object.ReferenceEquals(rlist, rbuf))
                                        {
                                            rbuf = rlist as byte[];
#if DEBUG
                                            if (null == rbuf)
                                            {
                                                throw new Exception("DEBUG:  _CreateLargeResultFile: (null == rbuf) (expected KvSplit.Key to be byte[])");
                                            }
#endif
                                        }
                                        result.Write(rbuf, roffset, rlength);
                                    }

                                    {
                                        ByteSlice bs = kvs.Value;
                                        bs.GetComponents(out rlist, out roffset, out rlength);
                                        if (!object.ReferenceEquals(rlist, rbuf))
                                        {
                                            rbuf = rlist as byte[];
#if DEBUG
                                            if (null == rbuf)
                                            {
                                                throw new Exception("DEBUG:  _CreateLargeResultFile: (null == rbuf) (expected KvSplit.Value to be byte[])");
                                            }
#endif
                                        }
                                        {
                                            Entry.ToBytes(rlength, parent._smallbuf, 0);
                                            result.Write(parent._smallbuf, 0, 4);
                                        }
                                        result.Write(rbuf, roffset, rlength);
                                    }

                                }

#if REDUCE_SPLIT_CALLS_GC
                                System.GC.Collect();
                                System.GC.WaitForPendingFinalizers();
#endif

                            }

                        }
                        catch (Exception e)
                        {
                            if (!cooking_inIO)
                            {
                                throw;
                            }
                            //----------------------------COOKING--------------------------------
                            bool firstcook = cooking_cooksremain == parent.CookRetries;
                            if (cooking_cooksremain-- <= 0)
                            {
                                string ns = " (unable to get connection count)";
                                try
                                {
                                    ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                        + " total connections on this machine)";
                                }
                                catch
                                {
                                }
                                throw new System.IO.IOException("cooked too many times (retries="
                                    + parent.CookRetries.ToString()
                                    + "; timeout=" + parent.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName() + ns, e);
                            }
                            System.Threading.Thread.Sleep(parent.CookTimeout);
                            if (firstcook)
                            {
                                try
                                {
                                    XLog.errorlog("cooking started (retries=" + parent.CookRetries.ToString()
                                        + "; timeout=" + parent.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName()
                                        + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                }
                                catch
                                {
                                }
                            }
                            continue;
                            //----------------------------COOKING--------------------------------
                        }
                        break;
                    }

                }

            }


            internal interface IKeyBlockEntry
            {
                ByteSlice GetKey();
                void SetKey(ByteSlice key);
                int GetValueOffsetSize();
                int GetValueOffset32();
                long GetValueOffset64();
                void SetValueOffset(int x);
                void SetValueOffset(long x);
                int SetValueOffsetFromBytes(IList<byte> buf, int offset); // Returns number of bytes.
                int GetValueOffsetToBytes(byte[] buf, int offset); // Returns number of bytes.
            }

            internal struct KeyBlockEntry32v : IKeyBlockEntry
            {
                public ByteSlice key;
                public int valueoffset;

                public int GetValueOffsetSize()
                {
                    return 4;
                }

                public ByteSlice GetKey()
                {
                    return key;
                }

                public void SetKey(ByteSlice key)
                {
                    this.key = key;
                }

                public int GetValueOffset32()
                {
                    return valueoffset;
                }

                public long GetValueOffset64()
                {
                    return valueoffset;
                }

                public void SetValueOffset(int x)
                {
                    valueoffset = x;
                }

                public void SetValueOffset(long x)
                {
                    valueoffset = checked((int)x);
                }

                public int SetValueOffsetFromBytes(IList<byte> buf, int offset)
                {
                    valueoffset = Entry.BytesToInt(buf, offset);
                    return 4;
                }

                public int GetValueOffsetToBytes(byte[] buf, int offset)
                {
                    Entry.ToBytes(valueoffset, buf, offset);
                    return 4;
                }

            }

            internal struct KeyBlockEntry64v : IKeyBlockEntry
            {
                public ByteSlice key;
                public long valueoffset;

                public int GetValueOffsetSize()
                {
                    return 8;
                }

                public ByteSlice GetKey()
                {
                    return key;
                }

                public void SetKey(ByteSlice key)
                {
                    this.key = key;
                }

                public int GetValueOffset32()
                {
                    return checked((int)valueoffset);
                }

                public long GetValueOffset64()
                {
                    return valueoffset;
                }

                public void SetValueOffset(int x)
                {
                    valueoffset = x;
                }

                public void SetValueOffset(long x)
                {
                    valueoffset = x;
                }

                public int SetValueOffsetFromBytes(IList<byte> buf, int offset)
                {
                    valueoffset = Entry.BytesToLong(buf, offset);
                    return 8;
                }

                public int GetValueOffsetToBytes(byte[] buf, int offset)
                {
                    Entry.LongToBytes(valueoffset, buf, offset);
                    return 8;
                }

            }


            internal class BlockInfo<TKeyBlockEntry>
                where TKeyBlockEntry : struct, IKeyBlockEntry
            {

                ArrayComboListPart acl;
                ZBlock zblock;

                int TValueOffset_Size;

                internal BlockInfo(ArrayComboListPart acl, ZBlock zblock)
                {
                    TValueOffset_Size = default(TKeyBlockEntry).GetValueOffsetSize();

                    this.acl = acl;
                    this.zblock = zblock;
                }


                // Starts reading from current position in stm.
                // returns true if limit was hit before EOF (more to read).
                bool _XCopyLimitInto_needcooking(System.IO.Stream stm, int NumberOfKeysLimit, List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {
                    bool result = false;
                    int ebufoffset = 0;
                    kentries.Clear();
                    //----------------------------COOKING--------------------------------
                    int cooking_cooksremain = acl.CookRetries;
                    bool cooking_inIO = false;
                    long cooking_seekpos = long.MinValue;
                    //----------------------------COOKING--------------------------------
                    checked
                    {
                        for (; ; )
                        {
                            try
                            {
                                cooking_inIO = true;
                                if (long.MinValue == cooking_seekpos)
                                {
                                    cooking_seekpos = stm.Position;
                                }
                                else
                                {
                                    stm.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                }
                                long kflenremain = stm.Length - cooking_seekpos;
                                cooking_inIO = false;

                                int keycount; // Key count for this turn.
                                {
                                    long _keycount = kflenremain / (acl.keylen + TValueOffset_Size);
                                    if (_keycount > NumberOfKeysLimit)
                                    {
                                        _keycount = NumberOfKeysLimit;
                                        result = true;
                                    }
                                    keycount = (int)_keycount;
                                }

                                if (ebuf.Length < acl.keylen * keycount + TValueOffset_Size)
                                {
                                    ebuf = null;
                                    long lnew = Entry.Round2Power(acl.keylen * keycount + TValueOffset_Size);
                                    if (lnew > Int32.MaxValue)
                                    {
                                        lnew = Int32.MaxValue;
                                    }
                                    ebuf = new byte[lnew];
                                }
                                byte[] buf = ebuf;

                                int newcap = keycount;
                                /*if (isfirstklist)
                                {
                                    isfirstklist = false;
                                    newcap *= 2;
                                }*/
                                if (newcap > kentries.Capacity)
                                {
                                    kentries.Capacity = newcap;
                                }

                                int kentreadsize = acl.keylen + TValueOffset_Size;
                                TKeyBlockEntry kent = default(TKeyBlockEntry);
                                //kent.key = ByteSlice.Create();
                                //kent.valueoffset = 0;
                                for (int i = 0; i != keycount; i++)
                                {
                                    cooking_inIO = true;
                                    int rd3991 = stm.Read(buf, ebufoffset, kentreadsize);
#if DEBUG
                                    if (rd3991 != kentreadsize)
                                    {
                                        throw new Exception("DEBUG:  (rd3991 != kentreadsize)");
                                    }
#endif
                                    cooking_inIO = false;
                                    cooking_seekpos += kentreadsize;

                                    kent.SetKey(ByteSlice.Create(buf, ebufoffset, acl.keylen));
                                    ebufoffset += acl.keylen;
                                    int sz815 = kent.SetValueOffsetFromBytes(buf, ebufoffset);
#if DEBUG
                                    if (sz815 != TValueOffset_Size)
                                    {
                                        throw new Exception("DEBUG:  (sz815 != TValueOffset_Size)");
                                    }
#endif
                                    kentries.Add(kent);
                                }

                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                //----------------------------COOKING--------------------------------
                                throw new NeedCookingException(e);
                                //----------------------------COOKING--------------------------------
                            }
                            break;
                        }
                    }
                    return result;

                }


                //--B-------------ADDCOOK------------------
                protected void XCopyInto(string keyblockfilename, List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {
#if DEBUG
                    if (zblock.splitzkeyfile)
                    {
                        throw new Exception("DEBUG:  XCopyInto: cannot load all if splitzkeyfile");
                    }
#endif

                    //----------------------------COOKING--------------------------------
                    int cooking_cooksremain = acl.CookRetries;
                    bool cooking_inIO = false;
                    //----------------------------COOKING--------------------------------
                    for (; ; )
                    {
                        try
                        {
                            cooking_inIO = true;
                            zblock._justclose();
                            using (System.IO.Stream stm = new System.IO.FileStream(keyblockfilename, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                            {
                                cooking_inIO = false;

                                _XCopyLimitInto_needcooking(stm, int.MaxValue, kentries, ref ebuf);

                            }
                        }
                        catch (NeedCookingException e)
                        {
                            //----------------------------COOKING--------------------------------
                            bool firstcook = cooking_cooksremain == acl.CookRetries;
                            if (cooking_cooksremain-- <= 0)
                            {
                                string ns = " (unable to get connection count)";
                                try
                                {
                                    ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                        + " total connections on this machine)";
                                }
                                catch
                                {
                                }
                                throw new System.IO.IOException("cooked too many times (retries="
                                    + acl.CookRetries.ToString()
                                    + "; timeout=" + acl.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName() + ns, e.InnerException); // InnerException!
                            }
                            System.Threading.Thread.Sleep(acl.CookTimeout);
                            if (firstcook)
                            {
                                try
                                {
                                    XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                        + "; timeout=" + acl.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName()
                                        + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                }
                                catch
                                {
                                }
                            }
                            continue;
                            //----------------------------COOKING--------------------------------
                        }
                        catch (Exception e)
                        {
                            if (!cooking_inIO)
                            {
                                throw;
                            }
                            //----------------------------COOKING--------------------------------
                            bool firstcook = cooking_cooksremain == acl.CookRetries;
                            if (cooking_cooksremain-- <= 0)
                            {
                                string ns = " (unable to get connection count)";
                                try
                                {
                                    ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                        + " total connections on this machine)";
                                }
                                catch
                                {
                                }
                                throw new System.IO.IOException("cooked too many times (retries="
                                    + acl.CookRetries.ToString()
                                    + "; timeout=" + acl.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName() + ns, e);
                            }
                            System.Threading.Thread.Sleep(acl.CookTimeout);
                            if (firstcook)
                            {
                                try
                                {
                                    XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                        + "; timeout=" + acl.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName()
                                        + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                }
                                catch
                                {
                                }
                            }
                            continue;
                            //----------------------------COOKING--------------------------------
                        }
                        break;
                    }
                }


                public void CopyInto(List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {
                    XCopyInto(zblock.fzkeyblockfilename, kentries, ref ebuf);
                }


                int _kcmp(TKeyBlockEntry x, TKeyBlockEntry y)
                {
                    int keylen = acl.keylen;
                    ByteSlice xkey = x.GetKey();
                    ByteSlice ykey = y.GetKey();
                    for (int i = 0; i != keylen; i++)
                    {
                        int diff = (int)xkey[i] - (int)ykey[i];
                        if (0 != diff)
                        {
                            return diff;
                        }
                    }
                    return 0;
                }


                public void _NormalSort(List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {

#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
                    int ij3j3j3 = 33 + 3;
#endif

                    zblock.ensurefzblock(false);

#if DEBUG
                    if (zblock.zkeyblocksize != zblock.fzkeyblock.Length)
                    {
                        throw new Exception("DEBUG:  _NormalSort: (zblock.zkeyblocksize != zblock.fzkeyblock.Length) before sort");
                    }
#endif

                    try
                    {
                        try
                        {
                            if (zblock.splitzkeyfile)
                            {
                                throw new NotImplementedException("Cannot normal-sort a split zBlock");
                            }
                            else
                            {
                                CopyInto(kentries, ref ebuf);
                            }
                        }
                        finally
                        {
                            // Delete old (unsorted) file; prepare new (sorted) one.
                            // Keep these together so that there's always one on file; so cleanup sees it and continues.
                            zblock._justclose();
                            System.IO.File.Delete(zblock.fzkeyblockfilename);
                            zblock.fzkeyblockfilename = CreateZBlockFileName(zblock.zblockID, "key_sorted");
                            zblock.ensurefzblock(true);
                        }

                        // Sort the sortbuffer.
                        kentries.Sort(new System.Comparison<TKeyBlockEntry>(_kcmp));

                        // From (sorted) sortbuffer write into new sorted zblock file.
                        foreach (TKeyBlockEntry kent in kentries)
                        {
                            kent.GetKey().CopyTo(acl._smallbuf);
                            zblock.fzkeyblock.Write(acl._smallbuf, 0, acl.keylen);
#if DEBUG
                            if (kent.GetValueOffset64() >= zblock.zvalueblocksize)
                            {
                                throw new Exception("DEBUG:  (kent.GetValueOffset64() >= zblock.zvalueblocksize)");
                            }
                            if (kent.GetValueOffset64() < 0)
                            {
                                throw new Exception("DEBUG:  (kent.GetValueOffset64() < 0)");
                            }
#endif
                            int sz313 = kent.GetValueOffsetToBytes(acl._smallbuf, 0);
#if DEBUG
                            if (sz313 != TValueOffset_Size)
                            {
                                throw new Exception("DEBUG:  (sz313 != TValueOffset_Size)");
                            }
#endif
                            zblock.fzkeyblock.Write(acl._smallbuf, 0, TValueOffset_Size);
                        }
#if DEBUG
                        zblock.fzkeyblock.Flush();
                        if (zblock.zkeyblocksize != zblock.fzkeyblock.Length)
                        {
                            throw new Exception("DEBUG:  _NormalSort: (zblock.zkeyblocksize{"
                                + zblock.zkeyblocksize.ToString() + "} != zblock.fzkeyblock.Length{"
                                + zblock.fzkeyblock.Length.ToString() + "}) after sort");
                        }
#endif

                    }
                    finally
                    {
                        zblock._justclose();
                    }
                }


                internal void _writezblockpart(string zblockpartbasename, int partnum, List<TKeyBlockEntry> kentries)
                {
                    System.IO.Stream outstm;
                    //----------------------------COOKING--------------------------------
                    int cooking_cooksremain = acl.CookRetries;
                    bool cooking_inIO = false;
                    //----------------------------COOKING--------------------------------
                    for (; ; )
                    {
                        try
                        {
                            outstm = new System.IO.FileStream(CreateZBlockFileName(zblock.zblockID, "key_part" + partnum.ToString()),
                                System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None);
                        }
                        catch (Exception e)
                        {
                            if (!cooking_inIO)
                            {
                                throw;
                            }
                            //----------------------------COOKING--------------------------------
                            bool firstcook = cooking_cooksremain == acl.CookRetries;
                            if (cooking_cooksremain-- <= 0)
                            {
                                string ns = " (unable to get connection count)";
                                try
                                {
                                    ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                        + " total connections on this machine)";
                                }
                                catch
                                {
                                }
                                throw new System.IO.IOException("cooked too many times (retries="
                                    + acl.CookRetries.ToString()
                                    + "; timeout=" + acl.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName() + ns, e);
                            }
                            System.Threading.Thread.Sleep(acl.CookTimeout);
                            if (firstcook)
                            {
                                try
                                {
                                    XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                        + "; timeout=" + acl.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName()
                                        + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                }
                                catch
                                {
                                }
                            }
                            continue;
                            //----------------------------COOKING--------------------------------
                        }
                        break;
                    }
                    try
                    {
                        // From (sorted) sortbuffer write into new sorted zblock file.
                        foreach (TKeyBlockEntry kent in kentries)
                        {
                            kent.GetKey().CopyTo(acl._smallbuf);
                            outstm.Write(acl._smallbuf, 0, acl.keylen);
                            int sz8812 = kent.GetValueOffsetToBytes(acl._smallbuf, 0);
#if DEBUG
                            if (sz8812 != TValueOffset_Size)
                            {
                                throw new Exception("DEBUG:  (sz8812 != TValueOffset_Size)");
                            }
#endif
                            outstm.Write(acl._smallbuf, 0, TValueOffset_Size);
                        }
                    }
                    finally
                    {
                        //if (null != outstm)
                        {
                            outstm.Close();
                        }
                    }
                }

                internal class _ZBlockPartReader : IEnumerator<byte[]>
                {
                    internal _ZBlockPartReader(int zblockID, int partnum, ZBlock parent)
                    {
                        this.zbpfn = CreateZBlockFileName(zblockID, "key_part" + partnum.ToString());
                        this.parent = parent;
                        this.keylen = parent.parent.keylen;
                        this.TValueOffset_Size = parent.TValueOffset_Size;
                    }

                    public void Delete()
                    {
#if DEBUG
                        if (null != stm)
                        {
                            throw new System.IO.IOException("DEBUG:  _ZBlockPartReader.Delete: (null != stm)  (must Close before Delete)");
                        }
#endif
                        System.IO.File.Delete(zbpfn);
                    }


                    public void Close()
                    {
                        if (null != stm)
                        {
                            stm.Close();
                            stm = null;
                        }
                    }


                    // Consistently returns null at end-of-file.
                    // Returns buffer of same memory, overwritten.
                    public byte[] Current
                    {
                        get
                        {
                            if (!readany)
                            {
                                throw new InvalidOperationException("Must call MoveNext before Current");
                            }
                            if (eof)
                            {
                                return null;
                            }
                            return buf;
                        }
                    }

                    object System.Collections.IEnumerator.Current
                    {
                        get
                        {
                            byte[] result = Current;
                            return result;
                        }
                    }


                    public bool MoveNext()
                    {
                        {
                            if (eof)
                            {
                                return false;
                            }
                            _Read();
                            if (eof)
                            {
                                return false;
                            }
                        }
                        return true;
                    }


                    public bool EndOfStream
                    {
                        get
                        {
                            return eof;
                        }
                    }


                    public void Reset()
                    {
                        Close();
                        readany = false;
                    }


                    void System.IDisposable.Dispose()
                    {
                        Close();
                    }


                    //----------------------------COOKING--------------------------------
                    long cooking_seekpos = 0;
                    //----------------------------COOKING--------------------------------

                    void _Read()
                    {
                        readany = true;
                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = parent.parent.CookRetries;
                        bool cooking_inIO = false;
                        //----------------------------COOKING--------------------------------
                        for (; ; )
                        {
                            try
                            {
                                if (null == stm)
                                {
                                    buf = new byte[keylen + TValueOffset_Size];
                                    cooking_inIO = true;
                                    stm = new System.IO.FileStream(zbpfn, System.IO.FileMode.Open,
                                        System.IO.FileAccess.Read, System.IO.FileShare.Read);
                                    if (0 != cooking_seekpos)
                                    {
                                        stm.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                    }
                                    cooking_inIO = false;
                                }

                                cooking_inIO = true;
                                bool readwhole = ((keylen + TValueOffset_Size) == StreamReadLoop(stm, buf, keylen + TValueOffset_Size));
                                cooking_inIO = false;
                                cooking_seekpos += keylen + TValueOffset_Size;
                                if (!readwhole)
                                {
                                    eof = true;
                                }

                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                stm = null; // Reopen.
                                //----------------------------COOKING--------------------------------
                                bool firstcook = cooking_cooksremain == parent.parent.CookRetries;
                                if (cooking_cooksremain-- <= 0)
                                {
                                    string ns = " (unable to get connection count)";
                                    try
                                    {
                                        ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                            + " total connections on this machine)";
                                    }
                                    catch
                                    {
                                    }
                                    throw new System.IO.IOException("cooked too many times (retries="
                                        + parent.parent.CookRetries.ToString()
                                        + "; timeout=" + parent.parent.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                }
                                System.Threading.Thread.Sleep(parent.parent.CookTimeout);
                                if (firstcook)
                                {
                                    try
                                    {
                                        XLog.errorlog("cooking started (retries=" + parent.parent.CookRetries.ToString()
                                            + "; timeout=" + parent.parent.CookTimeout.ToString()
                                            + ") on " + System.Net.Dns.GetHostName()
                                            + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                    }
                                    catch
                                    {
                                    }
                                }
                                continue;
                                //----------------------------COOKING--------------------------------
                            }
                            break;
                        }
                    }

                    bool readany = false;
                    bool eof = false;
                    string zbpfn;
                    System.IO.Stream stm = null;
                    ZBlock parent;
                    int keylen;
                    int TValueOffset_Size;
                    byte[] buf;


                    internal static int CompareKeys(byte[] x, byte[] y, int keylen)
                    {
                        for (int i = 0; i != keylen; i++)
                        {
                            int diff = (int)x[i] - (int)y[i];
                            if (0 != diff)
                            {
                                return diff;
                            }
                        }
                        return 0;
                    }


                }

                public void _SplitSort(List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {
                    zblock.ensurefzblock(false);

                    try
                    {
                        {

                            int numsplits;
                            int keyspersplit;
                            checked
                            {
                                numsplits = (int)(zblock.zkeyblocksize / ZFILE_SPLITBY_SIZE);
                                numsplits++;
                                {
                                    long bes = zblock.zkeyblocksize / numsplits;
                                    if (0 != (zblock.zkeyblocksize % numsplits))
                                    {
                                        bes++;
                                    }
                                    keyspersplit = (int)(bes / (acl.keylen + TValueOffset_Size));
                                    if (0 != (bes % (acl.keylen + TValueOffset_Size)))
                                    {
                                        keyspersplit++;
                                    }
                                }
                            }

                            if ((zblock.zkeyblocksize / (acl.keylen + TValueOffset_Size))
                                > ((long)keyspersplit * (long)numsplits))
                            {
                                throw new Exception("Split-sort key count miscalculation");
                            }

                            if (zblock.splitzkeyfile)
                            {
                                //----------------------------COOKING--------------------------------
                                int cooking_cooksremain = acl.CookRetries;
                                bool cooking_inIO = false;
                                long cooking_seekpos = 0;
                                //----------------------------COOKING--------------------------------
                                string keyblockfilename = zblock.fzkeyblockfilename;
                                System.IO.Stream stm = null;
                                int zbpCount = 0; // zblock part count.
                                string zblockpartbasename = zblock.fzkeyblockfilename;
                                for (; ; )
                                {
                                    try
                                    {
                                        for (bool again = true; again; )
                                        {
                                            if (null == stm)
                                            {
                                                cooking_inIO = true;
                                                stm = new System.IO.FileStream(keyblockfilename, System.IO.FileMode.Open,
                                                    System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                                                if (0 != cooking_seekpos)
                                                {
                                                    stm.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                                }
                                                cooking_inIO = false;
                                            }
                                            again = _XCopyLimitInto_needcooking(stm, keyspersplit, kentries, ref ebuf);
                                            cooking_seekpos = stm.Position;

                                            {
                                                // Sort the sortbuffer.
                                                kentries.Sort(new System.Comparison<TKeyBlockEntry>(_kcmp));
                                            }

                                            _writezblockpart(zblockpartbasename, zbpCount++, kentries);

                                        }
                                        if (null != stm)
                                        {
                                            stm.Close();
                                        }
                                    }
                                    catch (NeedCookingException e)
                                    {
                                        stm = null; // Reopen.
                                        //----------------------------COOKING--------------------------------
                                        bool firstcook = cooking_cooksremain == acl.CookRetries;
                                        if (cooking_cooksremain-- <= 0)
                                        {
                                            string ns = " (unable to get connection count)";
                                            try
                                            {
                                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                                    + " total connections on this machine)";
                                            }
                                            catch
                                            {
                                            }
                                            throw new System.IO.IOException("cooked too many times (retries="
                                                + acl.CookRetries.ToString()
                                                + "; timeout=" + acl.CookTimeout.ToString()
                                                + ") on " + System.Net.Dns.GetHostName() + ns, e.InnerException); // InnerException!
                                        }
                                        System.Threading.Thread.Sleep(acl.CookTimeout);
                                        if (firstcook)
                                        {
                                            try
                                            {
                                                XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                                    + "; timeout=" + acl.CookTimeout.ToString()
                                                    + ") on " + System.Net.Dns.GetHostName()
                                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                            }
                                            catch
                                            {
                                            }
                                        }
                                        continue;
                                        //----------------------------COOKING--------------------------------
                                    }
                                    catch (Exception e)
                                    {
                                        if (!cooking_inIO)
                                        {
                                            throw;
                                        }
                                        stm = null; // Reopen.
                                        //----------------------------COOKING--------------------------------
                                        bool firstcook = cooking_cooksremain == acl.CookRetries;
                                        if (cooking_cooksremain-- <= 0)
                                        {
                                            string ns = " (unable to get connection count)";
                                            try
                                            {
                                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                                    + " total connections on this machine)";
                                            }
                                            catch
                                            {
                                            }
                                            throw new System.IO.IOException("cooked too many times (retries="
                                                + acl.CookRetries.ToString()
                                                + "; timeout=" + acl.CookTimeout.ToString()
                                                + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                        }
                                        System.Threading.Thread.Sleep(acl.CookTimeout);
                                        if (firstcook)
                                        {
                                            try
                                            {
                                                XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                                    + "; timeout=" + acl.CookTimeout.ToString()
                                                    + ") on " + System.Net.Dns.GetHostName()
                                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                            }
                                            catch
                                            {
                                            }
                                        }
                                        continue;
                                        //----------------------------COOKING--------------------------------
                                    }
                                    break;
                                }

                                // Delete old (unsorted) file; prepare new (sorted) one.
                                // Keep these together so that there's always one on file; so cleanup sees it and continues.
                                zblock._justclose();
                                System.IO.File.Delete(zblock.fzkeyblockfilename);
                                zblock.fzkeyblockfilename = CreateZBlockFileName(zblock.zblockID, "key_sorted");
                                zblock.ensurefzblock(true);
                                try
                                {
                                    _ZBlockPartReader[] zbparts = new _ZBlockPartReader[zbpCount];
                                    for (int partnum = 0; partnum < zbpCount; partnum++)
                                    {
                                        zbparts[partnum] = new _ZBlockPartReader(zblock.zblockID, partnum, zblock);
                                        if (!zbparts[partnum].MoveNext()) // Get it ready for first key.
                                        {
                                            throw new Exception("Expected at least one key in zblock-part file"
                                                + " ('" + zblockpartbasename + "' index " + partnum.ToString() + ")");
                                        }
                                    }
                                    {
                                        List<_ZBlockPartReader> zbpRemain = new List<_ZBlockPartReader>(zbparts);
                                        int keylen = acl.keylen;
                                        for (; ; )
                                        {
                                            int zbpRemainCount = zbpRemain.Count;
                                            if (zbpRemainCount <= 0)
                                            {
                                                break;
                                            }
                                            int lowestindex = 0;
                                            byte[] lowestbuf = zbpRemain[0].Current;
                                            for (int pi = 1; pi < zbpRemainCount; pi++)
                                            {
                                                byte[] curbuf = zbpRemain[pi].Current;
                                                if (_ZBlockPartReader.CompareKeys(curbuf, lowestbuf, keylen) < 0)
                                                {
                                                    lowestindex = pi;
                                                    lowestbuf = curbuf;
                                                }
                                            }
                                            {
                                                zblock.fzkeyblock.Write(lowestbuf, 0, keylen + TValueOffset_Size);
                                                if (!zbpRemain[lowestindex].MoveNext())
                                                {
                                                    zbpRemain.RemoveAt(lowestindex);
                                                }
                                            }
                                        }
                                    }
                                    for (int partnum = 0; partnum < zbpCount; partnum++)
                                    {
                                        try
                                        {
                                            zbparts[partnum].Close();
                                            zbparts[partnum].Delete();
                                        }
                                        catch
                                        {
#if DEBUG
                                            throw;
#endif
                                        }
                                    }
                                }
                                finally
                                {
                                    zblock._justclose();
                                }

                            }
                            else
                            {
                                //CopyInto(kentries, ref ebuf);
                                throw new Exception("DEBUG:  Expected split zkeyblock (zblock)");
                            }

                        }

                    }
                    finally
                    {
                        zblock._justclose();
                    }
                }

                public void Sort(List<TKeyBlockEntry> kentries, ref byte[] ebuf)
                {
                    if (zblock.splitzkeyfile)
                    {
                        _SplitSort(kentries, ref ebuf);
                    }
                    else
                    {
                        _NormalSort(kentries, ref ebuf);
                    }
                }

            }


            internal class NeedCookingException : System.IO.IOException
            {
                internal NeedCookingException(Exception innerException)
                    : base("<Need cooking>", innerException)
                {
                }
            }


            // Only valid right after sort!
            // keepzblocks==false is faster if enum/reduce isn't needed.
            internal void CommitZBall(string cachename, string sBlockID, bool keepzblock)
            {
                _justclose();

                string zkeyballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "key");
                string zvalueballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "value");

                if (keepzblock)
                {
                    System.IO.File.Copy(fzkeyblockfilename, zkeyballname, true); // overwrite=true
                    System.IO.File.Copy(fzvalueblockfilename, zvalueballname, true); // overwrite=true
                }
                else //if (!keepzblock)
                {
                    if(System.IO.File.Exists(zkeyballname))
                    {
                        System.IO.File.Delete(zkeyballname);
                    }
                    System.IO.File.Move(fzkeyblockfilename, zkeyballname);
                    if (System.IO.File.Exists(zvalueballname))
                    {
                        System.IO.File.Delete(zvalueballname);
                    }
                    System.IO.File.Move(fzvalueblockfilename, zvalueballname);

                    fzkeyblockfilename = null;
                    fzvalueblockfilename = null;
                }
            }


            // Only valid right after sort!
            internal bool IntegrateZBall(string cachename, string sBlockID, ref byte[] evaluesbuf)
            {
                string zkeyballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "key");
                string zvalueballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "value");

                if (!System.IO.File.Exists(zkeyballname))
                {
                    return true; // Valid case: no cache output to this file.
                }

                int keylen = parent.keylen;

                if (evaluesbuf.Length < 0x400 * 0x400 * 8)
                {
                    evaluesbuf = new byte[0x400 * 0x400 * 8];
                }
                if (evaluesbuf.Length < (keylen + TValueOffset_Size) * 2)
                {
                    evaluesbuf = new byte[Entry.Round2PowerLong((keylen + TValueOffset_Size) * 2)];
                }
                byte[] valuesbuf = evaluesbuf;

                _justclose();
                string deltafilename = CreateZBlockFileName(zblockID, "key_delta");
                System.IO.File.Move(this.fzkeyblockfilename, deltafilename);
                try
                {
                    zkeyblocksize = 0;
                    numadded = 0;
                    ensurefzblock(false, true); // zballing!

#if DEBUG
                    if (zvalueblocksize != fzvalueblock.Length)
                    {
                        throw new Exception("DEBUG:  IntegrateZBall: (zvalueblocksize != fzvalueblock.Length) before cache integration");
                    }
#endif
                    fzvalueblock.Seek(0, System.IO.SeekOrigin.End);
                    long oldzvalueblocksize = zvalueblocksize; // zvalueblocksize of just delta, the old size.

                    {

                        // Cooking for reading from fzvalueball.
                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = parent.CookRetries;
                        bool cooking_inIO = false;
                        long cooking_seekpos = 0;
                        //----------------------------COOKING--------------------------------
                        for (; ; )
                        {
                            try
                            {
                                cooking_inIO = true;
                                using (System.IO.FileStream fzvalueball = new System.IO.FileStream(
                                    zvalueballname, System.IO.FileMode.Open, System.IO.FileAccess.Read,
                                    System.IO.FileShare.None, FILE_BUFFER_SIZE))
                                {
                                    cooking_inIO = false;
                                    if (0 != cooking_seekpos)
                                    {
                                        cooking_inIO = true;
                                        fzvalueball.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                        cooking_inIO = false;
                                    }
                                    long zvalueballsize = fzvalueball.Length;
                                    if (zvalueblocksize + zvalueballsize > ZVALUEBLOCK32_LIMIT
                                        && 4 == TValueOffset_Size)
                                    {
                                        throw new Exception("Unable to merge cache with delta: resulting values (zValueBlock) too large ("
                                            + (zvalueblocksize + zvalueballsize).ToString() + " bytes)");
                                    }

                                    for (long zvbsofar = 0; zvbsofar < zvalueballsize; )
                                    {
#if DEBUG
                                        for (int i = 0; i < valuesbuf.Length; i++)
                                        {
                                            valuesbuf[i] = 42;
                                        }
#endif
                                        cooking_inIO = true;
                                        int zvbread = fzvalueball.Read(valuesbuf, 0, valuesbuf.Length);
                                        cooking_seekpos += zvbread;
                                        cooking_inIO = false;
                                        fzvalueblock.Write(valuesbuf, 0, zvbread);
                                        zvbsofar += zvbread;
                                    }
                                    zvalueblocksize += zvalueballsize;

                                }
                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                //----------------------------COOKING--------------------------------
                                bool firstcook = cooking_cooksremain == parent.CookRetries;
                                if (cooking_cooksremain-- <= 0)
                                {
                                    string ns = " (unable to get connection count)";
                                    try
                                    {
                                        ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                            + " total connections on this machine)";
                                    }
                                    catch
                                    {
                                    }
                                    throw new System.IO.IOException("cooked too many times (retries="
                                        + parent.CookRetries.ToString()
                                        + "; timeout=" + parent.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                }
                                System.Threading.Thread.Sleep(IOUtils.RealRetryTimeout(parent.CookTimeout));
                                if (firstcook)
                                {
                                    try
                                    {
                                        XLog.errorlog("cooking started (retries=" + parent.CookRetries.ToString()
                                            + "; timeout=" + parent.CookTimeout.ToString()
                                            + ") on " + System.Net.Dns.GetHostName()
                                            + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                    }
                                    catch
                                    {
                                    }
                                }
                                continue;
                                //----------------------------COOKING--------------------------------
                            }
                            break;
                        }
#if DEBUG
                        if (zvalueblocksize != fzvalueblock.Length)
                        {
                            throw new Exception("DEBUG:  IntegrateZBall: (zvalueblocksize != fzvalueblock.Length) after cache integration");
                        }
#endif
                    }

                    if (zvalueblocksize >= ZFILE_SPLIT_SIZE)
                    {
                        splitzblocks = true;
                    }

                    {

                        // Cooking for reading from deltafilename and fzkbCache.
                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = parent.CookRetries;
                        bool cooking_inIO = false;
                        long cooking_seekposDelta = 0;
                        long cooking_seekposCache = 0;
                        //----------------------------COOKING--------------------------------

                        const int deltakeyoffset = 0;
                        int cachekeyoffset = deltakeyoffset + keylen + TValueOffset_Size;

                        for (; ; )
                        {
                            try
                            {
                                cooking_inIO = true;
                                using (System.IO.FileStream fzkbDelta = new System.IO.FileStream(
                                    deltafilename, System.IO.FileMode.Open, System.IO.FileAccess.Read,
                                    System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                {
                                    cooking_inIO = false;
                                    if (0 != cooking_seekposDelta)
                                    {
                                        cooking_inIO = true;
                                        fzkbDelta.Seek(cooking_seekposDelta, System.IO.SeekOrigin.Begin);
                                        cooking_inIO = false;
                                    }
                                    cooking_inIO = true;
                                    using (System.IO.FileStream fzkbCache = new System.IO.FileStream(
                                        zkeyballname, System.IO.FileMode.Open, System.IO.FileAccess.Read,
                                        System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                    {
                                        cooking_inIO = false;
                                        if (0 != cooking_seekposCache)
                                        {
                                            cooking_inIO = true;
                                            fzkbCache.Seek(cooking_seekposCache, System.IO.SeekOrigin.Begin);
                                            cooking_inIO = false;
                                        }

                                        bool needdelta = true, needcache = true;
                                        bool eofdelta = false, eofcache = false;
                                        for (; ; )
                                        {
                                            if (needdelta)
                                            {
                                                // Note: updating cook pos later when using the value.
                                                cooking_inIO = true;
                                                bool gotdelta = (keylen + TValueOffset_Size == fzkbDelta.Read(
                                                    valuesbuf, deltakeyoffset, keylen + TValueOffset_Size));
                                                cooking_inIO = false;
                                                if (!gotdelta)
                                                {
                                                    eofdelta = true;
                                                }
                                                needdelta = false;
                                            }
                                            if (needcache)
                                            {
                                                // Note: updating cook pos later when using the value.
                                                cooking_inIO = true;
                                                bool gotcache = (keylen + TValueOffset_Size == fzkbCache.Read(
                                                    valuesbuf, cachekeyoffset, keylen + TValueOffset_Size));
                                                cooking_inIO = false;
                                                if (!gotcache)
                                                {
                                                    eofcache = true;
                                                }
                                                needcache = false;
                                            }

                                            int which = 0;
                                            if (eofdelta)
                                            {
                                                if (eofcache)
                                                {
                                                    break;
                                                }
                                                which = 1;
                                            }
                                            else if (eofcache)
                                            {
                                                which = -1;
                                            }
                                            else
                                            {
                                                for (int ik = 0; ik < keylen; ik++)
                                                {
                                                    which = valuesbuf[deltakeyoffset + ik]
                                                        - valuesbuf[cachekeyoffset + ik];
                                                    if (0 != which)
                                                    {
                                                        break;
                                                    }
                                                }
                                            }
                                            int whichkeyoffset;
                                            bool needadjustoldvalueoffset;
                                            if (which <= 0)
                                            {
                                                needdelta = true;
                                                whichkeyoffset = deltakeyoffset;
                                                needadjustoldvalueoffset = false;
                                                cooking_seekposDelta += keylen + TValueOffset_Size; // Used value.
                                            }
                                            else
                                            {
                                                needcache = true;
                                                whichkeyoffset = cachekeyoffset;
                                                needadjustoldvalueoffset = true;
                                                cooking_seekposCache += keylen + TValueOffset_Size; // Used value.
                                            }

                                            if (needadjustoldvalueoffset)
                                            {
                                                if (8 == TValueOffset_Size)
                                                {
                                                    long valueoffset = Entry.BytesToLong(valuesbuf, whichkeyoffset + keylen);
                                                    valueoffset += oldzvalueblocksize;
#if DEBUG
                                                    if (valueoffset >= zvalueblocksize)
                                                    {
                                                        System.Diagnostics.Debugger.Launch();
                                                        throw new Exception("DEBUG:  (valueoffset{"
                                                            + valueoffset.ToString() + "} >= zvalueblocksize{"
                                                            + zvalueblocksize.ToString() + "})");
                                                    }
#endif
                                                    Entry.LongToBytes(valueoffset, valuesbuf, whichkeyoffset + keylen);
                                                }
                                                else
                                                {
                                                    uint valueoffset = Entry.BytesToUInt32(valuesbuf, whichkeyoffset + keylen);
                                                    valueoffset += (uint)oldzvalueblocksize;
#if DEBUG
                                                    if ((long)valueoffset >= zvalueblocksize)
                                                    {
                                                        System.Diagnostics.Debugger.Launch();
                                                        throw new Exception("DEBUG:  (valueoffset{"
                                                            + valueoffset.ToString() + "} >= zvalueblocksize{"
                                                            + zvalueblocksize.ToString() + "})");
                                                    }
#endif
                                                    Entry.UInt32ToBytes(valueoffset, valuesbuf, whichkeyoffset + keylen);
                                                }
                                            }
                                            fzkeyblock.Write(valuesbuf, whichkeyoffset, keylen + TValueOffset_Size);
                                            zkeyblocksize += keylen + TValueOffset_Size;
                                            numadded++;

                                        }
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                //----------------------------COOKING--------------------------------
                                bool firstcook = cooking_cooksremain == parent.CookRetries;
                                if (cooking_cooksremain-- <= 0)
                                {
                                    string ns = " (unable to get connection count)";
                                    try
                                    {
                                        ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                            + " total connections on this machine)";
                                    }
                                    catch
                                    {
                                    }
                                    throw new System.IO.IOException("cooked too many times (retries="
                                        + parent.CookRetries.ToString()
                                        + "; timeout=" + parent.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                }
                                System.Threading.Thread.Sleep(IOUtils.RealRetryTimeout(parent.CookTimeout));
                                if (firstcook)
                                {
                                    try
                                    {
                                        XLog.errorlog("cooking started (retries=" + parent.CookRetries.ToString()
                                            + "; timeout=" + parent.CookTimeout.ToString()
                                            + ") on " + System.Net.Dns.GetHostName()
                                            + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                    }
                                    catch
                                    {
                                    }
                                }
                                continue;
                                //----------------------------COOKING--------------------------------
                            }
                            break;
                        }
#if DEBUG
                        if (zkeyblocksize != fzkeyblock.Length)
                        {
                            throw new Exception("DEBUG:  IntegrateZBall: (zkeyblocksize != fzkeyblock.Length) after cache integration");
                        }
#endif

                    }

                    if (zkeyblocksize >= ZFILE_SPLIT_SIZE)
                    {
                        splitzkeyfile = true;
                    }

                }
                finally
                {
                    try
                    {
                        System.IO.File.Delete(deltafilename);
                    }
                    catch
                    {
                    }

                    _justclose();
                }

                return true;

            }


        }


        // Only valid right after sort!
        // keepzblocks==false is faster if enum/reduce isn't needed.
        void CommitZBalls(string cachename, string sBlockID, bool keepzblocks)
        {
            for (int iz = 0; iz < zblocks.Length; iz++)
            {
                zblocks[iz].CommitZBall(cachename, sBlockID, keepzblocks);
            }
        }


        // Only valid right after sort!
        bool IntegrateZBalls(string cachename, string sBlockID)
        {
            //List<ZBlock.KeyBlockEntry> kentries2 = new List<ZBlock.KeyBlockEntry>(this.kentries.Capacity);
            //byte[] ebuf2 = new byte[this.ebytes.Length];
            for (int iz = 0; iz < zblocks.Length; iz++)
            {
                //if (!zblocks[iz].IntegrateZBall(cachename, sBlockID, this.kentries, ref this.ebytes, kentries2, ref ebuf2, ref this.evaluesbuf))
                if (!zblocks[iz].IntegrateZBall(cachename, sBlockID, ref this.evaluesbuf))
                {
                    return false;
                }
            }
            return true;
        }


        internal int TValueOffset_Size = 4;

        List<byte[]> net;
        internal byte[] ebytes; // Current key/value pairs.
        internal byte[] evaluesbuf; // Whole value-zblock in memory for fast seek.
        List<Entry> entries;
        List<ZBlock.KeyBlockEntry32v> kentries32;
        List<ZBlock.KeyBlockEntry64v> kentries64;
        internal int keylen;
        int modbytes;
        ArrayComboListEnumerator benum = null;
        internal ZBlock[] zblocks;
        internal byte[] _smallbuf;
        internal byte[] streamwritebuf;

        internal bool compresszmaps = true;
        internal bool compressdfschunks = true;

        internal int CookTimeout = 1000 * 60;
        internal int CookRetries = 64;

        internal int FoilKeySkipFactor = 5000;

        internal int FoilSampleBlockSize = 104857600;

        bool jobfailover = false;


        Compiler _pluginslavecompiler = null;

        Compiler compiler
        {
            get
            {
                if (null == _pluginslavecompiler)
                {
                    _pluginslavecompiler = new Compiler();
                    CommonCs.AddCommonAssemblyReferences(_pluginslavecompiler.AssemblyReferences);
                }
                return _pluginslavecompiler;
            }
        }


        protected internal void StreamWrite(System.IO.Stream stm, IList<byte> buf, int offset, int length)
        {
            if (length > streamwritebuf.Length)
            {
                streamwritebuf = new byte[length * 2];
            }

            for (int i = 0; i < length; i++)
            {
                streamwritebuf[i] = buf[offset + i];
            }

            stm.Write(streamwritebuf, 0, length);
        }


        public static ArrayComboListPart Create(int count_capacity, int estimated_row_capacity, int keylength)
        {
            return new ArrayComboListPart(count_capacity, estimated_row_capacity, keylength);
        }


        protected ArrayComboListPart(int count_capacity, int estimated_row_capacity, int keylength)
            : this(count_capacity, estimated_row_capacity, keylength, true)
        {
        }

        internal ArrayComboListPart(int count_capacity, int estimated_row_capacity, int keylength, bool startzblocks)
        {
            try
            {
                System.Diagnostics.Process.GetCurrentProcess().PriorityClass = System.Diagnostics.ProcessPriorityClass.Normal;
                System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.AboveNormal;
            }
            catch
            {
            }

            count_capacity = 1024;
            estimated_row_capacity = 32;

            ZBlock.CleanZBlockFiles("value", "key_unsorted", "key_sorted");
            ZMapBlock.CleanZMapBlockFiles();

            this.keylen = keylength;
            this.modbytes = keylength;

            if (8 + keylength > 8 + 8)
            {
                _smallbuf = new byte[8 + keylength];
            }
            else
            {
                _smallbuf = new byte[8 + 8];
            }

            net = new List<byte[]>(8);

            entries = new List<Entry>(2097152); // 2097152 Entry`s is about 16MB

            ebytes = new byte[count_capacity * estimated_row_capacity];
            //evaluesbuf = // Moved down.
            net.Add(ebytes);

            this.streamwritebuf = new byte[1048576];

            // Pre-set with defaults.
            int numzblocks = 139;
            int zblockkeybufsize = FILE_BUFFER_SIZE;
            int zblockvaluebufsize = FILE_BUFFER_SIZE;

            if (null != DistributedObjectsSlave.xslave)
            {
                {
                    System.Xml.XmlNode xzblocks = DistributedObjectsSlave.xslave["zblocks"];
                    if (null != xzblocks)
                    {
                        {
                            System.Xml.XmlAttribute xnzb = xzblocks.Attributes["count"];
                            if (null != xnzb)
                            {
                                numzblocks = int.Parse(xnzb.Value);
                                /*{
                                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                                    if (computer_name == "MAPDCMILLER")
                                    {
                                        numzblocks = 3;
                                    }
                                }*/
                            }
                        }
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addbuffersize"];
                            if (null != xzbs)
                            {
                                int x = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                zblockkeybufsize = x;
                                zblockvaluebufsize = x;
                            }
                        }*/
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addkeybuffersize"];
                            if (null != xzbs)
                            {
                                zblockkeybufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                            }
                        }*/
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addvaluebuffersize"];
                            if (null != xzbs)
                            {
                                zblockvaluebufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                            }
                        }*/
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CompressZMapBlocks"];
                    if (null != xe)
                    {
                        byte b = byte.Parse(xe.InnerText);
                        if (128 != b) compresszmaps = 1 == b;
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CompressDfsChunks"];
                    if (null != xe)
                    {
                        byte b = byte.Parse(xe.InnerText);
                        if (128 != b) compressdfschunks = 1 == b;
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CookTimeout"];
                    if (null != xe)
                    {
                        CookTimeout = int.Parse(xe.InnerText);
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CookRetries"];
                    if (null != xe)
                    {
                        CookRetries = int.Parse(xe.InnerText);
                    }
                }

                /*{
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["FoilKeySkipFactor"];
                    if (null != xe)
                    {
                        FoilKeySkipFactor = int.Parse(xe.InnerText);
                    }
                }*/

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["FoilSampleBlockSize"];
                    if (null != xe)
                    {
                        FoilSampleBlockSize = int.Parse(xe.InnerText);
                    }
                }

            }

            evaluesbuf = new byte[(count_capacity * estimated_row_capacity) / numzblocks * 2];

            if (XLog.logging)
            {
                XLog.log("Creating " + numzblocks.ToString() + " ZBlock`s");
            }

            if (startzblocks)
            {
                zblocks = new ZBlock[numzblocks];
                for (int i = 0; i != numzblocks; i++)
                {
                    zblocks[i] = new ZBlock(this, i, zblockkeybufsize, zblockvaluebufsize);
                }
            }

        }


        public override byte[] GetValue(byte[] key, out int valuelength)
        {
            throw new Exception("Not supported");
        }


        public static long BytesToModLongSeed(IList<byte> bytes, int seed, int offset, int length)
        {
            unchecked
            {
                long result = seed;
                uint shift = (uint)seed;
                int stop = offset + length;
                for (; offset != stop; offset++)
                {
                    uint x = bytes[offset];
                    x = x << (int)(shift & 0xF); // Bound the shift to first 4 bits (shifts up to 15)
                    result += x;
                    shift += 5;
                }
                return Math.Abs(result);
            }
        }

        public static long BytesToModLongSeed(IList<byte> bytes, int seed)
        {
            return BytesToModLongSeed(bytes, seed, 0, bytes.Count);
        }


        public bool EAdd(byte[] key, int keyoffset, byte[] value, int valueoffset, int valuelen)
        {

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
            int ij38j3 = 33 + 3;
#endif

            int zbid;
            if (null != distro)
            {
                //zbid = distro.Distro(key, keyoffset, this.keylen).MinorID;
                zbid = distro.Distro(key, keyoffset, modbytes).MinorID;
            }
            else
            {
                zbid = (int)(BytesToModLongSeed(key, 0, keyoffset, modbytes) % zblocks.Length);
            }

            return zblocks[zbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }

        public bool EAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset, int valuelen)
        {

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
            int ij38j3 = 33 + 3;
#endif

            int zbid;
            if (null != distro)
            {
                //zbid = distro.Distro(key, keyoffset, this.keylen).MinorID;
                zbid = distro.Distro(key, keyoffset, modbytes).MinorID;
            }
            else
            {
                zbid = (int)(BytesToModLongSeed(key, 0, keyoffset, modbytes) % zblocks.Length);
            }

            return zblocks[zbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }


        public bool TimedAdd(byte[] key, int keyoffset, byte[] value, int valueoffset, int valuelength)
        {
            bool result = false;

#if ENABLE_TIMING
            long start = 0;
            if (XLog.logging)
            {
                QueryPerformanceCounter(out start);
            }
#endif

            result = EAdd(key, keyoffset, value, valueoffset, valuelength);

#if ENABLE_TIMING
            if (XLog.logging)
            {
                long stop;
                QueryPerformanceCounter(out stop);
                long freq;
                if (QueryPerformanceFrequency(out freq))
                {
                    long secs = (stop - start) / freq;
                    if (secs > 4)
                    {
                        XLog.log("ArrayComboListPart add seconds: " + secs.ToString());
                    }
                }
            }
#endif

            return result;
        }

        //public bool TimedAdd(IList<byte> key, IList<byte> value, int valuelength)
        public bool TimedAdd(byte[] key, byte[] value, int valuelength)
        {
            return TimedAdd(key, 0, value, 0, valuelength);
        }


        // Adds key and value from buf starting at offset.
        // Returns number of bytes this key/value used (i.e. number of bytes to skip to next).
        protected int TimedAddKVBuf(byte[] buf, int offset)
        {
            int valuelen = Entry.BytesToInt(buf, offset + this.keylen);
            TimedAdd(buf, offset, buf, offset + this.keylen + 4, valuelen);
            return this.keylen + 4 + valuelen;
        }


        // Warning: key needs to be immutable!
        public override void CopyAndSetValue(byte[] key, byte[] value, int valuelength)
        {
            TimedAdd(key, value, valuelength);
        }


        public void CloseZBlocks()
        {
            if (null != zblocks)
            {
                foreach (ZBlock zb in zblocks)
                {
                    zb.Close();
                }
                zblocks = null;
            }
        }

        public void CloseZMapBlocks()
        {
            if (null != zmblocks)
            {
                foreach (ZMapBlock zmb in zmblocks)
                {
                    zmb.Close();
                }
            }
        }


        private static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

        public string CreateDllFileName(string part)
        {
            return "ylib_" + part + "_" + _spid + ".ylib";
        }

        public string CreateXlibFileName(string part)
        {
            return "xlib_" + part + "_" + _spid + ".xlib";
        }


        internal class ACLEntriesOutput : EntriesOutput
        {
            internal ACLEntriesOutput(byte[] buf, NetworkStream nstm)
             : base(buf)
            {
                this.nstm = nstm;
            }


            public override bool SendBatchedEntriesOutput(byte[] buf, int length)
            {
                if (sentany)
                {
                    return false;
                }

                nstm.WriteByte((byte)'+');
                XContent.SendXContent(nstm, buf, length);

                sentany = true;
                return true;
            }


            // Returns false if at end, true if more.
            public override bool EndBatch()
            {
                if (!sentany)
                {
                    nstm.WriteByte((byte)'-');
                    return false;
                }
                sentany = false; // For next time around!
                return true;
            }


            bool sentany = false;
            NetworkStream nstm;
        }


        internal EntriesOutput CreatePluginOutput()
        {
            return new ArrayComboListPart.ACLEntriesOutput(new byte[this.buf.Length], this.nstm);
        }


        internal class SampleMapOutput : MySpace.DataMining.DistributedObjects.MapOutput
        {
            internal List<IList<byte>> samples;
            internal System.IO.Stream cacheout = null;
            internal ArrayComboListPart parent;


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != parent.keylen)
                {
                    throw new Exception("Key length mismatch for SampleMapOutput.Add():  got " + keylength.ToString() + " bytes, expected " + parent.keylen.ToString());
                }
#if DEBUG
                if (parent.modbytes > keylength)
                {
                    throw new Exception("SampleMapOutput.Add():  parent.modbytes{" + parent.modbytes.ToString() + "} > keylength{" + keylength.ToString() + "}");
                }

                if (keyoffset + parent.modbytes > keybuf.Count)
                {
                    throw new Exception("SampleMapOutput.Add():  keyoffset{" + keyoffset.ToString() + "} + parent.modbytes{" + parent.modbytes.ToString() + "} > keybuf.Count{" + keybuf.Count.ToString() + "}");
                }
#endif

                List<byte> newkey = new List<byte>(parent.modbytes);
                for (int i = 0; i < parent.modbytes; i++)
                {
                    newkey.Add(keybuf[keyoffset + i]);
                }
                //lock (samples)
                {
                    AddSampleToList(newkey, samples);
                    if (null != cacheout)
                    {
                        for (int ik = 0; ik < parent.modbytes; ik++)
                        {
                            cacheout.WriteByte(keybuf[keyoffset + ik]);
                        }
                    }
                }
            }
        }


        internal class SampleMapInput : MySpace.DataMining.DistributedObjects.MapInput
        {
            //public string Name; // Not necessarily a file name; for tracking purposes.
            //public System.IO.Stream Stream; // Relative to this input; may be a logical EndOfStream!

            public override void Reopen(long position)
            {
                throw new Exception("method not supported");
            }

            // Position from the original source file as loaded into DFS.
            public override long Position
            {
                get
                {
                    // TODO...
                    throw new Exception("SampleMapInput.Position not supported");
                }
            }


            // Length of this input, in bytes. e.g. number of bytes that can be read from Stream.
            public override long Length
            {
                get
                {
                    throw new Exception("SampleMapInput.Length not supported");
                }
            }


            public override long Fixup(long position, long length, byte[] buf, int bufoffset)
            {
                throw new Exception("SampleMapInput.Fixup() not supported");
            }

        }


        internal class FoilMapOutput : MySpace.DataMining.DistributedObjects.MapOutput, IComparer<List<byte>>
        {
            internal System.IO.Stream sampleout = null; // Actual kept samples.
            internal ArrayComboListPart parent;

            internal List<List<byte>> curmapoutput; // Current chunk's map output.
            internal int curmapoutputlen = 0; // Don't clear curmapoutput, but set this to 0 and reuse curmapoutput.
            internal int curmapoutputsize = 0; // curmapoutputlen*keymajor


            int IComparer<List<byte>>.Compare(List<byte> x, List<byte> y)
            {
#if DEBUG
                if (x.Count != y.Count)
                {
                    throw new Exception("FoilMapOutput sort compare: key length mismatch");
                }
#endif
                for (int i = 0; i < x.Count; i++)
                {
                    int diff = x[i] - y[i];
                    if (0 != diff)
                    {
                        return diff;
                    }
                }
                return 0;
            }


            int startpoint = 0;

            private void _ProcessMapOutputs()
            {
                // Process curmapoutput 0..curmapoutputlen

#if DEBUG
                if (curmapoutputsize != curmapoutputlen * parent.modbytes)
                {
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
#endif
                    throw new Exception("FoilMapOutput._ProcessMapOutputs: (curmapoutputsize != curmapoutputlen * parent.modbytes)");
                }
#endif

                curmapoutput.Sort(0, curmapoutputlen, this);

#if asdff
                //lock (samples)
                {
                    if (null != sampleout)
                    {
                        for (int ik = 0; ik < parent.modbytes; ik++)
                        {
                            sampleout.WriteByte(keybuf[keyoffset + ik]);
                        }
                    }
                }
#endif
                {
                    int curpoint = startpoint;
                    for (; curpoint < curmapoutputlen; curpoint += parent.FoilKeySkipFactor)
                    {
                        List<byte> key = curmapoutput[curpoint];
                        for (int ik = 0; ik < key.Count; ik++)
                        {
                            sampleout.WriteByte(key[ik]);
                        }
                    }
                    startpoint = curpoint - curmapoutputlen;
                }

#if DEBUG
                for (int i = 0; i < curmapoutputlen; i++)
                {
                    // Clear for debug so we can catch key length problem if used.
                    curmapoutput[i].Clear();
                }
#endif
                curmapoutputlen = 0;
                curmapoutputsize = 0;
            }


            internal void _ChunkDone()
            {
                //_ProcessMapOutputs(); // Not done here anymore.
            }


            internal void _AllChunksDone()
            {
                // Do the remainder.
                _ProcessMapOutputs();
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != parent.keylen)
                {
                    throw new Exception("Key length mismatch for FoilMapOutput.Add():  got " + keylength.ToString() + " bytes, expected " + parent.keylen.ToString());
                }
#if DEBUG
                if (parent.modbytes > keylength)
                {
                    throw new Exception("FoilMapOutput.Add():  parent.modbytes{" + parent.modbytes.ToString() + "} > keylength{" + keylength.ToString() + "}");
                }

                if (keyoffset + parent.modbytes > keybuf.Count)
                {
                    throw new Exception("FoilMapOutput.Add():  keyoffset{" + keyoffset.ToString() + "} + parent.modbytes{" + parent.modbytes.ToString() + "} > keybuf.Count{" + keybuf.Count.ToString() + "}");
                }
#endif

                List<byte> newkey;
                if (curmapoutputlen >= curmapoutput.Count)
                {
                    newkey = new List<byte>(parent.modbytes);
                    curmapoutput.Add(newkey);
                    curmapoutputlen++;
                }
                else
                {
                    newkey = curmapoutput[curmapoutputlen++];
                    newkey.Clear();
                }
                for (int i = 0; i < parent.modbytes; i++)
                {
                    newkey.Add(keybuf[keyoffset + i]);
                }
                curmapoutputsize += parent.modbytes;

                if (curmapoutputsize >= parent.FoilSampleBlockSize)
                {
                    _ProcessMapOutputs();
                }

            }

        }


        internal class FoilMapInput : ACLMapInput
        {
            internal FoilMapInput(ArrayComboListPart acl)
                : base(acl)
            {
            }
        }


        static void AddSampleToList(IList<byte> sample, List<IList<byte>> list)
        {
            list.Add(sample);
        }


        static Random samprand = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));

        void _MapSampleFromFn(string fn, IMap mpsamp, MapInput sampin, MapOutput sampout)
        {
            try
            {
                using (System.IO.Stream fs = new MySpace.DataMining.AELight.DfsFileNodeStream(fn, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                {
                    sampin.Name = fn;
                    sampin.Stream = fs;
                    mpsamp.OnMap(sampin, sampout);
                }
            }
            catch (System.IO.IOException e)
            {
                throw new System.IO.IOException("I/O Error while map sampling; integrity of sampling is critical for synchronized distribution", e);
            }
            catch (Exception e)
            {
                throw new Exception("Error while map sampling; integrity of sampling is critical for synchronized distribution", e);
            }
        }

        void _MapDfsFileNodeFromFn(string fn, IMap mpsamp, ACLMapInput sampin, MapOutput sampout, bool isLastFileChunk)
        {
            try
            {
                sampin.Name = fn;
                sampin._compression = compressdfschunks;
                sampin._open();
                StaticGlobals.DSpace_InputBytesRemain = (isLastFileChunk ? sampin.Stream.Length - sampin.Stream.Position : Int64.MaxValue);
                mpsamp.OnMap(sampin, sampout);
                sampin._close();
            }
            catch (System.IO.IOException e)
            {
                throw new System.IO.IOException("I/O Error while foil sampling; integrity of sampling is critical for synchronized distribution", e);
            }
            catch (Exception e)
            {
                throw new Exception("Error while foil sampling; integrity of sampling is critical for synchronized distribution", e);
            }
        }


        void LoadFoilSamples(IMap mpsamp, string samplesoutputfn, string[] dfsfiles,
            string[] dfsfilenames, int[] nodesoffsets, int[] inputrecordlengths
            )
        {
            if (System.IO.File.Exists(samplesoutputfn))
            {
                return;
            }
            using (System.IO.FileStream samplesoutputfs = new System.IO.FileStream(samplesoutputfn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
            {
                FoilMapInput sampin = new FoilMapInput(this);
                FoilMapOutput sampout = new FoilMapOutput();
                sampout.parent = this;
                sampout.sampleout = samplesoutputfs;
                sampout.curmapoutput = new List<List<byte>>(0x400 * 0x400);               

                if (dfsfiles.Length > 0)
                {
                    StaticGlobals.DSpace_Last = false;
                    int fi = 0;
                    int curoffset = nodesoffsets[fi];  
                    for (int i = 0; i < dfsfiles.Length; i++)
                    {
                        if (i == curoffset)
                        {
                            StaticGlobals.DSpace_InputFileName = dfsfilenames[fi];
                            StaticGlobals.DSpace_InputRecordLength = inputrecordlengths[fi];
                            fi++;
                            if (fi < nodesoffsets.Length)
                            {
                                curoffset = nodesoffsets[fi];
                            }
                        }
                        _MapDfsFileNodeFromFn(dfsfiles[i], mpsamp, sampin, sampout, i == dfsfiles.Length - 1);
                        sampout._ChunkDone();
                    }
                    sampout._AllChunksDone();                    
                }
            }
        }


        void LoadSamples(IMap mpsamp)
        {
            List<IList<byte>> samples = new List<IList<byte>>(0x400 * 0x40);

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            if ("FoilDistro" == sortclass)
            {
                for (int i = 0; i < samplefns.Length; i++)
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = CookRetries;
                        bool cooking_inIO = false;
                        long cooking_seekpos = 0;
                        for (; ; )
                        {
                            try
                            {
                                //----------------------------COOKING--------------------------------
                                cooking_inIO = true;
                                using (System.IO.FileStream fs = new System.IO.FileStream(samplefns[i], System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                {
                                    if (0 != cooking_seekpos)
                                    {
                                        fs.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                    }
                                    cooking_inIO = false;
                                    for (; ; )
                                    {
                                        byte[] modkeybuf = new byte[modbytes];
                                        cooking_inIO = true;
                                        if (StreamReadLoop(fs, modkeybuf, modbytes) != modbytes)
                                        {
                                            break;
                                        }
                                        cooking_inIO = false;
                                        cooking_seekpos += modbytes;
                                        samples.Add(modkeybuf);
                                    }
                                }
                                cooking_seekpos = 0;
                                //----------------------------COOKING--------------------------------
                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                {
                                    bool firstcook = cooking_cooksremain == CookRetries;
                                    if (cooking_cooksremain-- <= 0)
                                    {
                                        string ns = " (unable to get connection count)";
                                        try
                                        {
                                            ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                                + " total connections on this machine)";
                                        }
                                        catch
                                        {
                                        }
                                        throw new System.IO.IOException("cooked too many times (retries="
                                            + CookRetries.ToString()
                                            + "; timeout=" + CookTimeout.ToString()
                                            + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                    }
                                    System.Threading.Thread.Sleep(CookTimeout);
                                    if (firstcook)
                                    {
                                        try
                                        {
                                            XLog.errorlog("cooking started (retries=" + CookRetries.ToString()
                                                + "; timeout=" + CookTimeout.ToString()
                                                + ") on " + System.Net.Dns.GetHostName()
                                                + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                        }
                                        catch
                                        {
                                        }
                                    }
                                    continue; // !
                                }
                            }
                            break;
                        }
                        //----------------------------COOKING--------------------------------
                    }
                    catch (System.IO.IOException e)
                    {
                        throw new System.IO.IOException("I/O Error while loading distribution index; integrity of sampling is critical for synchronized distribution", e);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Error while loading distribution index; integrity of sampling is critical for synchronized distribution", e);
                    }
                }
            }
            else
            {
                SampleMapInput sampin = new SampleMapInput();
                SampleMapOutput sampout = new SampleMapOutput();
                sampout.parent = this;
                sampout.samples = samples;

                if (samplefns.Length >= 1 && '>' == samplefns[0][0])
                {
                    // Read or write to sample cache (depending if it exists).
                    // It's always local.
                    string sampcachefile = IOUtils.SafeTextPath(samplefns[0].Substring(1));
                    // Use a mutex on the file name, this way only one slave generates the cache, and the rest just load it!
                    // Otherwise the snowball file name would have to include the blockid... all for redundant data.
                    using (System.Threading.Mutex sampmutex = new System.Threading.Mutex(false, "DO5_Sample_" + sampcachefile))
                    {
                        try
                        {
                            sampmutex.WaitOne();
                        }
                        catch (System.Threading.AbandonedMutexException)
                        {
                        }
                        if (System.IO.File.Exists(sampcachefile))
                        {
                            sampmutex.ReleaseMutex(); // Don't need to stay in the mutex at this point.

                            // Load cache...
                            // Note: samplefns might contain more files, even ones that weren't in the original cache; they are ignored.

                            if (buf.Length < 0x400 * 0x40)
                            {
                                buf = new byte[0x400 * 0x40];
                            }

                            try
                            {
                                using (System.IO.FileStream fs = new System.IO.FileStream(sampcachefile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                {
                                    byte[] recbuf = new byte[modbytes];
                                    for (; ; )
                                    {
                                        int rd = fs.Read(recbuf, 0, modbytes);
                                        if (rd <= 0)
                                        {
                                            break;
                                        }
                                        if (rd != modbytes)
                                        {
                                            throw new Exception("Sample cache file not of expected size. Consider recreate input or delete cache file.");
                                        }
                                        List<byte> rec = new List<byte>(modbytes);
                                        for (int ir = 0; ir < modbytes; ir++)
                                        {
                                            rec.Add(recbuf[ir]);
                                        }
                                        AddSampleToList(rec, samples);
                                    }
                                }
                            }
                            catch (System.IO.IOException e)
                            {
                                throw new System.IO.IOException("I/O Error while loading cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Error while loading cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                        }
                        else
                        {
                            try
                            {
                                // Create & write cache...
                                using (System.IO.FileStream cacheout = new System.IO.FileStream(sampcachefile, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None, FILE_BUFFER_SIZE))
                                {
                                    sampout.cacheout = cacheout;

                                    // Need to start at index 1, since 0 is the target.
                                    int pivot = 1;
                                    if (samplefns.Length > 1)
                                    {
                                        pivot += samprand.Next() % (samplefns.Length - 1);
                                    }
                                    string samplefn = "";
                                    try
                                    {
                                        for (int i = pivot; i < samplefns.Length; i++)
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                        for (int i = 1; i < pivot; i++)
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                    catch (System.IO.IOException e)
                                    {
                                        throw new System.IO.IOException("I/O Problem while loading samples from file '" + samplefn + "' (also writing cache); integrity of sampling is critical for synchronized distribution", e);
                                    }
                                    catch (Exception e)
                                    {
                                        throw new Exception("Problem while loading samples from file '" + samplefn + "' (also writing cache); integrity of sampling is critical for synchronized distribution", e);
                                    }
                                }
                            }
                            catch (System.IO.IOException e)
                            {
                                throw new System.IO.IOException("I/O Error while map sampling and creating cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Error while map sampling and creating cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            sampmutex.ReleaseMutex();
                        }
                    }
                }
                else
                {
                    string samplefn = "";
                    try
                    {
                        if (samplefns.Length > 0)
                        {
                            if (btreeCapSize == 0)
                            {
                                int pivot = samprand.Next() % samplefns.Length;
                                for (int i = pivot; i < samplefns.Length; i++)
                                {
                                    samplefn = samplefns[i];
                                    _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                }
                                for (int i = 0; i < pivot; i++)
                                {
                                    samplefn = samplefns[i];
                                    _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                }
                            }
                            else
                            {
                                ulong aSampleSize = 0;

                                _MapSampleFromFn(samplefns[0], mpsamp, sampin, sampout);

                                if (samples.Count > 0)
                                {
                                    aSampleSize = (ulong)(samples.Count * samples[0].Count);
                                }

                                int filesToSample = samplefns.Length;

                                if ((aSampleSize * (ulong)samplefns.Length) >= btreeCapSize)
                                {
                                    filesToSample = (int)(btreeCapSize / aSampleSize);
                                }

                                if (filesToSample > 1)
                                {
                                    int factor = -1;
                                    bool isSkip = false;
                                    int filesCount = 0;

                                    if (filesToSample < samplefns.Length)
                                    {
                                        factor = samplefns.Length / filesToSample;
                                        filesCount = filesToSample;

                                        if (factor == 1)
                                        {
                                            filesCount = samplefns.Length - filesToSample;
                                            factor = samplefns.Length / filesCount;
                                            isSkip = true;
                                        }
                                    }

                                    int pivot = samprand.Next() % samplefns.Length;
                                    for (int i = pivot; i < samplefns.Length; i++)
                                    {
                                        if (IncludeSample(i, factor, isSkip, filesCount))
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                    for (int i = 0; i < pivot; i++)
                                    {
                                        if (IncludeSample(i, factor, isSkip, filesCount))
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (System.IO.IOException e)
                    {
                        throw new System.IO.IOException("I/O Problem while loading samples from file '" + samplefn + "'; integrity of sampling is critical for synchronized distribution", e);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Problem while loading samples from file '" + samplefn + "'; integrity of sampling is critical for synchronized distribution", e);
                    }
                }
            }

            switch (sortclass)
            {
                case "MdoDistro":
                    distro = new MdoDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "DsDistro":
                    distro = new DsDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "FoilDistro":
                    distro = new FoilDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "Distro2":
                    if (2 != modbytes)
                    {
                        throw new Exception("Distro2 (hashsorted/rhashsorted) expects modbytes (KeyMajor) 2");
                    }
                    distro = new Distro2(sampleblockcount, zblocks.Length);
                    break;
                case "Distro1":
                    if (1 != modbytes)
                    {
                        throw new Exception("Distro1 (hashsorted/rhashsorted) expects modbytes (KeyMajor) 1");
                    }
                    distro = new Distro1(sampleblockcount, zblocks.Length);
                    break;
                default:
                    throw new Exception("No such sort class '" + sortclass + "' (OutputMethod)");
            }

            XLog.log("ArrayComboList: " + samples.Count.ToString() + " samples produced from sample-map on this sub process; using sort class " + sortclass + "; memory usage " + distro.GetMemoryUsage());
        }

        private bool IncludeSample(int index, int factor, bool isSkipFactor, int filesCount)
        {
            if (index == 0)
            {
                return false;
            }

            if (factor == -1)
            {
                return true;
            }

            if (isSkipFactor)
            {
                return !(((index - 1) % factor == 0) && (index < factor * filesCount));
            }
            else
            {
                return ((index % factor == 0) && (index < factor * filesCount));
            }
        }

        internal class ACLLoadOutput: LoadOutput
        {
            internal ACLLoadOutput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != acl.keylen)
                {
                    throw new Exception("Key length mismatch for LoadOutput; got " + keylength.ToString() + " bytes, expected " + acl.keylen.ToString());
                }
                acl.EAdd(keybuf, keyoffset, valuebuf, valueoffset, valuelength);
            }


            ArrayComboListPart acl;
        }


        internal void ZMEAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset, int valuelen)
        {
#if _ZMEbwa
            if (keyoffset + modbytes > key.Count)
            {
                throw new Exception("keyoffset and modbytes are out of bounds for ZMEAdd (MapOutput key buffer) [" + ((null != distro) ? "distro" : "mod") + "]");
            }
#endif

            int zmbid;
            if (null != distro)
            {
                zmbid = distro.Distro(key, keyoffset, modbytes).MajorID;
            }
            else
            {
                zmbid = (int)(BytesToModLongSeed(key, 8934745, keyoffset, modbytes) % zmblocks.Length);
            }

            if (zmbid < 0 || zmbid >= zmblocks.Length)
            {
                StringBuilder keysb = new StringBuilder(keylen);
                for (int ki = 0; ki < keylen; ki++)
                {
                    if (keysb.Length > 0)
                    {
                        keysb.Append(',');
                    }
                    keysb.Append((byte)key[keyoffset + ki]);
                }
                throw new Exception("key mapped to invalid zMapBlock for ZMEAdd (MapOutput zMapBlockID=" + zmbid.ToString() + ") [" + ((null != distro) ? "distro" : "mod") + "] {key=" + keysb.ToString() + "}");
            }

            zmblocks[zmbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }


        internal class ACLMapOutput : MapOutput
        {
            internal ACLMapOutput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != acl.keylen)
                {
                    throw new Exception("Key length mismatch for MapOutput; got " + keylength.ToString() + " bytes, expected " + acl.keylen.ToString());
                }
                if (keyoffset + keylength > keybuf.Count)
                {
                    throw new Exception("Key length and key offset are out of bounds for MapOutput");
                }


                if(!StaticGlobals.DSpace_OutputDirection_ascending)
                {
                    if (flipbuf == null)
                    {
                        flipbuf = new byte[keylength];
                    }
                    for (int i = 0; i < keylength; i++)
                    {
                        flipbuf[i] = (byte)(~keybuf[i + keyoffset]);
                    }
                    acl.ZMEAdd(flipbuf, 0, valuebuf, valueoffset, valuelength);
                }
                else
                {
                    acl.ZMEAdd(keybuf, keyoffset, valuebuf, valueoffset, valuelength);
                }                
            }

            ArrayComboListPart acl;
            private static byte[] flipbuf = null;
        }


        internal class ACLMapInput : MapInput
        {
            internal ACLMapInput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }

            public override long Position
            {
                get
                {
                    throw new Exception("MapInput.Position not implemented");
                }
            }

            public override long Length
            {
                get
                {
                    throw new Exception("MapInput.Length not implemented");
                }
            }

            public override long Fixup(long position, long length, byte[] buf, int bufoffset)
            {
                throw new Exception("MapInput.Fixup() not implemented");
            }


            internal bool _compression = false;
            internal static byte[] openbuf = new byte[32]; // Shared!

            // Depends on Name being the full file path.
            internal void _open()
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = acl.CookRetries;
                bool cooking_stream_is_open = false;
#if DEBUGcook
                int tester = 2;
#endif
                for (; ; )
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
#if DEBUGcook
                        if (tester-- == -2)
                        {
                            throw new NotFiniteNumberException();
                        }
#endif
                        //Stream = new System.IO.FileStream(Name, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, DISTBUF_SIZE);
                        Stream = new MySpace.DataMining.AELight.DfsFileNodeStream(Name, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                        cooking_stream_is_open = true;
                        if (_compression)
                        {
                            Stream = new System.IO.Compression.GZipStream(Stream, System.IO.Compression.CompressionMode.Decompress);
                        }
#if DEBUGcook
                        if (tester-- == 0)
                        {
                            throw new NotFiniteNumberException();
                        }
#endif
                        {
                            // Skip the dfs-chunk file-header...
                            int headerlength = 0;
                            {
                                if (4 != Stream.Read(openbuf, 0, 4))
                                {
                                    return;
                                }
                                {
                                    headerlength = Entry.BytesToInt(openbuf);
                                    if (headerlength > 4)
                                    {
                                        int remain = headerlength - 4;
                                        if (remain > openbuf.Length)
                                        {
                                            openbuf = new byte[Entry.Round2Power(remain)];
                                        }
                                        StreamReadExact(Stream, openbuf, remain);
                                    }
                                }
                            }
                            // Future: exclude dfs-chunk file-header from MapInput.Length
                        }
                //----------------------------COOKING--------------------------------
                    }
                    catch(Exception e)
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        bool firstcook = cooking_cooksremain == acl.CookRetries;
                        if (cooking_cooksremain-- <= 0)
                        {
                            string ns = " (unable to get connection count)";
                            try
                            {
                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                    + " total connections on this machine)";
                            }
                            catch
                            {
                            }
                            throw new System.IO.IOException("cooked too many times (retries="
                                + acl.CookRetries.ToString()
                                + "; timeout=" + acl.CookTimeout.ToString()
                                + ") on " + System.Net.Dns.GetHostName() + ns, e);
                        }
                        if (cooking_stream_is_open)
                        {
                            Stream.Close();
                            Stream = null;
                            cooking_stream_is_open = false;
                        }
                        System.Threading.Thread.Sleep(acl.CookTimeout);
                        if (firstcook)
                        {
                            try
                            {
                                XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                    + "; timeout=" + acl.CookTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName()
                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                            }
                            catch
                            {
                            }
                        }
                        continue; // !
                    }
                    break;
                }
                //----------------------------COOKING--------------------------------
            }

            internal void _close()
            {
                Stream.Close();
                Stream = null;
            }

            public override void Reopen(long position)
            {
                _close();
                _open(); // Skips header!
                Stream.Seek(position, System.IO.SeekOrigin.Current); // SeekOrigin.Current because the position passed in is relative to after-header.
            }


            ArrayComboListPart acl;
        }


        RangeDistro distro;
        int sampleblockcount = 0; // Only valid if sampling.
        string[] samplefns = null;
        ulong btreeCapSize = 0;
        string sortclass = null;

        ZMapBlock[] zmblocks = null;

        public class ZMapBlock
        {
            ArrayComboListPart parent;

            int zmblockID; // ZMapBlock ID (0-based n)
            internal string fzmblockfilename = null;
            long zmblocksize = 0;
            System.IO.Stream fzmblock;
            bool xchg = false;


            public ZMapBlock(ArrayComboListPart parent, int zmblockID, string zmapblockbasename)
            {
                this.parent = parent;
                this.zmblockID = zmblockID;

                //fzmblockfilename = CreateZMapBlockFileName(zmblockID);
                fzmblockfilename = zmapblockbasename.Replace("%n", zmblockID.ToString());

                {
                    fzmblock = new System.IO.FileStream(fzmblockfilename, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                    if (parent.compresszmaps)
                    {
                        fzmblock = new System.IO.Compression.GZipStream(fzmblock, System.IO.Compression.CompressionMode.Compress);
                    }
                }
            }


            public void PrepareForExchange()
            {
                if (xchg)
                {
                    throw new Exception("Cannot exchange ZMapBlock more than once");
                }
                xchg = true;

                //fzmblock.Seek(0, System.IO.SeekOrigin.Begin);
                fzmblock.Close();
            }


            public bool Add(IList<byte> keybuf, int keyoffset, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (xchg)
                {
                    throw new Exception("Cannot add to ZMapBlock while/after exchange");
                }

                parent.StreamWrite(fzmblock, keybuf, keyoffset, parent.keylen);
                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzmblock.Write(parent._smallbuf, 0, 4);
                parent.StreamWrite(fzmblock, valuebuf, valueoffset, valuelength);

                zmblocksize += parent.keylen + 4 + valuelength;
                /* // Can't do .Length on gzip stream...
#if DEBUG
                if (zmblocksize != fzmblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zmblocksize mismatch");
                }
#endif
                 * */

                return true;
            }


            /*
            internal static string CreateZMapBlockFileName(int zmblockID)
            {
                return "zmap_" + ZBlock._spid + "_" + zmblockID.ToString() + ".zm";
            }
             */


            public static void CleanZMapBlockFiles()
            {
                /*
                // Clean any potential old zmblock files...
                bool found = true;
                for (int i = 0; found; i++)
                {
                    found = false;
                    string fn = CreateZMapBlockFileName(i);
                    if (System.IO.File.Exists(fn))
                    {
                        System.IO.File.Delete(fn);
                        found = true;
                    }
                }
                 * */
            }


            public void Close()
            {
                _justclose();

                if (null != fzmblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzmblockfilename);
                        fzmblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            private void _justclose()
            {
                if (null != fzmblock)
                {
                    fzmblock.Close();
                    fzmblock = null;
                }
            }
        }


        static bool IsValidsBlockID(string sBlockID)
        {
            bool gotany = false;
            for (int i = 0; i < sBlockID.Length; i++)
            {
                if (!char.IsLetterOrDigit(sBlockID[i]))
                {
                    return false;
                }
                gotany = true;
            }
            return gotany;
        }

        static string GetSafeZballName(string sBlockID)
        {
            string reason = "";
            if (MySpace.DataMining.AELight.dfs.IsBadFilename(sBlockID, out reason))
            {
                throw new Exception("Invalid delta cache name: " + reason);
            }
            return sBlockID;
        }


        protected override void ReloadConfig()
        {
            base.ReloadConfig();

            //_loadconfig();
        }


        protected override void ProcessCommand(NetworkStream nstm, char tag)
        {
            //string s;
            int len;

            switch (tag)
            {
                case 'F': // 'F' for BeforeLoad (first)
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("load");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("load");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            System.IO.FileStream stm = System.IO.File.Create(dllfn);
                            stm.Write(buf, 0, len);
                            stm.Close();
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IBeforeReduce plugin named " + xclassname + " for before-load: " + dllfn);
                        }

                        IBeforeLoad bl = LoadBeforeLoadPlugin(dllfn, classname);
                        LoadOutput loadoutput = new ACLLoadOutput(this);
                        bl.OnBeforeLoad(loadoutput);
                    }
                    break;

                /*
                case '': // Compression...
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        // 0 means no compression, 1 means gzip compression, and 128 means skip.
                        if (len > 0 && 128 != buf[0]) compresszmaps = 1 == buf[0]; // [0] is for zmaps.
                        if (len > 1 && 128 != buf[1]) compressdfschunks = 1 == buf[1]; // [1] is for DFS chunks.
                    }
                    break;
                 * */

                case 'f': // 'f' for foil sampling
                    {

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("foil");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("foil");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string samplesoutputfn = XContent.ReceiveXString(nstm, buf);

                        string[] dfsfiles = null;// XContent.ReceiveXString(nstm, buf).Split(';');
                        string[] dfsfilenames = null;
                        int[] nodesoffsets = null;
                        int[] inputrecordlengths = null;
                        {
                            string xfiles = XContent.ReceiveXString(nstm, buf);
                            if (xfiles.Length > 0)
                            {
                                int pipe = xfiles.IndexOf('|');
                                dfsfiles = xfiles.Substring(0, pipe).Split(';');
                                string[] xoffsets = xfiles.Substring(pipe + 1).Split(';');
                                dfsfilenames = new string[xoffsets.Length];
                                nodesoffsets = new int[xoffsets.Length];
                                inputrecordlengths = new int[xoffsets.Length];
                                for (int xi = 0; xi < xoffsets.Length; xi++)
                                {
                                    string xoffset = xoffsets[xi];
                                    pipe = xoffset.IndexOf('|');
                                    int offset = Int32.Parse(xoffset.Substring(0, pipe));
                                    string fname = xoffset.Substring(pipe + 1);
                                    {
                                        int inreclen = -1;
                                        int iat = fname.IndexOf('@');
                                        if (-1 != iat)
                                        {
                                            inreclen = int.Parse(fname.Substring(iat + 1));
                                            fname = fname.Substring(0, iat);
                                        }
                                        inputrecordlengths[xi] = inreclen;
                                    }
                                    dfsfilenames[xi] = fname;
                                    nodesoffsets[xi] = offset;
                                }
                            }
                            else
                            {
                                dfsfiles = new string[0];
                            }
                        }

                        if (dfsfiles.Length == 1 && 0 == dfsfiles[0].Length)
                        {
                            dfsfiles = new string[0];
                        }

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        if (len >= 4)
                        {
                            FoilKeySkipFactor = Entry.BytesToInt(buf, 0);
                        }

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IMap plugin named " + xclassname + " for foil: " + dllfn);
                        }

                        //if (!string.IsNullOrEmpty(sortclass))
                        {
                            string sampleclassname = classname + "_Sample";
                            //IMap mpsamp = LoadPluginInterface<IMap>(dllfn, sampleclassname);
                            IMap mpsamp = null == asm ? LoadPluginInterface<IMap>(dllfn, sampleclassname) : LoadPluginInterface<IMap>(asm, dllfn, sampleclassname);
                            LoadFoilSamples(mpsamp, samplesoutputfn, dfsfiles, dfsfilenames, nodesoffsets, inputrecordlengths);
                        }

                    }
                    break;

                case 'C': // Compile!
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        string code = XContent.ReceiveXString(nstm, buf);

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        bool exe = len > 0 && 0 != buf[0];
                        bool debugmode = len > 1 && 0 != buf[1];

                        string outputname = XContent.ReceiveXString(nstm, buf);

                        bool _olddebugmode = compiler.DebugMode;
                        try
                        {
                            try
                            {
                                compiler.DebugMode = debugmode;

                                compiler.CompileSource(code, exe, outputname);
                            }
                            catch (BadImageFormatException)
                            {
                            }

                            nstm.WriteByte((byte)'+');
                        }
                        catch(Exception ec)
                        {
                            nstm.WriteByte((byte)'-');
                            XContent.SendXContent(nstm, ec.GetType().Name);
                            XContent.SendXContent(nstm, ec.ToString());
                        }
                        compiler.DebugMode = _olddebugmode;

                    }
                    break;

                case 'M': // 'M' for Map
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("map");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("map");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string zmapblockbasename = XContent.ReceiveXString(nstm, buf);

                        string[] dfsfiles = null;// XContent.ReceiveXString(nstm, buf).Split(';');
                        string[] dfsfilenames = null;
                        int[] nodesoffsets = null;
                        int[] inputrecordlengths = null;
                        {
                            string xfiles = XContent.ReceiveXString(nstm, buf);
                            if (xfiles.Length > 0)
                            {
                                int pipe = xfiles.IndexOf('|');
                                dfsfiles = xfiles.Substring(0, pipe).Split(';');
                                string[] xoffsets = xfiles.Substring(pipe + 1).Split(';');
                                dfsfilenames = new string[xoffsets.Length];
                                nodesoffsets = new int[xoffsets.Length];
                                inputrecordlengths = new int[xoffsets.Length];
                                for (int xi = 0; xi < xoffsets.Length; xi++)
                                {
                                    string xoffset = xoffsets[xi];
                                    pipe = xoffset.IndexOf('|');
                                    int offset = Int32.Parse(xoffset.Substring(0, pipe));
                                    string fname = xoffset.Substring(pipe + 1);
                                    {
                                        int inreclen = -1;
                                        int iat = fname.IndexOf('@');
                                        if (-1 != iat)
                                        {
                                            inreclen = int.Parse(fname.Substring(iat + 1));
                                            fname = fname.Substring(0, iat);
                                        }
                                        inputrecordlengths[xi] = inreclen;
                                    }
                                    dfsfilenames[xi] = fname;
                                    nodesoffsets[xi] = offset;
                                }
                            }
                            else
                            {
                                dfsfiles = new string[0];
                            }
                        }

                        XContent.ReceiveXBytes(nstm, out len, buf); // !
                        int numzmblocks = 0;
                        if (len >= 4)
                        {
                            numzmblocks = Entry.BytesToInt(buf);
                        }

                        if (numzmblocks <= 0)
                        {
                            throw new Exception("Invalid number of ZMapBlocks: " + numzmblocks.ToString());
                        }
                        if (null != zmblocks)
                        {
                            throw new Exception("Map already called (zmblocks is set)");
                        }
                        zmblocks = new ZMapBlock[numzmblocks];
                        for (int i = 0; i < numzmblocks; i++)
                        {
                            zmblocks[i] = new ZMapBlock(this, i, zmapblockbasename);
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IMap plugin named " + xclassname + " for map: " + dllfn);
                        }

#if DEBUGdistrotests
                        System.Threading.Thread.Sleep(1000 * 10);
                        {
                            Distro2 xyz = new Distro2(1816, 271);
                            int highest = 0;
                            Dictionary<int, int> majors = new Dictionary<int, int>();
                            Dictionary<int, int> minors = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                RangeNode rn = xyz.Distro(bb, 0, 2);
                                if (majors.ContainsKey(rn.MajorID))
                                {
                                    majors[rn.MajorID]++;
                                }
                                else
                                {
                                    majors[rn.MajorID] = 1;
                                }
                                if (minors.ContainsKey(rn.MinorID))
                                {
                                    minors[rn.MinorID]++;
                                }
                                else
                                {
                                    minors[rn.MinorID] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in majors)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
                        {
                            Distro2 xyz = new Distro2(1816, 1);
                            int highest = 0;
                            Dictionary<int, int> sdf = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                int major = xyz.Distro(bb, 0, 2).MajorID;
                                if (sdf.ContainsKey(major))
                                {
                                    sdf[major]++;
                                }
                                else
                                {
                                    sdf[major] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in sdf)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
                        {
                            Distro2 xyz = new Distro2(17, 171);
                            int highest = 0;
                            Dictionary<int, int> sdf = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                int major = xyz.Distro(bb, 0, 2).MajorID;
                                if (sdf.ContainsKey(major))
                                {
                                    sdf[major]++;
                                }
                                else
                                {
                                    sdf[major] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in sdf)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
#endif

                        if (!string.IsNullOrEmpty(sortclass))
                        {
                            string sampleclassname = classname + "_Sample";
                            //IMap mpsamp = LoadPluginInterface<IMap>(dllfn, sampleclassname);
                            IMap mpsamp = null == asm ? LoadPluginInterface<IMap>(dllfn, sampleclassname) : LoadPluginInterface<IMap>(asm, dllfn, sampleclassname);
                            LoadSamples(mpsamp);
                        }

                        //System.Threading.Thread.Sleep(1000 * 10);
                        //IMap mp = LoadPluginInterface<IMap>(dllfn, classname);
                        IMap mp = null == asm ? LoadPluginInterface<IMap>(dllfn, classname) : LoadPluginInterface<IMap>(asm, dllfn, classname);
                        ACLMapInput mapinput = new ACLMapInput(this);
                        ACLMapOutput mapoutput = new ACLMapOutput(this);
                        int maxerrors = 10;
                        StaticGlobals.DSpace_Last = false;
                        int curoffset = 0;
                        int fi = 0;
                        if (dfsfiles.Length > 0)
                        {
                            curoffset = nodesoffsets[fi];
                        }
                        for (int i = 0; i < dfsfiles.Length; i++)
                        {
                            if (i == curoffset)
                            {
                                StaticGlobals.DSpace_InputFileName = dfsfilenames[fi];
                                StaticGlobals.DSpace_InputRecordLength = inputrecordlengths[fi];
                                fi++;
                                if (fi < nodesoffsets.Length)
                                {
                                    curoffset = nodesoffsets[fi];
                                }
                            }
                            if (0 == dfsfiles[i].Length)
                            {
                                continue;
                            }
                            try
                            {
                                mapinput.Name = dfsfiles[i];
                                mapinput._compression = compressdfschunks;
                                mapinput._open(); // Depends on MapInput.Name being the full file path.                              
                                StaticGlobals.DSpace_InputBytesRemain = (i == dfsfiles.Length - 1 ? mapinput.Stream.Length - mapinput.Stream.Position : Int64.MaxValue);                                
                                mp.OnMap(mapinput, mapoutput);
                                mapinput._close();
                            }
                            catch (Exception e)
                            {
                                if (jobfailover
                                    && null != e as System.IO.IOException)
                                {
                                    throw new System.IO.IOException("I/O Problem during Map with file '" + dfsfiles[i] + "'", e);
                                }
                                if (maxerrors <= 0)
                                {
                                    throw new Exception("Too may problems during Map", e);
                                }
                                maxerrors--;
                                //throw new Exception("Problem during Map with file '" + dfsfiles[i] + "'", e);
                                SetError("Problem during Map with file '" + dfsfiles[i] + "': " + e.ToString());
                            }
                        }
                    }
                    break;

                case 'd': // Get current directory.
                    nstm.WriteByte((byte)'+');
                    XContent.SendXContent(nstm, Environment.CurrentDirectory);
                    break;

                case 'D': // Enumerator DLL binary.
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("enum");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("enum");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IBeforeReduce plugin named " + xclassname + " for enumeration: " + dllfn);
                        }
                        //IBeforeReduce plugin = LoadPluginInterface<IBeforeReduce>(dllfn, classname);
                        IBeforeReduce plugin = null == asm ? LoadPluginInterface<IBeforeReduce>(dllfn, classname) : LoadPluginInterface<IBeforeReduce>(asm, dllfn, classname);
                        benum = new ArrayComboListEnumerator(this, plugin);
                    }
                    break;

                case 'c':
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        int n = Entry.BytesToInt(buf);
                        int count = 0;
                        if (null != benum)
                        {
                            IReducedToFile irf = benum.plugin as IReducedToFile;
                            if (null != irf)
                            {
                                List<long> appendsizes = new List<long>();
                                count = irf.GetReducedFileCount(n, appendsizes);
                                if (buf.Length < 4 + 8 * appendsizes.Count)
                                {
                                    buf = new byte[Entry.Round2Power(4 + 8 * appendsizes.Count)];
                                }
                                Entry.ToBytes(count, buf, 0);
                                int offset = 4;
                                for (int i = 0; i < appendsizes.Count; i++, offset += 8)
                                {
                                    Entry.LongToBytes(appendsizes[i], buf, offset);
                                }
                                XContent.SendXContent(nstm, buf, 4 + 8 * appendsizes.Count);
                                break; // !
                            }
                        }
                        Entry.ToBytes(count, buf, 0);
                        XContent.SendXContent(nstm, buf, 4);
                    }
                    break;

                case 'e': // Batch 'get next' enumeration.
                    {
                        try
                        {
                            if (null == benum)
                            {
                                benum = new ArrayComboListEnumerator(this, new EntryEnumerator());
                            }

                            benum.Go();
                        }
                        catch
                        {
                            if (!benum.IsFinished)
                            {
                                // Only send if not finished yet;
                                // because if finished, '-' was sent already.
                                nstm.WriteByte((byte)'-');
                            }
                            throw;
                        }
                    }
                    break;

                case 'S': // Stats...
                    {
                        int numsplits = 0;
                        for (int izb = 0; izb < zblocks.Length; izb++)
                        {
                            if (zblocks[izb].ZBlockSplit)
                            {
                                numsplits++;
                            }
                        }
                        Entry.ToBytes(numsplits, buf, 0);
                        XContent.SendXContent(nstm, buf, 4); // 4: split count
                    }
                    break;

                case 's':
                    try
                    {
                        // Sort..

                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.BelowNormal;
                        }
                        catch
                        {
                        }

#if ENABLE_TIMING
                        long start = 0;
                        if(XLog.logging)
                        {
                            QueryPerformanceCounter(out start);
                        }
#endif

                        int readkeybuflen = 1048576;
                        int readvaluebuflen = 1048576;
                        if (null != DistributedObjectsSlave.xslave)
                        {
                            System.Xml.XmlNode xzblocks = DistributedObjectsSlave.xslave["zblocks"];
                            if (null != xzblocks)
                            {
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readbuffersize"];
                                    if (null != xzbs)
                                    {
                                        int x = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                        readkeybuflen = x;
                                        readvaluebuflen = x;
                                    }
                                }
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readkeybuffersize"];
                                    if (null != xzbs)
                                    {
                                        readkeybuflen = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                    }
                                }
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readvaluebuffersize"];
                                    if (null != xzbs)
                                    {
                                        readvaluebuflen = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                    }
                                }
                            }
                        }

                        foreach (ZBlock zb in zblocks)
                        {
                            zb.LeaveAddMode(readkeybuflen, readvaluebuflen);
                        }

#if DEBUG
                        System.Threading.Thread.Sleep(1000 * 8);
#endif
                        if (8 == TValueOffset_Size)
                        {
                            foreach (ZBlock zb in zblocks)
                            {
                                zb.block64.Sort(kentries64, ref ebytes);
                            }
                            kentries64 = new List<ZBlock.KeyBlockEntry64v>(1);
                        }
                        else
                        {
                            foreach (ZBlock zb in zblocks)
                            {
                                zb.block32.Sort(kentries32, ref ebytes);
                            }
                            kentries32 = new List<ZBlock.KeyBlockEntry32v>(1);
                        }

#if ENABLE_TIMING
                        if(XLog.logging)
                        {
                            long stop;
                            QueryPerformanceCounter(out stop);
                            long freq;
                            if(QueryPerformanceFrequency(out freq))
                            {
                                long secs = (stop - start) / freq;
                                if(secs > 10)
                                {
                                    XLog.log("ArrayComboListPart sort seconds: " + secs.ToString());
                                }
                            }
                        }
#endif

                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'p': // Batch put/publish...
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        uint gbfree = (uint)(DistributedObjectsSlave.GetCurrentDiskFreeBytes() / 1073741824);
#if DEBUG
                        {
                            string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
									 if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                            {
                                gbfree = uint.MaxValue;
                            }
                        }
#endif
                        if (gbfree > 20)
                        {
                            for (int y = 0; y < len; )
                            {
                                /*if (y + this.keylen + 4 > len)
                                {
                                    SetError("batch publish about to step out of bounds: offset " + y.ToString() + " into " + len.ToString() + " length buffer");
                                    break;
                                }*/
                                y += TimedAddKVBuf(buf, y);
                            }
                        }
                        else
                        {
                            if (!nofreedisklog)
                            {
                                nofreedisklog = true;
                                SetError("Low free disk space; now dropping keys/values.");
                                if (jobfailover)
                                {
                                    XLog.failoverlog("x disk Low free disk space"); // Don't recover.
                                }
                            }
                        }
                    }
                    break;

                case 'z': // CommitZBalls
                    try
                    {
                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                        }
                        catch
                        {
                        }

                        string zballname = XContent.ReceiveXString(nstm, buf);
                        string sBlockID = XContent.ReceiveXString(nstm, buf);
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        bool b = buf.Length >= 0 && 0 != buf[0];
                        if (!IsValidsBlockID(sBlockID))
                        {
                            throw new Exception("Invalid sBlockID");
                        }
                        CommitZBalls(GetSafeZballName(zballname), sBlockID, b);
                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'Z': // IntegrateZBalls
                    try
                    {
                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                        }
                        catch
                        {
                        }

                        string zballname = XContent.ReceiveXString(nstm, buf);
                        string sBlockID = XContent.ReceiveXString(nstm, buf);
                        if (!IsValidsBlockID(sBlockID))
                        {
                            throw new Exception("Invalid sBlockID");
                        }
                        if (IntegrateZBalls(GetSafeZballName(zballname), sBlockID))
                        {
                            nstm.WriteByte((byte)'+');
                        }
                        else
                        {
                            nstm.WriteByte((byte)'-');
                        }
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'x': // Start ZMapBlockServer.
                    try
                    {
                        int xport = StartZMapBlockServer();
                        Entry.ToBytes(xport, buf, 0);
                        XContent.SendXContent(nstm, buf, 4);
                    }
                    catch
                    {
                        Entry.ToBytes(0, buf, 0); // Port 0 on error.
                        XContent.SendXContent(nstm, buf, 4);
                        throw;
                    }
                    break;

                case 'X': // Stop ZMapBlockServer.
                    StopZMapBlockServer();
                    break;

                case 'k':
                    buf = XContent.ReceiveXBytes(nstm, out len, buf);
                    if (buf.Length >= 4)
                    {
                        modbytes = Entry.BytesToInt(buf);
                    }
                    break;

                case 'j':
                    buf = XContent.ReceiveXBytes(nstm, out len, buf);
                    if (buf.Length >= 4)
                    {
                        jobfailover = 0 != Entry.BytesToInt(buf);
                    }
                    break;

                case 'W': // Overwrite compiler assembly references and options...
                    {
                        string wstr = XContent.ReceiveXString(nstm, buf);
                        int splitpos = wstr.IndexOf(";;");
                        string refs = wstr, opts = "";
                        if (-1 != splitpos)
                        {
                            refs = wstr.Substring(0, splitpos);
                            opts = wstr.Substring(splitpos + 2);
                        }
                        compiler.AssemblyReferences.Clear(); // 'W' overwrites!
                        foreach (string sref in refs.Split(';'))
                        {
                            if (sref.Length > 0)
                            {
                                compiler.AssemblyReferences.Add(sref);
                            }
                        }
                        compiler.CompilerOptions = opts;
                    }
                    break;

                case 'K': // Samples!
                    try
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        sampleblockcount = 0;
                        if (len >= 4)
                        {
                            sampleblockcount = Entry.BytesToInt(buf, 0);
                        }
                        if(sampleblockcount <= 0)
                        {
                            throw new Exception("Samples K failure (invalid sampleblockcount of " + sampleblockcount.ToString() + ")");
                        }

                        {
                            string ss = XContent.ReceiveXString(nstm, buf);
                            if (ss.Length > 0)
                            {
                                samplefns = ss.Split(';');
                            }
                            else
                            {
                                samplefns = new string[0];
                            }
                        }

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        btreeCapSize = 0;
                        if (len >= 8)
                        {
                            btreeCapSize = Entry.BytesToULong(buf, 0);
                        }

                        sortclass = XContent.ReceiveXString(nstm, buf);

                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'y': // ZMapBlock Exchange.
                    {                        
                        string lastxfile = null;
                        try
                        {
                            try
                            {
                                System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Normal;
                            }
                            catch
                            {
                            }

                            int ymaxerrorprint = 5;

                            int rangelen;
                            byte[] rangebuf = XContent.ReceiveXBytes(nstm, out rangelen, null);
                            List<int> zmranges = new List<int>(rangelen / 4); // My ranges.
                            for (int ioffset = 0; ioffset < rangelen; ioffset += 4)
                            {
                                zmranges.Add(Entry.BytesToInt(rangebuf, ioffset));
                            }
                            string hostsandports = XContent.ReceiveXString(nstm, buf).Trim();
                            if (0 != hostsandports.Length)
                            {
                                string[] otherbasepaths = hostsandports.Split(';');
                                {
                                    foreach (string basepath in otherbasepaths)
                                    {
                                        try
                                        {
                                            for (int ir = 0; ir < zmranges.Count; ir++)
                                            {
                                                string fn = basepath.Replace("%n", zmranges[ir].ToString());
                                                lastxfile = fn;
                                                //----------------------------COOKING--------------------------------
                                                int cooking_cooksremain = CookRetries;
                                                bool cooking_stream_is_open = false;
                                                for (; ; )
                                                {
                                                    try
                                                    {
                                                    //----------------------------COOKING--------------------------------
                                                        using (System.IO.FileStream fs = new System.IO.FileStream(fn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                                        {
                                                            cooking_stream_is_open = true;
                                                            System.IO.Stream gzs = fs;
                                                            if (compresszmaps)
                                                            {
                                                                gzs = new System.IO.Compression.GZipStream(gzs, System.IO.Compression.CompressionMode.Decompress);
                                                            }
                                                            ZMapStreamToZBlocks(gzs, -1, fn, FILE_BUFFER_SIZE, compresszmaps);
                                                        }
                                                    //----------------------------COOKING--------------------------------
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        bool firstcook = cooking_cooksremain == CookRetries;
                                                        if (cooking_cooksremain-- <= 0)
                                                        {
                                                            string ns = " (unable to get connection count)";
                                                            try
                                                            {
                                                                ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                                                    + " total connections on this machine)";
                                                            }
                                                            catch
                                                            {
                                                            }
                                                            throw new System.IO.IOException("cooked too many times (retries="
                                                                + CookRetries.ToString()
                                                                + "; timeout=" + CookTimeout.ToString()
                                                                + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                                        }
                                                        if (cooking_stream_is_open)
                                                        {
                                                            throw;
                                                        }
                                                        System.Threading.Thread.Sleep(CookTimeout);
                                                        if (firstcook)
                                                        {
                                                            try
                                                            {
                                                                XLog.errorlog("cooking started (retries=" + CookRetries.ToString()
                                                                    + "; timeout=" + CookTimeout.ToString()
                                                                    + ") on " + System.Net.Dns.GetHostName()
                                                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                                            }
                                                            catch
                                                            {
                                                            }
                                                        }
                                                        continue; // !
                                                    }
                                                    break;
                                                }
                                                //----------------------------COOKING--------------------------------
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            if (jobfailover
                                                && null != e as System.IO.IOException)
                                            {
                                                throw new System.IO.IOException("I/O Problem loading remote zMapBlock '" + lastxfile + "'", e);
                                            }
                                            if (ymaxerrorprint > 0)
                                            {
                                                ymaxerrorprint--;
                                                SetError("Problem loading remote zMapBlock '" + lastxfile + "': " + e.ToString());
                                            }
                                        }
                                    }
                                }
                                lastxfile = null;
                            }

                            // Now process the zmapblocks of mine that I own.
                            for (int i = 0; i < zmranges.Count; i++)
                            {
                                string zmfn = zmblocks[zmranges[i]].fzmblockfilename;
                                try
                                {
                                    using (System.IO.FileStream fs = new System.IO.FileStream(zmfn, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                    {
                                        System.IO.Stream gzs = fs;
                                        if (compresszmaps)
                                        {
                                            gzs = new System.IO.Compression.GZipStream(gzs, System.IO.Compression.CompressionMode.Decompress);
                                        }
                                        ZMapStreamToZBlocks(gzs, -1, zmfn, FILE_BUFFER_SIZE, compresszmaps);
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (jobfailover
                                        && null != e as System.IO.IOException)
                                    {
                                        throw new System.IO.IOException("I/O Problem loading local zMapBlock '" + zmfn + "'", e);
                                    }
                                    if (ymaxerrorprint != 0)
                                    {
                                        ymaxerrorprint--;
                                        SetError("Problem loading local zMapBlock '" + zmfn + "': " + e.ToString());
                                    }
                                }
                            }

                            nstm.WriteByte((byte)'+');
                        }
                        catch(Exception e)
                        {
                            nstm.WriteByte((byte)'-');
                            if (null != lastxfile)
                            {
                                if (null != e as System.IO.IOException)
                                {
                                    throw new System.IO.IOException("Problem during ZBlockExchange with file '" + lastxfile + "'", e);
                                }
                                throw new Exception("Problem during ZBlockExchange with file '" + lastxfile + "'", e);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                    break;

                case 'b':
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        if (null == zblocks)
                        {
                            break;
                        }
                        if (len >= 4)
                        {
                            int vallensz = Entry.BytesToInt(buf, 0);
                            if (8 == vallensz)
                            {
                                this.TValueOffset_Size = 8;
                                this.kentries32 = null;
                                this.kentries64 = new List<ZBlock.KeyBlockEntry64v>(1);
                                foreach (ZBlock zb in zblocks)
                                {
                                    zb.block32 = null;
                                    zb.block64 = new ZBlock.BlockInfo<ZBlock.KeyBlockEntry64v>(this, zb);
                                    zb.TValueOffset_Size = 8;
                                }
                            }
                            else if (4 == vallensz)
                            {
                                this.TValueOffset_Size = 4;
                                this.kentries32 = new List<ZBlock.KeyBlockEntry32v>(1);
                                this.kentries64 = null;
                                foreach (ZBlock zb in zblocks)
                                {
                                    zb.block32 = new ZBlock.BlockInfo<ZBlock.KeyBlockEntry32v>(this, zb);
                                    zb.block64 = null;
                                    zb.TValueOffset_Size = 4;
                                }
                            }
                            else
                            {
                                throw new Exception("Invalie");
                            }
                        }
                    }
                    break;

                case '.': // Ping?
                    nstm.WriteByte((byte)','); // Pong!
                    break;

                default:
                    base.ProcessCommand(nstm, tag);
                    break;
            }
        }


        internal static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            while (sofar < len)
            {
                int xread = stm.Read(buf, sofar, len - sofar);
                if (xread <= 0)
                {
                    break;
                }
                sofar += xread;
            }
            return sofar;
        }

        internal static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            if (len != StreamReadLoop(stm, buf, len))
            {
                throw new System.IO.IOException("Unable to read from stream");
            }
        }


        void ZMapStreamToZBlocks(System.IO.Stream stm, long len, string sfn, int iFILE_BUFFER_SIZE, bool bcompresszmaps)
        {
            if (len < 0)
            {
                len = long.MaxValue;
            }

#if COOK_TEST_ZMapStreamToZBlocks
            int throwon = 2;
#endif
            //----------------------------COOKING--------------------------------
            bool cooking_is_cooked = false;
            int cooking_cooksremain = CookRetries;
            bool cooking_is_read = false;
            long cooking_pos = 0; // Last known good position between records/lines.
            //----------------------------COOKING--------------------------------

            while (true)
            {
                try
                {
                    //----------------------------COOKING--------------------------------
                    cooking_is_read = true; // Important!

                    if (cooking_is_cooked)
                    {
                        cooking_is_cooked = false;
                        stm.Close();
                        stm = new System.IO.FileStream(sfn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, iFILE_BUFFER_SIZE);
                        {
                            if (compresszmaps)
                            {
                                stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
                            }
                        }
                        stm.Seek(cooking_pos, System.IO.SeekOrigin.Begin);
                    }
                    //----------------------------COOKING--------------------------------

                    int read7810 = StreamReadLoop(stm, _smallbuf, keylen + 4);
                    if (0 == read7810)
                    {
                        break;
                    }
#if DEBUG
                    if (read7810 != keylen + 4)
                    {
                        throw new Exception("DEBUG:  (read7810 != keylen + 4)");
                    }
#endif

                    int valuelen = Entry.BytesToInt(_smallbuf, keylen);
                    if (valuelen > buf.Length)
                    {
                        buf = new byte[Entry.Round2Power(valuelen)];
                    }
#if COOK_TEST_ZMapStreamToZBlocks
                    if (throwon > 0 && --throwon == 0)
                    {
                        throw new Exception("COOK_TEST_ZMapStreamToZBlocks");
                    }
#endif
                    StreamReadExact(stm, buf, valuelen);
                    //----------------------------COOKING--------------------------------
                    cooking_pos += keylen + 4 + valuelen;
                    cooking_is_read = false;
                    //----------------------------COOKING--------------------------------

                    EAdd(_smallbuf, 0, buf, 0, valuelen);

                }
                catch (Exception e)
                {
                    //----------------------------COOKING--------------------------------
                    if (!cooking_is_read)
                    {
                        throw;
                    }
                    bool firstcook = cooking_cooksremain == CookRetries;
                    if (cooking_cooksremain-- <= 0)
                    {
                        string ns = " (unable to get connection count)";
                        try
                        {
                            ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                + " total connections on this machine)";
                        }
                        catch
                        {
                        }
                        throw new System.IO.IOException("cooked too many times (retries="
                            + CookRetries.ToString()
                            + "; timeout=" + CookTimeout.ToString()
                            + ") on " + System.Net.Dns.GetHostName() + ns, e);
                    }
                    System.Threading.Thread.Sleep(CookTimeout);
                    if (firstcook)
                    {
                        try
                        {
                            XLog.errorlog("cooking started (retries=" + CookRetries.ToString()
                                + "; timeout=" + CookTimeout.ToString()
                                + ") on " + System.Net.Dns.GetHostName()
                                + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                        }
                        catch
                        {
                        }
                    }
                    cooking_is_cooked = true;
                    continue;
                    //----------------------------COOKING--------------------------------
                }
            }
        }


        // Returns the port number.
        int StartZMapBlockServer()
        {
            lock (this)
            {
                for (int i = 0; i < zmblocks.Length; i++)
                {
                    zmblocks[i].PrepareForExchange();
                }

                return 0;
            }
        }

        void StopZMapBlockServer()
        {
            CloseZMapBlocks(); // !
        }


        static bool nofreedisklog = false;


        public override void ProcessCommands(NetworkStream nstm)
        {
            try
            {
                base.ProcessCommands(nstm);

                CloseZBlocks();
                CloseZMapBlocks();

                if (jobfailover)
                {
                    XLog.failoverlog("d done");
                }
            }
            catch (OutOfMemoryException e)
            {
                SetError("ArrayComboList Sub Process exception: " + e.ToString() + "  {" + CurrentCommand.ToString() + "}");
                if (jobfailover)
                {
                    XLog.failoverlog("x " + e.Message); // Non-recoverable.
                }
                throw;
            }
            catch (Exception e)
            {
                uint gbfree = (uint)(DistributedObjectsSlave.GetCurrentDiskFreeBytes() / 1073741824);
                if (gbfree <= 20)
                {
                    Exception newe = new Exception("Free disk space too low", e);
                    SetError("ArrayComboList Sub Process exception: " + newe.ToString() + "  {" + CurrentCommand.ToString() + "}");
                    if (jobfailover)
                    {
                        XLog.failoverlog("x " + e.Message); // Non-recoverable; other exception.
                        //throw new DistributedObjectsSlave.DistObjectAbortException(newe); // No.. let it attempt to resume...
                    }
                    throw newe;
                }
                else
                {
                    SetError("ArrayComboList Sub Process exception: " + e.ToString() + "  {" + CurrentCommand.ToString() + "}");
                    if (jobfailover)
                    {
                        if (DistributedObjectsSlave.CanSlaveExceptionFailover(e))
                        {
                            XLog.failoverlog("r " + e.Message); // Recoverable.
                            throw new DistributedObjectsSlave.DistObjectAbortException(e);
                        }
                        else
                        {
                            XLog.failoverlog("x " + e.Message); // Non-recoverable; other exception.
                        }
                    }
                    throw;
                }
            }
        }


#if USE_DfsMapper
        public class DfsMapper : MySpace.DataMining.DistributedObjects.IMap
        {
            List<byte> _mapbuf = new List<byte>();
            public void OnMap(MapInput input, MapOutput output)
            {
                for (; ; )
                {
                    _mapbuf.Clear();
                    int ib;
                    for (; ; )
                    {
                        ib = input.Stream.ReadByte();
                        if (-1 == ib)
                        {
                            if (0 != _mapbuf.Count)
                            {
                                Map(ByteSlice.Create(_mapbuf), output);
                            }
                            return; // !
                        }
                        _mapbuf.Add((byte)ib);
                        if ('\n' == ib)
                        {
                            break;
                        }
                    }
                    Map(ByteSlice.Create(_mapbuf), output);
                }
            }

            //----------

            // Helper function for OnBeforeLoad.
            void AddWordHelper(List<byte> word, List<byte> valuebuf, MapOutput output)
            {
                if (word.Count > 0)
                {
                    if (word.Count >= 16)
                    {
                        word.Clear();
                        return; // Long words skipped.
                    }
                    for (int i = word.Count; i < 16; i++)
                    {
                        word.Add(0); // Pad with 0s.
                    }
                    output.Add(ByteSlice.Create(word), ByteSlice.Create(valuebuf));
                    word.Clear();
                }
            }

            public virtual void Map(ByteSlice line, MapOutput output)
            {
                List<byte> valuebuf = new List<byte>(); // Preallocate value buffer.
                Entry.ToBytesAppend(1, valuebuf); // Set with 1 for all values, since they're the same.

                List<byte> word = new List<byte>(); // Preallocate word buffer.
                for (int i = 0; i < line.Length; i++)
                {
                    byte ib = line[i];
                    if (ib >= 0x80
                        || (!char.IsLetterOrDigit((char)ib) && (char)ib != '-' && (char)ib != '_' && (char)ib != '\'')) // Find word break.
                    {
                        AddWordHelper(word, valuebuf, output);
                    }
                    else
                    {
                        word.Add((byte)char.ToLower((char)ib)); // Keep track of word characters.
                    }
                }
                AddWordHelper(word, valuebuf, output);
            }
            //----------

        }
#endif


        public class LoadPluginException : Exception
        {
            public LoadPluginException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }

            public LoadPluginException(string msg)
                : base(msg)
            {
            }
        }

        public static Iface LoadPluginInterface<Iface>(System.Reflection.Assembly assembly, string name, string classname)
        {
            try
            {
                Iface plugin = default(Iface);
                string ifacename = typeof(Iface).FullName;
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface(ifacename) != null)
                            {
                                plugin = (Iface)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new LoadPluginException("Class " + classname + " was found, but does not implement interface " + typeof(Iface).Name);
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new LoadPluginException("Plugin from '" + name + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.Message;
                }
                throw new LoadPluginException("ReflectionTypeLoadException error(s) with plugin '" + name + "': " + x);
            }
        }

        public static Iface LoadPluginInterface<Iface>(System.Reflection.Assembly assembly, string classname)
        {
            return LoadPluginInterface<Iface>(assembly, assembly.FullName, classname);
        }

        public static Iface LoadPluginInterface<Iface>(string dllfilepath, string classname)
        {
            System.Reflection.Assembly assembly = System.Reflection.Assembly.LoadFrom(dllfilepath);
            return LoadPluginInterface<Iface>(assembly, dllfilepath, classname);
        }


        protected internal IBeforeLoad LoadBeforeLoadPlugin(string dllfilepath, string classname)
        {
            return LoadPluginInterface<IBeforeLoad>(dllfilepath, classname);
        }


        internal EntriesInput GetEntriesInput()
        {
            EntriesInput input = new EntriesInput();
            input.entries = this.entries;
            input.KeyLength = this.keylen;
            input.net = this.net;
            return input;
        }


#if ENABLE_TIMING
        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);
#endif

    }


    public class ArrayComboListEnumerator
    {
        int curzblock = -1;


        internal ArrayComboListEnumerator(ArrayComboListPart acl, IBeforeReduce plugin)
        {
            this.acl = acl;
            this.input = acl.GetEntriesInput();
            this.output = acl.CreatePluginOutput();
            this.plugin = plugin;
            this.ordplugin = plugin as IBeforeReduceOrd;
        }


        System.IO.Stream _largezblockresultfile = null;
        string _largezblockresultfilename = null;
        List<ArrayComboListPart.ZBlock.KvSplit> _largekvsbuf = null;
        ByteSlice _laregezblockprevinfo;

        // Now with zblock splitting, this loads the next virtual zblock, not necessarily physical.
        bool LoadNextZBlock()
        {
            do
            {
                ArrayComboListPart.ZBlock zb;
                if (null == _largezblockresultfile)
                {
#if DEBUG
                    if (0 != _laregezblockprevinfo.Length)
                    {
                        throw new Exception("DEBUG:  LoadNextZBlock: (0 != _laregezblockprevinfo.Length) upon loading next physical zblock");
                    }
#endif
                    if (curzblock + 1 >= acl.zblocks.Length)
                    {
                        input.entries.Clear();
                        input.net.Clear();
                        return false;
                    }
                    curzblock++;
                    zb = acl.zblocks[curzblock];
                    if (zb.splitzblocks)
                    {
                        _largezblockresultfilename = ArrayComboListPart.ZBlock.CreateZBlockFileName(
                            zb.zblockID, "LargeResult");
                        if (null == _largekvsbuf)
                        {
                            _largekvsbuf = new List<ArrayComboListPart.ZBlock.KvSplit>();
                        }
                        zb._CreateLargeResultFile(_largezblockresultfilename, _largekvsbuf, ref acl.ebytes, ref acl.evaluesbuf);
                        _largezblockresultfile = new System.IO.FileStream(_largezblockresultfilename,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                        // Continues down to Reading a large zblock result file...
                    }
                    else
                    {
                        zb.CopyInto(input.net, input.entries, ref acl.ebytes, ref acl.evaluesbuf);
                    }
                }
                if (null != _largezblockresultfile)
                {
                    // Reading a large zblock result file.

                    bool done = false;
                    {

                        input.entries.Clear();
                        input.net.Clear();

                        Entry ent;
                        ent.NetIndex = 0;
                        ent.NetEntryOffset = 0;

                        int keylen = acl.keylen;

                        if (acl.ebytes.Length < ArrayComboListPart.ZBlock.MAXKVBUFSIZE)
                        {
                            acl.ebytes = new byte[ArrayComboListPart.ZBlock.MAXKVBUFSIZE];
                        }
                        byte[] buf = acl.ebytes;

                        input.net.Add(buf);

                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = acl.CookRetries;
                        bool cooking_inIO = false;
                        long cooking_seekpos = long.MinValue;
                        //----------------------------COOKING--------------------------------

                        for (; ; )
                        {
                            try
                            {

                                if (long.MinValue == cooking_seekpos)
                                {
                                    cooking_inIO = true;
                                    cooking_seekpos = _largezblockresultfile.Position;
                                    cooking_inIO = false;
                                }
                                else
                                {
                                    cooking_inIO = true;
                                    _largezblockresultfile.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                    cooking_inIO = false;
                                }

                                bool needread = (0 == _laregezblockprevinfo.Length);

                                cooking_inIO = true;
                                if (needread
                                    && (keylen + 4 != _largezblockresultfile.Read(buf, 0, keylen + 4)))
                                {
                                    cooking_inIO = false;
                                    done = true;
                                }
                                else
                                {
                                    cooking_inIO = false;
                                    if (needread)
                                    {
                                        // Add to seekpos if read this time, otherwise it was added last time.
                                        cooking_seekpos += keylen + 4;
                                    }
                                    else //if (0 != _laregezblockprevinfo.Length)
                                    {
#if DEBUG
                                        if (0 == _laregezblockprevinfo.Length)
                                        {
                                            throw new Exception("DEBUG:  LoadNextZBlock: (0 == _laregezblockprevinfo.Length) && (!needread)");
                                        }
#endif
                                        for (int ip = 0; ip < keylen + 4; ip++)
                                        {
                                            buf[ip] = _laregezblockprevinfo[ip];
                                        }
                                        _laregezblockprevinfo = ByteSlice.Prepare();
                                    }
                                    int netbuflen = 0;
                                    for (; ; )
                                    {
                                        int valuelen = Entry.BytesToInt(buf, netbuflen + keylen);
#if DEBUG
                                        if (valuelen < 0)
                                        {
                                            throw new Exception("DEBUG:  LoadNextZBlock: (valuelen{"
                                                + valuelen.ToString() + "} < 0) large zblock file");
                                        }
                                        if (valuelen >= ArrayComboListPart.ZBlock.ZFILE_SPLIT_SIZE)
                                        {
                                            throw new Exception("DEBUG:  LoadNextZBlock: (valuelen{"
                                                + valuelen.ToString() + "} >= ZFILE_SPLIT_SIZE) large zblock file");
                                        }
#endif
                                        int newnetbuflen = netbuflen + keylen + 4 + valuelen;
                                        if (newnetbuflen > buf.Length)
                                        {
                                            _laregezblockprevinfo = ByteSlice.Prepare(buf, netbuflen, keylen + 4);
                                            break;
                                        }
                                        // Current key is at index 0 of buf; check against it.
                                        if (netbuflen != 0)
                                        {
                                            bool samekey = true;
                                            for (int ix = 0; ix < keylen; ix++)
                                            {
                                                if (buf[ix] != buf[netbuflen + ix])
                                                {
                                                    samekey = false;
                                                    break;
                                                }
                                            }
                                            if (!samekey)
                                            {
                                                _laregezblockprevinfo = ByteSlice.Prepare(buf, netbuflen, keylen + 4);
                                                break;
                                            }
                                        }

                                        // Same key (or first) and it fits, so read and add it and keep going...
                                        cooking_inIO = true;
                                        int read2911 = _largezblockresultfile.Read(buf, netbuflen + keylen + 4, valuelen);
#if DEBUG
                                        if (read2911 != valuelen)
                                        {
                                            throw new Exception("DEBUG:  (read2911 != valuelen)");
                                        }
#endif
                                        cooking_inIO = false;
                                        cooking_seekpos += valuelen;

                                        ent.NetEntryOffset = netbuflen;
                                        input.entries.Add(ent);
                                        netbuflen = newnetbuflen;

                                        // Next key?
                                        if (netbuflen + keylen + 4 > buf.Length)
                                        {
                                            // Doesn't fit!
                                            break;
                                        }
                                        cooking_inIO = true;
                                        if (keylen + 4 != _largezblockresultfile.Read(buf, netbuflen, keylen + 4))
                                        {
                                            cooking_inIO = false;
                                            done = true;
                                            break;
                                        }
                                        cooking_inIO = false;
                                        cooking_seekpos += keylen + 4;
                                        // Read next key, continue loop to handle it.

                                    }
                                    // All good, go on and return.

                                }

                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                //----------------------------COOKING--------------------------------
                                bool firstcook = cooking_cooksremain == acl.CookRetries;
                                if (cooking_cooksremain-- <= 0)
                                {
                                    string ns = " (unable to get connection count)";
                                    try
                                    {
                                        ns = " (" + NetUtils.GetActiveConnections().Length.ToString()
                                            + " total connections on this machine)";
                                    }
                                    catch
                                    {
                                    }
                                    throw new System.IO.IOException("cooked too many times (retries="
                                        + acl.CookRetries.ToString()
                                        + "; timeout=" + acl.CookTimeout.ToString()
                                        + ") on " + System.Net.Dns.GetHostName() + ns, e);
                                }
                                System.Threading.Thread.Sleep(acl.CookTimeout);
                                if (firstcook)
                                {
                                    try
                                    {
                                        XLog.errorlog("cooking started (retries=" + acl.CookRetries.ToString()
                                            + "; timeout=" + acl.CookTimeout.ToString()
                                            + ") on " + System.Net.Dns.GetHostName()
                                            + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                                    }
                                    catch
                                    {
                                    }
                                }
                                continue;
                                //----------------------------COOKING--------------------------------
                            }
                            break;
                        }

                    }
                    if (done)
                    {
                        try
                        {
                            _largezblockresultfile.Close();
                            System.IO.File.Delete(_largezblockresultfilename);
                        }
                        catch(Exception e)
                        {
                            XLog.errorlog("Warning with large zblock handling: " + e.ToString());
                        }
                        _largezblockresultfile = null;
                        _largezblockresultfilename = null;
                        // Likely place where finishing with (0 == input.entries.Count), but not always.
                    }

#if REDUCE_SPLIT_CALLS_GC
                    System.GC.Collect();
                    System.GC.WaitForPendingFinalizers();
#endif

                }
            }
            while (0 == input.entries.Count); // Loop to next zblock if 0 entries.
            if (null != this.ordplugin)
            {
                this.ordplugin.SetReduceOrd(curzblock);
            }
            return true;
        }

        bool EnsureStarted()
        {
            if (curzblock < 0)
            {
                try
                {
                    System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                }
                catch
                {
                }

                return LoadNextZBlock();
            }
            return true;
        }


        internal void Go()
        {            
            if (EnsureStarted())
            {                
                int lastnonemptyzblock = 0;
                {
                    for (int iz = acl.zblocks.Length - 1; iz >= 0; iz--)
                    {
                        if (acl.zblocks[iz].getzkeyblocksize() > 0)
                        {
                            lastnonemptyzblock = iz;
                            break;
                        }
                    }
                }
                StaticGlobals.DSpace_Last = false;
                for (; ; )
                {
                    StaticGlobals.DSpace_InputBytesRemain = (curzblock == lastnonemptyzblock ? 0 : 1);
                    bool cont = plugin.OnGetEnumerator(input, output);
                    if (!cont)
                    {
                        cont = LoadNextZBlock();
                    }
                    if (output.CheckFlushedReset())
                    {
                        break;
                    }
                    if (!cont)
                    {
                        break;
                    }
                }
            }   
            output.Finish();
            if (!output.EndBatch())
            {
                if (!finished)
                {
                    finished = true;
                    IAfterReduce after = plugin as IAfterReduce;
                    if (null != after)
                    {
                        after.OnEnumeratorFinished();
                    }
                }
            }            
            
           
                   
        }


        public bool IsFinished
        {
            get
            {
                return finished;
            }
        }


        bool finished = false;
        private ArrayComboListPart acl;
        private EntriesInput input;
        private EntriesOutput output;
        internal IBeforeReduce plugin;
        internal IBeforeReduceOrd ordplugin;
    }

}
