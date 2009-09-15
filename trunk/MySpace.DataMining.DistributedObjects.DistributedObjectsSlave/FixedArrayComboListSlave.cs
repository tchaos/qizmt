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


using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{
    public class FixedArrayComboListPart : DistObjectBase
    {
        public class ZBlock
        {
            const long ROGUE_ZBLOCK_SIZE = 8589934592 + 1; // > 8 GB
            const long ZVALUEBLOCK_MAX_BYTES = Int32.MaxValue;
            const int ZFILE_MAX_BYTES = 1073741824; // Actual limit when writing to zkey/zvalue files.

            FixedArrayComboListPart parent;
            int zblockID; // ZBlock ID (0-based n)

            int keyblockbuflen, valueblockbuflen;
            System.IO.FileStream fzkeyblock;
            System.IO.FileStream fzvalueblock;
            string fzkeyblockfilename;
            string fzvalueblockfilename;
            int valueaddedbytes = 0; // Does not include length-bytes (the extra 4 for each length).
            long zvalueblocksize = 0;
            long zkeyblocksize = 0;
            int numadded = 0;
            bool addmode = true;
            bool isrogue = false;


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


            private static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            private static string CreateFileName(int zblockID, string otherinfo)
            {
                return "zblock_" + _spid + "_" + zblockID.ToString() + "_" + otherinfo + ".zb";
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
                        string fn = ZBlock.CreateFileName(i, otherinfo);
                        if (System.IO.File.Exists(fn))
                        {
                            System.IO.File.Delete(fn);
                            found = true;
                        }
                    }
                }
            }


            internal void ensurefzblock(bool sorting)
            {
                {
                    System.IO.FileAccess access = System.IO.FileAccess.Read;
                    System.IO.FileMode mode = System.IO.FileMode.Open;
                    if (addmode || sorting)
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
                    fzvalueblock = new System.IO.FileStream(fzvalueblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                }
            }


            internal ZBlock(FixedArrayComboListPart parent, int zblockID, int addkeybuflen, int addvaluebuflen)
            {
                this.parent = parent;
                this.zblockID = zblockID;

                this.keyblockbuflen = addkeybuflen;
                this.valueblockbuflen = addvaluebuflen;

                fzkeyblockfilename = CreateFileName(zblockID, "key_unsorted");
                fzvalueblockfilename = CreateFileName(zblockID, "value");
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


            public bool Add(byte[] keybuf, int keyoffset, byte[] valuebuf, int valueoffset)
            {
                long x = (long)parent.keylen + (long)parent.valuelen;
                if (x > ZFILE_MAX_BYTES)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //XLog.errorlog("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }

                if (zvalueblocksize + parent.valuelen > ZFILE_MAX_BYTES)
                {
                    return false;
                }
                if (zkeyblocksize + parent.keylen + 4 > ZFILE_MAX_BYTES)
                {
                    return false;
                }

                fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                //Entry.ToBytes((int)fzvalueblock.Length, parent._smallbuf, 0);
                Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                fzkeyblock.Write(parent._smallbuf, 0, 4);

                fzvalueblock.Write(valuebuf, valueoffset, parent.valuelen);

                valueaddedbytes += parent.valuelen;
                numadded++;

                zvalueblocksize += parent.valuelen;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + 4;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                return true;
            }

            public bool Add(IList<byte> keybuf, int keyoffset, IList<byte> valuebuf, int valueoffset)
            {
                long x = (long)parent.keylen + (long)parent.valuelen;
                if (x > ZFILE_MAX_BYTES)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //XLog.errorlog("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }

                if (zvalueblocksize + parent.valuelen > ZFILE_MAX_BYTES)
                {
                    return false;
                }
                if (zkeyblocksize + parent.keylen + 4 > ZFILE_MAX_BYTES)
                {
                    return false;
                }

                //fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                parent.StreamWrite(fzkeyblock, keybuf, keyoffset, parent.keylen);
                //Entry.ToBytes((int)fzvalueblock.Length, parent._smallbuf, 0);
                Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                fzkeyblock.Write(parent._smallbuf, 0, 4);

                parent.StreamWrite(fzvalueblock, valuebuf, valueoffset, parent.valuelen);

                valueaddedbytes += parent.valuelen;
                numadded++;

                zvalueblocksize += parent.valuelen;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + 4;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                return true;
            }


            void makerogue()
            {
                isrogue = true;

                // ... rename zblock files; don't let it auto delete but have batch file delete it.
            }


            void checkrogue()
            {
                if (!isrogue)
                {
                    long x = fzkeyblock.Length + fzvalueblock.Length;
                    if (x >= ROGUE_ZBLOCK_SIZE)
                    {
                        makerogue();
                    }
                }
            }


            // buf must be at least 8 bytes.
            // Truncates zblocks at ROGUE_ZBLOCK_SIZE.
            //--E-------------ADDCOOK------------------
            public void CopyInto(List<byte[]> net, List<Entry> entries, ref byte[] ebuf, ref byte[] evaluesbuf)
            {
                ensurefzblock(false);
                checkrogue();

                net.Clear();
                entries.Clear();

                long kflen = fzkeyblock.Length;
                int keycount = (int)(kflen / (parent.keylen + 4));

                long vflen = fzvalueblock.Length;

                if (vflen > ZVALUEBLOCK_MAX_BYTES)
                {
                    vflen = ZVALUEBLOCK_MAX_BYTES;
                }

                try
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

                    int newcap = keycount;
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
                    fzvalueblock.Read(valuesbuf, 0, (int)vflen); // NOTE: vflen capped to ZVALUEBLOCK_MAX_BYTES for now!

                    // Read from existing unsorted zblock file into buffer.
                    fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                    long sofar = 0;
                    Entry ent;
                    ent.NetEntryOffset = 0;
                    ent.NetIndex = 0;
                    int offset = 0;
                    unchecked
                    {
                        for (int i = 0; i != keycount; i++)
                        {
                            int morespace = offset + parent.keylen + 4;
                            if (morespace < offset || morespace > buf.Length)
                            {
                                offset = 0;
                                buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                net.Add(buf);
                                ent.NetEntryOffset++;
                            }

                            ent.NetEntryOffset = offset;
                            fzkeyblock.Read(buf, offset, parent.keylen);
                            offset += parent.keylen;

                            fzkeyblock.Read(parent._smallbuf, 0, 4);
                            int valueoffset = Entry.BytesToInt(parent._smallbuf);

                            int valuelen = parent.valuelen;
                            Entry.ToBytes(valuelen, buf, offset);
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
                                if (sofar + Int32.MaxValue >= ROGUE_ZBLOCK_SIZE)
                                {
                                    makerogue(); // Should already be set, but just to be sure.
                                    break;
                                }
                                else
                                {
                                    offset = 0;
                                    buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                    net.Add(buf);
                                    ent.NetEntryOffset++;
                                    i--; // So next iteration does this index again.
                                    continue;
                                }
                            }

                            Buffer.BlockCopy(valuesbuf, valueoffset, buf, offset, valuelen);
                            offset += valuelen;

                            sofar += parent.keylen + 4 + valuelen;
                            if (sofar >= ROGUE_ZBLOCK_SIZE)
                            {
                                makerogue(); // Should already be set, but just to be sure.
                                break;
                            }

                            entries.Add(ent);
                        }
                    }
                }
                catch (Exception e)
                {
                    net.Clear();
                    entries.Clear();
                    XLog.errorlog("Enumeration failure; zblock skipped: " + e.ToString());
                }

                _justclose();
            }


            public struct KeyBlockEntry
            {
                public ByteSlice key;
                public int valueoffset;
            }


            //--C-------------ADDCOOK------------------
            public void CopyInto(List<KeyBlockEntry> kentries, ref byte[] ebuf)
            {
                ensurefzblock(false);
                checkrogue();

                int kflen = (int)fzkeyblock.Length;
                int keycount = kflen / (parent.keylen + 4);

                try
                {
                    if (ebuf.Length < parent.keylen * keycount + 4)
                    {
                        ebuf = null;
                        long lnew = Entry.Round2Power(parent.keylen * keycount + 4);
                        if (lnew > Int32.MaxValue)
                        {
                            lnew = Int32.MaxValue;
                        }
                        ebuf = new byte[lnew];
                    }
                    byte[] buf = ebuf;

                    kentries.Clear();

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

                    // Read from existing unsorted zblock file into buffer.
                    fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                    KeyBlockEntry kent;
                    //kent.key = ByteSlice.Create();
                    //kent.valueoffset = 0;
                    int offset = 0;
                    for (int i = 0; i != keycount; i++)
                    {
                        fzkeyblock.Read(buf, offset, parent.keylen + 4);
                        kent.key = ByteSlice.Create(buf, offset, parent.keylen);
                        offset += parent.keylen;
                        kent.valueoffset = Entry.BytesToInt(buf, offset);
                        kentries.Add(kent);
                    }
                }
                catch (Exception e)
                {
                    kentries.Clear();
                    XLog.errorlog("Sort failure; zblock skipped: " + e.ToString());
                }
            }


            // buf must be at least 8 bytes.
            public void Sort(List<KeyBlockEntry> kentries, ref byte[] ebuf)
            {
                ensurefzblock(false);

                try
                {
                    try
                    {
                        CopyInto(kentries, ref ebuf);
                    }
                    finally
                    {
                        // Delete old (unsorted) file; prepare new (sorted) one.
                        // Keep these together so that there's always one on file; so cleanup sees it and continues.
                        _justclose();
                        System.IO.File.Delete(fzkeyblockfilename);
                        fzkeyblockfilename = CreateFileName(zblockID, "key_sorted");
                        ensurefzblock(true);
                    }

                    // Sort the sortbuffer.
                    kentries.Sort(new System.Comparison<KeyBlockEntry>(parent._kcmp));

                    fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                    // From (sorted) sortbuffer write into new sorted zblock file.
                    foreach (KeyBlockEntry kent in kentries)
                    {
                        kent.key.CopyTo(parent._smallbuf);
                        fzkeyblock.Write(parent._smallbuf, 0, parent.keylen);
                        Entry.ToBytes(kent.valueoffset, parent._smallbuf, 0);
                        fzkeyblock.Write(parent._smallbuf, 0, 4);
                    }
                }
                finally
                {
                    _justclose();
                }
            }
        }


        List<byte[]> net;
        internal byte[] ebytes; // Current key/value pairs.
        internal byte[] evaluesbuf; // Whole value-zblock in memory for fast seek.
        List<Entry> entries;
        List<ZBlock.KeyBlockEntry> kentries;
        int keylen, valuelen;
        FixedArrayComboListEnumerator benum = null;
        internal ZBlock[] zblocks;
        internal byte[] _smallbuf;
        internal byte[] streamwritebuf;


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


        public static FixedArrayComboListPart Create(int keylength, int valuelength)
        {
            return new FixedArrayComboListPart(keylength, valuelength);
        }


        private FixedArrayComboListPart(int keylength, int valuelength)
        {
            int count_capacity = 1024;
            int estimated_row_capacity = 32;

            ZBlock.CleanZBlockFiles("value", "key_unsorted", "key_sorted");

            this.keylen = keylength;
            this.valuelen = valuelength;

            if (keylength > 4)
            {
                _smallbuf = new byte[keylength];
            }
            else
            {
                _smallbuf = new byte[4];
            }

            net = new List<byte[]>(8);

            entries = new List<Entry>(1);
            kentries = new List<ZBlock.KeyBlockEntry>(1);

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
                    {
                        System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addbuffersize"];
                        if (null != xzbs)
                        {
                            int x = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                            zblockkeybufsize = x;
                            zblockvaluebufsize = x;
                        }
                    }
                    {
                        System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addkeybuffersize"];
                        if (null != xzbs)
                        {
                            zblockkeybufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                        }
                    }
                    {
                        System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addvaluebuffersize"];
                        if (null != xzbs)
                        {
                            zblockvaluebufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                        }
                    }
                }
            }

            evaluesbuf = new byte[(count_capacity * estimated_row_capacity) / numzblocks * 2];

            if (XLog.logging)
            {
                XLog.log("Creating " + numzblocks.ToString() + " ZBlock`s");
            }

            zblocks = new ZBlock[numzblocks];
            for (int i = 0; i != numzblocks; i++)
            {
                zblocks[i] = new ZBlock(this, i, zblockkeybufsize, zblockvaluebufsize);
            }
        }


        public override byte[] GetValue(byte[] key, out int valuelength)
        {
            throw new Exception("Not supported");
        }


        public static long BytesToModLong(IList<byte> bytes, int offset, int length)
        {
            unchecked
            {
                long result = 0;
                uint shift = 0;
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

        public static long BytesToModLong(IList<byte> bytes)
        {
            return BytesToModLong(bytes, 0, bytes.Count);
        }



        public bool EAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset)
        {
            int zbid = (int)(BytesToModLong(key, keyoffset, this.keylen) % zblocks.Length);

            return zblocks[zbid].Add(key, keyoffset, value, valueoffset);
        }


        public bool TimedAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset)
        {
            bool result = false;

#if ENABLE_TIMING
            long start = 0;
            if (XLog.logging)
            {
                QueryPerformanceCounter(out start);
            }
#endif

            result = EAdd(key, keyoffset, value, valueoffset);

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

        public bool TimedAdd(IList<byte> key, IList<byte> value)
        {
            return TimedAdd(key, 0, value, 0);
        }


        // Adds key and value from buf starting at offset.
        // Returns number of bytes this key/value used (i.e. number of bytes to skip to next).
        protected int TimedAddKVBuf(IList<byte> buf, int offset)
        {
            TimedAdd(buf, offset, buf, offset + this.keylen);
            return this.keylen + valuelen;
        }


        // Warning: key needs to be immutable!
        public override void CopyAndSetValue(byte[] key, byte[] value, int valuelength)
        {
            if (valuelength != this.valuelen)
            {
                throw new Exception("valuelength != this.valuelen");
            }
            TimedAdd(key, value);
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


        private static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

        public string CreateDllFileName(string part)
        {
            return "ylib_" + part + "_" + _spid + ".ylib";
        }

        public string CreateXlibFileName(string part)
        {
            return "xlib_" + part + "_" + _spid + ".xlib";
        }


        internal class ACLFixedEntriesOutput : FixedEntriesOutput
        {
            internal ACLFixedEntriesOutput(byte[] buf, NetworkStream nstm)
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


        internal FixedEntriesOutput CreatePluginOutput()
        {
            return new FixedArrayComboListPart.ACLFixedEntriesOutput(new byte[this.buf.Length], this.nstm);
        }


        internal class ACLLoadOutput : LoadOutput
        {
            internal ACLLoadOutput(FixedArrayComboListPart acl)
            {
                this.acl = acl;
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != acl.keylen)
                {
                    throw new Exception("Key length mismatch; got " + keylength.ToString() + " bytes, expected " + acl.keylen.ToString());
                }
                if (valuelength != acl.valuelen)
                {
                    throw new Exception("Value length mismatch; got " + valuelength.ToString() + " bytes, expected " + acl.valuelen.ToString());
                }
                acl.EAdd(keybuf, keyoffset, valuebuf, valueoffset);
            }


            FixedArrayComboListPart acl;
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
                            if (0 != len)
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
                            XLog.log("Loading IBeforeReduceFixed plugin named " + xclassname + " for before-load: " + dllfn);
                        }

                        IBeforeLoad bl = LoadBeforeLoadPlugin(dllfn, classname);
                        LoadOutput loadoutput = new ACLLoadOutput(this);
                        bl.OnBeforeLoad(loadoutput);
                    }
                    break;

                case 'D': // Enumerator DLL binary.
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("enum");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("enum");
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
                            XLog.log("Loading IBeforeReduceFixed plugin named " + xclassname + " for enumeration: " + dllfn);
                        }
                        IBeforeReduceFixed plugin = LoadBeforeReducePlugin(dllfn, classname);
                        benum = new FixedArrayComboListEnumerator(this, plugin);
                    }
                    break;

                case 'e': // Batch 'get next' enumeration.
                    {
                        try
                        {
                            if (null == benum)
                            {
                                benum = new FixedArrayComboListEnumerator(this, new FixedEntryEnumerator());
                            }

                            benum.Go();
                        }
                        catch
                        {
                            nstm.WriteByte((byte)'-'); //...
                            throw;
                        }
                    }
                    break;

                case 's':
                    try
                    {
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

                        foreach (ZBlock zb in zblocks)
                        {
                            zb.Sort(kentries, ref ebytes);
                        }
                        kentries = new List<ZBlock.KeyBlockEntry>(1); // Non-null, but release larger kentries.

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
                        uint gbfree = (uint)(GetCurrentDiskFreeBytes() / 1073741824);
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
                                y += TimedAddKVBuf(buf, y);
                            }
                        }
                        else
                        {
                            if (!nofreedisklog)
                            {
                                nofreedisklog = true;
                                XLog.errorlog("Low free disk space; now dropping keys/values.");
                            }
                        }
                    }
                    break;

                default:
                    base.ProcessCommand(nstm, tag);
                    break;
            }
        }

        static bool nofreedisklog = false;


        public override void ProcessCommands(NetworkStream nstm)
        {
            try
            {
                base.ProcessCommands(nstm);

                CloseZBlocks();
            }
            catch (Exception e)
            {
                SetError("ArrayComboList Sub Process: " + e.ToString());
                throw;
            }
        }


        int _kcmp(ZBlock.KeyBlockEntry x, ZBlock.KeyBlockEntry y)
        {
            for (int i = 0; i != this.keylen; i++)
            {
                int diff = (int)x.key[i] - (int)y.key[i];
                //Console.WriteLine("Comparing {0} with {1} = {2}", (int)kbuf1[k1 + i], (int)kbuf2[k2 + i], diff);
                if (0 != diff)
                {
                    return diff;
                }
            }
            return 0;
        }


        protected internal IBeforeLoad LoadBeforeLoadPlugin(string dllfilepath, string classname)
        {
            try
            {
                MySpace.DataMining.DistributedObjects.IBeforeLoad plugin = null;
                System.Reflection.Assembly assembly = System.Reflection.Assembly.LoadFrom(dllfilepath);
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface("MySpace.DataMining.DistributedObjects.IBeforeLoad") != null)
                            {
                                plugin = (MySpace.DataMining.DistributedObjects.IBeforeLoad)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new Exception("Class " + classname + " was found, but does not implement interface IBeforeLoad");
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new Exception("Plugin from '" + dllfilepath + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.ToString();
                }
                throw new Exception("ReflectionTypeLoadException error(s) with plugin '" + dllfilepath + "': " + x + "  [Note: ensure DLL was linked against distributed IBeforeLoad (IMapReduce.dll), not single-machine]");
            }
        }


        protected internal IBeforeReduceFixed LoadBeforeReducePlugin(string dllfilepath, string classname)
        {
            try
            {
                MySpace.DataMining.DistributedObjects.IBeforeReduceFixed plugin = null;
                System.Reflection.Assembly assembly = System.Reflection.Assembly.LoadFrom(dllfilepath);
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface("MySpace.DataMining.DistributedObjects.IBeforeReduceFixed") != null)
                            {
                                plugin = (MySpace.DataMining.DistributedObjects.IBeforeReduceFixed)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new Exception("Class " + classname + " was found, but does not implement interface IBeforeReduceFixed");
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new Exception("Plugin from '" + dllfilepath + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.ToString();
                }
                throw new Exception("ReflectionTypeLoadException error(s) with plugin '" + dllfilepath + "': " + x + "  [Note: ensure DLL was linked against distributed IBeforeReduceFixed (IMapReduce.dll), not single-machine]");
            }
        }


        internal EntriesInput GetEntriesInput()
        {
            EntriesInput input = new EntriesInput();
            input.entries = this.entries;
            input.KeyLength = this.keylen;
            input.net = this.net;
            return input;
        }


        ulong GetCurrentDiskFreeBytes()
        {
            ulong fba, tnb, tnfb;
            if (!GetDiskFreeSpaceEx(null, out fba, out tnb, out tnfb))
            {
                return 0;
            }
            return fba;
        }


#if ENABLE_TIMING
        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);
#endif

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern bool GetDiskFreeSpaceEx(
            string DirectoryName,
            out ulong FreeBytesAvailable,
            out ulong TotalNumberOfBytes,
            out ulong TotalNumberOfFreeBytes);

    }


    public class FixedArrayComboListEnumerator
    {
        int curzblock = -1;


        internal FixedArrayComboListEnumerator(FixedArrayComboListPart acl, IBeforeReduceFixed plugin)
        {
            this.acl = acl;
            this.input = acl.GetEntriesInput();
            this.output = acl.CreatePluginOutput();
            this.plugin = plugin;
        }


        bool LoadNextZBlock()
        {
            if (curzblock + 1 >= acl.zblocks.Length)
            {
                input.entries.Clear();
                input.net.Clear();
                return false;
            }
            curzblock++;

            acl.zblocks[curzblock].CopyInto(input.net, input.entries, ref acl.ebytes, ref acl.evaluesbuf);
            return true;
        }

        bool EnsureStarted()
        {
            if (curzblock < 0)
            {
                return LoadNextZBlock();
            }
            return true;
        }


        internal void Go()
        {
            if (EnsureStarted())
            {
                for (; ; )
                {
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


        bool finished = false;
        private FixedArrayComboListPart acl;
        private EntriesInput input;
        private FixedEntriesOutput output;
        private IBeforeReduceFixed plugin;
    }

}
