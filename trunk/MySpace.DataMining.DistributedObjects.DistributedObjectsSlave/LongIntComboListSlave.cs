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


namespace MySpace.DataMining.DistributedObjects5
{
    public class LongIntComboListPart : DistObjectBase
    {
        public class ZBlock
        {
            const int ZFILE_MAX_BYTES = 1073741824; // Actual limit when writing to zblock files.

            LongIntComboListPart parent;
            int zblockID; // ZBlock ID (0-based n)

            int blockbuflen;
            System.IO.FileStream fzblock;
            string fzblockfilename;
            long zblocksize = 0;


            public void Close()
            {
                if (null != fzblock)
                {
                    fzblock.Close();
                    fzblock = null;
                }

                if (null != fzblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzblockfilename);
                        fzblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            private void _justclose()
            {
                if (null != fzblock)
                {
                    fzblock.Close();
                    fzblock.Dispose();
                    fzblock = null;
                }
            }


            public void LeaveAddMode(int readbuflen)
            {
                blockbuflen = readbuflen;
                _justclose();
            }


            private static string _spid = null;

            public static string CreateFileName(int zblockID, string otherinfo)
            {
                if (null == _spid)
                {
                    _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
                }

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


            internal void ensurefzblock(bool create, int buflen)
            {
                if (null == fzblock)
                {
                    System.IO.FileAccess access = System.IO.FileAccess.Read;
                    System.IO.FileMode mode = System.IO.FileMode.Open;
                    if (create)
                    {
                        access = System.IO.FileAccess.ReadWrite;
                        mode = System.IO.FileMode.Create;
                    }
                    fzblock = new System.IO.FileStream(fzblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                }
            }


            internal ZBlock(LongIntComboListPart parent, int zblockID, int addbuflen)
            {
                this.parent = parent;
                this.zblockID = zblockID;

                this.blockbuflen = addbuflen;

                fzblockfilename = CreateFileName(zblockID, "unsorted");
                ensurefzblock(true, blockbuflen);
            }


            public void Flush()
            {
                if (null != fzblock)
                {
                    fzblock.Flush();
                }
            }


            public bool Add(byte[] buf, int offset)
            {
                if (zblocksize + 12 > ZFILE_MAX_BYTES)
                {
                    return false;
                }

                fzblock.Write(buf, offset, 12);

                zblocksize += 12;
#if oldDEBUG
                if (zblocksize != fzblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zblocksize mismatch");
                }
#endif

                return true;
            }


            //static bool isfirst = false;

            // buf must be at least 12 bytes.
            //--F-------------ADDCOOK------------------
            public void CopyInto(List<LongIntComboListPart.B12> cbuffer, byte[] buf)
            {
                ensurefzblock(false, blockbuflen);

                try
                {
                    int flen = (int)fzblock.Length;
                    int b12count = flen / 12;

                    cbuffer.Clear();
                    int newcap = b12count;
                    /*if (isfirst)
                    {
                        isfirst = false;
                        newcap *= 2;
                    }*/
                    if (newcap > cbuffer.Capacity)
                    {
                        cbuffer.Capacity = newcap;
                    }

                    // Read from existing unsorted zblock file into buffer.
                    fzblock.Seek(0, System.IO.SeekOrigin.Begin);
                    LongIntComboListPart.B12 b12;
                    b12.A = 0;
                    b12.B = 0;
                    for (int i = 0; i != b12count; i++)
                    {
                        int szr = fzblock.Read(buf, 0, 12);
#if DEBUG
                        if (12 != szr)
                        {
                            throw new Exception("Unable to read");
                        }
#endif
                        b12.SetFromArray(buf);
                        cbuffer.Add(b12);
                    }
                }
                catch (Exception e)
                {
                    cbuffer.Clear();
                    XLog.errorlog("LongIntComboList CopyInto failure; zblock skipped: " + e.ToString());
                }

                _justclose();
            }


            // buf must be at least 12 bytes.
            public void Sort(List<LongIntComboListPart.B12> sortbuffer, byte[] buf)
            {
                ensurefzblock(false, blockbuflen);

                try
                {
                    try
                    {
                        CopyInto(sortbuffer, buf);
                    }
                    finally
                    {
                        // Delete old (unsorted) file; prepare new (sorted) one.
                        // Keep these together so that there's always one on file; so cleanup sees it and continues.
                        _justclose();
                        System.IO.File.Delete(fzblockfilename);
                        fzblockfilename = CreateFileName(zblockID, "sorted");
                        ensurefzblock(true, blockbuflen);
                    }

                    // Sort the sortbuffer.
                    sortbuffer.Sort();

                    // From (sorted) sortbuffer write into new sorted zblock file.
                    int count = sortbuffer.Count;
                    for (int i = 0; i != count; i++)
                    {
                        sortbuffer[i].CopyToArray(buf);
                        fzblock.Write(buf, 0, 12);
                    }
                }
                finally
                {
                    _justclose();
                }
            }
        }


        LongIntComboListPartEnumerator[] enums;
        ZBlock[] zblocks;
        List<B12> b12buffer;


        public static LongIntComboListPart Create(int capacity)
        {
            return new LongIntComboListPart(capacity);
        }


        private LongIntComboListPart(int capacity)
        {
            ZBlock.CleanZBlockFiles("unsorted", "sorted");

            b12buffer = new List<B12>(0);

            this.enums = new LongIntComboListPartEnumerator[32];

            // Pre-set with defaults.
            int numzblocks = 139;
            int zblockbufsize = 131072; // 128 KB default.

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
                        }
                    }
                    {
                        System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addbuffersize"];
                        if (null != xzbs)
                        {
                            zblockbufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                        }
                    }
                }
            }

            if (XLog.logging)
            {
                XLog.log("Creating " + numzblocks.ToString() + " ZBlock`s");
            }

            zblocks = new ZBlock[numzblocks];
            for (int i = 0; i != numzblocks; i++)
            {
                zblocks[i] = new ZBlock(this, i, zblockbufsize);
            }
        }


        public override byte[] GetValue(byte[] key, out int valuelength)
        {
            valuelength = -1;
            return null;
        }


        public static long ToModLong(long a, int b)
        {
            return Math.Abs(a);
        }


        public void TimedAdd(byte[] buf, int offset)
        {
#if ENABLE_TIMING
            long start = 0;
            if (XLog.logging)
            {
                QueryPerformanceCounter(out start);
            }
#endif

            //int zbid = BytesToInt(buf, offset) % zblocks.Length;
            int zbid = (int)(ToModLong(BytesToLong(buf, offset), BytesToInt(buf, offset + 8)) % zblocks.Length);

            zblocks[zbid].Add(buf, offset);

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
                        XLog.log("IntComboListPart add seconds: " + secs.ToString());
                    }
                }
            }
#endif
        }


        // Warning: key needs to be immutable!
        public override void CopyAndSetValue(byte[] key, byte[] value, int valuelength)
        {
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


        public override void ProcessCommands(NetworkStream nstm)
        {
            try
            {
                base.ProcessCommands(nstm);

                CloseZBlocks();
            }
            catch (Exception e)
            {
                SetError("LongIntComboList Sub Process: " + e.ToString());
                throw;
            }
        }


        protected override void ProcessCommand(NetworkStream nstm, char tag)
        {
            //string s;
            int len;

            switch (tag)
            {
                case 'e': // Batch 'get next' enumeration.
                    {
                        try
                        {
                            int ienumid = nstm.ReadByte();
                            if (ienumid >= 0)
                            {
                                byte enumid = (byte)ienumid;
                                if (enumid >= this.enums.Length)
                                {
                                    nstm.WriteByte((byte)'-');
                                }
                                else
                                {
                                    if (null == this.enums[enumid])
                                    {
                                        this.enums[enumid] = new LongIntComboListPartEnumerator(this);
                                    }
                                    int offset = 0;
                                    if (null == buf || buf.Length < 60)
                                    {
                                        throw new Exception("Enumeration batch buffer too small!");
                                    }
                                    //if (uniquecompression) // Compressed...
                                    {
                                        for (; ; )
                                        {
                                            if (!this.enums[enumid].MoveNext())
                                            {
                                                break;
                                            }
                                            B12 b12 = this.enums[enumid].Current;
                                            // Using Big Endian!
                                            b12.CopyToArray(buf, offset);
                                            offset += 12;
                                            if (offset + 12 > buf.Length)
                                            {
                                                break;
                                            }
                                        }
                                    }
                                    if (offset > 0)
                                    {
                                        nstm.WriteByte((byte)'+');
                                        XContent.SendXContent(nstm, buf, offset);
                                    }
                                    else
                                    {
                                        nstm.WriteByte((byte)'-');
                                    }
                                }
                            }
                        }
                        catch
                        {
                            nstm.WriteByte((byte)'-');
                            throw;
                        }
                    }
                    break;

                case 'n': // Reset next in enumeration..
                    {
                        int ienumid = nstm.ReadByte();
                        if (ienumid >= 0)
                        {
                            byte enumid = (byte)ienumid;
                            if (XLog.logging)
                            {
                                XLog.log("Starting enumeration (enumid:" + enumid.ToString() + ")");
                            }
                            if (enumid < this.enums.Length
                                && null != this.enums[enumid])
                            {
                                //this.enums[enumid].Reset();
                                this.enums[enumid] = null;
                            }
                        }
                    }
                    break;

                case 's':
                    {
                        try
                        {
#if ENABLE_TIMING
                        long start = 0;
                        if(XLog.logging)
                        {
                            QueryPerformanceCounter(out start);
                        }
#endif

                            int readbuflen = 1048576;
                            if (null != DistributedObjectsSlave.xslave)
                            {
                                System.Xml.XmlNode xzblocks = DistributedObjectsSlave.xslave["zblocks"];
                                if (null != xzblocks)
                                {
                                    {
                                        System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readbuffersize"];
                                        if (null != xzbs)
                                        {
                                            readbuflen = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                        }
                                    }
                                }
                            }

                            foreach (ZBlock zb in zblocks)
                            {
                                zb.LeaveAddMode(readbuflen);
                            }

                            foreach (ZBlock zb in zblocks)
                            {
                                zb.Sort(b12buffer, this.buf);
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
                                    XLog.log("IntComboListPart sort seconds: " + secs.ToString());
                                }
                            }
                        }
#endif
                        }
                        finally
                        {
                            nstm.WriteByte((byte)'+');
                        }
                    }
                    break;

                case 'p': // Batch push/publish...
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        uint gbfree = (uint)(GetCurrentDiskFreeBytes() / 1073741824);
#if DEBUG
                        {
                            string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
									 if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER" || computer_name == "computer_name")
                            {
                                gbfree = uint.MaxValue;
                            }
                        }
#endif
                        if (gbfree > 20)
                        {
                            int pcount = len / 12; // size of B12
                            int y = 0;
                            for (int i = 0; i != pcount; i++)
                            {
                                TimedAdd(buf, y);
                                y += 12;
                            }
                        }
                        else
                        {
                            if (!nofreedisklog)
                            {
                                nofreedisklog = true;
                                XLog.errorlog("Low free disk space; now dropping entries.");
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


        public struct B12 : IComparable
        {
            public Int64 A;
            public Int32 B;


            public int CompareTo(object obj)
            {
                B12 that = (B12)obj;
                {
                    long diff = this.A - that.A;
                    if (diff > 0) return 1; else if (diff < 0) return -1;
                }
                {
                    int diff = this.B - that.B; if (0 != diff) return diff;
                }
                return 0;
            }


            public void CopyToArray(byte[] buf, int offset)
            {
                DistObjectBase.LongToBytes(A, buf, offset + 0);
                DistObjectBase.ToBytes(B, buf, offset + 8);
            }

            public void CopyToArray(byte[] buf)
            {
                CopyToArray(buf, 0);
            }


            public void SetFromArray(byte[] buf, int offset)
            {
                A = DistObjectBase.BytesToLong(buf, offset + 0);
                B = DistObjectBase.BytesToInt(buf, offset + 8);
            }

            public void SetFromArray(byte[] buf)
            {
                SetFromArray(buf, 0);
            }
        }


        public class LongIntComboListPartEnumerator
        {
            System.Collections.Generic.IEnumerator<B12> en = null;
            int nextzblock = 0;
            LongIntComboListPart icl;
            byte[] buf;
            bool back = false;


            public LongIntComboListPartEnumerator(LongIntComboListPart icl)
            {
                this.icl = icl;
                this.buf = new byte[12];
            }


            public B12 Current
            {
                get
                {
                    return this.en.Current;
                }
            }


            void killprevzblock()
            {
                if (nextzblock > 0)
                {
                    icl.zblocks[nextzblock - 1].Close();
                }
            }


            internal void MoveBack()
            {
                if (back)
                {
                    throw new Exception("Back error");
                }
                back = true;
            }


            public bool MoveNext()
            {
                if (back)
                {
                    back = false;
                    return true;
                }

                if (null != this.en && this.en.MoveNext())
                {
                    return true;
                }

                for (; ; )
                {
                    if (nextzblock >= icl.zblocks.Length)
                    {
                        break;
                    }

                    killprevzblock();

                    icl.zblocks[nextzblock].CopyInto(icl.b12buffer, buf);
                    this.en = icl.b12buffer.GetEnumerator();
                    nextzblock++;
                    if (this.en.MoveNext())
                    {
                        return true;
                    }
                }
                killprevzblock();
                return false;
            }
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

}
