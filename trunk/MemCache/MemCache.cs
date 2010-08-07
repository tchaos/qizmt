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

namespace MySpace.DataMining.DistributedObjects
{

    public class MemCache : IEnumerator<MemCache.Tuple>, IEnumerable<MemCache.Tuple>
    {
        string dfsname;
        System.Threading.Mutex mutex;
        int rowlen;
        int keyoffset;
        int keylen;
        int blocksize;
        int[] coloffsets;
        SM sminfoblock;
        const int infoblocksize = 0x400 * 0x400 * 10;
        const int infoblocklocaloffset = 0x400 * 4;
        const int infoblockworkerbytesoffset = 2;
        const int infoblockworkerbytes = (0x400 * 3) - infoblockworkerbytesoffset;
        SegmentCollection segs;
        int highestworkerid;
        int workerID;
        bool localworkerlocked;
        int maxrowsperseg;
        int maxseglen;
        bool _readonly;
        bool eof = false;
        bool trunc = false;
        bool finished = false;

        int curviewindex;
        int curoffsetinview;

        int curupdateoffset;
        Segment curupdateseg;

        int readupdateoffset;
        bool needcopycurrent;

        const int ONEMB = 0x400 * 0x400;
        const int ONEGB = ONEMB * 0x400;
        const long ONETB = (long)ONEGB * 0x400;

        List<Segment> updating; // segs has the current ones.
        Queue<Segment> reserves;


        public string Name
        {
            get { return dfsname; }
        }

        public int RowLength
        {
            get { return rowlen; }
        }

        public int KeyOffset
        {
            get { return keyoffset; }
        }

        public int KeyLength
        {
            get { return keylen; }
        }


        public void GetFieldInfo(int index, out int foffset, out int fsize)
        {
            foffset = coloffsets[index];
            if (index + 1 < coloffsets.Length)
            {
                fsize = coloffsets[index + 1] - coloffsets[index];
            }
            else
            {
                fsize = rowlen - foffset;
            }
        }


        public int WorkerID
        {
            get { return workerID; }
        }


        public bool EOF
        {
            get { return eof; }
        }


        public void Reset()
        {
            unsafe
            {
                if (null != updating)
                {
                    throw new InvalidOperationException("Must first finish updating");
                }
                needcopycurrent = false;
                curviewindex = -1;
                curoffsetinview = ONEGB;
                curupdateoffset = ONEGB;
                curupdateseg = default(Segment);
                eof = false;
                trunc = false;
                finished = false;
            }
        }


        public void Truncate()
        {
            /*
            {
                if (needcopycurrent)
                {
                    this.DeleteRow();
                }
                while (this.MoveNext())
                {
                    this.DeleteRow();
                }
            }
             * */

            trunc = true;
            eof = true;
            needcopycurrent = false;
        }


        int NextUpdateRow()
        {
            unsafe
            {
                if (-1 == curviewindex)
                {
                    throw new InvalidOperationException("Row not ready for update (need MoveFirst or MoveNext)");
                }
                curupdateoffset += rowlen;
                if (curupdateoffset <= blocksize)
                {
                    return curupdateoffset - rowlen;
                }
                else
                {
                    Segment newseg = NewSegment(0);
                    System.Diagnostics.Debug.Assert(newseg.seglen == 0);

                    curupdateseg = newseg;

                    segs.Add(newseg);

                    curupdateoffset = rowlen;
                    return 0;
                }
            }
        }

        public bool MoveFirst()
        {
            Reset();
            return MoveNext();
        }

        unsafe void _copycurrent()
        {
            if (!needcopycurrent)
            {
                throw new Exception("DEBUG:  _copycurrent: (!needcopycurrent)");
            }
            needcopycurrent = false;
            int coffset = curoffsetinview - rowlen;
            int upoffset = NextUpdateRow();
            for (int i = 0; i < rowlen; i++)
            {
                curupdateseg.sm.pview[upoffset + i] = updating[curviewindex].sm.pview[coffset + i];
            }
            curupdateseg.seglen += rowlen;
            readupdateoffset = upoffset;
        }


        public bool MoveNext()
        {
            if (_readonly)
            {
                throw new Exception("Cannot MoveNext() on MemCache '" + this.Name
                    + "' because it was attached as read-only");
            }
            unsafe
            {
                if (EOF)
                {
                    return false;
                }
                if (needcopycurrent)
                {
                    _copycurrent();
                }
                for (; ; )
                {
                    curoffsetinview += rowlen;
                    if(null != updating && curoffsetinview <= updating[curviewindex].Length)
                    {
                        // Same view.
                        needcopycurrent = true;
                        return true;
                    }
                    else
                    {
                        // Move to next view.
                        if (-1 != curviewindex)
                        {
                            if (reserves.Count < 2)
                            {
                                reserves.Enqueue(updating[curviewindex]);
                            }
                            else
                            {
                                updating[curviewindex].Dispose();
                            }
                            updating[curviewindex] = null;
                            curviewindex++;
                        }
                        else
                        {
                            // First time.
                            _ensureworkerlocked();
                            updating = segs._StartUpdatingAll();
                            curviewindex = 0;
                        }
                        curoffsetinview = 0;
                        if (curviewindex >= updating.Count)
                        {
                            // At the end.
                            needcopycurrent = false;
                            updating = null;
                            eof = true;
                            return false;
                        }
                        continue;
                    }
                }
            }
        }


        public void Finish()
        {
            if (_readonly)
            {
                return;
            }
            if (!finished)
            {
                finished = true;
                if (!trunc)
                {
                    if (updating != null)
                    {
                        while (this.MoveNext())
                        {
                        }
                    }
                }
                if (updating != null && curviewindex < updating.Count)
                {
                    for (int iu = curviewindex; iu < updating.Count; iu++)
                    {
                        updating[iu].Dispose();
                        updating[iu] = null;
                    }
                    updating = null;
                }
                needcopycurrent = false;
                curupdateseg = default(Segment);
                segs._DoneUpdatingAll();
#if DEBUG
                if (curupdateseg != null
                    && curupdateseg.sm.hmap != IntPtr.Zero)
                {
                    throw new Exception("DEBUG:  MemCache.Finish: (curupdateseg.sm.hmap != IntPtr.Zero)");
                }
                if (needcopycurrent)
                {
                    throw new Exception("DEBUG:  MemCache.Finish: (needcopycurrent)");
                }
#endif
                while (reserves.Count > 0)
                {
                    Segment rseg = reserves.Dequeue();
                    rseg.Dispose();
                }
                eof = true;
                _ensureworkerunlocked();
            }
        }


        public Tuple ReadRow()
        {
            unsafe
            {
                if (needcopycurrent)
                {
                    _copycurrent();
                }
                if (null == curupdateseg)
                {
                    throw new InvalidOperationException("Row not ready (need MoveFirst or MoveNext)");
                }
                // Uses readoffset so it returns a consistent row,
                // even if inserts happen.
                return Tuple.Prepare(this, curupdateseg.sm.pview + readupdateoffset);
            }
        }

        public Tuple PeekRow()
        {
            unsafe
            {
                if (null == updating)
                {
                    throw new InvalidOperationException("Row not ready (need MoveFirst or MoveNext)");
                }
                int coffset = curoffsetinview - rowlen;
                return Tuple.Prepare(this, updating[curviewindex].sm.pview + coffset);
            }
        }

        // Deletes the current row, not an inserted row.
        public void DeleteRow()
        {
            if (null == updating)
            {
                throw new InvalidOperationException("Row not ready (need MoveFirst or MoveNext)");
            }
            if (!needcopycurrent)
            {
                throw new InvalidOperationException("Cannot delete a read or deleted row");
            }
            needcopycurrent = false;
        }

        public Tuple InsertRow()
        {
            unsafe
            {
                int upoffset = NextUpdateRow();
                curupdateseg.seglen += rowlen;
                return Tuple.Prepare(this, curupdateseg.sm.pview + upoffset);
            }
        }


        public Tuple this[int RowIndex]
        {
            get
            {
                unsafe
                {
                    if (RowIndex < 0)
                    {
                        throw new IndexOutOfRangeException("MemCache[" + RowIndex + "] is out of bounds");
                    }
                    int segindex = RowIndex / maxrowsperseg;
                    int segsubindex = (RowIndex % maxrowsperseg) * rowlen;
                    if (segindex > segs.Count
                        || segsubindex + rowlen > segs[segindex].seglen)
                    {
                        throw new IndexOutOfRangeException("MemCache[" + RowIndex + "] is out of bounds");
                    }
                    return Tuple.Prepare(this, segs[segindex].sm.pview + segsubindex);
                }
            }
        }


        public int NumberOfRows
        {
            get
            {
                if (segs.Count <= 1)
                {
                    if (segs.Count == 0)
                    {
                        return 0;
                    }
                    return segs[0].Length / rowlen;
                }
                int minusone = segs.Count - 1;
                return (minusone * maxrowsperseg) + (segs[minusone].Length / rowlen);
            }
        }


        public struct Tuple
        {
            MemCache mc;
            unsafe byte* p;

            internal unsafe static Tuple Prepare(MemCache mc, byte* p)
            {
                Tuple t;
                t.mc = mc;
                t.p = p;
                return t;
            }

            public struct Field
            {
                MemCache mc;
                unsafe byte* p;
                int fieldsize;
              

                internal unsafe static Field Prepare(MemCache mc, byte* p, int fieldsize)
                {
                    Field f;
                    f.mc = mc;
                    f.p = p;
                    f.fieldsize = fieldsize;
                    return f;
                }

                public string GetString()
                {
                    unsafe
                    {
                        
                        return BytesToString(p, fieldsize);
                        
                    }
                }

                public mstring GetMString()
                {
                    unsafe
                    {

                        return BytesToMString(p, fieldsize);

                    }
                }
                public int GetInt()
                {
                    unsafe
                    {
                        if (fieldsize < 4)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        return BytesToInt(p);
                    }
                }
                public void SetString(string value)
                {
                    unsafe
                    {

                        StringToBytes(value,p,fieldsize);
                    }
                                        
                }
                public void SetMString(mstring value)
                {
                    unsafe
                    {
                                          
                        MStringToBytes(value, p, fieldsize);
                    }

                }
                public long GetLong()
                {
                    unsafe
                    {
                        if (fieldsize < 8)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        return BytesToLong(p);
                    }
                }

                public byte GetByte()
                {
                    unsafe
                    {
                        if (fieldsize < 1)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        return *p;
                    }
                }

                public double GetDouble()
                {
                    unsafe
                    {
                        if (fieldsize < 9)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        return BytesToDouble(p);
                    }
                }

                
                public void SetInt(int value)
                {
                    unsafe
                    {
                        if (fieldsize < 4)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        ToBytes(value, p);
                    }
                }

                public void SetLong(long value)
                {
                    unsafe
                    {
                        if (fieldsize < 8)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        LongToBytes(value, p);
                    }
                }

                public void SetByte(byte value)
                {
                    unsafe
                    {
                        if (fieldsize < 1)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        *p = value;
                    }
                }

                public void SetDouble(double value)
                {
                    unsafe
                    {
                        if (fieldsize < 9)
                        {
                            string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
                        }
                        DoubleToBytes(value, p);
                    }
                }


                public TupleBytes Bytes
                {
                    get
                    {
                        unsafe
                        {
                            return TupleBytes.Prepare(this.p, this.fieldsize);
                        }
                    }
                }

            }

            public Field this[int index]
            {
                get
                {
                    unsafe
                    {
                        int foffset, fsize;
                        mc.GetFieldInfo(index, out foffset, out fsize);
                        return Field.Prepare(mc, p + foffset, fsize);
                    }
                }
            }

            public TupleBytes Bytes
            {
                get
                {
                    unsafe
                    {
                        return TupleBytes.Prepare(this.p, this.mc.rowlen);
                    }
                }
            }


            public struct TupleBytes
            {
                unsafe byte* p;
                int len;

                unsafe static internal TupleBytes Prepare(byte* p, int len)
                {
                    TupleBytes tb;
                    tb.p = p;
                    tb.len = len;
                    return tb;
                }

                public int Length
                {
                    get
                    {
                        return len;
                    }
                }

                public byte this[int index]
                {
                    get
                    {
                        if (index < 0 || index > len)
                        {
                            throw new IndexOutOfRangeException("Index into Tuple Bytes out of range (index = "
                                + index + ") (length = " + len + ")");
                        }
                        unsafe
                        {
                            return p[index];
                        }
                    }

                    set
                    {
                        if (index < 0 || index > len)
                        {
                            throw new IndexOutOfRangeException("Index into Tuple Bytes out of range (index = "
                                + index + ") (length = " + len + ")");
                        }
                        unsafe
                        {
                            p[index] = value;
                        }
                    }
                }

            }

        }


        void _lockworker(bool yes)
        {
            mutex.WaitOne(); // -->
            try
            {
                unsafe
                {
                    if (sminfoblock.pview[0] == 99
                        && sminfoblock.pview[1] == 243)
                    {
                        throw new Exception("This MemCache '" + this.Name + "' has been released but is still in use");
                    }
                    bool alreadylocked;
                    bool lockmode = sminfoblock.pview[0] == 173
                        && sminfoblock.pview[1] == 152;
                    if (lockmode)
                    {
                        byte b = (byte)(sminfoblock.pview[infoblockworkerbytesoffset + (workerID / 8)]
                            & (0x80 >> (workerID % 8)));
                        alreadylocked = 0 != b;
                    }
                    else
                    {
                        alreadylocked = false;
                    }
                    if (yes == alreadylocked)
                    {
                        if (yes)
                        {
                            throw new Exception("It is possible that workers from different jobs are"
                                + " attempting to access this MemCache at the same time,"
                                + " or that a job was killed and this MemCache needs to be reloaded",
                                new Exception("This worker already has exclusive access to this range of MemCache data"
                                    + " (MemCache named " + this.Name + ")"));
                        }
                        else
                        {
                            throw new Exception("DEBUG: attempted to unlock worker when worker was already unlocked"
                                + " (lockmode=" + (lockmode ? "true" : "false") + ")"
                                + " (MemCache named " + this.Name + ")");
                        }
                    }
                    if (!lockmode)
                    {
                        sminfoblock.pview[0] = 173;
                        sminfoblock.pview[1] = 152;
                    }
                    {
                        if (yes)
                        {
                            sminfoblock.pview[infoblockworkerbytesoffset + (workerID / 8)]
                                |= (byte)(0x80 >> (workerID % 8));
                        }
                        else
                        {
                            sminfoblock.pview[infoblockworkerbytesoffset + (workerID / 8)]
                                &= (byte)~(0x80 >> (workerID % 8));
                        }
                    }
                    localworkerlocked = yes;
                }
            }
            finally
            {
                mutex.ReleaseMutex(); // <--
            }
        }

        void _ensureworkerlocked()
        {
            if (!localworkerlocked)
            {
                _lockworker(true);
            }
        }

        void _ensureworkerunlocked()
        {
            if (localworkerlocked)
            {
                _lockworker(false);
            }
        }


        public static MemCache Attach(string dfsname, int workerID, bool ReadOnly)
        {
            if (workerID >= infoblockworkerbytes * 8)
            {
                throw new IndexOutOfRangeException("Invalid WorkerID: " + workerID);
            }
            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfsname = dfsname.Substring(6);
            }
            MemCache mc = new MemCache();
            mc.dfsname = dfsname;
            mc.workerID = workerID;
            mc._readonly = ReadOnly;
            mc.localworkerlocked = false;
            mc.mutex = new System.Threading.Mutex(false, "MemCache_" + dfsname + "_lock");
            mc._Attaching();
            return mc;
        }

        public static MemCache Attach(string dfsname, int workerID)
        {
            return Attach(dfsname, workerID, false);
        }

        unsafe void _Attaching()
        {
            curviewindex = -1;
            curoffsetinview = ONEGB;
            curupdateoffset = ONEGB;
            needcopycurrent = false;
            reserves = new Queue<Segment>(2);

            IList<string> slaveseginfos = null;
            mutex.WaitOne(); // -->
            try
            {
                {
                    bool existed;
                    string infoblockname = "MemCache_" + dfsname + "_info";
                    sminfoblock = Segment.GetSM(infoblockname, infoblocksize, out existed); // Always infoblocksize.
                    if (existed)
                    {
                        if (sminfoblock.pview[0] == 99
                            && sminfoblock.pview[1] == 243)
                        {
                            throw new Exception("This MemCache '" + this.Name + "' has been released but is still in use");
                        }
                    }
                    else
                    {
                        for (int i = 0; i < infoblocklocaloffset; i++)
                        {
                            sminfoblock.pview[i] = 0;
                        }
                        byte[] buf = new byte[0x400 * 8];
                        string MetaFileName = "mcm." + SafeTextPath(this.Name) + ".mcm";
                        byte* pvp = sminfoblock.pview + infoblocklocaloffset;
                        byte* pvpEnd = sminfoblock.pview + infoblocksize - 1;
                        using (System.IO.FileStream fs = new System.IO.FileStream(MetaFileName,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                        {
                            for (; ; )
                            {
                                int read = fs.Read(buf, 0, buf.Length);
                                if (read <= 0)
                                {
                                    break;
                                }
                                for (int ij = 0; ij < read; ij++)
                                {
                                    if (pvp >= pvpEnd)
                                    {
                                        throw new Exception("MemCache metadata in file '" + MetaFileName + "' is too large"
                                            + " for metadata shared memory in MemCache named '" + this.Name + "'");
                                    }
                                    *pvp++ = buf[ij];
                                }
                            }
                        }
                        *pvp++ = 0;
                        Segment.pinsm.Pin(infoblockname);
                    }
                }
                int endattribs = -1;
                {
                    // Get attributes from info block.
                    string[] attribs;
                    {
                        int ipos = infoblocklocaloffset;
                        {
                            int numdashes = 0;
                            for (; ipos < infoblocksize; ipos++)
                            {
                                if (sminfoblock.pview[ipos] == '-')
                                {
                                    if (++numdashes >= 10)
                                    {
                                        endattribs = ipos;
                                        break;
                                    }
                                }
                                else if (sminfoblock.pview[ipos] == 0)
                                {
                                    break;
                                }
                                else
                                {
                                    numdashes = 0;
                                }
                            }
                            if (-1 == endattribs)
                            {
                                throw new Exception("Corrupt info block: 'MemCache_" + dfsname + "_info'; dash separator not found");
                            }
                            StringBuilder sbattribs = new StringBuilder(endattribs);
                            for (int i = 0; i < endattribs; i++)
                            {
                                sbattribs.Append((char)sminfoblock.pview[i]);
                            }
                            attribs = sbattribs.ToString().Split(new char[] { '\r', '\n' },
                                StringSplitOptions.RemoveEmptyEntries);
                        }
                    }
                    foreach (string attrib in attribs)
                    {
                        {
                            string fattrib = "blocksize=";
                            if (attrib.StartsWith(fattrib))
                            {
                                string value = attrib.Substring(fattrib.Length);
                                blocksize = int.Parse(value);
                                continue;
                            }
                        }
                        {
                            string fattrib = "rowlen=";
                            if (attrib.StartsWith(fattrib))
                            {
                                string value = attrib.Substring(fattrib.Length);
                                rowlen = int.Parse(value);
                                continue;
                            }
                        }
                        {
                            string fattrib = "keyoffset=";
                            if (attrib.StartsWith(fattrib))
                            {
                                string value = attrib.Substring(fattrib.Length);
                                keyoffset = int.Parse(value);
                                continue;
                            }
                        }
                        {
                            string fattrib = "keylen=";
                            if (attrib.StartsWith(fattrib))
                            {
                                string value = attrib.Substring(fattrib.Length);
                                keylen = int.Parse(value);
                                continue;
                            }
                        }
                        {
                            string fattrib = "coloffsets=";
                            if (attrib.StartsWith(fattrib))
                            {
                                string value = attrib.Substring(fattrib.Length);
                                string[] scoloffsets = value.Split(',');
                                coloffsets = new int[scoloffsets.Length];
                                int prevcolstart = -1;
                                for (int i = 0; i < scoloffsets.Length; i++)
                                {
                                    int colstart = int.Parse(scoloffsets[i]);
                                    if (colstart <= prevcolstart)
                                    {
                                        throw new Exception("init failure, coloffsets not increasing");
                                    }
                                    prevcolstart = colstart;
                                    coloffsets[i] = colstart;
                                }
                                continue;
                            }
                        }
                    }
                }
                maxrowsperseg = blocksize / rowlen;
                maxseglen = blocksize - (blocksize % rowlen);
                {
                    // Get info for this slave.
                    int ipos = endattribs;
                    for (; ; )
                    {
                        int slaveindex = -1;
                        for (; ipos < infoblocksize - 1024; ipos++)
                        {
                            if (sminfoblock.pview[ipos] == 0)
                            {
                                break;
                            }
                            if (sminfoblock.pview[ipos] == '#'
                                && sminfoblock.pview[ipos + 1] == '#')
                            {
                                ipos += 2;
                                StringBuilder sbslaveindex = new StringBuilder(8);
                                for (int i = 0; i < 20; i++, ipos++)
                                {
                                    if (sminfoblock.pview[ipos] == ':')
                                    {
                                        ipos++;
                                        break;
                                    }
                                    if (sminfoblock.pview[ipos] == 0)
                                    {
                                        break;
                                    }
                                    sbslaveindex.Append((char)sminfoblock.pview[ipos]);
                                }
                                slaveindex = int.Parse(sbslaveindex.ToString());
                                if (slaveindex > highestworkerid)
                                {
                                    highestworkerid = slaveindex;
                                }
                                if (slaveindex == this.workerID)
                                {
                                    break;
                                }
                            }
                        }
                        if (-1 == slaveindex)
                        {
                            break;
                        }
                        if (slaveindex == this.workerID)
                        {
                            StringBuilder sbslaveinfo = new StringBuilder(1024 * 4);
                            for (; ipos < infoblocksize - 1024; ipos++)
                            {
                                if (sminfoblock.pview[ipos] == '#'
                                    && sminfoblock.pview[ipos + 1] == '#')
                                {
                                    break;
                                }
                                if (sminfoblock.pview[ipos] == 0)
                                {
                                    break;
                                }
                                sbslaveinfo.Append((char)sminfoblock.pview[ipos]);
                            }
                            slaveseginfos = sbslaveinfo.ToString().Split(
                                new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            break;
                        }
                    }
                    // slaveseginfos can be null if no chunks for this slave.
                }
            }
            finally
            {
                mutex.ReleaseMutex(); // <--
            }
            {
                if (coloffsets == null)
                {
                    throw new Exception("init failure, coloffsets == null");
                }
                if (blocksize < 1024)
                {
                    throw new Exception("init failure, blocksize < 1024");
                }
                if (rowlen < 1)
                {
                    throw new Exception("init failure, rowlen");
                }
                if (keylen < 1 || keylen > rowlen)
                {
                    throw new Exception("init failure, keylen");
                }
                if (keyoffset < 0 || keyoffset > rowlen)
                {
                    throw new Exception("init failure, keyoffset");
                }
                if (coloffsets[coloffsets.Length - 1] >= rowlen)
                {
                    throw new Exception("init failure, coloffsets do not fit within keylen");
                }
            }
            if (null == slaveseginfos)
            {
                this.segs = new SegmentCollection(this, new List<Segment>());
            }
            else
            {
                byte[] buf = null;
                List<Segment> seglist = new List<Segment>(slaveseginfos.Count + 4);
                for (int i = 0; i < slaveseginfos.Count; i++)
                {
                    string[] slavesegparts = slaveseginfos[i].Split(' ');
                    string slavesegsmname = slavesegparts[0];
                    string slavesegzd = slavesegparts[1];
                    int seglen = maxseglen;
                    {
                        if (slavesegparts.Length > 2)
                        {
                            string sseglen = slavesegparts[2];
                            try
                            {
                                if (i != slaveseginfos.Count - 1)
                                {
                                    throw new Exception("Can only specify segment length for last segment (i=" + i + ")");
                                }
                                seglen = int.Parse(sseglen,
                                    System.Globalization.NumberStyles.HexNumber);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Problem with segment length (" + sseglen
                                    + ") in MemCache named '" + this.Name + "'", e);
                            }
                        }
                    }
                    // Don't add zero-length chunks!
                    if (seglen > 0)
                    {
                        bool segsmExisted;
                        SM segsm = Segment.GetSM(slavesegsmname, blocksize, out segsmExisted);
                        if (!segsmExisted)
                        {
                            byte* pvp = segsm.pview;
                            byte* pvpEnd = pvp + blocksize;
                            using (System.IO.FileStream fs = new System.IO.FileStream(slavesegzd,
                                System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                            {
                                if (null == buf)
                                {
                                    buf = new byte[0x400 * 8];
                                }
                                {
                                    // Skip header.
                                    if (4 != fs.Read(buf, 0, 4)
                                        || 4 != BytesToInt(buf, 0))
                                    {
                                        throw new Exception("DEBUG: DFS chunk '" + slavesegzd
                                            + "' header expected to be 4 bytes for MemCache '" + this.Name + "'");
                                    }
                                }
                                for (; ; )
                                {
                                    int read = fs.Read(buf, 0, buf.Length);
                                    if (read <= 0)
                                    {
                                        break;
                                    }
                                    for (int ij = 0; ij < read; ij++)
                                    {
                                        if (pvp >= pvpEnd)
                                        {
                                            throw new Exception("DFS chunk '" + slavesegzd + "' is too large"
                                                + " for segment size " + blocksize + " in MemCache named '" + this.Name + "'");
                                        }
                                        *pvp++ = buf[ij];
                                    }
                                }
                            }
                            Segment.pinsm.Pin(slavesegsmname);
                        }
                        Segment seg = new Segment(this, slavesegsmname, segsm);
                        seg.seglen = seglen;
                        seglist.Add(seg);
                    }
                    else
                    {
                        //emptychunkname = slavesegzd;
                    }
                }
                this.segs = new SegmentCollection(this, seglist);
            }
            

        }


        static IEnumerable<char> MakeChunkName(string mcname, string sslaveid)
        {
            // Make up a data node chunk name.
            foreach (char ch in "zd.mc~")
            {
                yield return ch;
            }
            foreach (char ch in sslaveid)
            {
                yield return ch;
            }
            yield return '~';
            for (int i = 0, j = 0; i < 10 && j < mcname.Length; j++)
            {
                if (mcname[j] < 128 && char.IsLetterOrDigit(mcname[j]))
                {
                    i++;
                    yield return mcname[j];
                }
            }
            yield return '.';
            foreach (char ch in Guid.NewGuid().ToString())
            {
                yield return ch;
            }
            foreach (char ch in ".zd")
            {
                yield return ch;
            }
        }

        public static IEnumerable<char> MakeChunkName(string mcname, int slaveid)
        {
            return MakeChunkName(mcname, slaveid.ToString());
        }


        internal unsafe void _UpdateMetadata()
        {
            if (_readonly)
            {
                throw new Exception("Cannot update metadata for MemCache '" + this.Name
                    + "' because it was attached as read-only");
            }
            mutex.WaitOne(); // -->
            try
            {
                byte[] mymarker;
                string sslaveid = this.workerID.ToString();
                {
                    mymarker = new byte[2 + sslaveid.Length + 1];
                    mymarker[0] = (byte)'#';
                    mymarker[1] = (byte)'#';
                    for (int i = 0; i < sslaveid.Length; i++)
                    {
                        mymarker[2 + i] = (byte)sslaveid[i];
                    }
                    mymarker[2 + sslaveid.Length] = (byte)':';
                }
                int idestpos = -1;
                int ipos = infoblocklocaloffset;
                for (; ipos < infoblocksize - mymarker.Length; ipos++)
                {
                    if (sminfoblock.pview[ipos] == 0)
                    {
                        break;
                    }
                    if (sminfoblock.pview[ipos] == '#'
                        && sminfoblock.pview[ipos + 1] == '#')
                    {
                        bool match = true;
                        for (int i = 2; i < mymarker.Length; i++)
                        {
                            if (mymarker[i] != sminfoblock.pview[ipos + i])
                            {
                                match = false;
                                break;
                            }
                        }
                        if (match)
                        {
                            idestpos = ipos;
                            ipos += mymarker.Length;
                            break;
                        }
                    }
                }
                // If idestpos is still -1 this worker didn't have any chunks,
                // so just add it to the end.
                if (-1 == idestpos)
                {
                    for (; ipos < infoblocksize - (0x400 * 4); ipos++)
                    {
                        if (sminfoblock.pview[ipos] == 0)
                        {
                            idestpos = ipos;
                            break;
                        }
                    }
                    if (-1 == idestpos)
                    {
                        throw new Exception("Not enough space in metadata storage; " + ipos + " bytes");
                    }
                }
                else
                {
                    // Look for next section and start copying everyting.
                    for (; ipos < infoblocksize - 2; ipos++)
                    {
                        if (sminfoblock.pview[ipos] == '#'
                            && sminfoblock.pview[ipos + 1] == '#')
                        {
                            break;
                        }
                        if (sminfoblock.pview[ipos] == 0)
                        {
                            // This was the last worker, so I'll just copy the null.
                            break;
                        }
                    }
                    for (; ipos < infoblocksize; ipos++)
                    {
                        if (sminfoblock.pview[ipos] == 0)
                        {
                            //sminfoblock.pview[idestpos] = 0;
                            break;
                        }
                        sminfoblock.pview[idestpos++] = sminfoblock.pview[ipos];
                    }
                }
                // Now write this worker's info!
                for (int i = 0; i < mymarker.Length; i++)
                {
                    sminfoblock.pview[idestpos++] = mymarker[i];
                }
                sminfoblock.pview[idestpos++] = (byte)'\r';
                sminfoblock.pview[idestpos++] = (byte)'\n';
                if (segs._segments.Count > 0)
                {
                    foreach (Segment seg in segs._segments)
                    {
                        foreach (char snc in seg.smname)
                        {
                            sminfoblock.pview[idestpos++] = (byte)snc;
                        }
                        {
                            sminfoblock.pview[idestpos++] = (byte)' ';
                            // Make up a data node chunk name.
                            foreach (char ch in MakeChunkName(this.Name, sslaveid))
                            {
                                sminfoblock.pview[idestpos++] = (byte)ch;
                            }
                        }
                        if (seg.IsLastSegment)
                        {
                            sminfoblock.pview[idestpos++] = (byte)' ';
                            string shexlen = string.Format("{0:x8}", seg.seglen);
                            for (int i = 0; i < shexlen.Length; i++)
                            {
                                sminfoblock.pview[idestpos++] = (byte)shexlen[i];
                            }
                        }
                        sminfoblock.pview[idestpos++] = (byte)'\r';
                        sminfoblock.pview[idestpos++] = (byte)'\n';
                    }
                }
                else
                {
                    if (!_readonly)
                    {
                        // There's no segments, but write a dummy one for bookkeeping.
                        foreach (char snc in "MemCache_" + dfsname + "_empty")
                        {
                            sminfoblock.pview[idestpos++] = (byte)snc;
                        }
                        {
                            sminfoblock.pview[idestpos++] = (byte)' ';
                            // Make up a data node chunk name.
                            foreach (char ch in MakeChunkName(this.Name, sslaveid))
                            {
                                sminfoblock.pview[idestpos++] = (byte)ch;
                            }
                        }
                        //if (IsLastSegment) // true
                        {
                            sminfoblock.pview[idestpos++] = (byte)' ';
                            string shexlen = string.Format("{0:x8}", 0); // Zero-length!
                            for (int i = 0; i < shexlen.Length; i++)
                            {
                                sminfoblock.pview[idestpos++] = (byte)shexlen[i];
                            }
                        }
                        sminfoblock.pview[idestpos++] = (byte)'\r';
                        sminfoblock.pview[idestpos++] = (byte)'\n';
                    }
                }
                sminfoblock.pview[idestpos] = 0; // End!

            }
            finally
            {
                mutex.ReleaseMutex(); // <--
            }
        }


        #region IEnumerator<Tuple> Members

        public MemCache.Tuple Current
        {
            get { return PeekRow(); }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            Finish();
            if (DisposeReleasesWorkerMemory)
            {
                if (sminfoblock.hmap != default(IntPtr))
                {
                    sminfoblock.Dispose();
                    sminfoblock = new SM();
                }
                segs._Done();
                segs = null;
            }
        }

        #endregion

        #region IEnumerator Members

        object System.Collections.IEnumerator.Current
        {
            get { return this.Current; }
        }

        #endregion


        #region IEnumerable<Tuple> Members

        public IEnumerator<MemCache.Tuple> GetEnumerator()
        {
            return this;
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion


        public Segment NewSegment(int length)
        {
            if (length < 0 || length > blocksize)
            {
                throw new InvalidOperationException("Invalid length for new segment: " + length
                    + "; must be between 0 and segment size");
            }
            if (reserves.Count > 0)
            {
                Segment rseg = reserves.Dequeue();
                rseg.seglen = length;
                return rseg;
            }
            {
                string smname = "MemCache_" + dfsname + "_" + Guid.NewGuid() + "_" + workerID;
                Segment nseg = new Segment(this, smname, Segment.NewSM(smname, blocksize));
                nseg.seglen = length;
                return nseg;
            }
        }

        public Segment NewSegment()
        {
            return NewSegment(this.blocksize);
        }


        public bool ThrowUnpinExceptions = false;
        public bool DisposeReleasesWorkerMemory = true;


        public class Segment : IDisposable
        {
            internal MemCache mc;
            internal string smname;
            internal SM sm;
            internal int seglen = 0;


            public int Length
            {
                get { return seglen; }

                set
                {
                    if (!IsLastSegment)
                    {
                        throw new InvalidOperationException("Can only set length of last segment");
                    }
                    if (null != mc.updating)
                    {
                        // This can be optimized to edit the length in-place.
                        mc._UpdateMetadata();
                    }
                }
            }

            public int Capacity
            {
                get { return mc.blocksize; }
            }


            public bool IsLastSegment
            {
                get
                {
                    unsafe
                    {
                        return (mc.segs.Count > 0
                            && this.sm.pview == mc.segs[mc.segs.Count - 1].sm.pview);
                    }
                }
            }


            public byte this[int index]
            {
                get
                {
                    unsafe
                    {
                        if (index < 0 || index > mc.blocksize)
                        {
                            throw new IndexOutOfRangeException("Index into Segment out of range (index = "
                                + index + ") (segment length = " + mc.blocksize + ")");
                        }
                        return sm.pview[index];
                    }
                }

                set
                {
                    unsafe
                    {
                        if (index < 0 || index > mc.blocksize)
                        {
                            throw new IndexOutOfRangeException("Index into Segment out of range (index = "
                                + index + ") (segment length = " + mc.blocksize + ")");
                        }
                        sm.pview[index] = value;
                    }
                }
            }


            internal Segment(MemCache mc, string smname, SM sm)
            {
                this.mc = mc;
                this.smname = smname;
                this.sm = sm;
            }


            static unsafe SM _MapSM(string smname, long size, ref int state)
            {
                SM sm = new SM();
                IntPtr hf = INVALID_HANDLE_VALUE;
                sm.hmap = CreateFileMapping(hf, IntPtr.Zero, PAGE_READWRITE, size, @"Global\" + smname);
                int lasterror = Marshal.GetLastWin32Error();
                if (IntPtr.Zero == sm.hmap)
                {
                    if (8 == lasterror)
                    {
                        throw new Exception("Shared memory segment named '" + smname + "' cannot be allocated; CreateFileMapping failed with ERROR_NOT_ENOUGH_MEMORY",
                            new OutOfMemoryException());
                    }
                    throw new Exception("Shared memory segment named '" + smname + "' cannot be allocated; CreateFileMapping failed with GetLastWin32Error=" + lasterror);
                }
                if (ERROR_ALREADY_EXISTS == lasterror)
                {
                    if (0 == state)
                    {
                        sm.Dispose();
                        throw new Exception("Shared memory segment named '" + smname + "' already exists, cannot create");
                    }
                    state = 1;
                }
                else
                {
                    if (smname.EndsWith("_empty") && smname.StartsWith("MemCache_"))
                    {
                        sm.Dispose();
                        throw new Exception("DEBUG:  _MapSM: cannot map _empty MemCache shared-memory segment");
                    }
                    if (1 == state)
                    {
                        sm.Dispose();
                        throw new Exception("Shared memory segment named '" + smname + "' not found");
                    }
                    state = 0;
                }
                sm.pview = (byte*)MapViewOfFile(sm.hmap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
                if (null == sm.pview)
                {
                    lasterror = Marshal.GetLastWin32Error();
                    sm.Dispose();
                    if (8 == lasterror)
                    {
                        throw new Exception("Shared memory segment named '" + smname + "' cannot be mapped into memory; MapViewOfFile failed with ERROR_NOT_ENOUGH_MEMORY",
                            new OutOfMemoryException());
                    }
                    throw new Exception("Shared memory segment named '" + smname + "' cannot be mapped into memory; MapViewOfFile failed with GetLastWin32Error=" + lasterror);
                }
                return sm;
            }

            // Can't exist.
            internal static SM NewSM(string smname, long size)
            {
                int state = 0;
                return _MapSM(smname, size, ref state);
            }

            // Must exist.
            internal static SM FindSM(string smname)
            {
                int state = 1;
                return _MapSM(smname, 1, ref state);
            }

            // Creates if doesn't exist.
            internal static SM GetSM(string smname, long size, out bool existed)
            {
                int state = 2;
                SM sm = _MapSM(smname, size, ref state);
                existed = state == 1;
                return sm;
            }


            private void _cleanup()
            {
                sm.Dispose();
            }

            public void Dispose()
            {
                _cleanup();
                GC.SuppressFinalize(this);
            }

            ~Segment()
            {
                _cleanup();
            }


            public struct PinSM : IDisposable
            {
                System.Net.Sockets.NetworkStream pinstm;
                string pinhost;

                public PinSM(string host)
                {
                    pinstm = null;
                    pinhost = host;
                }

                void _DoConnPinSM()
                {
                    if (null == pinhost)
                    {
                        pinhost = "localhost";
                    }
                    if (null != pinstm)
                    {
                        try
                        {
                            pinstm.Close();
                        }
                        catch
                        {
                        }
                        pinstm = null;
                    }
                    try
                    {
                        System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                            System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                        sock.Connect(pinhost, 55906);
                        pinstm = new System.Net.Sockets.NetworkStream(sock, true); // ownsSocket=true
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Unable to connect to MemCachePin service [ensure MemCachePin Windows services are installed and running]", e);
                    }
                }

                bool _StartCmdPinSM(char tagchar)
                {
                    bool oldconn = true;
                    if (null == pinstm)
                    {
                        oldconn = false;
                        _DoConnPinSM();
                    }
                    try
                    {
                        pinstm.WriteByte((byte)tagchar);
                    }
                    catch
                    {
                        if (!oldconn)
                        {
                            throw;
                        }
                        _DoConnPinSM();
                        pinstm.WriteByte((byte)tagchar);
                    }
                    return oldconn;
                }

                void _SimpleCmdPinSM(char tagchar, string arg)
                {
                    _StartCmdPinSM(tagchar);
                    XContent.SendXContent(pinstm, arg);
                    int ich = pinstm.ReadByte();
                    if ('+' != ich)
                    {
                        string errmsg = null;
                        try
                        {
                            if ('-' == ich)
                            {
                                errmsg = XContent.ReceiveXString(pinstm, null);
                            }
                        }
                        catch
                        {
                        }
                        if (null != errmsg)
                        {
                            throw new Exception("MemCachePin service error: " + errmsg + "  {" + tagchar + "}");
                        }
                        throw new Exception("MemCachePin service did not return a success signal  {" + tagchar + "}");
                    }
                }

                public void Pin(string smname)
                {
                    _SimpleCmdPinSM('p', smname);
                }

                public void Unpin(string smname)
                {
                    _SimpleCmdPinSM('u', smname);
                }


                #region IDisposable Members

                public void Dispose()
                {
                    if (null != pinstm)
                    {
                        pinstm.Close();
                        pinstm = null;
                    }
                }

                #endregion
            }

            internal static PinSM pinsm;


        }

        internal struct SM : IDisposable
        {
            internal IntPtr hmap;
            internal unsafe byte* pview;

            public void Dispose()
            {
                unsafe
                {
                    if (null != pview)
                    {
                        UnmapViewOfFile(new IntPtr(pview));
                        pview = null;
                    }
                    if (IntPtr.Zero != hmap)
                    {
                        CloseHandle(hmap);
                        hmap = IntPtr.Zero;
                    }
                }
            }
        }


        public class SegmentCollection : IList<Segment>
        {
            internal MemCache mc;
            internal List<Segment> _segments;
            bool _updating = false;


            #region IList<Segment> Members

            public int IndexOf(Segment item)
            {
                return _segments.IndexOf(item);
            }

            public void Insert(int index, Segment item)
            {
                _segments.Insert(index, item);
                _added(item);
            }

            public void RemoveAt(int index)
            {
                Segment seg = _segments[index];
                _segments.RemoveAt(index);
                _removed(seg);
            }

            public Segment this[int index]
            {
                get
                {
                    return _segments[index];
                }
                set
                {
                    Segment remseg = _segments[index];
                    _segments[index] = value;
                    _removed(remseg);
                    _added(value);
                }
            }

            #endregion

            #region ICollection<Segment> Members

            public void Add(Segment item)
            {
                _segments.Add(item);
                _added(item);
            }

            public void Clear()
            {
                throw new NotSupportedException();
            }

            public bool Contains(Segment item)
            {
                throw new NotSupportedException();
            }

            public void CopyTo(Segment[] array, int arrayIndex)
            {
                throw new NotSupportedException();
            }

            public int Count
            {
                get { return _segments.Count; }
            }

            public bool IsReadOnly
            {
                get { return false; }
            }

            public bool Remove(Segment item)
            {
                int index = this.IndexOf(item);
                if (-1 == index)
                {
                    return false;
                }
                this.RemoveAt(index);
                return true;
            }

            #endregion

            #region IEnumerable<Segment> Members

            IEnumerator<Segment> IEnumerable<Segment>.GetEnumerator()
            {
                return _segments.GetEnumerator();
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return _segments.GetEnumerator();
            }

            #endregion

            public List<Segment>.Enumerator GetEnumerator()
            {
                return _segments.GetEnumerator();
            }

            internal SegmentCollection(MemCache mc, List<Segment> seglist)
            {
                this.mc = mc;
                this._segments = seglist;
            }


            void _added(Segment seg)
            {
                if (_updating)
                {
                    return;
                }
                _recognize(seg);
                mc._UpdateMetadata();
            }

            void _removed(Segment seg)
            {
                if (_updating)
                {
                    return;
                }
                _unrecognize(seg);
                mc._UpdateMetadata();
            }

            void _recognize(Segment seg)
            {
                Segment.pinsm.Pin(seg.smname);
            }

            void _unrecognize(Segment seg)
            {
                try
                {
                    Segment.pinsm.Unpin(seg.smname);
                }
                catch
                {
                    if (mc.ThrowUnpinExceptions)
                    {
                        throw;
                    }
                }
            }

            internal List<Segment> _StartUpdatingAll()
            {
                foreach (Segment seg in _segments)
                {
                    _unrecognize(seg);
                }
                List<Segment> result = _segments;
                _segments = new List<Segment>(result.Count);
                _updating = true;
                return result;
            }

            internal void _DoneUpdatingAll()
            {
                if (_updating)
                {
                    foreach (Segment seg in _segments)
                    {
                        _recognize(seg);
                    }
                    _updating = false;
                    mc._UpdateMetadata();
                }
            }

            internal void _Done()
            {
                List<Segment> segs = _segments;
                _segments = null;
                for (int i = 0; i < segs.Count; i++)
                {
                    segs[i].Dispose();
                    segs[i] = null;
                }
                segs = null;
            }

        }

        public SegmentCollection Segments
        {
            get
            {
                return segs;
            }
        }


        static List<List<string>> _GetMetaDataChunkInfo_unlocked(string dfsname)
        {
            int SegmentSize;
            return _GetMetaDataChunkInfo_unlocked(dfsname, out SegmentSize);
        }

        static List<List<string>> _GetMetaDataChunkInfo_unlocked(string dfsname, out int SegmentSize)
        {
            SegmentSize = -1;
            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                //dfsname = dfsname.Substring(6);
                throw new ArgumentException();
            }
            unsafe
            {
                {
                    string infoblockname = "MemCache_" + dfsname + "_info";
                    using (SM info = Segment.FindSM(infoblockname)) // Always infoblocksize.
                    {
                        List<List<string>> chunkinfosperslave = new List<List<string>>();
                        {
                            StringBuilder sb = new StringBuilder(128);
                            int ipos = infoblocklocaloffset;
                            int slaveindex = -1;
                            bool foundchunks = false;
                            {
                                sb.Length = 0;
                                for (; ipos < infoblocksize; ipos++)
                                {
                                    if (ipos >= infoblocksize || info.pview[ipos] == 0)
                                    {
                                        //infoend = true;
                                        break;
                                    }
                                    if (info.pview[ipos] == '\r'
                                        || info.pview[ipos] == '\n')
                                    {
                                        if (sb.Length != 0)
                                        {
                                            string mdln = sb.ToString();
                                            if (foundchunks)
                                            {
                                                if (mdln.StartsWith("##"))
                                                {
                                                    int icolon = mdln.IndexOf(':', 2);
                                                    slaveindex = int.Parse(mdln.Substring(2, icolon - 2));
                                                    while (slaveindex >= chunkinfosperslave.Count)
                                                    {
                                                        chunkinfosperslave.Add(new List<string>());
                                                    }
                                                }
                                                else
                                                {
                                                    chunkinfosperslave[slaveindex].Add(mdln);
                                                }
                                            }
                                            else
                                            {
                                                if (mdln.StartsWith("blocksize="))
                                                {
                                                    SegmentSize = int.Parse(mdln.Substring(10));
                                                }
                                                else if (mdln.StartsWith("----------"))
                                                {
                                                    foundchunks = true;
                                                }
                                            }
                                            sb.Length = 0;
                                        }
                                    }
                                    else
                                    {
                                        sb.Append((char)info.pview[ipos]);
                                    }
                                }
                            }
                            if (!foundchunks)
                            {
                                throw new Exception("Corrupt info block: 'MemCache_" + dfsname + "_info'; dash separator not found");
                            }
                        }
                        return chunkinfosperslave;
                    }
                }
            }
        }


        unsafe static bool _AnyWorkersLocked_unlocked(SM info)
        {
            bool result = false;
            byte* pos = info.pview + infoblockworkerbytesoffset;
            byte* posend = pos + infoblockworkerbytes;
            for (; pos < posend; pos++)
            {
                if (0 != *pos)
                {
                    result = true;
                    break;
                }
            }
            return result;
        }


        public static void FlushInternal(string dfsname, List<KeyValuePair<string, int>> append)
        {
#if DEBUG

#endif
            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                //dfsname = dfsname.Substring(6);
                throw new ArgumentException();
            }
            LoadInternal(dfsname); // Make sure it's loaded in case some machines are loaded and some aren't.
            string MetaFileName = "mcm." + SafeTextPath(dfsname) + ".mcm";
            string infoblockname = "MemCache_" + dfsname + "_info";
            for (bool done = false; !done; )
            {
                System.Threading.Mutex mutex = new System.Threading.Mutex(false, "MemCache_" + dfsname + "_lock");
                mutex.WaitOne(); // -->
                try
                {
                    unsafe
                    {
                        if (!System.IO.File.Exists(MetaFileName))
                        {
                            throw new Exception("MemCache '" + dfsname + "' metadata file not found: " + MetaFileName);
                        }

                        bool blocked;
                        try
                        {
                            using (SM info = Segment.FindSM(infoblockname)) // Always infoblocksize.
                            {
                                blocked = _AnyWorkersLocked_unlocked(info);
                            }
                        }
                        catch (Exception e)
                        {
                            /*
                            throw new Exception("Problem occured accessing MemCache metadata for '" + dfsname + "'"
                                + "; perhaps this MemCache is not fully loaded (issue memcache load command)", e);
                             * */
                            throw new Exception("Problem occured accessing MemCache metadata for '" + dfsname + "'", e);
                        }

                        if (blocked)
                        {
                            throw new InvalidOperationException("Unable to commit MemCache '" + dfsname + "' while it is in use");
                        }
                        else
                        {
                            done = true;

                            int SegmentSize;
                            List<List<string>> chunkinfosperslave = _GetMetaDataChunkInfo_unlocked(dfsname, out SegmentSize);
                            if (SegmentSize < 1)
                            {
                                throw new Exception("Invalid segment size (blocksize) in metadata for MemCache '" + dfsname + "'");
                            }
                            byte[] HEADER = new byte[4];
                            ToBytes(4, HEADER, 0);
                            foreach (List<string> chunkinfos in chunkinfosperslave)
                            {
                                foreach (string chunkinfo in chunkinfos)
                                {
                                    string smname;
                                    string chunkname;
                                    int chunklen = SegmentSize;
                                    {
                                        string[] parts = chunkinfo.Split(' ');
                                        smname = parts[0];
                                        chunkname = parts[1];
                                        if (parts.Length > 2)
                                        {
                                            try
                                            {
                                                chunklen = int.Parse(parts[2], System.Globalization.NumberStyles.HexNumber);
                                            }
                                            catch (Exception e3322)
                                            {
                                                throw new FormatException("Invalid format: " + parts[2], e3322);
                                            }
                                        }
                                    }

                                    {
                                        using (System.IO.FileStream fs = new System.IO.FileStream(chunkname,
                                            System.IO.FileMode.Create, System.IO.FileAccess.Write))
                                        {
                                            fs.Write(HEADER, 0, HEADER.Length);
                                            if (chunklen > 0)
                                            {
                                                using (SM sm = Segment.FindSM(smname))
                                                {
                                                    for (int i = 0; i < chunklen; i++)
                                                    {
                                                        fs.WriteByte(sm.pview[i]);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    append.Add(new KeyValuePair<string, int>(chunkname, chunklen));

                                }
                            }

                            {
                                bool anylockednow = false;
                                string tmpmfn = MetaFileName + "." + Guid.NewGuid().ToString() + ".tmp";
                                using (System.IO.FileStream fs = new System.IO.FileStream(tmpmfn,
                                    System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
                                {
                                    using (SM info = Segment.FindSM(infoblockname)) // Always infoblocksize.
                                    {
                                        unsafe
                                        {
                                            if (info.pview[0] == 99
                                                && info.pview[1] == 243)
                                            {
                                                throw new Exception("This MemCache '" + dfsname + "' has been released but is still in use");
                                            }
                                            for (int ipos = infoblocklocaloffset; ipos < infoblocksize; ipos++)
                                            {
                                                if (info.pview[ipos] == 0)
                                                {
                                                    break;
                                                }
                                                fs.WriteByte(info.pview[ipos]);
                                            }
                                            anylockednow = _AnyWorkersLocked_unlocked(info);
                                        }
                                    }
                                }
                                System.IO.File.Copy(tmpmfn, MetaFileName, true);
                                try
                                {
                                    System.IO.File.Delete(tmpmfn);
                                }
                                catch
                                {
                                }
                                if (anylockednow)
                                {
                                    throw new Exception("This MemCache '" + dfsname + "' started being used during commit and may be in a bad state");
                                }
                            }

                        }

                    }

                }
                finally
                {
                    mutex.ReleaseMutex(); // <--
                }
            }
        }


        public static void ReleaseInternal(string dfsname, bool force)
        {
            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                //dfsname = dfsname.Substring(6);
                throw new ArgumentException();
            }
            for (bool done = false; !done; )
            {
                System.Threading.Mutex mutex = new System.Threading.Mutex(false, "MemCache_" + dfsname + "_lock");
                mutex.WaitOne(); // -->
                try
                {
                    string infoblockname = "MemCache_" + dfsname + "_info";

                    bool blocked;
                    try
                    {
                        using (SM info = Segment.FindSM(infoblockname)) // Always infoblocksize.
                        {
                            unsafe
                            {
                                // Mark as garbage.
                                info.pview[0] = 99;
                                info.pview[1] = 243;
                            }

                            blocked = _AnyWorkersLocked_unlocked(info);
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception("MemCacheWarning: Problem occured accessing MemCache metadata for '" + dfsname + "'"
                            + "; perhaps this MemCache has already been rolled back", e);
                    }

                    if (blocked)
                    {
                        if (force)
                        {
                            blocked = false;
                        }
                        else
                        {
                            throw new InvalidOperationException("Unable to rollback MemCache '" + dfsname + "' while it is in use"
                                + "; rollback not complete on all machines");
                        }
                    }
                    if (!blocked)
                    {
                        List<List<string>> chunkinfosperslave = _GetMetaDataChunkInfo_unlocked(dfsname);
                        foreach (List<string> chunkinfos in chunkinfosperslave)
                        {
                            foreach (string chunkinfo in chunkinfos)
                            {
                                try
                                {
                                    Segment.pinsm.Unpin(chunkinfo.Split(' ')[0]);
                                }
                                catch
                                {
                                }
                            }
                        }
                        try
                        {
                            Segment.pinsm.Unpin(infoblockname);
                        }
                        catch
                        {
                        }

                        done = true;
                    }

                }
                finally
                {
                    mutex.ReleaseMutex(); // <--
                }
            }
        }


        public static void LoadInternal(string dfsname)
        {
            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                //dfsname = dfsname.Substring(6);
                throw new ArgumentException();
            }

            int highestwid = 0;
            for (int wid = 0; wid <= highestwid; wid++)
            {
                using (MemCache mc = MemCache.Attach(dfsname, wid, true)) // ReadOnly=true
                {
                    if (mc.highestworkerid > highestwid)
                    {
                        highestwid = mc.highestworkerid;
                    }
                }
            }
            
        }


        static Int32 BytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        unsafe static Int32 BytesToInt(byte* x)
        {
            int result = 0;
            result |= (int)x[0] << 24;
            result |= (int)x[1] << 16;
            result |= (int)x[2] << 8;
            result |= x[3];
            return result;
        }
        unsafe static string  BytesToString(byte* p,int fieldsize)
        {   
            string result = null;
            for(byte* p1=p;p1<p+(fieldsize);p1++)
            {
                int ch = 0; 
                ch |=(int)(*p1) << 8;
                p1++;
                ch |= (*p1);
                result += (char)ch;     
            }
            return result;
        }
        unsafe static mstring BytesToMString(byte* p, int fieldsize)
        {
             string result = null;
             
            for (byte* p1 = p; p1 < p + (fieldsize); p1++)
            {
                int ch = 0;
                ch |= (int)(*p1) << 8;
                p1++;
                ch |= (*p1);
                result += (char)ch;
            }
            mstring result1;
            return result1 = mstring.Prepare(result);
        }
       
        unsafe static void  StringToBytes(string value,byte* p,int fieldsize)
        {
          if (value.Length <= fieldsize/2)
            {
                string value2;
                value2 = value.PadRight(fieldsize/2,'\0');

                int  j = 0;
                for (int i = 0; i < fieldsize/2 ; i++)
                {
                   p[j] = (byte)(value2[i]>>8);
                   j++;
                   p[j] = (byte)(value2[i]);
                   j++;
                }
            }
            else
            {
               string FunctionName = "<this method>";
                            try
                            {
                                FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                            }
                            catch
                            {
                            }
                            throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
            }
                        
        }
        unsafe static void MStringToBytes(mstring value, byte* p, int fieldsize)
        {
            if (value.Length <= fieldsize / 2)
            {
                mstring value2;
                
                value2 = value.MPadRight(fieldsize / 2, '\0');
                string value3 = value2.ToString();
                

                int j = 0;
                for (int i = 0; i < fieldsize / 2; i++)
                {
                    p[j] = (byte)(value3[i] >> 8);
                    j++;
                    p[j] = (byte)(value3[i]);
                    j++;
                }
            }
            else
            {
                string FunctionName = "<this method>";
                try
                {
                    FunctionName = (new System.Diagnostics.StackFrame()).GetMethod().Name;
                }
                catch
                {
                }
                throw new InvalidOperationException("Cannot " + FunctionName + " on field of " + fieldsize + " bytes");
            }

        }

        unsafe static Int64 BytesToLong(byte* x)
        {
            return BytesToLong(x, 0);
        }

        unsafe static Int64 BytesToLong(byte* x, int offset)
        {
            long result = 0;
            result |= (long)x[0 + offset] << 56;
            result |= (long)x[1 + offset] << 48;
            result |= (long)x[2 + offset] << 40;
            result |= (long)x[3 + offset] << 32;
            result |= (long)x[4 + offset] << 24;
            result |= (long)x[5 + offset] << 16;
            result |= (long)x[6 + offset] << 8;
            result |= x[7 + offset];
            return result;
        }

        unsafe static double BytesToDouble(byte* x)
        {
            if (x[0] == 0x66)
            {
                long l = BytesToLong(x, 1);
                return BitConverter.Int64BitsToDouble(l);
            }

            if (x[0] == 0x63)
            {
                long l = BytesToLong(x, 1);
                return BitConverter.Int64BitsToDouble(~l);
            }

            if (x[0] == 0x65)
            {
                return 0;
            }

            if (x[0] == 0x61)
            {
                return double.NaN;
            }

            if (x[0] == 0x62)
            {
                return double.NegativeInfinity;
            }

            if (x[0] == 0x67)
            {
                return double.PositiveInfinity;
            }

            return -0d;
        }

        static void ToBytes(Int32 x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 24);
            resultbuf[bufoffset + 1] = (byte)(x >> 16);
            resultbuf[bufoffset + 2] = (byte)(x >> 8);
            resultbuf[bufoffset + 3] = (byte)x;
        }

        unsafe static void ToBytes(Int32 x, byte* resultbuf)
        {
            resultbuf[0] = (byte)(x >> 24);
            resultbuf[1] = (byte)(x >> 16);
            resultbuf[2] = (byte)(x >> 8);
            resultbuf[3] = (byte)x;
        }

        unsafe static void LongToBytes(Int64 x, byte* resultbuf)
        {
            LongToBytes(x, resultbuf, 0);
        }

        unsafe static void LongToBytes(Int64 x, byte* resultbuf, int offset)
        {
            resultbuf[0 + offset] = (byte)(x >> 56);
            resultbuf[1 + offset] = (byte)(x >> 48);
            resultbuf[2 + offset] = (byte)(x >> 40);
            resultbuf[3 + offset] = (byte)(x >> 32);
            resultbuf[4 + offset] = (byte)(x >> 24);
            resultbuf[5 + offset] = (byte)(x >> 16);
            resultbuf[6 + offset] = (byte)(x >> 8);
            resultbuf[7 + offset] = (byte)x;
        }

        unsafe static void DoubleToBytes(double x, byte* resultbuf)
        {
            if (double.IsNaN(x))
            {
                resultbuf[0] = 0x61;
                for (int i = 1; i < 9; i++)
                {
                    resultbuf[i] = 0x0;
                }
                return;
            }

            if (double.IsNegativeInfinity(x))
            {
                resultbuf[0] = 0x62;
                for (int i = 1; i < 9; i++)
                {
                    resultbuf[i] = 0x0;
                }
                return;
            }

            if (isNegativeZero(x))
            {
                resultbuf[0] = 0x64;
                for (int i = 1; i < 9; i++)
                {
                    resultbuf[i] = 0x0;
                }
                return;
            }

            if (x == 0)
            {
                resultbuf[0] = 0x65;
                for (int i = 1; i < 9; i++)
                {
                    resultbuf[i] = 0x0;
                }
                return;
            }

            if (double.IsPositiveInfinity(x))
            {
                resultbuf[0] = 0x67;
                for (int i = 1; i < 9; i++)
                {
                    resultbuf[i] = 0x0;
                }
                return;
            }

            long l = BitConverter.DoubleToInt64Bits(x);

            if (x > 0)
            {
                resultbuf[0] = 0x66;
                LongToBytes(l, resultbuf, 1);
                return;
            }

            resultbuf[0] = 0x63;
            LongToBytes(~l, resultbuf, 1);
        }

        static bool isNegativeZero(double x)
        {
            return x == 0 && 1 / x < 0;
        }
        
        static string SafeTextPath(string s)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char ch in s)
            {
                if (sb.Length >= 150)
                {
                    sb.Append('`');
                    sb.Append(s.GetHashCode());
                    break;
                }
                if ('.' == ch)
                {
                    if (0 == ch)
                    {
                        sb.Append("%2E");
                        continue;
                    }
                }
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch)
                {
                    sb.Append('%');
                    if (ch > 0xFF)
                    {
                        sb.Append('u');
                        sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                    }
                    else
                    {
                        sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (0 == sb.Length)
            {
                return "_";
            }
            return sb.ToString();
        }


        #region WIN32API

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr CreateFile(
           String lpFileName, int dwDesiredAccess, int dwShareMode,
           IntPtr lpSecurityAttributes, int dwCreationDisposition,
           int dwFlagsAndAttributes, IntPtr hTemplateFile);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr CreateFileMapping(
           IntPtr hFile, IntPtr lpAttributes, int flProtect,
           uint dwMaximumSizeHigh, uint dwMaximumSizeLow,
           String lpName);

        internal static IntPtr CreateFileMapping(
           IntPtr hFile, IntPtr lpAttributes, int flProtect,
           long dwMaximumSize,
           String lpName)
        {
            return CreateFileMapping(hFile, lpAttributes, flProtect,
                (uint)((ulong)dwMaximumSize >> 32), (uint)dwMaximumSize,
                lpName);
        }

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool FlushViewOfFile(
           IntPtr lpBaseAddress, int dwNumBytesToFlush);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern IntPtr MapViewOfFile(
           IntPtr hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh,
           int dwFileOffsetLow, int dwNumBytesToMap);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr OpenFileMapping(
           int dwDesiredAccess, bool bInheritHandle, String lpName);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool UnmapViewOfFile(IntPtr lpBaseAddress);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool CloseHandle(IntPtr handle);

        internal const int ERROR_ALREADY_EXISTS = 183;

        internal static IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        internal const int PAGE_READWRITE = 0x4;

        internal const int FILE_MAP_WRITE = 0x2;
        internal const int FILE_MAP_READ = 0x4;
        internal const int FILE_MAP_ALL_ACCESS = 0xF001F;

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall, SetLastError = true)]
        internal static extern IntPtr CreateFile(
              string lpFileName,
              uint dwDesiredAccess,
              uint dwShareMode,
              IntPtr SecurityAttributes,
              uint dwCreationDisposition,
              uint dwFlagsAndAttributes,
              IntPtr hTemplateFile
              );

        #endregion


    }


}
