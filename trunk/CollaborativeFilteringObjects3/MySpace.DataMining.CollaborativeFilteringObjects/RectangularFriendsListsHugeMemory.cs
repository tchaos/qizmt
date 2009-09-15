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
using System.Text;


namespace MySpace.DataMining.CollaborativeFilteringObjects3
{
    public class RectangularFriendsListsHugeMemory
    {
        public RectangularFriendsListsHugeMemory(int filebuffersize, string file)
        {
            Load(filebuffersize, file);
        }

        public RectangularFriendsListsHugeMemory()
        {
        }


        public int MaxFriends
        {
            get
            {
                return maxfriends;
            }

            set
            {
                maxfriends = value;
            }
        }


        public int MinimumScores
        {
            get
            {
                return minscores;
            }

            set
            {
                minscores = value;
            }
        }


        public int SkippedLines
        {
            get
            {
                return numskippedlines;
            }
        }


        /*
        public Int32 GetUserIDFromRowID(int rowid)
        {
            return table[rowid, 0];
        }
         * */


        public int GetRowIDFromUserID(Int32 userid)
        {
            for (int i = 0; i != usertotableranges.Count; i++)
            {
                int x = usertotableranges[i].Lookup(userid, -1);
                if (x > 0) // Important: treat 0 as -1
                {
                    return x;
                }
            }
            return -1;
        }

        public int GetRowIDFromUserIDSameRange(Int32 userid, Int32 rangewith)
        {
            for (int i = 0; i != usertotableranges.Count; i++)
            {
                if (usertotableranges[i].IsInRange(rangewith))
                {
                    int x = usertotableranges[i].Lookup(userid, -1);
                    if (x <= 0) // Important: treat 0 as -1
                    {
                        return -1;
                    }
                    return x;
                }
            }
            return -1;
        }


        public bool SetRowIDForUserID(Int32 userid, int rowid)
        {
            for (int i = 0; i != usertotableranges.Count; i++)
            {
                UserIDRange uidr = usertotableranges[i];
                if (uidr.Add(userid, rowid))
                {
                    usertotableranges[i] = uidr;
                    return true;
                }
            }
            return false;
        }


        public static void Int32ToBytes(Int32 x, IList<byte> resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)((UInt32)x >> 8);
            resultbuf[bufoffset + 2] = (byte)((UInt32)x >> 16);
            resultbuf[bufoffset + 3] = (byte)((UInt32)x >> 24);
        }


        public static Int32 BytesToInt32(IList<byte> x, int offset)
        {
            Int32 result = 0;
            result |= x[offset + 0];
            result |= (Int32)((UInt32)x[offset + 1] << 8);
            result |= (Int32)((UInt32)x[offset + 2] << 16);
            result |= (Int32)((UInt32)x[offset + 3] << 24);
            return result;
        }

        public static Int32 BytesToInt32(IList<byte> x)
        {
            return BytesToInt32(x, 0);
        }


        internal class ThreadBatchData
        {
            internal ThreadBatchData(RectangularFriendsListsHugeMemory rfl, int nbatchedrows)
            {
                this.rfl = rfl;
                this.batchusersbuf = new byte[(nbatchedrows * (1 + rfl.MaxFriends)) << 2];
                this.batch = new List<SlaveMemory.ThreadView.Batch>(nbatchedrows);
            }


            internal int GetBatchRowByteOffset(int n)
            {
                return n * ((1 + rfl.MaxFriends) << 2);
            }


            internal void SetRowData(int n, Int32 uid, IList<Int32> friends)
            {
                int offset = GetBatchRowByteOffset(n);
                Int32ToBytes(uid, batchusersbuf, offset);
                for(int i = 0; i < rfl.MaxFriends; i++)
                {
                    offset += 4;
                    Int32ToBytes(friends[i], batchusersbuf, offset);
                }
            }


            // Returns UserID.
            internal Int32 GetRowData(int n, IList<Int32> friends)
            {
                int offset = GetBatchRowByteOffset(n);
                Int32 result = BytesToInt32(batchusersbuf, offset);
                for (int i = 0; i < rfl.MaxFriends; i++)
                {
                    offset += 4;
                    friends[i] = BytesToInt32(batchusersbuf, offset);
                }
                return result;
            }


            internal void BatchSet(SlaveMemory.ThreadView tv)
            {
                tv.Set(batch);
                batch.Clear();
            }

            internal void BatchGet(SlaveMemory.ThreadView tv)
            {
                tv.Get(batch);
                batch.Clear();
            }


            internal void BatchRow(int n, int rowid)
            {
                int offset = GetBatchRowByteOffset(n);
#if DEBUG
                if (offset < n)
                {
                    throw new Exception("DEBUG:  offset < n");
                }
#endif
                long soffset = rfl.RowIDToSlaveOffset(rowid);
#if DEBUG
                if (soffset < (long)rowid)
                {
                    throw new Exception("DEBUG:  soffset < (long)rowid");
                }
#endif
                SlaveMemory.ThreadView.Batch b = SlaveMemory.ThreadView.Batch.Create(
                    soffset,
                    batchusersbuf,
                    offset,
                    (1 + rfl.MaxFriends) << 2);
                batch.Add(b);
            }


            internal RectangularFriendsListsHugeMemory rfl;
            internal byte[] batchusersbuf;
            internal List<SlaveMemory.ThreadView.Batch> batch;
        }


        public long RowIDToSlaveOffset(int rowid)
        {
            return (long)rowid * ((1 + MaxFriends) << 2);
        }


        protected static bool GetNextLine(System.IO.StreamReader rstm, out Int32 uid, out Int32 fid, byte[] linebuf)
        {
#if DEBUG
#else
            unchecked
#endif
            {
                uid = 0;
                fid = 0;
                int offset = 0;
                for (; ; )
                {
                    if (offset >= linebuf.Length)
                    {
                        break;
                    }
                    int iby = rstm.Read();
                    if (-1 == iby)
                    {
                        if (0 == offset)
                        {
                            return false;
                        }
                        break;
                    }
                    if ('\n' == iby)
                    {
                        break;
                    }
                    linebuf[offset++] = (byte)iby;
                }
                {
                    int i = 0;
                    for (; ; i++)
                    {
                        if (i == offset)
                        {
                            return true;
                        }
                        byte by = linebuf[i];
                        if (by >= '0' && by <= '9')
                        {
                            uid *= 10;
                            uid += (byte)by - '0';
                        }
                        else
                        {
                            i++;
                            break;
                        }
                    }
                    for (; i < offset; i++)
                    {
                        byte by = linebuf[i];
                        if (by >= '0' && by <= '9')
                        {
                            fid *= 10;
                            fid += (byte)by - '0';
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
                return true;
            }
        }


        class SharedLoadContext
        {
            public SeqFileStream.SeqFileStreamReader streamqueue;
            public int nexttableindex = 1;
        }


        class ThreadLoadData
        {
            public SharedLoadContext slc;
            public SlaveMemory.ThreadView tv;
        }


        void loadthreadproc(object obj)
        {
            try
            {
#if DEBUG
#else
                unchecked
#endif
                {
                    ThreadBatchData batchdata = new ThreadBatchData(this, nbatchedrows);
                    int curbatch = 0; // Current row from nbatchedrows in batchdata.
                    ThreadLoadData tld = (ThreadLoadData)obj;
                    SharedLoadContext slc = tld.slc;
                    SlaveMemory.ThreadView tv = tld.tv;
                    SeqFileStream.SeqFileStreamReader streamqueue = slc.streamqueue;
                    Int32[] friendsbuf = new Int32[maxfriends];
                    byte[] linebuf = new byte[32];
#if DEBUG
                    for (int i = 0; i < maxfriends; i++)
                    {
                        friendsbuf[i] = -929292;
                    }
#endif
                    // Assumes trailing friend-IDs are 0.
                    while (slc.nexttableindex < usercount)
                    {
                        long linnum = 0;
                        string curfilename = null;
                        System.IO.Stream stm = null;
                        try
                        {
                            lock (streamqueue)
                            {
                                stm = streamqueue.GetNextStream(out curfilename);
                            }
                            if (null == stm)
                            {
                                break;
                            }
                            using (System.IO.StreamReader rstm = new System.IO.StreamReader(stm))
                            {
                                linnum = 0;
                                int friendindex = 0;
                                Int32 previd = 0;
                                for (; ; )
                                {
                                    linnum++;
                                    Int32 uid, fid;
                                    if (GetNextLine(rstm, out uid, out fid, linebuf))
                                    {
                                        if (uid <= 0)
                                        {
                                            //throw new Exception("bad line: user ID invalid");
                                            numskippedlines++;
                                            continue;
                                        }
                                        if (fid <= 0)
                                        {
                                            //throw new Exception("bad line: friend ID invalid");
                                            numskippedlines++;
                                            continue;
                                        }
                                    }

                                    if (uid != previd)
                                    {
                                        if (0 != previd)
                                        {
                                            if (slc.nexttableindex >= usercount)
                                            {
                                                break;
                                            }
                                            for (int i = friendindex; i < maxfriends; i++)
                                            {
                                                friendsbuf[i] = 0;
                                            }
                                            friendindex = 0;
                                            lock (slc)
                                            {
                                                SetRowIDForUserID(previd, slc.nexttableindex);
                                                batchdata.SetRowData(curbatch, previd, friendsbuf);
                                                batchdata.BatchRow(curbatch, slc.nexttableindex);
                                                curbatch++;
                                                if (curbatch >= nbatchedrows)
                                                {
                                                    curbatch = 0;
                                                    batchdata.BatchSet(tv);
                                                }
                                                slc.nexttableindex++;
                                            }
#if DEBUG
                                            for (int i = 0; i < maxfriends; i++)
                                            {
                                                friendsbuf[i] = -929292;
                                            }
#endif
                                        }
                                    }
                                    if (0 == uid)
                                    {
                                        break;
                                    }
                                    if (friendindex < maxfriends)
                                    {
                                        friendsbuf[friendindex] = fid;
                                        friendindex++;
                                    }
                                    previd = uid;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (null == goterr)
                            {
                                goterr = new Exception("Load error: " + e.ToString() + " file: " + (null == curfilename ? "<null>" : "'" + curfilename + "'") + "; line: " + linnum.ToString() + "; table index: " + slc.nexttableindex.ToString(), e);
                            }
                            try
                            {
                                LogLine("\r\nLoad error: " + e.ToString() + " file: " + (null == curfilename ? "<null>" : "'" + curfilename + "'") + "; line: " + linnum.ToString() + "; table index: " + slc.nexttableindex.ToString() + "; exception: " + e.ToString() + "\r\n(Skipping the rest of this file)\r\n");
                            }
                            catch (Exception e2)
                            {
                                LogLine("\r\nError error: " + e2.ToString() + "\r\n");
                            }
                        }
                        finally
                        {
                            if (null != stm)
                            {
                                stm.Close();
                            }
                        }
                    }
                    if (curbatch > 0)
                    {
                        try
                        {
                            batchdata.BatchSet(tv);
                        }
                        catch (Exception e33)
                        {
                            int i33 = 33 + 33;
                            throw e33;
                        }
                    }

                    realusercount = slc.nexttableindex;
                }
            }
            catch (Exception e)
            {
                if (null == goterr)
                {
                    goterr = new Exception("Load error: loadthreadproc catch-all exception", e);
                }
                LogLine("\r\nLoad error: loadthreadproc catch-all exception: " + e.ToString() + "\r\n");
            }
        }


        public void Load(int filebuffersize, ISequenceInput input)
        {
            string seqfn = "seq_temp_" + Guid.NewGuid().ToString() + ".txt";
            try
            {
                using (System.IO.StreamWriter sw = new System.IO.StreamWriter(seqfn))
                {
                    sw.WriteLine("*sequence*");
                    IList<string> inputs = input.GetSequenceInputs();
                    for (int i = 0; i < inputs.Count; i++)
                    {
                        sw.WriteLine(inputs[i]);
                    }
                }

                Load(filebuffersize, seqfn);
            }
            finally
            {
                try
                {
                    System.IO.File.Delete(seqfn);
                }
                catch
                {
                }
            }
        }

        public void Load(int filebuffersize, string file)
        {
            LogLine("---- RectangularFriendsListsHugeMemory.Load ----");

            //usercount = GetUniqueUserCount(file);

            int blockusercount = usercount / nslaves;
            if (0 != (usercount % nslaves))
            {
                blockusercount++;
            }
            long blocksize = ((long)blockusercount * (1 + (long)MaxFriends)) << 2;

            int packetsize = nbatchedrows * (((1 + MaxFriends) << 2) + 1 + 8 + 4) + 1;
            smtable = new SlaveMemory(objname, "CollaborativeFilteringObjectsSlave.exe", blocksize, packetsize, nthreads, nslaves);
            numskippedlines = 0;

            if (0 == usertotableranges.Count)
            {
                AddUserIDRange(0, 400000000 - 1);
            }

            using (MySpace.DataMining.SeqFileStream.SeqFileStreamReader stm = new MySpace.DataMining.SeqFileStream.SeqFileStreamReader(file, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, filebuffersize))
            {
                SharedLoadContext slc = new SharedLoadContext();
                slc.streamqueue = stm;
                List<System.Threading.Thread> threads = new List<System.Threading.Thread>(nthreads);
                smtable.Open(); // !
                for (int i = 0; i != nthreads; i++)
                {
                    ThreadLoadData tld = new ThreadLoadData();
                    tld.slc = slc;
                    tld.tv = smtable.ThreadViews[i];
                    System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(loadthreadproc));
                    thd.Name = "CFO_Load_" + i.ToString();
                    thd.Start(tld);
                    threads.Add(thd);
                }
                for (int i = 0; i != nthreads; i++)
                {
                    threads[i].Join();
                }
            }
            if (null != goterr)
            {
                Exception e = goterr;
                goterr = null;
                throw e;
            }

#if DEBUG
            if (smtable._batchspilled)
            {
                LogLine("RectangularFriendsListsHugeMemory.Load: SlaveMemory batching spilled to another slave");
                smtable._batchspilled = false;
            }
#endif

            if (realusercount >= usercount)
            {
                //GotException(new Exception("row limit reached"));
                LogLine("row limit reached");
            }

            LogLine("Load finished: number of rows: " + realusercount.ToString() + " (max " + usercount.ToString() + ")");
        }


        void _clean()
        {
            if (null != smtable)
            {
                smtable.Close();
            }
        }

        public void Dispose()
        {
            _clean();
            System.GC.SuppressFinalize(this);
        }


        ~RectangularFriendsListsHugeMemory()
        {
            _clean();
        }


        public int Count
        {
            get
            {
                return realusercount;
            }
        }


        internal struct OutRange
        {
            internal Int32 upper, lower;
            internal System.IO.StreamWriter sw;
        }

        List<OutRange> outranges;
        Int32 rangeinc = 0;
        int noranges = 0;


        public int NoRangeOutputCount
        {
            get
            {
                return noranges;
            }
        }


        int FindOutputRange(Int32 id)
        {
            if (id < 0)
            {
                return -1;
            }
            if (0 == rangeinc)
            {
                return -1;
            }
            int i = id / rangeinc;
            if (i >= outranges.Count)
            {
                return -1;
            }
            return i;
        }


        public void AddOutputRange(Int32 lowerID, Int32 upperID, string tofilename, bool togzip)
        {
            System.IO.Stream stm = new System.IO.FileStream(tofilename, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read);
            if (togzip)
            {
                stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Compress);
            }
            AddOutputRange(lowerID, upperID, stm);
        }


        public void AddOutputRange(Int32 lowerID, Int32 upperID, System.IO.Stream tostream)
        {
            OutRange or;
            or.lower = lowerID;
            or.upper = upperID;
            if (null == outranges)
            {
                outranges = new List<OutRange>();
                rangeinc = upperID - lowerID + 1; // Inclusive.
                if (0 == rangeinc)
                {
                    if (rangeinc <= 0)
                    {
                        throw new Exception("AddOutputRange: bad range; expected increments greater than zero");
                    }
                }
            }
            else
            {
                if (upperID - lowerID + 1 != rangeinc)
                {
                    throw new Exception("AddOutputRange: uneven ranges; expecting more increments of " + rangeinc.ToString());
                }
            }
            or.sw = new System.IO.StreamWriter(tostream, Encoding.ASCII);

            outranges.Add(or);
        }


        public int MaxFriendsInCommon
        {
            get
            {
                return maxfics;
            }

            set
            {
                maxfics = value;
            }
        }

        int maxfics = 0;


        class DoScores
        {
            internal int iTotalThreads;
            internal int iMyThread;
            internal System.IO.StreamWriter sw;
            internal System.Threading.Thread thd;
            internal SlaveMemory.ThreadView tv;
        }


        void tofofscorespartthreadproc(object obj)
        {
            try
            {
                ThreadBatchData fbatchdata = new ThreadBatchData(this, nbatchedrows);
                ThreadBatchData fofbatchdata = new ThreadBatchData(this, nbatchedrows);
                DoScores ds = (DoScores)obj;
                SlaveMemory.ThreadView tv = ds.tv;

                Int32[] friendsbuf = new Int32[maxfriends];
                Int32[] afriends = new Int32[maxfriends];

                DHT htfof = new DHT(maxfics); // Friends of my friends.
                DHT htf = new DHT(); // My friends, excluding from fof.

                // Start at row 1 instead of 0...
                int rowA = 1 + ds.iMyThread;
                int rowAstart = rowA;
                int ifbatch = 0;
                for ( ; ; rowA += ds.iTotalThreads)
                {
                    if (ifbatch >= nbatchedrows
                        || rowA >= realusercount)
                    {
                        fbatchdata.BatchGet(tv);
                        for (int br = 0; br < ifbatch; br++)
                        {
#if DEBUG
                            for (int i = 0; i < maxfriends; i++)
                            {
                                afriends[i] = -929292;
                            }
#endif
                            Int32 uidA = fbatchdata.GetRowData(br, afriends);
                            for (int ifriend = 0; ifriend < maxfriends; ifriend++)
                            {
                                Int32 fid = afriends[ifriend];
                                if (0 == fid)
                                {
                                    break;
                                }
#if DEBUG
                                if (-929292 == fid)
                                {
                                    throw new Exception("fid -929292");
                                }
#endif
                                htf.Add(fid);
                            }
                            {
                                int ibatch = 0;
                                int ifriend = 0;
                                //int ifriendstart = 0;
                                for (; ; ifriend++)
                                {
                                    if (ibatch >= nbatchedrows
                                        || ifriend >= maxfriends
                                        || 0 == afriends[ifriend])
                                    {
                                        fofbatchdata.BatchGet(tv);
                                        for (int bfriend = 0; bfriend < ibatch; bfriend++)
                                        {
#if DEBUG
                                            for (int i = 0; i < maxfriends; i++)
                                            {
                                                friendsbuf[i] = -929292;
                                            }
#endif
                                            int xfid = fofbatchdata.GetRowData(bfriend, friendsbuf);
                                            //int brealifriend = ifriendstart + bfriend;
                                            for (int ifof = 0; ifof < maxfriends; ifof++)
                                            {
                                                Int32 fofid = friendsbuf[ifof];
                                                if (0 == fofid)
                                                {
                                                    break;
                                                }
#if DEBUG
                                                if (-929292 == fofid)
                                                {
                                                    throw new Exception("fofid -929292");
                                                }
#endif
                                                if (fofid != uidA && !htf.ContainsKey(fofid))
                                                {
#if DEBUG
                                                    if (uidA == 17 && fofid == 10)
                                                    {
                                                        int i33 = 33 + 33;
                                                    }
#endif
                                                    // Fof only if not self and not one of my direct friends.
                                                    htfof.Add(fofid, xfid);
                                                }
                                            }
                                        }
                                        if (ifriend >= maxfriends
                                            || 0 == afriends[ifriend])
                                        {
                                            break;
                                        }
                                        ibatch = 0;
                                        //ifriendstart = ifriend + 1;
                                    }

                                    Int32 fid = afriends[ifriend];
                                    if (0 == fid)
                                    {
                                        break;
                                    }
#if DEBUG
                                    if (-929292 == fid)
                                    {
                                        throw new Exception("fid -929292");
                                    }
#endif
                                    //int frow = GetRowIDFromUserID(fid); // Row index of fid. -1 if not found (0 is valid).
                                    int frow = GetRowIDFromUserIDSameRange(fid, uidA); // Row index of fid in same id range as uidA. -1 if not found (0 is valid).
                                    if (-1 != frow)
                                    {
                                        fofbatchdata.BatchRow(ibatch++, frow);
                                    }
                                }
                            }

                            htf.DeepClean(); // !
                            // Count up friend dupes.
                            DHTSlot[] htslots = htfof.slots;
                            foreach (int i in htfof.dirtyslots)
                            {
                                int score = htslots[i].score; // 1 is base score.
                                if (score >= minscores)
                                {
                                    if (null == ds.sw)
                                    {
                                        int ior = FindOutputRange(uidA);
                                        if (-1 == ior)
                                        {
                                            noranges++;
                                        }
                                        else
                                        {
                                            lock (outranges[ior].sw)
                                            {
                                                int nfics = score;
                                                if (score > maxfics)
                                                {
                                                    nfics = maxfics;
                                                }
                                                WriteOutputLine(outranges[ior].sw, uidA, htslots[i].key, score, htslots[i].values, nfics);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        int nfics = score;
                                        if (score > maxfics)
                                        {
                                            nfics = maxfics;
                                        }
                                        WriteOutputLine(ds.sw, uidA, htslots[i].key, score, htslots[i].values, nfics);
                                    }
                                }
                                htfof.UnsetSlot(i); // Deep clean pt 1/2.
                            }
                            htfof.ZeroDirty(); // Deep clean pt 2/2.
                        }

                        if (rowA >= realusercount)
                        {
                            break;
                        }
                        ifbatch = 0;
                        rowAstart = rowA + ds.iTotalThreads;
                    }

                    fbatchdata.BatchRow(ifbatch++, rowA);
                }

                if (null != ds.sw)
                {
                    ds.sw.Flush();
                }
            }
            catch (Exception e)
            {
                if (null == goterr)
                {
                    goterr = new Exception("Processing error: tofofscorespartthreadproc catch-all exception", e);
                }
                LogLine("\r\nProcessing error: tofofscorespartthreadproc catch-all exception: " + e.ToString() + "\r\n");
            }
        }


        static void WriteOutputLine(System.IO.StreamWriter w, int uid, int rid, int score, IList<Int32> fics, int ficscount)
        {
            w.Write(uid);
            w.Write(',');
            w.Write(rid);
            w.Write(',');
            w.Write(score);
            for (int i = 0; i < ficscount; i++)
            {
                w.Write(',');
                w.Write(fics[i]);
            }
            w.Write(w.NewLine);
        }


        public void ToFofScores(int iTotalThreads, IList<int> iMyThreads)
        {
            ToFofScores(iTotalThreads, iMyThreads, null);
        }


        // Note: iTotalThreads is >= ThreadCount; it is considering multiple passes.
        public void ToFofScores(int iTotalThreads, IList<int> iMyThreads, string base_output_filename)
        {
            LogLine("---- RectangularFriendsListsHugeMemory.ToFofScores ----");

            if (null == base_output_filename)
            {
                if (null == outranges)
                {
                    throw new Exception("No output file specified");
                }
            }

            if (iMyThreads.Count != nthreads)
            {
                throw new Exception("ToFofScores: iMyThreads.Count is not the same as ThreadCount");
            }

            List<DoScores> threads = new List<DoScores>(nthreads);
            for (int i = 0; i != iMyThreads.Count; i++)
            {
                DoScores ds = new DoScores();
                ds.tv = smtable.ThreadViews[i];
                ds.iTotalThreads = iTotalThreads;
                ds.iMyThread = iMyThreads[i];
                if (null != base_output_filename)
                {
                    ds.sw = System.IO.File.CreateText(base_output_filename + ".thread" + ds.iMyThread.ToString() + ".txt");
                }
                System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(tofofscorespartthreadproc));
                thd.Name = "CFO_ToFofScores_" + i.ToString();
                ds.thd = thd;
                thd.Start(ds);
                threads.Add(ds);
            }
            for (int i = 0; i != iMyThreads.Count; i++)
            {
                threads[i].thd.Join();
                if (null != threads[i].sw)
                {
                    threads[i].sw.Close();
                }
            }
            if (null != outranges)
            {
                foreach (OutRange or in outranges)
                {
                    or.sw.Close();
                }
            }
            if (null != goterr)
            {
                Exception e = goterr;
                goterr = null;
                throw e;
            }

#if DEBUG
            if (smtable._batchspilled)
            {
                LogLine("RectangularFriendsListsHugeMemory.ToFofScores: SlaveMemory batching spilled to another slave");
                smtable._batchspilled = false;
            }
#endif

            //LogLine("Total exceptions caught: " + exceptioncount.ToString());

            LogLine("---- RectangularFriendsListsHugeMemory processing done ----");
        }


        public int SlaveCount
        {
            get
            {
                return nslaves;
            }

            set
            {
                nslaves = value;
            }
        }


        public int ThreadCount
        {
            get
            {
                return nthreads;
            }

            set
            {
                nthreads = value;
            }
        }


        internal struct UserIDRange
        {
            int minID, maxID;
            OneDInt32 usertotableindex;


            public UserIDRange(int minID, int maxID)
            {
                this.minID = minID;
                this.maxID = maxID;
                usertotableindex = new OneDInt32(maxID - minID + 1);
                /* Now 0 (default) is treated as -1.
                for (int i = 0; i != usertotableindex.Length; i++)
                {
                    usertotableindex[i] = -1;
                }
                 * */
            }


            // Returns true if 'id' falls in this range and was added; false if out of range.
            public bool Add(int id, int value)
            {
                if (id >= minID && id <= maxID)
                {
                    usertotableindex[id - minID] = value;
                    return true;
                }
                return false;
            }


            // Returns the value if in range, or missingvalue if not in range.
            public int Lookup(int id, int missingvalue)
            {
                if (id >= minID && id <= maxID)
                {
                    return usertotableindex[id - minID];
                }
                return missingvalue;
            }


            public bool IsInRange(int id)
            {
                return id >= minID && id <= maxID;
            }
        }


        public void AddUserIDRange(int minID, int maxID)
        {
            usertotableranges.Add(new UserIDRange(minID, maxID));
        }


        public int BatchedRowsPerThread
        {
            get
            {
                return nbatchedrows;
            }

            set
            {
                if (value <= 0)
                {
                    throw new Exception("Invalid BatchedRowsPerThread value: " + value.ToString());
                }
                nbatchedrows = value;
            }
        }


        public string ObjectName
        {
            get
            {
                return objname;
            }

            set
            {
                objname = value;
            }
        }


        public void SetMaxUserCount(int max)
        {
            if (max < 1)
            {
                throw new Exception("Bad SetMaxUserCount: " + max.ToString());
            }
            usercount = max;
        }


        int nslaves = 3;
        int nthreads = 16;
        int usercount = 100000;
        int realusercount = 0;
        int maxfriends = 28;
        SlaveMemory smtable;
        int minscores = 1;
        int numskippedlines;
        List<UserIDRange> usertotableranges = new List<UserIDRange>(8);
        int nbatchedrows = 128;
        string objname = "RFL_" + DateTime.Now.Ticks.ToString();
        Exception goterr = null;


        internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "cfolog");

        public static void LogLine(string line)
        {
            //lock (typeof(XLog))
            logmutex.WaitOne();
            try
            {
                System.IO.StreamWriter fstm = System.IO.File.AppendText("cfolog.txt");
                fstm.WriteLine("[{0} {1}ms] {2}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, line);
                fstm.Close();
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }

    }
}

