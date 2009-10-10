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
    public class RectangularFriendsLists
    {
        public RectangularFriendsLists(int filebuffersize, string file, int loadthreadcount)
        {
            Load(filebuffersize, file, loadthreadcount);
        }

        public RectangularFriendsLists(int filebuffersize, string file)
        {
            Load(filebuffersize, file);
        }

        public RectangularFriendsLists()
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


        public Int32 GetUserIDFromRowID(int rowid)
        {
            return table[rowid, 0];
        }


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
                    if ('\r' == iby || '\n' == iby)
                    {
                        if (0 != offset)
                        {
                            break;
                        }
                        continue;
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


        void loadthreadproc(object obj)
        {
            try
            {
#if DEBUG
#else
                unchecked
#endif
                {
                    SharedLoadContext slc = (SharedLoadContext)obj;
                    SeqFileStream.SeqFileStreamReader streamqueue = slc.streamqueue;
                    Int32[] friendsbuf = new Int32[maxfriends];
                    byte[] linebuf = new byte[32];
                    // Assumes trailing friend-IDs are 0.
                    while (slc.nexttableindex < usercount)
                    {
                        long linnum = 0;
                        string curfilename = null;
                        System.IO.Stream stm;
                        lock (streamqueue)
                        {
                            stm = streamqueue.GetNextStream(out curfilename);
                        }
                        if (null == stm)
                        {
                            break;
                        }
                        try
                        {
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
                                            int myfriendindex = friendindex;
                                            friendindex = 0;
                                            lock (slc)
                                            {
                                                SetRowIDForUserID(previd, slc.nexttableindex);
                                                table[slc.nexttableindex, 0] = previd;
                                                for (int i = 0; i != myfriendindex; i++)
                                                {
                                                    table[slc.nexttableindex, 1 + i] = friendsbuf[i];
                                                }
                                                slc.nexttableindex++;
                                            }
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
                            stm.Close();
                        }
                    }

                    realusercount = slc.nexttableindex;
                }
            }
            catch (Exception e)
            {
                LogLine("\r\nLoad error: loadthreadproc catch-all exception: " + e.ToString() + "\r\n");
            }
        }


        public void Load(int filebuffersize, ISequenceInput input, int loadthreadcount)
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

                Load(filebuffersize, seqfn, loadthreadcount);
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

        public void Load(int filebuffersize, ISequenceInput input)
        {
            Load(filebuffersize, input, 16);
        }

        public void Load(int filebuffersize, string file, int loadthreadcount)
        {
            LogLine("---- RectangularFriendsLists.Load ----");

            if (loadthreadcount < 1)
            {
                throw new Exception("Bad load thread count");
            }

            //usercount = GetUniqueUserCount(file);

            table = new TwoDInt32(usercount, 1 + maxfriends);
            numskippedlines = 0;

            if (0 == usertotableranges.Count)
            {
                AddUserIDRange(0, 400000000 - 1);
            }

            using (MySpace.DataMining.SeqFileStream.SeqFileStreamReader stm = new MySpace.DataMining.SeqFileStream.SeqFileStreamReader(file, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, filebuffersize))
            {
                SharedLoadContext slc = new SharedLoadContext();
                slc.streamqueue = stm;
                List<System.Threading.Thread> threads = new List<System.Threading.Thread>(loadthreadcount);
                for (int i = 0; i != loadthreadcount; i++)
                {
                    System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(loadthreadproc));
                    thd.Name = "CFO_Load_" + i.ToString();
                    thd.Start(slc);
                    threads.Add(thd);
                }
                for (int i = 0; i != loadthreadcount; i++)
                {
                    threads[i].Join();
                }
            }

            if (realusercount >= usercount)
            {
                //GotException(new Exception("row limit reached"));
                LogLine("row limit reached");
            }

            LogLine("Load finished: number of rows: " + realusercount.ToString() + " (max " + usercount.ToString() + ")");
        }

        public void Load(int filebuffersize, string file)
        {
            Load(filebuffersize, file, 16);
        }


        public int Count
        {
            get
            {
                return realusercount;
            }
        }


        public int Compare(int rowA, int rowB)
        {
            int result = 0;

            // fidX is offset by 1 due to user-ID.
            int fidxA = 1;
            int fidxB = 1;
            for (; ; )
            {
                Int32 fa = table[rowA, fidxA];
                Int32 fb = table[rowB, fidxB];
                if (fa == fb)
                {
                    if (0 == fa) // || 0 == fb
                    {
                        break;
                    }
                    result++;
                    fidxA++;
                    fidxB++;
                    if (fidxA > maxfriends || fidxB > maxfriends)
                    {
                        break;
                    }
                }
                else if (fa > fb)
                {
                    if (0 == fb)
                    {
                        break;
                    }
                    fidxB++;
                    if (fidxB > maxfriends)
                    {
                        break;
                    }
                }
                else //if (fa < fb)
                {
                    if (0 == fa)
                    {
                        break;
                    }
                    fidxA++;
                    if (fidxA > maxfriends)
                    {
                        break;
                    }
                }
            }

            return result;
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
        }


        void tofofscorespartthreadproc(object obj)
        {
            try
            {
                DoScores ds = (DoScores)obj;

                DHT htfof = new DHT(maxfics); // Friends of my friends.
                DHT htf = new DHT(); // My friends, excluding from fof.

                // Start at row 1 instead of 0...
                for (int rowA = 1 + ds.iMyThread; rowA < realusercount; rowA += ds.iTotalThreads)
                {
                    Int32 uidA = GetUserIDFromRowID(rowA);
                    for (int colA = 1; colA < 1 + maxfriends; colA++)
                    {
                        Int32 fid = table[rowA, colA];
                        if (0 == fid)
                        {
                            break;
                        }
                        htf.Add(fid);
                    }
                    for (int colA = 1; colA < 1 + maxfriends; colA++)
                    {
                        Int32 fid = table[rowA, colA];
                        if (0 == fid)
                        {
                            break;
                        }
                        //int frow = GetRowIDFromUserID(fid); // Row index of fid. -1 if not found (0 is valid).
                        int frow = GetRowIDFromUserIDSameRange(fid, uidA); // Row index of fid in same id range as uidA. -1 if not found (0 is valid).
                        if (-1 != frow)
                        {
                            for (int fofcol = 1; fofcol != 1 + maxfriends; fofcol++) // fof column.
                            {
                                Int32 fofid = table[frow, fofcol]; // fof user ID.
                                if (0 == fofid)
                                {
                                    break;
                                }
                                if (fofid != uidA && !htf.ContainsKey(fofid))
                                {
                                    // Fof only if not self and not one of my direct friends.
                                    htfof.Add(fofid, fid);
                                }
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

                if (null != ds.sw)
                {
                    ds.sw.Flush();
                }
            }
            catch (Exception e)
            {
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


        public void ToFofScores(int iTotalThreads, IList<int> iMyThreads, string base_output_filename)
        {
            LogLine("---- RectangularFriendsLists.ToFofScores ----");

            if (null == base_output_filename)
            {
                if (null == outranges)
                {
                    throw new Exception("No output file specified");
                }
            }

            List<DoScores> threads = new List<DoScores>(iMyThreads.Count);
            for (int i = 0; i != iMyThreads.Count; i++)
            {
                DoScores ds = new DoScores();
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

            //LogLine("Total exceptions caught: " + exceptioncount.ToString());

            LogLine("---- RectangularFriendsLists processing done ----");
        }


        public void SetMaxUserCount(int maxusers)
        {
            if (maxusers <= 0)
            {
                throw new Exception("Invalid max user count");
            }
            usercount = maxusers;
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


        int usercount = 120000000;
        int realusercount = 0;
        int maxfriends = 28;
        TwoDInt32 table;
        int minscores = 1;
        int numskippedlines;
        List<UserIDRange> usertotableranges = new List<UserIDRange>(8);


        internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "cfolog");

        public static void LogLine(string line)
        {
            try
            {
                logmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
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

