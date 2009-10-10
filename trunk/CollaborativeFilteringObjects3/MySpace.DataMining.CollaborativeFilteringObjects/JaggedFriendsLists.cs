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
    public class JaggedFriendsLists
    {
        public JaggedFriendsLists()
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


        public struct UserData
        {
            internal static UserData Create(OneDInt32 table, long rowid)
            {
                UserData ud;
                ud.table = table;
                ud.rowid = rowid;
                return ud;
            }


            internal static bool DoesUserFit(OneDInt32 table, long rowid, IList<Int32> friends)
            {
                return rowid + 1 + 1 + friends.Count <= table.LongLength;
            }


            // This rowid + header + friends data.
            // Note: RowID is actually just an index in the table.
            public long GetNextRowID()
            {
                return this.rowid + 1 + 1 + table[this.rowid + 1];
            }


            public int NumberOfFriends
            {
                get
                {
                    return table[this.rowid + 1];
                }
            }


            public Int32 UserID
            {
                get
                {
                    return table[this.rowid];
                }
            }

            internal void _setuserid(Int32 uid)
            {
#if DEBUG
                if (0 != table[this.rowid])
                {
                    Console.WriteLine("UserData._setuserid: replacing uid; to " + uid.ToString() + " from " + table[this.rowid].ToString());
                }
#endif
                table[this.rowid] = uid;
            }


            public Int32 GetFriendAt(int i)
            {
#if DEBUG
                if (i < 0 || i >= NumberOfFriends)
                {
                    throw new IndexOutOfRangeException("GetFriendAt(" + i.ToString() + ") out of range; only " + NumberOfFriends.ToString() + " friends");
                }
#endif
                return table[this.rowid + 1 + 1 + i];
            }


            internal void _setfriends(IList<Int32> fids)
            {
                int count = fids.Count;
                table[this.rowid + 1] = count;
                for (int i = 0; i < count; i++)
                {
#if DEBUG
                    if (0 != table[this.rowid + 1 + 1 + i])
                    {
                        Console.WriteLine("UserData._setfriends: replacing uid " + UserID.ToString() + "'s friend from " + table[this.rowid + 1 + 1 + i].ToString() + " to " + fids[i].ToString());
                    }
#endif
                    table[this.rowid + 1 + 1 + i] = fids[i];
                }
            }


            OneDInt32 table;
            long rowid;
        }


        // Note: RowID is actually just an index in the table.
        public Int32 GetUserIDFromRowID(long rowid)
        {
            UserData ud = UserData.Create(this.table, rowid);
            return ud.UserID;
        }


        public long GetRowIDFromUserID(Int32 userid)
        {
            for (int i = 0; i != usertotableranges.Count; i++)
            {
                long x = usertotableranges[i].Lookup(userid, -1);
                if (-1 != i)
                {
                    return x;
                }
            }
            return -1;
        }

        public long GetRowIDFromUserIDSameRange(Int32 userid, Int32 rangewith)
        {
            for (int i = 0; i != usertotableranges.Count; i++)
            {
                if (usertotableranges[i].IsInRange(rangewith))
                {
                    return usertotableranges[i].Lookup(userid, -1);
                }
            }
            return -1;
        }


        protected bool SetRowIDForUserID(Int32 userid, long rowid)
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
            public long nexttableoffset = 0; // Offset into integers (not bytes)
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
                    List<Int32> friendsbuf = new List<int>(256);
                    byte[] linebuf = new byte[32];
                    // Assumes trailing friend-IDs are 0.
                    while (!hittablelimit)
                    {
                        if (slc.nexttableoffset + 1 + 1 + 1 + 1 >= table.LongLength) // Need room for header and a couple friends.
                        {
                            hittablelimit = true;
                            break;
                        }
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
                                Int32 previd = 0;
                                for (; ; )
                                {
                                    linnum++;
                                    Int32 uid, fid;
#if DEBUG
                                    if (_sleep)
                                    {
                                        System.Threading.Thread.Sleep(DateTime.Now.Millisecond % 256);
                                    }
#endif
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
                                            lock (slc)
                                            {
                                                if (!UserData.DoesUserFit(this.table, slc.nexttableoffset, friendsbuf))
                                                {
                                                    hittablelimit = true;
                                                    break;
                                                }
                                                UserData ud = UserData.Create(this.table, slc.nexttableoffset);
                                                SetRowIDForUserID(previd, slc.nexttableoffset);
                                                ud._setuserid(previd);
                                                ud._setfriends(friendsbuf);
                                                slc.nexttableoffset = ud.GetNextRowID();
                                                realusercount++;
                                            }
                                            friendsbuf.Clear();
                                        }
                                    }
                                    if (0 == uid)
                                    {
                                        break;
                                    }
                                    if (friendsbuf.Count < maxfriends)
                                    {
                                        friendsbuf.Add(fid);
                                    }
                                    previd = uid;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            try
                            {
                                LogLine("\r\nLoad error: " + e.ToString() + " file: " + (null == curfilename ? "<null>" : "'" + curfilename + "'") + "; line: " + linnum.ToString() + "; table offset: " + slc.nexttableoffset.ToString() + "; exception: " + e.ToString() + "\r\n(Skipping the rest of this file)\r\n");
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
                }
            }
            catch (Exception e)
            {
                LogLine("\r\nLoad error: loadthreadproc catch-all exception: " + e.ToString() + "\r\n");
            }
        }


        public void Load(int filebuffersize, string file, int loadthreadcount)
        {
            LogLine("---- JaggedFriendsLists.Load ----");

            if (loadthreadcount < 1)
            {
                throw new Exception("Bad load thread count");
            }

            //usercount = GetUniqueUserCount(file);

            if (table.IsNull)
            {
                //table = new OneDInt32(1024 * 1024 * 1024 / 2); // ...
                throw new Exception("Table is null; must call SetUserTableSize");
            }
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

            if (hittablelimit)
            {
                //GotException(new Exception("table limit reached"));
                LogLine("table limit reached");
            }

            LogLine("Load finished: number of rows: " + realusercount.ToString());
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


        class DoScores
        {
            internal int iTotalThreads;
            internal int iMyThread;
            internal System.IO.StreamWriter sw;
            internal System.Threading.Thread thd;
            internal int fofmax;
            internal int fofhashsize;
            internal int fhashsize;
        }


        void tofofscorespartthreadproc(object obj)
        {
            try
            {
                DoScores ds = (DoScores)obj;
                System.IO.StreamWriter sw = ds.sw;
                int fofmax = ds.fofmax;

                DHT htfof = new DHT(ds.fofmax, ds.fofhashsize); // Friends of my friends.
                DHT htf = new DHT(maxfriends, ds.fhashsize); // My friends, excluding from fof.

                long rowA = 0;
                int curusercount = 0;
                for (int i = 0; i < ds.iMyThread; i++)
                {
                    if (curusercount >= realusercount)
                    {
                        break;
                    }
                    UserData ud = UserData.Create(this.table, rowA);
                    rowA = ud.GetNextRowID();
                    curusercount++;
                }

                while (curusercount < realusercount)
                {
                    Int32 uidA = GetUserIDFromRowID(rowA);
                    UserData udA = UserData.Create(this.table, rowA);
                    for (int ifriend = 0; ifriend < udA.NumberOfFriends; ifriend++)
                    {
                        Int32 fid = udA.GetFriendAt(ifriend);
                        htf.Add(fid);
                    }
                    int fofcount = 0;
                    for (int ifriend = 0; ifriend < udA.NumberOfFriends; ifriend++)
                    {
#if DEBUG
                        if (_sleep)
                        {
                            System.Threading.Thread.Sleep(DateTime.Now.Millisecond % 128);
                        }
#endif
                        Int32 fid = udA.GetFriendAt(ifriend);
                        //int frow = GetRowIDFromUserID(fid); // Row index of fid. -1 if not found (0 is valid).
                        long frowid = GetRowIDFromUserIDSameRange(fid, uidA); // Row index of fid in same id range as uidA. -1 if not found (0 is valid).
                        if (-1 != frowid)
                        {
                            UserData udf = UserData.Create(this.table, frowid);
                            for (int ifof = 0; ifof < udf.NumberOfFriends; ifof++) // fof index.
                            {
                                Int32 fofid = udf.GetFriendAt(ifof);
                                if (fofid != uidA && !htf.ContainsKey(fofid))
                                {
                                    // Fof only if not self and not one of my direct friends.
                                    htfof.Add(fofid);
                                    if (++fofcount >= fofmax)
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        if (fofcount >= fofmax)
                        {
                            break;
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
                            sw.Write(uidA);
                            sw.Write(',');
                            sw.Write(htslots[i].key);
                            sw.Write(',');
                            sw.Write(score);
                            sw.Write(ds.sw.NewLine);
                        }
                        htfof.UnsetSlot(i); // Deep clean pt 1/2.
                    }
                    htfof.ZeroDirty(); // Deep clean pt 2/2.

                    for (int i = 0; i < ds.iTotalThreads; i++)
                    {
                        if (curusercount >= realusercount)
                        {
                            break;
                        }
                        UserData ud = UserData.Create(this.table, rowA);
                        rowA = ud.GetNextRowID();
                        curusercount++;
                    }
                }

                sw.Flush();
            }
            catch (Exception e)
            {
                LogLine("\r\nProcessing error: tofofscorespartthreadproc catch-all exception: " + e.ToString() + "\r\n");
            }
        }


        // fofmax is max number of friends-of-friends.
        // fofhashsize should be prime near (fofmax * 10)
        // fhashsize should be prime near (MaxFriends * 10)
        public void ToFofScores(int iTotalThreads, IList<int> iMyThreads, string base_output_filename, int fofmax, int fofhashsize, int fhashsize)
        {
            LogLine("---- JaggedFriendsLists.ToFofScores ----");

            List<DoScores> threads = new List<DoScores>(iMyThreads.Count);
            for (int i = 0; i != iMyThreads.Count; i++)
            {
                DoScores ds = new DoScores();
                ds.iTotalThreads = iTotalThreads;
                ds.iMyThread = iMyThreads[i];
                ds.sw = System.IO.File.CreateText(base_output_filename + ".thread" + ds.iMyThread.ToString() + ".txt");
                ds.fofmax = fofmax;
                ds.fofhashsize = fofhashsize;
                ds.fhashsize = fhashsize;
                System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(tofofscorespartthreadproc));
                thd.Name = "CFO_ToFofScores_" + i.ToString();
                ds.thd = thd;
                thd.Start(ds);
                threads.Add(ds);
            }
            for (int i = 0; i != iMyThreads.Count; i++)
            {
                threads[i].thd.Join();
                threads[i].sw.Close();
            }

            //LogLine("Total exceptions caught: " + exceptioncount.ToString());

            LogLine("---- JaggedFriendsLists processing done ----");
        }

        public void ToFofScores(int iTotalThreads, IList<int> iMyThreads, string base_output_filename)
        {
            ToFofScores(iTotalThreads, iMyThreads, base_output_filename, 28 * 28, 7853, 9973);
        }


        public void SetUserTableSize(long tablesize)
        {
            if (tablesize <= 0)
            {
                throw new Exception("Invalid table size: " + tablesize.ToString());
            }
            table = new OneDInt32(tablesize / 4 + ((0 == (tablesize % 4)) ? 0 : 1));
        }


        internal struct UserIDRange
        {
            int minID, maxID;
            OneDInt64 usertotableindex;


            public UserIDRange(int minID, int maxID)
            {
                this.minID = minID;
                this.maxID = maxID;
                usertotableindex = new OneDInt64(maxID - minID + 1);
                for (int i = 0; i != usertotableindex.Length; i++)
                {
                    usertotableindex[i] = -1;
                }
            }


            // Returns true if 'id' falls in this range and was added; false if out of range.
            public bool Add(int id, long value)
            {
                if (id >= minID && id <= maxID)
                {
#if DEBUG
                    //SetRowIDForUserID
                    if (-1 != usertotableindex[id - minID])
                    {
                        Console.WriteLine("UserIDRange.Add: replacing ID " + id.ToString() + "; from " + usertotableindex[id - minID].ToString() + " to " + value.ToString());
                    }
#endif
                    usertotableindex[id - minID] = value;
                    return true;
                }
                return false;
            }


            // Returns the value if in range, or missingvalue if not in range.
            public long Lookup(int id, int missingvalue)
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


        int realusercount = 0;
        int maxfriends = 1000;
        OneDInt32 table;
        int minscores = 1;
        int numskippedlines;
        List<UserIDRange> usertotableranges = new List<UserIDRange>(8);
        bool hittablelimit = false;

#if DEBUG
        public bool _sleep = false;
#endif


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

