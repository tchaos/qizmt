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

#if DEBUG
//#define DEBUG_TESTDFSSTREAMCOOKING
#endif

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.AELight;

namespace MySpace.DataMining.DistributedObjects
{

    public class DfsStream : System.IO.Stream
    {

        int ReplicateStartIndex;
        System.Threading.Mutex Mutex;
        string surrogatedir;
        dfs.DfsFile.FileNode[] nodes;
        int reclen = -1;

        public int RetryTimeout;
        public int RetryCount;

        public DfsStream(string dfsfile, bool PreserveOrder, bool MachineLock)
        {
            if (MachineLock)
            {
                this.Mutex = new System.Threading.Mutex(false, "DfsStream{24A86864-EED6-4680-AB0E-3BDE97262339}");
                this.Mutex.WaitOne();
            }

            ReplicateStartIndex = StaticGlobals.Qizmt_BlockID;
            surrogatedir = Surrogate.NetworkPathForHost(Surrogate.MasterHost);

            dfs dc = dfs.ReadDfsConfig_unlocked(surrogatedir + @"\" + dfs.DFSXMLNAME);

            this.RetryTimeout = dc.slave.CookTimeout;
            this.RetryCount = dc.slave.CookRetries;

            dfs.DfsFile df = dc.FindAny(dfsfile);
            if (null == df)
            {
                throw new System.IO.FileNotFoundException("DFS file '" + dfsfile + "' not found", dfsfile);
            }

            if (0 != string.Compare(DfsFileTypes.NORMAL, df.Type, StringComparison.OrdinalIgnoreCase)
                && 0 != string.Compare(DfsFileTypes.BINARY_RECT, df.Type, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("DFS file '" + df.Name + "' cannot be opened because file is of type " + df.Type);
            }

            this.reclen = df.RecordLength;

            nodes = df.Nodes.ToArray();
            if (!PreserveOrder)
            {
                Random rnd = new Random(unchecked(
                    System.Threading.Thread.CurrentThread.ManagedThreadId
                    + DateTime.Now.Millisecond * 351
                    + ReplicateStartIndex + nodes.Length * 6131));
                for (int i = 0; i < nodes.Length; i++)
                {
                    int ridx = rnd.Next(0, nodes.Length);
                    dfs.DfsFile.FileNode tmpnode = nodes[i];
                    nodes[i] = nodes[ridx];
                    nodes[ridx] = tmpnode;
                }
            }

        }

        public DfsStream(string dfsfile, bool PreserveOrder)
            : this(dfsfile, PreserveOrder, true)
        {
        }

        public DfsStream(string dfsfile)
            : this(dfsfile, true, true)
        {
        }

        public int RecordLength
        {
            get
            {
                return reclen;
            }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }


        public void CreateSequenceFile(string name)
        {
            using (System.IO.StreamWriter sf = System.IO.File.CreateText(name))
            {
                sf.WriteLine("*sequence*");
                for (int i = 0; i < nodes.Length; i++)
                {
                    sf.WriteLine(Surrogate.NetworkPathForHost(
                        nodes[i].Host.Split(';')[0]) + @"\" + nodes[i].Name);
                }
            }
        }


        public bool EndOfStream
        {
            get
            {
                return seqend;
            }
        }


        // From 0 to NumberOfParts-1
        public long CurrentPart
        {
            get
            {
                if (whichseqreader < 0)
                {
                    return 0;
                }
                if (whichseqreader >= nodes.Length)
                {
                    return nodes.Length - 1;
                }
                return whichseqreader;
            }
        }

        // From 0 to NumberOfParts-1
        // partposition is the position within the part, defaults to 0.
        // partposition does not include header.
        // Note: partposition can seek to the middle of a record or line.
        public void SeekToPart(long partindex, long partposition)
        {
            if (partindex >= nodes.Length || partindex < 0)
            {
                throw new IndexOutOfRangeException("Part index out of range");
            }
            _seekseq((int)partindex);
            if (0 != partposition)
            {
                curpartpos = curseqfile.Seek((long)curpartheadersize + partposition,
                    System.IO.SeekOrigin.Begin);
            }
        }

        public void SeekToPart(long partindex)
        {
            SeekToPart(partindex, 0);
        }

        public long NumberOfParts
        {
            get
            {
                return nodes.Length;
            }
        }

        // Does not include header.
        public long CurrentPartPosition
        {
            get
            {
                if (whichseqreader < 0)
                {
                    return 0;
                }
                if (whichseqreader >= nodes.Length)
                {
                    return curpartfulllength - curpartheadersize;
                }
                //return curseqfile.Position - curpartheadersize;
#if DEBUGmore
                if(curseqfile.Position != curpartpos)
                {
                    throw new Exception("DEBUG:  CurrentPartPosition: (curseqfile.Position != curpartpos)");
                }
#endif
                return curpartpos - curpartheadersize;
            }
        }

        // Does not include header.
        public long CurrentPartLength
        {
            get
            {
                return curpartfulllength - curpartheadersize;
            }
        }

#if DEBUG_TESTDFSSTREAMCOOKING
        static int _ct_readcounter = 0;
#endif

        public override int Read(byte[] array, int offset, int count)
        {
#if DEBUG_TESTDFSSTREAMCOOKING
            bool _ct_nextseq = false;
            bool _ct_reopen = false;
#endif
            //--------COOKING
            int cooking_cooksremain = this.RetryCount;
            bool cooking_reopen = false;
            //--------COOKING
            int rd = 0;
            for (; ; )
            {
                try
                {
                    if (cooking_reopen)
                    {
                        _seqreopen();
#if DEBUG_TESTDFSSTREAMCOOKING
                        if (!_ct_reopen)
                        {
                            _ct_reopen = true;
                            throw new System.IO.IOException("DEBUG: test DfsStream cooking (_ct_reopen)");
                        }
#endif
                        cooking_reopen = false;
                    }
                    if (-1 == whichseqreader)
                    {
                        _seqinit();
                    }
                    if (curpartpos == curpartfulllength)
                    {
                        _movenextseq();
                    }
                    while (rd < count)
                    {
                        if (seqend)
                        {
                            break;
                        }
                        int onerd = curseqfile.Read(array, offset + rd, count - rd);
#if DEBUG_TESTDFSSTREAMCOOKING
                        if (++_ct_readcounter == 2)
                        {
                            throw new System.IO.IOException("DEBUG: test DfsStream cooking (_ct_readcounter)");
                        }
#endif
                        curpartpos += onerd;
                        rd += onerd;
                        if (rd < count)
                        {
                            _movenextseq();
#if DEBUG_TESTDFSSTREAMCOOKING
                            if (!_ct_nextseq)
                            {
                                _ct_nextseq = true;
                                throw new System.IO.IOException("DEBUG: test DfsStream cooking (_ct_nextseq)");
                            }
#endif
                            continue;
                        }
                        break;
                    }
                    break;
                }
                catch (System.IO.IOException e)
                {
                    //--------COOKING
                    bool firstcook = cooking_cooksremain == this.RetryCount;
                    if (cooking_cooksremain-- <= 0)
                    {
                        throw new System.IO.IOException("cooked too many times (retries="
                            + this.RetryCount.ToString()
                            + "; timeout=" + this.RetryTimeout.ToString()
                            + ") on " + System.Net.Dns.GetHostName(), e);
                    }
                    if (null != curseqfile)
                    {
                        curseqfile.Close();
                    }
                    System.Threading.Thread.Sleep(IOUtils.RealRetryTimeout(this.RetryTimeout));
                    cooking_reopen = true;
                    if (firstcook)
                    {
                        try
                        {
                            errorlog("cooking started (retries=" + this.RetryCount.ToString()
                                + "; timeout=" + this.RetryTimeout.ToString()
                                + ") on " + System.Net.Dns.GetHostName()
                                + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod());
                        }
                        catch
                        {
                        }
                    }
                    else
                    {
                        try
                        {
                            if ((this.RetryCount - (cooking_cooksremain + 1)) % 60 == 0)
                            {
                                errorlog("cooking continues with " + cooking_cooksremain
                                    + " more retries (retries=" + this.RetryCount.ToString()
                                    + "; timeout=" + this.RetryTimeout.ToString()
                                    + ") on " + System.Net.Dns.GetHostName()
                                    + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod()
                                    + Environment.NewLine + e.ToString());
                            }
                        }
                        catch
                        {
                        }
                    }
                    //--------COOKING
                }
            }
#if DEBUGmore
            if(null != curseqfile)
            {
                if(curpartpos != curseqfile.Position)
                {
                    throw new Exception("DEBUG:  Read: (curpartpos{" + curpartpos + "} != curseqfile.Position{" + curseqfile.Position + "})");
                }
                if(curpartfulllength != curseqfile.Length)
                {
                    throw new Exception("DEBUG:  Read: (curpartfulllength != curseqfile.Length)");
                }
            }
            if(curpartpos > curpartfulllength || curpartpos < 0)
            {
                throw new Exception("DEBUG:  Read: (curpartpos > curpartfulllength || curpartpos < 0)");
            }
#endif
            return rd;
        }

        byte[] _rb = null;

        public override int ReadByte()
        {
            if (null == _rb)
            {
                _rb = new byte[32];
            }
            if (1 != this.Read(_rb, 0, 1))
            {
                return -1;
            }
            return _rb[0];
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                Seek(value, System.IO.SeekOrigin.Begin);
            }
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
                curseqfile = null;
            }
            base.Dispose(disposing);
            if (null != this.Mutex)
            {
                this.Mutex.ReleaseMutex();
                this.Mutex.Close();
                this.Mutex = null;
            }
        }


        System.IO.Stream curseqfile = null;
        int whichseqreader = -1;
        int curpartheadersize = 0;
        long curpartfulllength = 0; // Includes header size.
        long curpartpos = 0; // Includes header size.


        void _seqinit()
        {
            curpartpos = 0;
            if (0 != nodes.Length)
            {
                whichseqreader = 0;
                curseqfile = GetStreamFromDfsNode(nodes[0], true);
            }
        }

        void _skipseq()
        {
            if (null != curseqfile)
            {

                if (whichseqreader < nodes.Length)
                {
                    whichseqreader++;
                    if (whichseqreader < nodes.Length)
                    {
                        curseqfile = GetStreamFromDfsNode(nodes[whichseqreader], true);
                        return;
                    }
                }
            }
            curseqfile = null;
        }

        // Note: this can be called before _seqinit.
        void _seekseq(int seektopart)
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
            }
            whichseqreader = seektopart;
            curseqfile = GetStreamFromDfsNode(nodes[whichseqreader], true);
        }

        void _movenextseq()
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
                _skipseq();
            }
        }

        void _seqreopen()
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
            }
            if (0 == curpartpos)
            {
                curseqfile = GetStreamFromDfsNode(nodes[whichseqreader], true);
            }
            else
            {
                curseqfile = _OpenStream(nodes[whichseqreader]);
                curseqfile.Position = curpartpos;
            }
        }

        System.IO.Stream _OpenStream(dfs.DfsFile.FileNode node)
        {
            string[] nodehosts = node.Host.Split(';');
            string[] fullnames = new string[nodehosts.Length];
            int ReplicateCurrentIndex = ReplicateStartIndex;
            for (int i = 0; i < fullnames.Length; i++)
            {
                fullnames[i] = Surrogate.NetworkPathForHost(
                    nodehosts[ReplicateCurrentIndex % nodehosts.Length])
                    + @"\" + node.Name;
                ReplicateCurrentIndex++;
            }
            return new MySpace.DataMining.AELight.DfsFileNodeStream(fullnames, true,
                System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x400 * 4);
        }

        protected System.IO.Stream GetStreamFromDfsNode(dfs.DfsFile.FileNode node, bool skipdfschunkheader)
        {
            curpartheadersize = 0;
            curpartpos = 0;
            bool anydata = false;
            System.IO.Stream stm = _OpenStream(node);
            curpartfulllength = stm.Length;
            anydata = 0 != curpartfulllength;
            if (skipdfschunkheader && anydata)
            {
                if (null == _rb)
                {
                    _rb = new byte[32];
                }
                _StreamReadExact(stm, _rb, 4);
                int hlen = Entry.BytesToInt(_rb);
                if (hlen > 4)
                {
                    int hremain = hlen - 4;
                    if (hremain > _rb.Length)
                    {
                        _rb = new byte[hremain];
                    }
                    _StreamReadExact(stm, _rb, hremain);
                }
                curpartheadersize = hlen;
            }
            curpartpos = curpartheadersize;
            return stm;
        }

        static void _StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            while (sofar < len)
            {
                int xread = stm.Read(buf, sofar, len - sofar);
                if (xread <= 0)
                {
                    throw new System.IO.IOException("Unable to read from stream");
                }
                sofar += xread;
            }
        }

        bool seqend
        {
            get
            {
                return whichseqreader >= nodes.Length;
            }
        }


        static System.Threading.Mutex errorlogmutex = new System.Threading.Mutex(false, "distobjlog");

        static void errorlog(string line)
        {
            string fn = "slave-log.txt";
            try
            {
                errorlogmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                using (System.IO.StreamWriter fstm = System.IO.File.AppendText(fn))
                {
                    string build = "";
                    try
                    {
                        System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                        System.Reflection.AssemblyName an = asm.GetName();
                        int bn = an.Version.Build;
                        int rv = an.Version.Revision;
                        build = "(build:" + bn.ToString() + "." + rv.ToString() + ") ";
                    }
                    catch
                    {
                    }
                    fstm.WriteLine(@"[{0} {1}ms] \\{2} DistributedObjectsSlave error: {3}{4} [DfsStream]",
                        System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, System.Net.Dns.GetHostName(), build, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.WriteLine();
                }
            }
            catch
            {
            }
            finally
            {
                errorlogmutex.ReleaseMutex();
            }
        }


    }

}
