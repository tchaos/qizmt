using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Protocol
{    
    public class DfsFileNodeStream : System.IO.Stream
    {        
        public DfsFileNodeStream(string allhosts, string name, bool failover,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize, string rootdir)
        {
            if (null == allhosts || null == name)
            {
                throw new DfsFileNodeIOException("Invalid input DFS file chunk paths");
            }
            this.chosts = allhosts.Split(';');
            if (null == chosts || chosts.Length == 0 || string.IsNullOrEmpty(chosts[0]))
            {
                throw new DfsFileNodeIOException("No hosts for DFS file chunk '" + name + "'");
            }
            fcount = chosts.Length;
            this.name = name;
            this.failover = failover;

            this.fmode = fmode;
            this.faccess = faccess;
            this.fshare = fshare;
            this.fbuffersize = fbuffersize;
            this.rootdir = rootdir;

            _firstopen();
        }

        public override bool CanRead
        {
            get
            {
                try
                {
                    return stm.CanRead;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                }
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                try
                {
                    return stm.CanSeek;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                }
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                try
                {
                    return stm.CanWrite;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                }
                return false; // ...
            }
        }

        public override void Close()
        {
            stmdone = true;
            if (null != stm)
            {
                for (; ; )
                {
                    try
                    {
                        stm.Close();
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (pendingwrites)
                        {
                            throw new DfsFileNodeIOExceptionRetry("Close failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new DfsFileNodeIOException("Close failure", LastFailoverException);
                    }
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            stmdone = true;
            if (null != stm)
            {
                for (; ; )
                {
                    try
                    {
                        //stm.Dispose(disposing); // Can't.
                        stm.Dispose(); // ...
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (pendingwrites)
                        {
                            throw new DfsFileNodeIOExceptionRetry("Dispose failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new DfsFileNodeIOException("Dispose failure", LastFailoverException);
                    }
                }
            }
        }

        public override void Flush()
        {
            if (pendingwrites)
            {
                try
                {
                    stm.Flush();
                    pendingwrites = false; // !
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to flush DFS file chunk", LastFailoverException);
                }
            }
        }

        public override long Length
        {
            get
            {
                for (; ; )
                {
                    try
                    {
                        return stm.Length;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("Length: failover not enabled", LastFailoverException);
                        }
                        _dofailover("Length");
                    }
                }
            }
        }

        public override long Position
        {
            get
            {
                // Note: getting Position directly from stm would be slower and need failover.
                return stmpos;
            }
            set
            {
                this.Seek(value, System.IO.SeekOrigin.Begin); // Has failover as necessary.
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            for (; ; )
            {
                try
                {
                    int result = stm.Read(buffer, offset, count);
                    if (result > 0)
                    {
                        stmpos += result;
                    }
                    return result;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("Read: failover not enabled", LastFailoverException);
                    }
                    _dofailover("Read");
                }
            }
        }

        public override int ReadByte()
        {
            for (; ; )
            {
                try
                {
                    int result = stm.ReadByte();
                    if (result >= 0)
                    {
                        stmpos++;
                    }
                    return result;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("ReadByte: failover not enabled", LastFailoverException);
                    }
                    _dofailover("ReadByte");
                }
            }
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            for (; ; )
            {
                try
                {
                    return stmpos = stm.Seek(offset, origin);
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("Seek: failover not enabled", LastFailoverException);
                    }
                    _dofailover("Seek");
                }
            }
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (pendingwrites)
            {
                try
                {
                    stm.Write(buffer, offset, count);
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to write to DFS file chunk", LastFailoverException);
                }
            }
            else
            {
                for (; ; )
                {
                    try
                    {
                        stm.Write(buffer, offset, count);
                        pendingwrites = true; // !
                        stmpos += count;
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("Write: failover not enabled", LastFailoverException);
                        }
                        _dofailover("Write");
                    }
                }
            }
        }

        public override void WriteByte(byte value)
        {
            if (pendingwrites)
            {
                try
                {
                    stm.WriteByte(value);
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to write (WriteByte) to DFS file chunk", LastFailoverException);
                }
            }
            else
            {
                for (; ; )
                {
                    try
                    {
                        stm.WriteByte(value);
                        pendingwrites = true; // !
                        stmpos++;
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("WriteByte: failover not enabled", LastFailoverException);
                        }
                        _dofailover("WriteByte");
                    }
                }
            }
        }


        void _firstopen()
        {
            lastfailoverindex = -1;// fcount - 1;
            stmpos = 0;
            pendingwrites = false;
            xfailovers = 0;
            stmdone = false;
            _dofailover("Open", false); // currentfailed=false
        }

        bool _dofailoverfile(int f)
        {
            if (null != stm)
            {
                stm.Close();
                stm = null;
            }
            if (++xfailovers > fcount + 5)
            {
                throw new DfsFileNodeIOExceptionRetry("Too many attempts to failover from this machine");
            }
            try
            {
                if (null != fullnames)
                {
                    _fullname = fullnames[f];
                }
                else
                {
                    _fullname = @"\\" + chosts[f] + @"\" + rootdir + @"\\" + name;
                }
                stm = new System.IO.FileStream(_fullname, fmode, faccess, fshare, fbuffersize);
                if (0 != stmpos)
                {
                    Position = stmpos; // !
                }
                return true; // Good!
            }
            catch (Exception e)
            {
                LastFailoverException = e;
                if (!failover)
                {
                    throw new DfsFileNodeIOException("Unable to open DFS file chunk - failover not enabled", LastFailoverException);
                }
            }
            return false;
        }

        void _dofailover(string method, bool currentfailed)
        {
            if (currentfailed)
            {
                if (UnhealthyHostSeconds > 0)
                {
                    AddUnhealthyHost(GetHostForFileIndex(lastfailoverindex));
                }
            }
            //int oldlastfailoverindex = lastfailoverindex;
            for (++lastfailoverindex; lastfailoverindex < fcount; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostUnhealthy(h))
                {
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    //currentfailed = true;
                    AddUnhealthyHost(h);
                }
            }
            /*
            for (lastfailoverindex = 0; lastfailoverindex <= oldlastfailoverindex; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostUnhealthy(h))
                {
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    //currentfailed = true;
                    AddUnhealthyHost(h);
                }
            }*/
            if (null != LastFailoverException)
            {
                throw new DfsFileNodeIOExceptionRetry(method + ": unable to failover", LastFailoverException);
            }
            throw new DfsFileNodeIOException(method + ": unable to failover");
        }

        void _dofailover(string method)
        {
            _dofailover(method, true);
        }


        // Useful when writing to find actual full file written.
        // Changes as failover occurs; can be null; might file not exist if exception.
        public string FullName
        {
            get
            {
                return _fullname;
            }
        }


        System.IO.Stream stm;
        long stmpos;
        int lastfailoverindex;
        bool pendingwrites;
        int xfailovers;
        bool stmdone;

        // One or the other can be null.
        string[] chosts;
        string[] fullnames;
        int fcount;

        string name;
        bool failover;
        string _fullname;
        string rootdir;

        public bool HasFailover
        {
            get { return failover; }
        }

        public Exception LastFailoverException;

        System.IO.FileMode fmode;
        System.IO.FileAccess faccess;
        System.IO.FileShare fshare;
        int fbuffersize;


        string GetHostForFileIndex(int f)
        {
            if (null != chosts)
            {
                return chosts[f];
            }
            else
            {
                string fn = fullnames[f];
                if (!fn.StartsWith(@"\\"))
                {
                    return System.Net.Dns.GetHostName(); // ...
                }
                else
                {
                    int xs = fn.IndexOf('\\', 2);
                    if (-1 == xs)
                    {
                        return fn.Substring(2);
                    }
                    return fn.Substring(2, xs - 2);
                }
            }
        }


        // Seconds.
        public static int UnhealthyHostSeconds
        {
            get
            {
                return unhealthysecs;
            }

            set
            {
                lock (typeof(DfsFileNodeStream))
                {
                    if (value > 0)
                    {
                        /*if (null == unhealthy)
                        {
                            unhealthy = new Dictionary<string, int>(new Surrogate.CaseInsensitiveEqualityComparer());
                        }*/
                    }
                    else
                    {
                        unhealthy = null;
                    }
                }
                unhealthysecs = value;
            }
        }

        static int unhealthysecs = 300; // 5mins default.
        static Dictionary<string, int> unhealthy; // Indexed by hostname.


        static void AddUnhealthyHost(string host)
        {
            lock (typeof(DfsFileNodeStream))
            {
                if (null == unhealthy)
                {
                    unhealthy = new Dictionary<string, int>();
                }
                //if (!unhealthy.ContainsKey(host))
                {
                    unhealthy[host] = CurrentTs();
                }
            }
        }

        static bool IsHostUnhealthy(string host)
        {
            lock (typeof(DfsFileNodeStream))
            {
                if (null == unhealthy)
                {
                    return false;
                }
                if (unhealthy.ContainsKey(host))
                {
                    int uts = unhealthy[host];
                    if (uts + unhealthysecs < CurrentTs())
                    {
                        unhealthy.Remove(host);
                        return false; // Time expired; try again.
                    }
                    return true; // Still unhealthy!
                }
                return false;
            }
        }

        static int CurrentTs()
        {
            return (int)(DateTime.Now - new DateTime(2000, 1, 1)).TotalSeconds;
        }


    }

    public class DfsFileNodeIOException : System.IO.IOException
    {
        public DfsFileNodeIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeIOException(string message)
            : base(message)
        {
        }
    }

    public class DfsFileNodeIOExceptionRetry : DfsFileNodeIOException
    {
        public DfsFileNodeIOExceptionRetry(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeIOExceptionRetry(string message)
            : base(message)
        {
        }
    }

    public class DfsFileNodeWriteException : DfsFileNodeIOExceptionRetry
    {
        public DfsFileNodeWriteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeWriteException(string message)
            : base(message)
        {
        }
    }
}