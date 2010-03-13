using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.AELight;

namespace MySpace.DataMining.DistributedObjects
{
    public class FailoverFileStreamIOException : System.IO.IOException
    {
        public FailoverFileStreamIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FailoverFileStreamIOException(string message)
            : base(message)
        {
        }
    }

    public class FailoverFileStreamFatalException : FailoverFileStreamIOException
    {
        public FailoverFileStreamFatalException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FailoverFileStreamFatalException(string message)
            : base(message)
        {
        }
    }

    public class FailoverFileStreamIOExceptionRetry : FailoverFileStreamIOException
    {
        public FailoverFileStreamIOExceptionRetry(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FailoverFileStreamIOExceptionRetry(string message)
            : base(message)
        {
        }
    }

    public class FailoverFileStreamWriteException : FailoverFileStreamIOExceptionRetry
    {
        public FailoverFileStreamWriteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FailoverFileStreamWriteException(string message)
            : base(message)
        {
        }
    }

    // Note: file headers are not considered.
    // Compression can be used on top of this stream.
    public class FailoverFileStream : System.IO.Stream
    {
        public FailoverFileStream(string[] fullnames, System.IO.FileMode fmode,
            System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize, DiskCheck diskcheck)
        {
            if (null == fullnames || string.IsNullOrEmpty(fullnames[0]))
            {
                throw new FailoverFileStreamIOException("Invalid input fullnames paths");
            }
            this.fullnames = fullnames;
            fcount = fullnames.Length;

            this.fmode = fmode;
            this.faccess = faccess;
            this.fshare = fshare;
            this.fbuffersize = fbuffersize;
            this.diskcheck = diskcheck;

            _firstopen();
        }        

        public FailoverFileStream(string starfullnames,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize, DiskCheck diskcheck)
            : this(starfullnames.Split('*'), fmode, faccess, fshare, fbuffersize, diskcheck)
        {
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
                            throw new FailoverFileStreamIOExceptionRetry("Close failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new FailoverFileStreamIOException("Close failure", LastFailoverException);
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
                            throw new FailoverFileStreamIOExceptionRetry("Dispose failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new FailoverFileStreamIOException("Dispose failure", LastFailoverException);
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
                    throw new FailoverFileStreamWriteException("Unable to flush file", LastFailoverException);
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
                    throw new FailoverFileStreamWriteException("Unable to write to file", LastFailoverException);
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
                    throw new FailoverFileStreamWriteException("Unable to write (WriteByte) to file", LastFailoverException);
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
                        _dofailover("WriteByte");
                    }
                }
            }
        }

        void _firstopen()
        {
            lastfailoverindex = fcount - 1;
            stmpos = 0;
            pendingwrites = false;
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
            try
            {               
                _fullname = fullnames[f];                
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
            }
            return false;
        }

        void _dofailover(string method, bool currentfailed)
        {
            if (currentfailed)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (IsDiskFailure(h))
                {
                    AddDiskFailure(h);
                }
            }
            bool anytry = false;
            int oldlastfailoverindex = lastfailoverindex;
            for (++lastfailoverindex; lastfailoverindex < fcount; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostOnDiskFailureList(h))
                {
                    anytry = true;
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    if (IsDiskFailure(h))
                    {
                        AddDiskFailure(h);
                    }
                }
            }
            for (lastfailoverindex = 0; lastfailoverindex <= oldlastfailoverindex; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostOnDiskFailureList(h))
                {
                    anytry = true;
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    if (IsDiskFailure(h))
                    {
                        AddDiskFailure(h);
                    }
                }
            }
            if (!anytry)
            {
                throw new FailoverFileStreamFatalException(method + ": unable to failover.  Fatal exception:  No more healthy hosts to failover to.  Too many failed hard disks.");
            }
            if (null != LastFailoverException)
            {
                throw new FailoverFileStreamIOExceptionRetry(method + ": unable to failover", LastFailoverException);
            }
            throw new FailoverFileStreamIOException(method + ": unable to failover");
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
        bool stmdone;

        string[] fullnames;
        int fcount;

        string name;
        string _fullname;

        public bool HasFailover
        {
            get { return true; }
        }

        public Exception LastFailoverException;

        System.IO.FileMode fmode;
        System.IO.FileAccess faccess;
        System.IO.FileShare fshare;
        int fbuffersize;
        DiskCheck diskcheck;
        string dfreason;


        string GetHostForFileIndex(int f)
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

        bool IsDiskFailure(string host)
        {
            return diskcheck.IsDiskFailure(host, out dfreason);
        }

        static Dictionary<string, int> diskfailures; // Indexed by hostname.

        static void AddDiskFailure(string host)
        {
            lock (typeof(FailoverFileStream))
            {
                if (null == diskfailures)
                {
                    diskfailures = new Dictionary<string, int>();
                }
                diskfailures[host] = 0;
            }
        }

        static bool IsHostOnDiskFailureList(string host)
        {
            lock (typeof(FailoverFileStream))
            {
                if (null == diskfailures)
                {
                    return false;
                }
                return diskfailures.ContainsKey(host);
            }
        }
    }
}