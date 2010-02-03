using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    public class QaConnection : DbConnection
    { 
        internal QaConnectionString connstr;
        private ConnectionState state = ConnectionState.Closed;
        private string sessionID = null;
        private System.Net.Sockets.Socket sock = null;
        private byte[] buf = null;
        internal System.Net.Sockets.NetworkStream netstm = null;
        bool _islocked = false;
        internal bool islocked
        {
            get
            {
                return _islocked;
            }
            set
            {
                _islocked = value;
                lockedby = null;
            }
        }
        internal IDisposable lockedby = null;
        internal Dictionary<string, List<KeyValuePair<byte[], string>>> mindexes = null;
        private Dictionary<string, System.Net.Sockets.Socket> sockpool = null;
        private Dictionary<string, System.Net.Sockets.NetworkStream> netstmpool = null;
        internal Dictionary<string, Index> sysindexes = null;

        public QaConnection()
        {
            buf = new byte[1024 * 1024 * 8];
        }

        public QaConnection(string connectionstring): this()
        {
            ConnectionString = connectionstring;
        }

        void _CloseRIndex()
        {
            if (state == ConnectionState.Closed)
            {
                return;
            }
            mindexes = null;
            state = ConnectionState.Closed;
            _CloseSocketRIndex();
        }

        private void _CloseSocketRIndex()
        {
            try
            {
                if (netstmpool != null)
                {
                    foreach (System.Net.Sockets.NetworkStream ns in netstmpool.Values)
                    {
                        ns.WriteByte((byte)'c'); //close
                        int ib = ns.ReadByte();
                        if (ib == (byte)'-')
                        {
                            string errmsg = XContent.ReceiveXString(ns, buf);
                            throw new Exception("_CloseSocketRIndex() error: " + errmsg);
                        }
                        if (ib != (byte)'+')
                        {
                            throw new Exception("_CloseSocketRIndex() handshake with service failed.");
                        }
                    }
                }
                else if (netstm != null)
                {
                    netstm.WriteByte((byte)'c'); //close
                    int ib = netstm.ReadByte();
                    if (ib == (byte)'-')
                    {
                        string errmsg = XContent.ReceiveXString(netstm, buf);
                        throw new Exception("_CloseSocketRIndex() error: " + errmsg);
                    }
                    if (ib != (byte)'+')
                    {
                        throw new Exception("_CloseSocketRIndex() handshake with service failed.");
                    }
                }
            }
            finally
            {
                Cleanup();
            }            
        }

        internal void OpenSocketRIndex(string host)
        {
            _OpenSocketRIndex(host);
        }

        internal void CloseSocketRIndex()
        {
            if (connstr.RIndex == QaConnectionString.RIndexType.NOPOOL)
            {
                _CloseSocketRIndex();
            }
        }

        internal System.Net.Sockets.NetworkStream GetRIndexUpdateNetStream(string host)
        {
            if (connstr.RIndex == QaConnectionString.RIndexType.POOLED && sockpool != null && sockpool.Count > 0)
            {
                string _host = host.ToUpper();
                if (sockpool.ContainsKey(_host))
                {
                    return netstmpool[_host];
                }
                else
                {
                    throw new Exception(_host + " host not found in GetRIndexUpdateNetStream.");
                }
            }

            throw new Exception("Must be used with rindex=pooled");
        }

        private void _OpenSocketRIndex(string host)
        {
            if (connstr.RIndex == QaConnectionString.RIndexType.POOLED && sockpool != null && sockpool.Count > 0)
            {
                if (host == null || host.Length == 0)
                {
                    foreach (System.Net.Sockets.Socket sk in sockpool.Values)
                    {
                        sock = sk;
                        break;
                    }
                    foreach (System.Net.Sockets.NetworkStream ns in netstmpool.Values)
                    {
                        netstm = ns;
                        break;
                    }
                    return;
                }
                else
                {
                    string _host = host.ToUpper();
                    if (sockpool.ContainsKey(_host))
                    {
                        sock = sockpool[_host];
                        netstm = netstmpool[_host];
                        return;
                    }
                }
            }

            try
            {
                int tryremains = connstr.RetryMaxCount;
                string[] hosts = null;
                Random rnd = null;

                for (; ; )
                {
                    try
                    {
                        if (host == null || host.Length == 0)
                        {
                            if (rnd == null)
                            {
                                rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                                hosts = connstr.DataSource;
                            }
                            int index = rnd.Next() % hosts.Length;
                            host = hosts[index];
                            for (int startindex = index; ; )
                            {
                                try
                                {
                                    sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                                    sock.Connect(host, 55904);
                                    break;
                                }
                                catch
                                {
                                    sock.Close();
                                    index++;
                                    if (index == hosts.Length)
                                    {
                                        index = 0;
                                    }
                                    if (index == startindex)
                                    {
                                        throw;
                                    }
                                    host = hosts[index];
                                }
                            }
                        }
                        else
                        {
                            sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                            sock.Connect(host, 55904);
                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (connstr.RIndex == QaConnectionString.RIndexType.POOLED)
                        {
                            if (--tryremains <= 0)
                            {
                                throw new Exception("Cannot connect.  Tried this many of times to connect already: " + tryremains.ToString() +
                                    ". RetryMaxCount=" + connstr.RetryMaxCount.ToString() + 
                                    ".  RetrySleep=" + connstr.RetrySleep.ToString() + ".  " + ex.ToString());
                            }
                            System.Threading.Thread.Sleep(connstr.RetrySleep);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }    
                netstm = new System.Net.Sockets.NetworkStream(sock, true); // Owned.  

                if (connstr.RIndex == QaConnectionString.RIndexType.POOLED)
                {
                    string _host = host.ToUpper();
                    if (sockpool == null)
                    {
                        sockpool = new Dictionary<string, System.Net.Sockets.Socket>();
                        netstmpool = new Dictionary<string, System.Net.Sockets.NetworkStream>();
                    }
                    sockpool.Add(_host, sock);
                    netstmpool.Add(_host, netstm);
                }
            }
            catch
            {
                Cleanup();
                throw;
            }
        }

        void _OpenRIndex()
        {
            try
            {
                if (state == ConnectionState.Open)
                {
                    throw new Exception("Connnection is already open.");
                }

                if (connstr.RIndex == QaConnectionString.RIndexType.POOLED)
                {
                    string[] hosts = connstr.DataSource;
                    Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                    for (int hi = 0; hi < hosts.Length; hi++)
                    {
                        int swapindex = rnd.Next() % hosts.Length;
                        string oval = hosts[hi];
                        hosts[hi] = hosts[swapindex];
                        hosts[swapindex] = oval;
                    }
                    for (int hi = 0; hi < hosts.Length; hi++)
                    {
                        _OpenSocketRIndex(hosts[hi]);
                    }                 
                }
                else
                {
                    _OpenSocketRIndex(null);
                }
  
                netstm.WriteByte((byte)'i'); //get all master indexes.

                sysindexes = new Dictionary<string, Index>();                
                {
                    string xml = XContent.ReceiveXString(netstm, buf);
                    if (xml.Length > 0)
                    {
                        System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                        xi.LoadXml(xml);
                        System.Xml.XmlNodeList xnIndexes = xi.SelectNodes("/indexes/index");
                        foreach (System.Xml.XmlNode xnIndex in xnIndexes)
                        {
                            string indName = xnIndex["name"].InnerText;
                            System.Xml.XmlElement xePinHash = xnIndex["pinHash"];  
                            System.Xml.XmlElement xeTable = xnIndex["table"];
                            System.Xml.XmlNodeList xnCols = xeTable.SelectNodes("column");

                            Column[] cols = new Column[xnCols.Count];
                            for (int ci = 0; ci < xnCols.Count; ci++)
                            {
                                System.Xml.XmlNode xnCol = xnCols[ci];
                                cols[ci].Name = xnCol["name"].InnerText;
                                cols[ci].Type = xnCol["type"].InnerText;
                                cols[ci].Bytes = Int32.Parse(xnCol["bytes"].InnerText);
                            }

                            Table tab;
                            tab.Name = xeTable["name"].InnerText;
                            tab.Columns = cols;

                            Index ind;
                            ind.Name = indName;
                            ind.Ordinal = Int32.Parse(xnIndex["ordinal"].InnerText);
                            ind.Table = tab;
                            ind.PinHash = (xePinHash != null && xePinHash.InnerText == "1");
                            ind.Hash = ind.PinHash ? new Position[256 * 256] : null;
                            ind.MaxKey = new byte[tab.Columns[ind.Ordinal].Bytes];
                            sysindexes.Add(indName.ToLower(), ind);
                        }
                    }
                }

                int micnt = 0;
                XContent.ReceiveXBytes(netstm, out micnt, buf);
                micnt = Utils.BytesToInt(buf, 0);
                mindexes = new Dictionary<string, List<KeyValuePair<byte[], string>>>(micnt);
                for (int mi = 0; mi < micnt; mi++)
                {
                    string indexname = XContent.ReceiveXString(netstm, buf).ToLower();
                    List<KeyValuePair<byte[], string>> lines = new List<KeyValuePair<byte[], string>>();
                    int keylen = 9;
                    bool pinhash = false;
                    Position[] hash = null;
                    byte[] maxkey = null;
                    if (sysindexes.ContainsKey(indexname))
                    {
                        Index thisindex = sysindexes[indexname];
                        int ordinal = thisindex.Ordinal;
                        keylen = thisindex.Table.Columns[ordinal].Bytes;
                        pinhash = thisindex.PinHash;
                        hash = thisindex.Hash;
                        maxkey = thisindex.MaxKey;
                    }
                    else
                    {
                        throw new Exception("Index version conflict, need to recreate indexes");
                    }

                    byte[] lastkeybuf = new byte[3];
                    int filelen = 0;
                    XContent.ReceiveXBytesNoCap(netstm, out filelen, ref buf);
                    if (filelen > 0)
                    {
                        int pos = 0;
                        int hoffset = 0;
                        int hlen = 0;
                        
                        for (int ki = 0; ki < keylen; ki++)
                        {
                            maxkey[ki] = buf[pos++];
                        }                        

                        while (pos < filelen)
                        {
                            byte[] keybuf = new byte[keylen];
                            for (int ki = 0; ki < keylen; ki++)
                            {
                                keybuf[ki] = buf[pos++];
                            }

                            /*bool samekey = true;
                            for (int ki = 0; ki < keybuf.Length; ki++)
                            {
                                if (lastkeybuf[ki] != keybuf[ki])
                                {
                                    samekey = false;
                                    break;
                                }
                            }*/

                            int chunknamestartpos = pos;
                            while (buf[pos++] != (byte)'\0')
                            {
                            }

                            //samekey = false;
                            //if (!samekey)
                            {
                                string chunkname = System.Text.Encoding.UTF8.GetString(buf, chunknamestartpos, pos - chunknamestartpos - 1);

                                if (pinhash)
                                {
                                    if (lines.Count > 0)
                                    {
                                        if (keybuf[1] != lastkeybuf[1] || keybuf[2] != lastkeybuf[2])
                                        {
                                            int shortkey = TwoBytesToInt(lastkeybuf[1], lastkeybuf[2]);
                                            hash[shortkey].Offset = hoffset;
                                            hash[shortkey].Length = hlen;
                                            hoffset = lines.Count;
                                            hlen = 0;
                                        }
                                    }
                                    hlen++;
                                }

                                lines.Add(new KeyValuePair<byte[], string>(keybuf, chunkname));
                                Buffer.BlockCopy(keybuf, 0, lastkeybuf, 0, 3);
                            }
                        }

                        if (pinhash)
                        {
                            //last flush
                            if (hlen > 0)
                            {
                                int shortkey = TwoBytesToInt(lastkeybuf[1], lastkeybuf[2]);
                                hash[shortkey].Offset = hoffset;
                                hash[shortkey].Length = hlen;
                            }

                            //fill in the gap
                            int prevoffset = 0;
                            int prevlen = 0;
                            for (int hi = 0; hi < hash.Length; hi++)
                            {
                                Position thispos = hash[hi];
                                if (thispos.Length == 0)
                                {
                                    thispos.Length = prevlen;
                                    thispos.Offset = prevoffset;
                                    hash[hi] = thispos;
                                }
                                else
                                {
                                    prevoffset = thispos.Offset;
                                    prevlen = thispos.Length;
                                }
                            }
                        }
                    }
                    mindexes.Add(indexname, lines);
                }                

                if (connstr.RIndex == QaConnectionString.RIndexType.NOPOOL)
                {
                    _CloseSocketRIndex(); 
                }                
                state = ConnectionState.Open;
            }
            catch
            {
                Cleanup();
                throw;
            }
        }

        internal static int TwoBytesToInt(byte b0, byte b1)
        {
            int x = b0 << 8;
            x = x | b1;
            return x;
        }

        internal bool isRIndexEnabled()
        {
            return (QaConnectionString.RIndexType.DISABLED != connstr.RIndex);
        }

        public override void Open()
        {
            if (isRIndexEnabled())
            {
                _OpenRIndex();
                return;
            }

            try
            {
                if (state == ConnectionState.Open)
                {
                    throw new Exception("Connnection is already open.");
                }

                string[] hosts = connstr.DataSource;
                Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                int index = rnd.Next() % hosts.Length;
                string host = hosts[index];
                for (int startindex = index; ; )
                {
                    try
                    {
                        sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                        sock.Connect(host, 55902);
                        break;
                    }
                    catch
                    {
                        sock.Close();
                        index++;
                        if (index == hosts.Length)
                        {
                            index = 0;
                        }
                        if (index == startindex)
                        {
                            throw;
                        }
                        host = hosts[index];
                    }
                }
                netstm = new System.Net.Sockets.NetworkStream(sock);
                netstm.WriteByte((byte)'o'); //Open connection.
                Utils.Int64ToBytes(connstr.BatchSize, buf, 0);
                XContent.SendXContent(netstm, buf, 8);
                Utils.Int64ToBytes(connstr.BlockSize, buf, 0);
                XContent.SendXContent(netstm, buf, 8);
                XContent.SendXContent(netstm, string.Join(";", hosts));

                if (netstm.ReadByte() != (byte)'+')
                {                    
                    throw new Exception("Cannot connect to host.  Handshake failed with protocol.");
                }
                sessionID = XContent.ReceiveXString(netstm, buf);
                state = ConnectionState.Open;
            }
            catch
            {
                Cleanup();
                throw;
            }            
        }

        public override void Close()
        {
            if (isRIndexEnabled())
            {
                _CloseRIndex();
                return;
            }

            if (state == ConnectionState.Closed)
            {
                return;
            }

            try
            {
                if (islocked)
                {
                    if (null != lockedby)
                    {
                        lockedby.Dispose();
                        islocked = false;
                    }
                    else
                    {
                        throw new Exception("QaConnection.Close: The connection is locked  [ensure all readers are closed]");
                    }
                }

                FlushBatchNqData();

                netstm.WriteByte((byte)'c'); //close
                int ib = netstm.ReadByte();
                if (ib == (byte)'-')
                {
                    string errmsg = XContent.ReceiveXString(netstm, buf);
                    throw new Exception("Connection.Close() error: " + errmsg);
                }
                if (ib != (byte)'+')
                {
#if DEBUG
                    throw new Exception("Connection.Close() handshake with service failed. (byte)" + ib.ToString());
#else
                    throw new Exception("Connection.Close() handshake with service failed.");
#endif
                }
            }
            finally
            {
                Cleanup();
                state = ConnectionState.Closed;     
            } 
        }

        private void Cleanup()
        {
            if (netstm != null)
            {
                netstm.Close();
                netstm = null;
            }
            if (sock != null)
            {
                sock.Close();
                sock = null;
            }
            if (netstmpool != null)
            {
                foreach (System.Net.Sockets.NetworkStream ns in netstmpool.Values)
                {
                    ns.Close();
                }
                netstmpool = null;
            }            
            if (sockpool != null)
            {
                foreach (System.Net.Sockets.Socket sk in sockpool.Values)
                {
                    sk.Close();
                }
                sockpool = null;
            }            
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Close();
            }
            base.Dispose(disposing);
        }


        internal byte[] batchnqbuf = null;
        internal int batchnqbuflen = 0;

        internal void BatchNqData(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new Exception("Query expected (user error)");
            }

            if (null == batchnqbuf)
            {
                long batchnqsize = connstr.BatchSize;
                if (batchnqsize < 1024 * 1024 * 1)
                {
                    batchnqsize = 1024 * 1024 * 1;
                }
                batchnqbuf = new byte[batchnqsize];
            }

            {
                int bslen = s.Length * 2 + 2;
                if (bslen > batchnqbuf.Length - batchnqbuflen)
                {
                    FlushBatchNqData();
                    if (bslen > batchnqbuf.Length)
                    {
                        batchnqbuf = new byte[bslen + 512];
                    }
                }
            }

            if (0 != batchnqbuflen)
            {
                batchnqbuf[batchnqbuflen++] = 0;
                batchnqbuf[batchnqbuflen++] = 0;
            }
            for (int i = 0; i < s.Length; i++)
            {
                batchnqbuf[batchnqbuflen++] = (byte)(((UInt16)s[i]) >> 8);
                batchnqbuf[batchnqbuflen++] = (byte)s[i];
            }

        }

        internal void FlushBatchNqData()
        {
            if (batchnqbuflen > 0)
            {
                netstm.WriteByte((byte)'N'); // Batched non-query.
                XContent.SendXContent(netstm, batchnqbuf, batchnqbuflen);
                int ib = netstm.ReadByte();
                if (ib == (byte)'-')
                {
                    string errmsg = XContent.ReceiveXString(netstm, buf);
                    throw new Exception("ExecuteNonQuery error: " + errmsg);
                }
                if (ib != (byte)'+')
                {
                    throw new Exception("ExecuteNonQuery did not receive a success signal from service.");
                }
            }
            batchnqbuflen = 0;
        }

        
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        public override string ServerVersion
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override string DataSource
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override ConnectionState State
        {
            get
            {
                return state;
            }
        }

        public override string ConnectionString
        {
            get
            {
                return connstr.ConnectionString;
            }
            set
            {
                if (value == null || value.Length == 0)
                {
                    throw new Exception("Connection string cannot be empty.");
                }
                connstr = QaConnectionString.Prepare(value);
            }
        }

        public override string Database
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        protected override DbCommand CreateDbCommand()
        {
            QaCommand cmd = new QaCommand();
            cmd.Connection = this;
            return cmd;
        }

        internal struct QaConnectionString
        {
            public string ConnectionString;
            public string[] DataSource;
            public long BatchSize;
            public long BlockSize;
            public RIndexType RIndex;
            public int RetryMaxCount;
            public int RetrySleep;

            internal enum RIndexType
            {
                DISABLED,
                NOPOOL,
                POOLED
            }

            public static QaConnectionString Prepare(string connstr)
            {
                if (connstr == null || connstr.Trim().Length == 0)
                {
                    throw new ArgumentException("Connection string cannot be null or empty.");
                }

                QaConnectionString cs;
                cs.ConnectionString = connstr;
                cs.DataSource = null;
                cs.BatchSize = 1024 * 1024 * 64;
                cs.BlockSize = 1024 * 1024 * 16;
                cs.RIndex = RIndexType.DISABLED;
                cs.RetryMaxCount = 20;
                cs.RetrySleep = 1000 * 5;

                string[] parts = connstr.Trim(';').Split(';');
                for (int i = 0; i < parts.Length; i++)
                {
                    string part = parts[i].Trim();
                    int del = part.IndexOf('=');
                    if (del == -1)
                    {
                        throw new ArgumentException("Connection string is not in the correct format. <name>=<value>");
                    }

                    string name = part.Substring(0, del).Trim().ToLower();
                    string val = "";
                    if (del < part.Length - 1)
                    {
                        val = part.Substring(del + 1).Trim();
                    }

                    switch (name)
                    {
                        case "data source":
                            {
                                if (val.Length == 0)
                                {
                                    throw new ArgumentException("Data Source in connection string cannot be empty.");
                                }
                                cs.DataSource = val.Split(',');
                                for (int di = 0; di < cs.DataSource.Length; di++)
                                {
                                    if (string.Compare("localhost", cs.DataSource[di], true) == 0)
                                    {
                                        cs.DataSource[di] = System.Net.Dns.GetHostName();
                                    }
                                }
                            }
                            break;
                        case "batch size":
                            {
                                cs.BatchSize = ParseLongCapacity(val);
                            }
                            break;
                        case "mr.dfs block size":
                            {
                                cs.BlockSize = ParseLongCapacity(val);
                            }
                            break;
                        case "rindex":
                            {
                                if (0 == string.Compare("POOLED", val, true))
                                {
                                    cs.RIndex = RIndexType.POOLED;
                                }
                                else if (0 == string.Compare("NOPOOL", val, true))
                                {
                                    cs.RIndex = RIndexType.NOPOOL;
                                }
                                else if (0 == string.Compare("DISABLED", val, true))
                                {
                                    cs.RIndex = RIndexType.DISABLED;
                                }
                                else
                                {
                                    throw new Exception("Invalid value for RINDEX=" + val);
                                }
                            }
                            break;
                        case "retrymaxcount":
                            {
                                cs.RetryMaxCount = Int32.Parse(val);
                            }
                            break;
                        case "retrysleep":
                            {
                                cs.RetrySleep = Int32.Parse(val);
                            }
                            break;
                    }
                }

                if (cs.DataSource == null)
                {
                    throw new ArgumentException("Data Source in connection string is not specified.");
                }

                return cs;
            }

            public static long ParseLongCapacity(string capacity)
            {
                try
                {
                    if (null == capacity || 0 == capacity.Length)
                    {
                        throw new Exception("Invalid capacity: capacity not specified");
                    }
                    if ('-' == capacity[0])
                    {
                        throw new FormatException("Invalid capacity: negative");
                    }
                    switch (capacity[capacity.Length - 1])
                    {
                        case 'B':
                            if (1 == capacity.Length)
                            {
                                throw new Exception("Invalid capacity: " + capacity);
                            }
                            switch (capacity[capacity.Length - 2])
                            {
                                case 'K': // KB
                                    return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                                case 'M': // MB
                                    return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                                case 'G': // GB
                                    return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                                case 'T': // TB
                                    return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024;

                                case 'P': // PB
                                    return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024 * 1024;

                                default: // Just bytes with B suffix.
                                    return long.Parse(capacity.Substring(0, capacity.Length - 1));
                            }
                        //break;

                        default: // Assume just bytes without a suffix.
                            return long.Parse(capacity);
                    }
                }
                catch (FormatException e)
                {
                    throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.ToString());
                }
                catch (OverflowException e)
                {
                    throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.ToString());
                }
            }
        }

        internal struct Column
        {
            public string Name;
            public string Type;
            public int Bytes;
        }

        internal struct Table
        {
            public string Name;
            public Column[] Columns;
        }

        internal struct Index
        {
            public string Name;
            public int Ordinal;
            public bool PinHash;
            public Table Table;
            public Position[] Hash;
            public byte[] MaxKey;
        }

        internal struct Position
        {
            public int Offset;
            public int Length;
        }
    }
}
