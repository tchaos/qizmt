﻿using System;
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
        internal bool islocked = false;
        internal Dictionary<string, List<KeyValuePair<byte[], string>>> mindexes = null;
        private Dictionary<string, System.Net.Sockets.Socket> sockpool = null;
        private Dictionary<string, System.Net.Sockets.NetworkStream> netstmpool = null;

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

        private void _OpenSocketRIndex(string host)
        {
            if (connstr.RIndex == QaConnectionString.RIndexType.POOLED && sockpool != null && sockpool.Count > 0)
            {
                if (host == null)
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
                if (host == null)
                {
                    string[] hosts = connstr.DataSource;
                    Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
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

                _OpenSocketRIndex(null);    
                netstm.WriteByte((byte)'i'); //get all master indexes.
                int micnt = 0;
                XContent.ReceiveXBytes(netstm, out micnt, buf);
                micnt = Utils.BytesToInt(buf, 0);
                mindexes = new Dictionary<string, List<KeyValuePair<byte[], string>>>(micnt);
                for (int mi = 0; mi < micnt; mi++)
                {
                    string indexname = XContent.ReceiveXString(netstm, buf);
                    List<KeyValuePair<byte[], string>> lines = new List<KeyValuePair<byte[], string>>();
                    byte[] lastkeybuf = new byte[9];
                    int filelen = 0;
                    XContent.ReceiveXBytesNoCap(netstm, out filelen, ref buf);
                    if (filelen > 0)
                    {
                        int pos = 0;
                        while(pos < filelen)
                        {                            
                            byte[] keybuf = new byte[9];
                            for (int ki = 0; ki < 9; ki++)
                            {
                                keybuf[ki] = buf[pos++];
                            }

                            bool samekey = true;
                            for (int ki = 0; ki < keybuf.Length; ki++)
                            {
                                if (lastkeybuf[ki] != keybuf[ki])
                                {
                                    samekey = false;
                                    break;
                                }
                            }

                            int chunknamestartpos = pos;
                            while(buf[pos++] != (byte)'\0')
                            {
                            }

                            string chunkname = System.Text.Encoding.UTF8.GetString(buf, chunknamestartpos, pos - chunknamestartpos - 1);

                            if (!samekey)
                            {
                                lines.Add(new KeyValuePair<byte[], string>(keybuf, chunkname));
                                Buffer.BlockCopy(keybuf, 0, lastkeybuf, 0, 9);
                            }                           
                        }
                    }
                    mindexes.Add(indexname.ToLower(), lines);
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
                    throw new Exception("Connection.Close() handshake with service failed.");
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
    }
}