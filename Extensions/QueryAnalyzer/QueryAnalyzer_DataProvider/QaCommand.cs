﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace QueryAnalyzer_DataProvider
{
    public class QaCommand : DbCommand
    { 
        private CommandType cmdType;
        private string cmdText = null;
        private QaParameterCollection parameters = null;
        private Dictionary<int, string> paramplaceholders = null;
        internal QaConnection conn = null;
        internal byte[] buf = null;

        public QaCommand()
        {
            buf = new byte[1024 * 1024 * 1];
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            QaParameter param = new QaParameter();
            DbParameterCollection.Add(param);
            return param;
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return false;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        protected override DbTransaction DbTransaction
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                if (parameters == null)
                {
                    parameters = new QaParameterCollection();
                }
                return parameters;
            }
        }

        public override CommandType CommandType
        {
            get
            {
                return cmdType;
            }
            set
            {
                if (value != CommandType.Text)
                {
                    throw new Exception("Only CommandType.CommandText is supported.");
                }
                cmdType = value;
            }
        }

        public override string CommandText
        {
            get
            {
                return cmdText;
            }
            set
            {
                cmdText = value;
                ParseCommandText(cmdText);
            }
        }

        protected override DbConnection DbConnection
        {
            get
            {
                return conn;
            }
            set
            {
                conn = (QaConnection)value;
            }
        }

        public override int CommandTimeout
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        private int _ExecuteNonQueryRIndex()
        {
            try
            {
                CheckOkToExecute();

                if (conn.mindexes == null)
                {
                    throw new Exception("Master indexes is null.");
                }

                {
                    string cleanedCmdText = cmdText;
                    if ((0 == string.Compare("BEGIN", Qa.NextPart(ref cleanedCmdText), true)
                   && 0 == string.Compare("BULKUPDATE", Qa.NextPart(ref cleanedCmdText), true)))
                    {
                        int del = cleanedCmdText.LastIndexOf("BULKUPDATE", StringComparison.OrdinalIgnoreCase);
                        if (del == -1)
                        {
                            throw new Exception("Expected END BULKUPDATE");
                        }
                        cleanedCmdText = cleanedCmdText.Substring(0, del);
                        del = cleanedCmdText.LastIndexOf("END", StringComparison.OrdinalIgnoreCase);
                        if (del == -1)
                        {
                            throw new Exception("Expected END BULKUPDATE");
                        }
                        cleanedCmdText = cleanedCmdText.Substring(0, del);
                        cmdText = cleanedCmdText;
                    }
                }

                string[] stmts = cmdText.Split('\0');
                Dictionary<string, Dictionary<string, BatchInfo>> hostBatches = new Dictionary<string, Dictionary<string, BatchInfo>>(conn.connstr.DataSource.Length, StringComparer.OrdinalIgnoreCase);
                Dictionary<string, int> chunkInsertCount = new Dictionary<string, int>(conn.connstr.DataSource.Length, StringComparer.OrdinalIgnoreCase);
                foreach (string stmt in stmts)
                {
                    if (stmt.Trim().Length == 0)
                    {
                        continue;
                    }
                    if (!RInsert(stmt, hostBatches, stmts.Length, chunkInsertCount)
                        && !RDelete(stmt, hostBatches, stmts.Length)
                        && !FlushRIndex(stmt, hostBatches, stmts.Length))
                    {
                        throw new Exception("Unknown query: " + stmt);
                    }
                }

                int rowsAffected = 0;
                {
                    RIndexUpdateThread[] threads = new RIndexUpdateThread[hostBatches.Count];
                    int thdcnt = 0;
                    foreach (string bhost in hostBatches.Keys)
                    {
                        RIndexUpdateThread thisthd = new RIndexUpdateThread();
                        threads[thdcnt++] = thisthd;
                        thisthd.HostName = bhost;
                        thisthd.chunkInsertCount = chunkInsertCount;
                        thisthd.conn = conn;
                        thisthd.hostBatches = hostBatches;
                        thisthd.thread = new System.Threading.Thread(new System.Threading.ThreadStart(thisthd.UpdateThreadProc));
                        thisthd.thread.Start();
                    }

                    for (int ti = 0; ti < threads.Length; ti++)
                    {
                        threads[ti].thread.Join();
                    }

                    for (int ti = 0; ti < threads.Length; ti++)
                    {
                        if (threads[ti].exception != null)
                        {
                            throw threads[ti].exception;
                        }
                    }
                }

                return rowsAffected;
            }
            catch(Exception e)
            {
                XLog.errorlog("QaCommand._ExecuteNonQueryRIndex error:" + e.ToString());
                try
                {
                    Abort();
                }
                catch
                {
                }
                throw e;
            }
        }

        private class RIndexUpdateThread
        {           
            internal string HostName;
            internal Dictionary<string, Dictionary<string, BatchInfo>> hostBatches;
            internal Dictionary<string, int> chunkInsertCount;
            internal QaConnection conn;
            internal Exception exception;
            internal System.Threading.Thread thread;

            private int SortStatement(Statement x, Statement y)
            {
                byte[] xbuf = x.KeyBuf;
                byte[] ybuf = y.KeyBuf;

                for (int i = 0; i < xbuf.Length; i++)
                {
                    if (xbuf[i] != ybuf[i])
                    {
                        return xbuf[i] - ybuf[i];
                    }
                }

                return x.Timestamp - y.Timestamp;
            }

            public void UpdateThreadProc()
            {
                StringBuilder bcmds = new StringBuilder(1024);               
                string bhost = HostName;

                Dictionary<string, BatchInfo> chunkBatches = hostBatches[bhost];
                foreach (KeyValuePair<string, BatchInfo> pair in chunkBatches)
                {
                    string chunkName = pair.Key;
                    string indexName = pair.Value.IndexName;
                    List<Statement> statements = pair.Value.Statements;
                    int insertCount = 0;
                    if (chunkInsertCount.ContainsKey(chunkName))
                    {
                        insertCount = chunkInsertCount[chunkName];
                    }

                    if (bcmds.Length > 0)
                    {
                        bcmds.Append("\0\0");
                    }
                    bcmds.Append(indexName);
                    bcmds.Append('\0');
                    bcmds.Append(chunkName);
                    bcmds.Append('\0');
                    bcmds.Append(insertCount);

                    statements.Sort(SortStatement);

                    foreach (Statement stm in statements)
                    {
                        bcmds.Append('\0');
                        bcmds.Append(stm.Str);
                    }                            
                }

                try
                {
                    System.Net.Sockets.NetworkStream thisnetstm = conn.GetRIndexUpdateNetStream(bhost);
                    thisnetstm.WriteByte((byte)'u'); //update rindex
                    XContent.SendXContent(thisnetstm, bcmds.ToString());
                    int ib = thisnetstm.ReadByte();
                    if (ib == (byte)'-')
                    {
                        throw new Exception("_ExecuteNonQueryRIndex error.");
                    }
                    if (ib != (byte)'+')
                    {
                        throw new Exception("_ExecuteNonQueryRIndex did not receive a success signal from service.");
                    }               
                }
                catch (Exception ex)
                {
                    exception = ex;
                }                
            }
        }

        class RFlushConn
        {
            internal string host;
            internal System.Net.Sockets.NetworkStream netstm;
            internal string chunkbasename;
            internal int counter = 0;
            internal int[] chunksizes;
        }

        private bool FlushRIndex(string xcmd, Dictionary<string, Dictionary<string, BatchInfo>> hostBatches, int cmdCount)
        {
            string originalcmd = xcmd;
            if (!(0 == string.Compare("FLUSH", Qa.NextPart(ref xcmd), true)
                && 0 == string.Compare("RINDEX", Qa.NextPart(ref xcmd), true)))
            {
                return false;
            }

#if DEBUG
            //System.Diagnostics.Debugger.Launch();
#endif

            string indexName = Qa.NextPart(ref xcmd).ToLower();
            if (0 == indexName.Length)
            {
                throw new Exception("RIndex name expected");
            }

            if (0 != string.Compare("TO", Qa.NextPart(ref xcmd), true))
            {
                throw new Exception("Expected TO after " + originalcmd);
            }

            string todfsfile;
            {
                string todfsfilesqlstr = Qa.NextPart(ref xcmd);
                if (0 == todfsfilesqlstr.Length
                    || '\'' != todfsfilesqlstr[0]
                    || '\'' != todfsfilesqlstr[todfsfilesqlstr.Length - 1])
                {
                    throw new Exception("Expected destination DFS file name string after " + originalcmd);
                }
                todfsfile = todfsfilesqlstr.Substring(1, todfsfilesqlstr.Length - 2).Replace("''", "'");
                if (todfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    todfsfile = todfsfile.Substring(6);
                }
            }

            if (cmdCount > 1)
            {
                // This is needed because updates haven't been sent yet.
                throw new Exception("Cannot FLUSH RINDEX within BULKUPDATE");
            }

            {
                CheckOkToExecute();
                conn.islocked = true;

                if (conn.mindexes == null)
                {
                    throw new Exception("Master indexes is null.");
                }

                if (!conn.mindexes.ContainsKey(indexName))
                {
                    throw new Exception("Index " + indexName + " is not found in the master indexes.");
                }

                if (!conn.sysindexes.ContainsKey(indexName))
                {
                    throw new Exception("Index " + indexName + " is not found in the sys indexes.");
                }
                QaConnection.Index sysindex = conn.sysindexes[indexName];

                {
                    QaConnection.QaConnectionString xconnstr = conn.connstr;
                    QaConnection.QaConnectionString.RIndexType xrindextype = xconnstr.RIndex;
                    if (xrindextype != QaConnection.QaConnectionString.RIndexType.POOLED)
                    {
                        throw new Exception("FLUSH RINDEX: cannot flush rindex if rindex is not pooled");
                    }
                }

                int RowLength = 0;
                foreach (QaConnection.Column col in sysindex.Table.Columns)
                {
                    RowLength += col.Bytes;
                }

                List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexName];

                // Indexed by n-th flushed chunk, value is host.
                List<string> flushedchunks = new List<string>(mi.Count);
                string somehost = conn.connstr.DataSource[0];

                // Indexed by host is the request of chunk names to be flushed.
                Dictionary<string, StringBuilder> reqs = new Dictionary<string, StringBuilder>(conn.connstr.DataSource.Length);
#if DEBUG
                // Ensuring there's no duplicate chunk names.
                Dictionary<string, bool> dbgchunknames = new Dictionary<string, bool>(100);
#endif
                for (int im = 0; im < mi.Count; im++)
                {
                    KeyValuePair<byte[], string> m = mi[im];
                    string chunkname = m.Value;
#if DEBUG
                    if (!chunkname.StartsWith(@"\\"))
                    {
                        throw new Exception("DEBUG: chunkinfo expected to start with 2 backslashes");
                    }
#endif
                    int cnsep = chunkname.IndexOf('\\', 2);
#if DEBUG
                    if (cnsep <= 2)
                    {
                        throw new Exception("DEBUG: name backslash invalid; index " + cnsep);
                    }
#endif
                    string[] chunkhosts = chunkname.Substring(2, cnsep - 2).Split(';');
                    //string chunknamepart = chunkname.Substring(cnsep + 1);
#if DEBUG
                    if (dbgchunknames.ContainsKey(chunkname))
                    {
                        throw new Exception("DEBUG: duplicate chunk name found: " + chunkname);
                    }
                    dbgchunknames[chunkname] = true;
#endif
                    StringBuilder req;
                    if (!reqs.TryGetValue(chunkhosts[0], out req))
                    {
                        req = new StringBuilder(2000);
                        reqs.Add(chunkhosts[0], req);
                    }
                    if (req.Length > 0)
                    {
                        req.Append('|');
                    }
                    req.Append(chunkname);

                    flushedchunks.Add(chunkhosts[0]);

                    somehost = chunkhosts[0];

                }

                Dictionary<string, RFlushConn> flushes = new Dictionary<string, RFlushConn>(reqs.Count);
                foreach (KeyValuePair<string, StringBuilder> kvp in reqs)
                {
                    string host = kvp.Key;
                    StringBuilder req = kvp.Value;
                    RFlushConn rfc = new RFlushConn();
                    rfc.host = host;
                    conn.OpenSocketRIndex(host);
                    rfc.netstm = conn.netstm;
                    rfc.chunkbasename = "zd.%n.rflush." + Guid.NewGuid() + ".zd";
                    flushes.Add(host, rfc);
                    rfc.netstm.WriteByte((byte)'f');
                    XContent.SendXContent(rfc.netstm, indexName);
                    XContent.SendXContent(rfc.netstm, rfc.chunkbasename);
                    XContent.SendXContent(rfc.netstm, req.ToString());
                }

                // Join.
                foreach (KeyValuePair<string, RFlushConn> kvp in flushes)
                {
                    RFlushConn rfc = kvp.Value;
                    int ib = rfc.netstm.ReadByte();
                    if ('+' != ib)
                    {
                        if ('-' == ib)
                        {
                            string err = null;
                            try
                            {
                                err = XContent.ReceiveXString(rfc.netstm, null);
                            }
                            catch
                            {
                            }
                            if (err != null)
                            {
                                throw new Exception("Error returned for flush rindex from machine " + rfc.host + ":"
                                    + Environment.NewLine + err);
                            }
                        }
#if DEBUG
                        throw new Exception("Remote machine " + rfc.host + " did not return a success signal for flush rindex; (byte)" + ib);
#else
                        throw new Exception("Remote machine " + rfc.host + " did not return a success signal for flush rindex");
#endif
                    }

                    int buflen;
                    //rfc.chunkbasename.Replace("%n", 0.ToString());
                    {
                        buf = XContent.ReceiveXBytes(rfc.netstm, out buflen, buf);
                        rfc.chunksizes = new int[buflen / 4];
                        for (int i = 0; i < rfc.chunksizes.Length; i++)
                        {
                            rfc.chunksizes[i] = Utils.BytesToInt(buf, i * 4);
                        }
                    }
                }

                // Lines of "<host> <chunkname> <size>" but <size> must exclude the size of the header.
                StringBuilder sbbp = new StringBuilder(1000);
                for (int fi = 0; fi < flushedchunks.Count; fi++)
                {
                    string host = flushedchunks[fi];
                    RFlushConn rfc = flushes[host];
                    int blockindex = rfc.counter++;
                    sbbp.AppendFormat("{0} {1} {2}{3}",
                        host,
                        rfc.chunkbasename.Replace("%n", blockindex.ToString()),
                        rfc.chunksizes[blockindex],
                        Environment.NewLine);
                }

                {
                    // Bulk put in one of the protocols.
                    conn.OpenSocketRIndex(somehost);
                    conn.netstm.WriteByte((byte)'P');
                    string todfsfiletype = "rbin@" + RowLength;
                    XContent.SendXContent(conn.netstm, todfsfile);
                    XContent.SendXContent(conn.netstm, todfsfiletype);
                    XContent.SendXContent(conn.netstm, sbbp.ToString());
                    int ib = conn.netstm.ReadByte();
                    if ('+' != ib)
                    {
                        if ('-' == ib)
                        {
                            string err = null;
                            try
                            {
                                err = XContent.ReceiveXString(conn.netstm, null);
                            }
                            catch
                            {
                            }
                            if (err != null)
                            {
                                throw new Exception("Error returned for flush rindex (bulk put) from machine " + somehost + ":"
                                    + Environment.NewLine + err);
                            }
                        }
#if DEBUG
                        throw new Exception("Remote machine " + somehost + " did not return a success signal for flush rindex (bulk put); (byte)" + ib);
#else
                        throw new Exception("Remote machine " + somehost + " did not return a success signal for flush rindex (bulk put)");
#endif
                    }
                }

                conn.islocked = false;
            }

            return true;
        }

        private bool RDelete(string xcmd, Dictionary<string, Dictionary<string, BatchInfo>> hostBatches, int cmdCount)
        {
            string originalcmd = xcmd;
            if (!(0 == string.Compare("RDELETE", Qa.NextPart(ref xcmd), true)
                   && 0 == string.Compare("FROM", Qa.NextPart(ref xcmd), true)))
            {
                return false;
            }

            string indexName = Qa.NextPart(ref xcmd).ToLower();
            if (0 == indexName.Length)
            {
                throw new Exception("RIndex name expected");
            }

            if (!conn.sysindexes.ContainsKey(indexName))
            {
                throw new Exception("RIndex name not found in conn sysindexes.");
            }
         
            QaConnection.Index sysindex = conn.sysindexes[indexName];
            bool UpdateMemoryOnly = sysindex.UpdateMemoryOnly;
#if DEBUG
            if (indexName.EndsWith("UMO"))
            {
                if (!UpdateMemoryOnly)
                {
                    throw new Exception("DEBUG: *UMO index isn't UpdateMemoryOnly");
                }
            }
#endif
            string keytype = sysindex.Table.Columns[sysindex.Ordinal].Type.ToLower();
            int keylen = sysindex.Table.Columns[sysindex.Ordinal].Bytes;
            if (keytype.StartsWith("char(", StringComparison.OrdinalIgnoreCase))
            {
                keytype = "char";
            }
            
            if (string.Compare(Qa.NextPart(ref xcmd), "WHERE", true) != 0)
            {
                throw new Exception("WHERE expected");
            }

            bool haskey = false;
            string skey = "";
            byte[] keybuf = new byte[keylen];
            StringBuilder sbAnds = new StringBuilder();
            for (string s = Qa.NextPart(ref xcmd); ; s = Qa.NextPart(ref xcmd))
            {
                if (0 == s.Length || ";" == s)
                {
                    break;
                }

                switch (s.ToUpper())
                {
                    case "KEY":
                        {
                            if (string.Compare(Qa.NextPart(ref xcmd), "=", true) != 0)
                            {
                                throw new Exception("Equal sign after KEY is expected");
                            }
                            skey = Qa.NextPart(ref xcmd);
                            if (skey == "-")
                            {
                                string numpart = Qa.NextPart(ref xcmd);
                                skey += numpart;
                            }
                            if (skey.Length == 0)
                            {
                                throw new Exception("RINSERT expects a key value of " + keytype + " data type in where clause.");
                            }
                            ConvertKeyToBinary(skey, keylen, keytype);
                            Buffer.BlockCopy(buf, 0, keybuf, 0, keylen);
                            haskey = true;                            
                        }
                        break;

                    case "AND":
                        continue;

                    default:
                        {
                            string colName = s;
                            if (string.Compare(Qa.NextPart(ref xcmd), "=", true) != 0)
                            {
                                throw new Exception("Equal sign after column name is expected:" + originalcmd);
                            }

                            int colOrdinal = -1;
                            for(int coli = 0; coli < sysindex.Table.Columns.Length; coli++)
                            {
                                QaConnection.Column col = sysindex.Table.Columns[coli];
                                if(string.Compare(colName, col.Name, true) == 0)
                                {
                                    colOrdinal = coli;
                                    break;
                                }
                            }
                            if (colOrdinal == -1)
                            {
                                throw new Exception("Column: " + colName + " is not found in sysindex.");
                            }

                            string colValue = Qa.NextPart(ref xcmd);
                            if (colValue == "-")
                            {
                                string numpart = Qa.NextPart(ref xcmd);
                                colValue += numpart;
                            }
                            sbAnds.Append(" and ");
                            sbAnds.Append(colOrdinal);
                            sbAnds.Append("=");
                            sbAnds.Append(colValue);
                        }
                        break;
                }
            }

            if (!haskey)
            {
                throw new Exception("KEY expected");
            }
                        
            List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexName];
            if (mi.Count > 0)
            {
                int left = 0;
                int right = mi.Count - 1;
                //pin hash look up?
                int[] results = BSearchRDelete(mi, buf, ref left, ref right, keylen);
                foreach (int result in results)
                {
                    string chunkName = mi[result].Value;
                    int del = chunkName.IndexOf(@"\", 2);
                    string[] chunkHosts = chunkName.Substring(2, del - 2).Split(';');
                    if (UpdateMemoryOnly)
                    {
                        chunkHosts = new string[] { chunkHosts[0] };
                    }

                    foreach (string host in chunkHosts)
                    {
                        Dictionary<string, BatchInfo> chunkBatches = null;
                        if (!hostBatches.ContainsKey(host))
                        {
                            chunkBatches = new Dictionary<string, BatchInfo>(cmdCount, new _CaseInsensitiveEqualityComparer_2664());
                            hostBatches.Add(host, chunkBatches);
                        }
                        else
                        {
                            chunkBatches = hostBatches[host];
                        }

                        List<Statement> statements;
                        if (!chunkBatches.ContainsKey(chunkName))
                        {
                            statements = new List<Statement>(cmdCount);
                            BatchInfo binfo = BatchInfo.Prepare(indexName, statements);
                            chunkBatches.Add(chunkName, binfo);
                        }
                        else
                        {
                            statements = chunkBatches[chunkName].Statements;
                        }

                        {
                            string str = "1 key=" + skey;
                            if (sbAnds.Length > 0)
                            {
                                str = str + sbAnds.ToString();
                            }
                            int timestamp = statements.Count;
                            Statement newstm = Statement.Prepare(str, timestamp, keybuf);
                            statements.Add(newstm);
                        }
                    }                    
                }               
            }

            return true;
        }

        private bool RInsert(string xcmd, Dictionary<string, Dictionary<string, BatchInfo>> hostBatches, int cmdCount, Dictionary<string, int> chunkInsertCount)
        {
            if (!(0 == string.Compare("RINSERT", Qa.NextPart(ref xcmd), true)
                   && 0 == string.Compare("INTO", Qa.NextPart(ref xcmd), true)))
            {
                return false;
            }

            string indexName = Qa.NextPart(ref xcmd).ToLower();
            if (0 == indexName.Length)
            {
                throw new Exception("RIndex name expected");
            }

#if TEMPTABLES
            if (indexName == "#")
            {
                string temptable = "#";
                if (xcmd.Length != 0 && !char.IsWhiteSpace(xcmd[0]))
                {
                    temptable += Qa.NextPart(ref xcmd);
                }
                this.cmdText = xcmd;
                DbDataReader reader = _ExecuteDbDataReaderRIndex(CommandBehavior.Default);
                if (null == conn.TempTables || !conn.TempTables.ContainsKey(temptable))
                {
                    // This temp table doesn't exist, so add it.
                    if (null == conn.TempTables)
                    {
                        conn.TempTables = new Dictionary<string, QaConnection.TempTableInfo>(new _CaseInsensitiveEqualityComparer_2664());
                    }
                    QaConnection.TempTableInfo tti = new QaConnection.TempTableInfo();
                    tti.Rows = new List<byte[]>();
                    {
                        int numcols = reader.FieldCount;
                        tti.Columns = new List<RDBMS_DBCORE.DbColumn>(numcols);
                        int curoffset = 0;
                        for (int ordinal = 0; ordinal < numcols; ordinal++)
                        {
                            RDBMS_DBCORE.DbColumn col;
                            col.ColumnName = reader.GetName(ordinal);
                            col.Type = RDBMS_DBCORE.DbType.Prepare(reader.GetQaTypeName(ordinal));
                            col.RowOffset = curoffset;
                            curoffset += col.Type.Size;
                            tti.Columns.Add(col);
                        }
                    }
                    conn.TempTables.Add(temptable, tti);
                }
                else
                {
                    // This temp table exists, so verify column types.
                    
                }
                while (reader.Read())
                {
                }
                return true;
            }
#endif

            if (!conn.sysindexes.ContainsKey(indexName))
            {
                throw new Exception("RIndex name not found in conn sysindexes.");
            }

            string keytype = "";
            int keylen = 0;
            bool UpdateMemoryOnly = false;
            {
                QaConnection.Index sysindex = conn.sysindexes[indexName];
                keytype = sysindex.Table.Columns[sysindex.Ordinal].Type.ToLower();
                keylen = sysindex.Table.Columns[sysindex.Ordinal].Bytes;
                UpdateMemoryOnly = sysindex.UpdateMemoryOnly;
#if DEBUG
                if (indexName.EndsWith("UMO"))
                {
                    if (!UpdateMemoryOnly)
                    {
                        throw new Exception("DEBUG: *UMO index isn't UpdateMemoryOnly");
                    }
                }
#endif
                if (keytype.StartsWith("char(", StringComparison.OrdinalIgnoreCase))
                {
                    keytype = "char";
                }
            }

            if (!(string.Compare(Qa.NextPart(ref xcmd), "VALUES", true) == 0 && string.Compare(Qa.NextPart(ref xcmd), "(", true) == 0))
            {
                throw new Exception("VALUES( expected");
            }

            StringBuilder sbValues = new StringBuilder();
            string part = "";
            while (xcmd.Length > 0)
            {
                string val = Qa.NextPart(ref xcmd);
                
                if (val != ")")
                {
                    sbValues.Append(val);
                }
                else
                {                    
                    break;
                }
            }

            if (xcmd.Length == 0 || string.Compare(Qa.NextPart(ref xcmd), "WHERE", true) != 0)
            {
                throw new Exception("WHERE expected");
            }

            if (0 == string.Compare("KEY", Qa.NextPart(ref xcmd), true
                && 0 == string.Compare("=", Qa.NextPart(ref xcmd), true)))
            {               
                string skey = Qa.NextPart(ref xcmd);
                if (skey == "-")
                {
                    string numpart = Qa.NextPart(ref xcmd);
                    skey += numpart;
                }
                if (skey.Length == 0)
                {
                    throw new Exception("RINSERT expects a key value of " + keytype + " data type in where clause.");
                }

                ConvertKeyToBinary(skey, keylen, keytype);
                byte[] keybuf = new byte[keylen];
                Buffer.BlockCopy(buf, 0, keybuf, 0, keylen);
                List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexName];
                if (mi.Count > 0)
                {
                    int left = 0;
                    int right = mi.Count - 1;
                    //pin hash look up?     
                    int result = BSearchRInsert(mi, buf, ref left, ref right, keylen);
                    string chunkName = mi[result].Value;
                    int del = chunkName.IndexOf(@"\", 2);
                    string[] chunkHosts = chunkName.Substring(2, del - 2).Split(';');
                    if (UpdateMemoryOnly)
                    {
                        chunkHosts = new string[] { chunkHosts[0] };
                    }

                    if (!chunkInsertCount.ContainsKey(chunkName))
                    {
                        chunkInsertCount.Add(chunkName, 1);
                    }
                    else
                    {
                        chunkInsertCount[chunkName]++;
                    }

                    foreach (string host in chunkHosts)
                    {
                        Dictionary<string, BatchInfo> chunkBatches = null;
                        if (!hostBatches.ContainsKey(host))
                        {
                            chunkBatches = new Dictionary<string, BatchInfo>(cmdCount, StringComparer.OrdinalIgnoreCase);
                            hostBatches.Add(host, chunkBatches);
                        }
                        else
                        {
                            chunkBatches = hostBatches[host];
                        }

                        List<Statement> statements;

                        if (!chunkBatches.ContainsKey(chunkName))
                        {
                            statements = new List<Statement>(cmdCount);
                            BatchInfo binfo = BatchInfo.Prepare(indexName, statements);
                            chunkBatches.Add(chunkName, binfo);
                        }
                        else
                        {
                            statements = chunkBatches[chunkName].Statements;
                        }

                        {
                            string str = string.Format("0 key={0} ({1})", skey, sbValues.ToString());
                            int timestamp = statements.Count;
                            Statement newstm = Statement.Prepare(str, timestamp, keybuf);
                            statements.Add(newstm);
                        }                    
                    }                    
                }
            }
            else
            {
                throw new Exception("KEY expected");
            }
            return true;
        }

        private int TypeToInt(string keytype)
        {
            switch (keytype)
            {
                case "long": return 1;
                case "int": return 2;
                case "double": return 3;
                case "datetime": return 4;
                case "char": return 5;
            }
            if (keytype.StartsWith("char"))
            {
                return 5;
            }
            return 0;
        }

        private void ConvertKeyToBinary(string skey, int keylen, string keytype)
        {
            ConvertKeyToBinary(Qa.StringSlice.Prepare(skey), keylen, TypeToInt(keytype));
        }

        private void ConvertKeyToBinary(Qa.StringSlice skey, int keylen, int ikeytype)
        {
            if (buf.Length < keylen)
            {
                buf = new byte[keylen];
            }
            buf[0] = 0; //is null = false on first byte.
            switch (ikeytype)
            {
                case 1: // "long"
                    {
                        long key = Qa.Int64Parse(skey);
                        UInt64 ukey = (ulong)(key + long.MaxValue + 1);
                        Utils.Int64ToBytes((Int64)ukey, buf, 1);
                    }
                    break;

                case 2: // "int"
                    {
                        int key = Qa.Int32Parse(skey);
                        uint ukey = (uint)(key + int.MaxValue + 1);
                        Utils.Int32ToBytes((int)ukey, buf, 1);
                    }
                    break;

                case 3: // "double"
                    {
                        double key = Double.Parse(skey.ToString());
                        Utils.DoubleToBytes(key, buf, 1);
                    }
                    break;

                case 4: // "datetime"
                    {
                        skey = skey.Substring(1, skey.Length - 2);
                        DateTime key = DateTime.Parse(skey.ToString());
                        Utils.Int64ToBytes(key.Ticks, buf, 1);
                    }
                    break;

                case 5: // "char"
                    {
                        skey = skey.Substring(1, skey.Length - 2);
                        string key = skey.ToString().Replace("''", "'");
                        byte[] strbuf = System.Text.Encoding.Unicode.GetBytes(key);
                        if (strbuf.Length > keylen - 1)
                        {
                            throw new Exception("String too large.");
                        }
                        for (int si = 0; si < strbuf.Length; si++)
                        {
                            buf[si + 1] = strbuf[si];
                        }
                        int padlen = keylen - 1 - strbuf.Length;
                        for (int si = strbuf.Length + 1; padlen > 0; padlen--)
                        {
                            buf[si++] = 0;
                        }
                    }
                    break;
            }
        }

        public override int ExecuteNonQuery()
        {
            if (conn.isRIndexEnabled())
            {
                return _ExecuteNonQueryRIndex();
            }

            try
            {
                CheckOkToExecute();

                conn.BatchNqData(SubParameterValues(cmdText));

                //# of rows affected.
                return 0;
            }
            catch
            {
                try
                {
                    Abort();
                }
                catch
                {
                }       
                throw;
            }
        }

        private DbDataReader _ExecuteDbDataReaderRIndex(CommandBehavior behavior)
        {
            try
            {
                CheckOkToExecute();
                conn.islocked = true;

                if (conn.mindexes == null)
                {
                    throw new Exception("Master indexes is null.");
                }
                Qa.StringSlice ssxcmd = Qa.StringSlice.Prepare(cmdText);
                if (0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "RSELECT", StringComparison.OrdinalIgnoreCase)
                    && 0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "*", StringComparison.OrdinalIgnoreCase)
                    && 0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "FROM", StringComparison.OrdinalIgnoreCase)
                    )
                {
                    string indexname = Qa.SliceNextPart(ref ssxcmd).ToString().ToLower();
                    if (indexname.Length == 0)
                    {
                        throw new Exception("RSELECT expects an index name.");
                    }
                    if (!conn.mindexes.ContainsKey(indexname))
                    {
                        throw new Exception("Index " + indexname + " is not found in the master indexes.");
                    }

                    int samplesize = 0;
                    Qa.StringSlice nextarg = Qa.SliceNextPart(ref ssxcmd);
                    if (0 == Qa.StringSlice.Compare(nextarg, "SAMPLE", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            samplesize = Qa.Int32Parse(Qa.SliceNextPart(ref ssxcmd));
                            if (samplesize <= 0)
                            {
                                throw new Exception("SAMPLE size expected to be greater than 0.");
                            }
                        }
                        catch
                        {
                            throw new Exception("SAMPLE size integer expected.");
                        }

                        nextarg = Qa.SliceNextPart(ref ssxcmd);
                    }

                    if (0 == Qa.StringSlice.Compare(nextarg, "WHERE", StringComparison.OrdinalIgnoreCase))
                    {
                        string keytype = "long";
                        int keylen = 9;
                        QaConnection.Index sysindex;
                        if (conn.sysindexes.ContainsKey(indexname))
                        {
                            sysindex = conn.sysindexes[indexname];
                            keytype = sysindex.Table.Columns[sysindex.Ordinal].Type.ToLower();
                            keylen = sysindex.Table.Columns[sysindex.Ordinal].Bytes;
                            if (keytype.StartsWith("char(", StringComparison.OrdinalIgnoreCase))
                            {
                                keytype = "char";
                            }
                        }
                        else
                        {
                            //sysindex = new QaConnection.Index();
                            throw new Exception("Table not found in sysindexes; must recreate rindex");
                        }
                        int ikeytype = TypeToInt(keytype);

                        Dictionary<string, StringBuilder> batches = new Dictionary<string, StringBuilder>(StringComparer.OrdinalIgnoreCase);
                        Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond
                            + System.Diagnostics.Process.GetCurrentProcess().Id
                            + System.Threading.Thread.CurrentThread.ManagedThreadId
                            + ssxcmd.Length));
                        char op = 'x';
                        if (buf.Length < keylen)
                        {
                            buf = new byte[keylen];
                        }
                        for (; ; )
                        {
                            if (0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "KEY", StringComparison.OrdinalIgnoreCase)
                                && 0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "=", StringComparison.OrdinalIgnoreCase))
                            {
                                Qa.StringSlice skey = Qa.SliceNextPart(ref ssxcmd);
                                if (0 == Qa.StringSlice.Compare(skey, "-"))
                                {
                                    Qa.StringSlice numpart = Qa.SliceNextPart(ref ssxcmd);
                                    skey = Qa.StringSlice.Concat(skey, numpart);
                                }
                                if (skey.Length == 0)
                                {
                                    throw new Exception("RSELECT expects a key value of " + keytype + " data type in where clause.");
                                }

                                ConvertKeyToBinary(skey, keylen, ikeytype);                          

                                List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexname];
                                string chunkname = "";
                                string host = "";
                                if (mi.Count > 0)
                                {
                                    int result = -2;
                                    bool isfirstkeyofchunk = false;
                                    int left = 0;
                                    int right = mi.Count - 1;
                                    byte[] maxkey = null;
                                    if (conn.sysindexes.ContainsKey(indexname))
                                    {
                                        QaConnection.Index thisIndex = conn.sysindexes[indexname];
                                        maxkey = thisIndex.MaxKey;
                                       
                                        if (thisIndex.PinHash)
                                        {
                                            int shortkey = QaConnection.TwoBytesToInt(buf[1], buf[2]);
                                            QaConnection.Position thispos = thisIndex.Hash[shortkey];
                                            if (thispos.Length > 0)
                                            {
                                                left = thispos.Offset;
                                                right = left + thispos.Length - 1;
                                                if (left > 0)
                                                {
                                                    left--;
                                                }
                                            }
                                        }                                       
                                    }
                                    if (right >= 0)
                                    {
                                        result = BSearch(mi, buf, ref left, ref right, keylen, ref isfirstkeyofchunk);
                                    }                                    
                                    /*if (result == -3 && CompareBytes(buf, maxkey, keylen) <= 0)
                                    {
                                        result = right;  //out of range: too big, but still smaller than the maxkey.
                                    }*/
                                    if (result >= 0)
                                    {
                                        chunkname = mi[result].Value;
                                        if (isfirstkeyofchunk)
                                        {
                                            chunkname = chunkname + "*";
                                        }
                                        int del = chunkname.IndexOf(@"\", 2);
                                        host = (chunkname.Substring(2, del - 2).Split(';'))[0];
                                    }
                                    else
                                    {
                                        host = conn.connstr.DataSource[rnd.Next() % conn.connstr.DataSource.Length];
                                    }
                                }

                                StringBuilder batch;
                                if (batches.ContainsKey(host))
                                {
                                    batch = batches[host];
                                }
                                else
                                {
                                    batch = new StringBuilder(1024);
                                    batches.Add(host, batch);
                                }
                                if ('x' != op)
                                {
                                    if (batch.Length > 0)
                                    {
                                        batch.Append(op);
                                    }
                                }
                                batch.Append(chunkname);
                                batch.Append('\0');
                                batch.Append(skey);

                                if (0 == Qa.StringSlice.Compare(Qa.SliceNextPart(ref ssxcmd), "OR", StringComparison.OrdinalIgnoreCase))
                                {
                                    {
                                        QaConnection.QaConnectionString xconnstr = conn.connstr;
                                        QaConnection.QaConnectionString.RIndexType xrindextype = xconnstr.RIndex;
                                        if (xrindextype != QaConnection.QaConnectionString.RIndexType.POOLED)
                                        {
                                            throw new Exception("RSELECT: cannot use OR in WHERE if rindex is not pooled");
                                        }
                                    }
                                    op = '\0';
                                    continue;
                                }
                                //else
                                {
                                    // Done!
                                    List<string> bhosts = new List<string>(batches.Keys);
                                    List<QaDataReader> xreaders = new List<QaDataReader>(batches.Count);                                    
                                    for (; ; )
                                    {
                                        int bhostindex = rnd.Next() % bhosts.Count;
                                        string bhost = bhosts[bhostindex];                                      
                                        StringBuilder bbatch = batches[bhost];
                                        conn.OpenSocketRIndex(bhost);
                                        conn.netstm.WriteByte((byte)'s'); //search master index
                                        XContent.SendXContent(conn.netstm, indexname);
                                        Utils.Int32ToBytes(samplesize, buf, 0);
                                        XContent.SendXContent(conn.netstm, buf, 4);
                                        XContent.SendXContent(conn.netstm, bbatch.ToString());

                                        int ib = conn.netstm.ReadByte();

                                        if (ib == (byte)'-')
                                        {
                                            string errmsg = XContent.ReceiveXString(conn.netstm, buf);
                                            throw new Exception("_ExecuteDbDataReaderRIndex error: " + errmsg);
                                        }
                                        if (ib != (byte)'+')
                                        {
                                            throw new Exception("_ExecuteDbDataReaderRIndex did not receive a success signal from service.");
                                        }

                                        QaDataReader xreader = new QaDataReaderRIndex(this, ref sysindex, conn.netstm);

                                        xreaders.Add(xreader);

                                        bhosts.RemoveAt(bhostindex);
                                        if(bhosts.Count == 0)
                                        {
                                            break;
                                        } 
                                    }                                    
                                    if (1 == xreaders.Count)
                                    {
                                        return xreaders[0];
                                    }
                                    return new QaDataReaderMulti(xreaders);
                                }

                            }
                            else
                            {
                                throw new Exception("Expected KEY = ... in WHERE clause");
                            }
                        }
                    }                    
                }
                throw new Exception("Query not recognized.");
            }
            catch(Exception e)
            {
                XLog.errorlog("QaCommand._ExecuteDbDataReaderRIndex error:" + e.ToString());
                try
                {                    
                    Abort();
                }
                catch
                {
                }
                throw e;
            }
        }
       
        private Dictionary<string, Dictionary<string, BatchInfo>> DoFailoverNonQueryRIndex(Dictionary<string, Dictionary<string, BatchInfo>> hostBatches, List<string> badHosts, int lastFailoverHostIndex, int cmdCount)
        {
            Dictionary<string, Dictionary<string, BatchInfo>> fHostBatches = new Dictionary<string, Dictionary<string, BatchInfo>>(conn.connstr.DataSource.Length, new _CaseInsensitiveEqualityComparer_2664());
            lastFailoverHostIndex++;

            foreach (string badhost in badHosts)
            {
                Dictionary<string, BatchInfo> chunkBatches = hostBatches[badhost];
                foreach (KeyValuePair<string, BatchInfo> pair in chunkBatches)
                {
                    string chunkName = pair.Key;
                    int del = chunkName.IndexOf(@"\", 2);
                    string[] chunkHosts = chunkName.Substring(2, del - 2).Split(';');
                    if (lastFailoverHostIndex >= chunkHosts.Length)
                    {
                        throw new Exception("Run out of hosts to fail over to: lastFailoverHostIndex: " + lastFailoverHostIndex.ToString());
                    }

                    string failoverHost = chunkHosts[lastFailoverHostIndex];
                    Dictionary<string, BatchInfo> fChunkBatches;
                    if (!fHostBatches.ContainsKey(failoverHost))
                    {
                        fChunkBatches = new Dictionary<string, BatchInfo>(cmdCount, new _CaseInsensitiveEqualityComparer_2664());
                        fHostBatches.Add(failoverHost, fChunkBatches);
                    }
                    else
                    {
                        fChunkBatches = fHostBatches[failoverHost];
                    }

                    fChunkBatches.Add(chunkName, pair.Value);
                }
            }

            return fHostBatches;
        }

        private Dictionary<string, StringBuilder> DoFailoverQueryRIndex(StringBuilder sbBatch, int lastFailoverHostIndex)
        {
            Dictionary<string, StringBuilder> newbatches = new Dictionary<string, StringBuilder>(new _CaseInsensitiveEqualityComparer_2664());
            string[] stms = sbBatch.ToString().Split('\0');
            ++lastFailoverHostIndex;

            for (int i = 0; i < stms.Length; i += 2)
            {
                string chunkname = stms[i];
                string skey = stms[i + 1];
                int del = chunkname.IndexOf(@"\", 2);
                string[] hosts = chunkname.Substring(2, del - 2).Split(';');

                if (lastFailoverHostIndex >= hosts.Length)
                {
                    throw new Exception("No more host to fail over to.");
                }

                string failoverhost = hosts[lastFailoverHostIndex];
                StringBuilder batch;
                if (!newbatches.ContainsKey(failoverhost))
                {
                    batch = new StringBuilder(1024);
                    newbatches.Add(failoverhost, batch);
                }
                else
                {
                    batch = newbatches[failoverhost];
                }

                if (batch.Length > 0)
                {
                    batch.Append('\0');
                }
                batch.Append(chunkname);
                batch.Append('\0');
                batch.Append(skey);
            }

            return newbatches;
        }

        private int FindFirstOccurrence(List<KeyValuePair<byte[], string>> keys, int startindex, byte[] key, int length)
        {
            int firstoccur = startindex;
            for (int i = startindex - 1; i >= 0; i--)
            {
                if (CompareBytes(key, keys[i].Key, length) == 0)
                {
                    firstoccur = i;
                }
                else
                {
                    break;
                }
            }
            return firstoccur;
        }

        private int FindLastOccurrence(List<KeyValuePair<byte[], string>> keys, int startindex, byte[] key, int length)
        {
            int lastoccur = startindex;
            for (int i = startindex + 1; i < keys.Count; i++)
            {
                if (CompareBytes(key, keys[i].Key, length) == 0)
                {
                    lastoccur = i;
                }
                else
                {
                    break;
                }
            }
            return lastoccur;
        }

        private int BSearch(List<KeyValuePair<byte[], string>> keys, byte[] key, ref int left, ref int right, int length, ref bool isfirstkeyofchunk)
        {
            isfirstkeyofchunk = false;

            int cl = CompareBytes(key, keys[left].Key, length);
            if (cl == 0)
            {
                isfirstkeyofchunk = true;
                return FindFirstOccurrence(keys, left, key, length);
            }
            int cr = CompareBytes(key, keys[right].Key, length);
            if (cr == 0)
            {
                isfirstkeyofchunk = true;
                return FindFirstOccurrence(keys, right, key, length);
            }
            if (cl < 0)   //out of range:  too small
            {
                return left;
            }
            if (cr > 0)   //out of range: too big
            {
                return right;
            }
            if (right - left < 2)
            {
                return left;  //nothing in between, but still in range.
            }
            int mid = (right - left) / 2 + left;
            int cm = CompareBytes(key, keys[mid].Key, length);
            if (cm == 0)
            {
                isfirstkeyofchunk = true;
                return FindFirstOccurrence(keys, mid, key, length);
            }
            if (cm > 0)
            {
                left = mid;
            }
            else
            {
                right = mid;
            }
            return BSearch(keys, key, ref left, ref right, length, ref isfirstkeyofchunk);  
        }

        private int BSearchRInsert(List<KeyValuePair<byte[], string>> keys, byte[] key, ref int left, ref int right, int length)
        {
            int cl = CompareBytes(key, keys[left].Key, length);
            if (cl == 0)
            {
                return FindLastOccurrence(keys, left, key, length);
            }
            int cr = CompareBytes(key, keys[right].Key, length);
            if (cr == 0)
            {
                return FindLastOccurrence(keys, right, key, length);
            }
            if (cl < 0)   //out of range:  too small
            {
                return left;
            }
            if (cr > 0)   //out of range: too big
            {
                return right;
            }
            if (right - left < 2)
            {
                return left;  //nothing in between, but still in range.
            }
            int mid = (right - left) / 2 + left;
            int cm = CompareBytes(key, keys[mid].Key, length);
            if (cm == 0)
            {
                return FindLastOccurrence(keys, mid, key, length);
            }
            if (cm > 0)
            {
                left = mid;
            }
            else
            {
                right = mid;
            }
            return BSearchRInsert(keys, key, ref left, ref right, length);
        }

        private int[] BSearchRDelete(List<KeyValuePair<byte[], string>> keys, byte[] key, ref int left, ref int right, int length)
        {
            int firstoccur = -1;
            int lastoccur = -1;

            int cl = CompareBytes(key, keys[left].Key, length);
            if (cl == 0)
            {
                firstoccur = FindFirstOccurrence(keys, left, key, length);
                if (firstoccur > 0)
                {
                    firstoccur--;
                }
                lastoccur = FindLastOccurrence(keys, left, key, length);
                return GetToDeleteRanges(lastoccur, firstoccur);
            }
            int cr = CompareBytes(key, keys[right].Key, length);
            if (cr == 0)
            {
                firstoccur = FindFirstOccurrence(keys, right, key, length);
                if (firstoccur > 0)
                {
                    firstoccur--;
                }
                lastoccur = FindLastOccurrence(keys, right, key, length);
                return GetToDeleteRanges(lastoccur, firstoccur);
            }
            if (cl < 0)   //out of range:  too small
            {
                firstoccur = left;
                lastoccur = left;
                return GetToDeleteRanges(lastoccur, firstoccur);
            }
            if (cr > 0)   //out of range: too big
            {
                firstoccur = right;
                lastoccur = right;
                return GetToDeleteRanges(lastoccur, firstoccur);
            }
            if (right - left < 2)
            {
                firstoccur = left; //nothing in between, but still in range.
                lastoccur = left;
                return GetToDeleteRanges(lastoccur, firstoccur);
            }
            int mid = (right - left) / 2 + left;
            int cm = CompareBytes(key, keys[mid].Key, length);
            if (cm == 0)
            {
                firstoccur = FindFirstOccurrence(keys, mid, key, length);
                if (firstoccur > 0)
                {
                    firstoccur--;
                }
                lastoccur = FindLastOccurrence(keys, mid, key, length);
                return GetToDeleteRanges(lastoccur, firstoccur);
            }

            if (cm > 0)
            {
                left = mid;
            }
            else
            {
                right = mid;
            }
            return BSearchRDelete(keys, key, ref left, ref right, length);
        }

        private int[] GetToDeleteRanges(int lastoccur, int firstoccur)
        {            
            int[] toDelete = new int[lastoccur - firstoccur + 1];
            for (int i = 0; i < toDelete.Length; i++)
            {
                toDelete[i] = firstoccur + i;
            }
            return toDelete;                     
        }

        private int CompareBytes(byte[] x, byte[] y, int length)
        {
            for (int i = 0; i < length; i++)
            {
                if (x[i] != y[i])
                {
                    return x[i] - y[i];
                }
            }
            return 0;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (conn.isRIndexEnabled())
            {
                return _ExecuteDbDataReaderRIndex(behavior);
            }

            try
            {
                CheckOkToExecute();

                conn.FlushBatchNqData();

                conn.islocked = true;
                conn.lockedby = this;

                conn.netstm.WriteByte((byte)'q'); //query

                XContent.SendXContent(conn.netstm, SubParameterValues(cmdText));

                int ib = conn.netstm.ReadByte();

                if (ib == (byte)'-')
                {
                    string errmsg = XContent.ReceiveXString(conn.netstm, buf);
                    throw new Exception("ExecuteDbReader error: " + errmsg);
                }
                if (ib != (byte)'+')
                {
                    throw new Exception("ExecuteDbDataReader did not receive a success signal from service.");
                }

                return new QaDataReader(this, conn.netstm);
            }
            catch
            {
                try
                {
                    Abort();
                }
                catch
                {
                }                
                throw;
            }            
        }

        private void ParseCommandText(string txt)
        {
            if (txt == null || txt.IndexOf('@') == -1)
            {
                if (paramplaceholders != null)
                {
                    paramplaceholders = null;
                }
                return;
            }

            paramplaceholders = new Dictionary<int, string>();

            //Find all 'string literals'
            Regex regx = new Regex("'[^']*'", RegexOptions.IgnoreCase);
            MatchCollection lits = regx.Matches(txt);

            //Find parameter placeholders @parameterName
            regx = new Regex(@"@[\w]+", RegexOptions.IgnoreCase);
            MatchCollection phs = regx.Matches(txt);

            foreach (Match p in phs)
            {
                //make sure not in string literals.
                bool isliteral = false;
                foreach (Match s in lits)
                {
                    int lower = s.Index;
                    int upper = s.Index + s.Length - 1;
                    if (p.Index >= lower && p.Index <= upper)
                    {
                        isliteral = true;
                        break;
                    }
                }
                if (!isliteral)
                {
                    paramplaceholders.Add(p.Index, p.Value);
                }
            }
        }

        private string SubParameterValues(string txt)
        {
            if (paramplaceholders == null || paramplaceholders.Count == 0)
            {
                return txt;
            }
            if (parameters == null || parameters.Count == 0)
            {
                throw new Exception("The DbCommand object has @parameter in its CommandText but it has an empty Parameters collection.");
            }

            string newsql = "";
            int prevIndex = 0;
            foreach (int hindex in paramplaceholders.Keys)
            {
                string pname = paramplaceholders[hindex];
                int pindex = parameters.IndexOf(pname);
                if (pindex == -1)
                {
                    throw new Exception("Parameter " + pname + " is not found in the DbCommand object");
                }

                QaParameter param = (QaParameter)parameters[pindex]; 
                string sub = "";
                if (null != param.Value && DBNull.Value.Equals(param.Value))
                {
                    sub = "NULL";
                }
                else
                {
                    switch (param.DbType)
                    {
                        case (DbType.String):
                            {
                                string str = (string)param.Value;
                                if (str == null)
                                {
                                    sub = "NULL";
                                }
                                else
                                {
                                    if (str.Length > param.Size)
                                    {
                                        str = str.Substring(0, param.Size);
                                    }
                                    sub = "'" + str.Replace("'", "''") + "'";
                                }
                            }
                            break;

                        case (DbType.Int16):
                            sub = Convert.ToInt16(param.Value).ToString();
                            break;

                        case (DbType.UInt16):
                            sub = Convert.ToUInt16(param.Value).ToString();
                            break;

                        case (DbType.Int32):
                            sub = Convert.ToInt32(param.Value).ToString();
                            break;

                        case (DbType.UInt32):
                            sub = Convert.ToUInt32(param.Value).ToString();
                            break;

                        case (DbType.Int64):
                            sub = Convert.ToInt64(param.Value).ToString();
                            break;

                        case (DbType.UInt64):
                            sub = Convert.ToUInt64(param.Value).ToString();
                            break;

                        case (DbType.Double):
                            sub = Convert.ToDouble(param.Value).ToString();
                            break;

                        case (DbType.DateTime):
                            sub = "'" + Convert.ToDateTime(param.Value).ToString() + "'";
                            break;

                        default:
                            throw new Exception("Parameter type not supported yet");
                    }
                }
                newsql += txt.Substring(prevIndex, hindex - prevIndex);
                newsql += sub;
                prevIndex = hindex + paramplaceholders[hindex].Length;
            }

            if (prevIndex < txt.Length)
            {
                newsql += txt.Substring(prevIndex);
            }

            return newsql;
        }

        private void CheckOkToExecute()
        {
            if (conn == null || conn.State != ConnectionState.Open)
            {
                throw new Exception("Connection is not open.");
            }

            if (cmdText == null || cmdText.Length == 0)
            {
                throw new Exception("Command text is empty.");
            }

            if (conn.islocked)
            {
                throw new Exception("Connection is busy with another execution.");
            }
        }

        internal void Abort()
        {
            if (conn != null && conn.State == ConnectionState.Open)
            {
                try
                {
                    conn.Close();
                }
                catch
                {
                }
            }
        }


        internal class _CaseInsensitiveEqualityComparer_2664 : IEqualityComparer<string>
        {
            bool IEqualityComparer<string>.Equals(string x, string y)
            {
                return 0 == string.Compare(x, y, StringComparison.OrdinalIgnoreCase);
            }

            int IEqualityComparer<string>.GetHashCode(string obj)
            {
                unchecked
                {
                    int result = 8385;
                    int cnt = obj.Length;
                    for (int i = 0; i < cnt; i++)
                    {
                        result <<= 4;
                        char ch = obj[i];
                        if (ch >= 'A' && ch <= 'Z')
                        {
                            ch = (char)('a' + (ch - 'A'));
                        }
                        result += ch;
                    }
                    return result;
                }
            }
        }

        private int SortStatement(Statement x, Statement y)
        {
            byte[] xbuf = x.KeyBuf;
            byte[] ybuf = y.KeyBuf;

            for (int i = 0; i < xbuf.Length; i++)
            {
                if (xbuf[i] != ybuf[i])
                {
                    return xbuf[i] - ybuf[i];
                }
            }

            return x.Timestamp - y.Timestamp;
        }

        private struct BatchInfo
        {
            public string IndexName;
            public List<Statement> Statements;

            public static BatchInfo Prepare(string indexName, List<Statement> statements)
            {
                BatchInfo binfo;
                binfo.IndexName = indexName;
                binfo.Statements = statements;
                return binfo;
            }
        }

        private struct Statement
        {
            public string Str;
            public int Timestamp;
            public byte[] KeyBuf;

            public static Statement Prepare(string str, int timestamp, byte[] keybuf)
            {
                Statement stm;
                stm.Str = str;
                stm.Timestamp = timestamp;
                stm.KeyBuf = keybuf;
                return stm;
            }
        }
    }
}
