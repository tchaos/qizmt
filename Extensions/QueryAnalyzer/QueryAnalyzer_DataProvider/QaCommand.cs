using System;
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

        public override int ExecuteNonQuery()
        {
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
                string xcmd = cmdText;
                if (0 == string.Compare("RSELECT", Qa.NextPart(ref xcmd), true) &&
                    0 == string.Compare("*", Qa.NextPart(ref xcmd), true) &&
                    0 == string.Compare("FROM", Qa.NextPart(ref xcmd), true))
                {
                    string indexname = Qa.NextPart(ref xcmd).ToLower();
                    if (indexname.Length == 0)
                    {
                        throw new Exception("RSELECT expects an index name.");
                    }
                    if (!conn.mindexes.ContainsKey(indexname))
                    {
                        throw new Exception("Index " + indexname + " is not found in the master indexes.");
                    }
                    if (0 == string.Compare("WHERE", Qa.NextPart(ref xcmd), true))
                    {
                        string keytype = "long";
                        int keylen = 9;
                        if (conn.sysindexes.ContainsKey(indexname))
                        {
                            QaConnection.Index sysindex = conn.sysindexes[indexname];
                            keytype = sysindex.Table.Columns[sysindex.Ordinal].Type.ToLower();
                            keylen = sysindex.Table.Columns[sysindex.Ordinal].Bytes;
                            if (keytype.StartsWith("char(", StringComparison.OrdinalIgnoreCase))
                            {
                                keytype = "char";
                            }
                        }

                        Dictionary<string, StringBuilder> batches = new Dictionary<string, StringBuilder>(new _CaseInsensitiveEqualityComparer_2664());
                        char op = 'x';
                        if (buf.Length < keylen)
                        {
                            buf = new byte[keylen];
                        }
                        for (; ; )
                        {
                            if (0 == string.Compare("KEY", Qa.NextPart(ref xcmd), true)
                                && 0 == string.Compare("=", Qa.NextPart(ref xcmd), true))
                            {
                                string skey = Qa.NextPart(ref xcmd);
                                if (skey == "-")
                                {
                                    string numpart = Qa.NextPart(ref xcmd);
                                    skey += numpart;
                                }
                                if (skey.Length == 0)
                                {
                                    throw new Exception("RSELECT expects a key value of " + keytype + " data type in where clause.");
                                }
                                                                
                                buf[0] = 0; //is null = false on first byte.
                                switch (keytype)
                                {
                                    case "long":
                                        {
                                            long key = Int64.Parse(skey);
                                            UInt64 ukey = (ulong)(key + long.MaxValue + 1);  
                                            Utils.Int64ToBytes((Int64)ukey, buf, 1);
                                        }
                                        break;

                                    case "int":
                                        {
                                            int key = Int32.Parse(skey);
                                            uint ukey = (uint)(key + int.MaxValue + 1);
                                            Utils.Int32ToBytes((int)ukey, buf, 1);
                                        }
                                        break;

                                    case "double":
                                        {
                                            double key = Double.Parse(skey);
                                            Utils.DoubleToBytes(key, buf, 1);
                                        }
                                        break;

                                    case "datetime":
                                        {
                                            skey = skey.Substring(1, skey.Length - 2);
                                            DateTime key = DateTime.Parse(skey);
                                            Utils.Int64ToBytes(key.Ticks, buf, 1);
                                        }
                                        break;

                                    case "char":
                                        {
                                            skey = skey.Substring(1, skey.Length - 2);
                                            string key = skey.Replace("''", "'");
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

                                List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexname];
                                string chunkname = "";
                                string host = "";
                                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                                if (mi.Count > 0)
                                {
                                    int result = -2;
                                    int left = 0;
                                    int right = mi.Count - 1;
                                    if (conn.sysindexes.ContainsKey(indexname))
                                    {
                                        QaConnection.Index thisIndex = conn.sysindexes[indexname];
                                        if (thisIndex.PinHash)
                                        {
                                            int shortkey = QaConnection.TwoBytesToInt(buf[1], buf[2]);
                                            QaConnection.Position thispos = thisIndex.Hash[shortkey];
                                            left = thispos.Offset;                                           
                                            right = left + thispos.Length - 1;
                                            if (left > 0)
                                            {
                                                left--;
                                            }
                                        }
                                    }
                                    if (right >= 0)
                                    {
                                        result = BSearch(mi, buf, ref left, ref right, keylen);
                                    }                                    
                                    if (result == -1)
                                    {
                                        result = left;   //Though not found in master index, but still in range.
                                    }
                                    else if (result == -3)
                                    {
                                        result = right;  //out of range: too big.
                                    }
                                    if (result >= 0)
                                    {
                                        chunkname = mi[result].Value;
                                        int del = chunkname.IndexOf(@"\", 2);
                                        host = chunkname.Substring(2, del - 2);
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

                                if (0 == string.Compare("OR", Qa.NextPart(ref xcmd), true))
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

                                        QaDataReader xreader = new QaDataReader(this, conn.netstm);

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

        private int BSearch(List<KeyValuePair<byte[], string>> keys, byte[] key, ref int left, ref int right, int length)
        {
            int cl = CompareBytes(key, keys[left].Key, length);
            if (cl == 0)
            {
                return left;
            }
            int cr = CompareBytes(key, keys[right].Key, length);
            if (cr == 0)
            {
                return right;
            }
            if (left == right && cl > 0)
            {
                return -1;
            }
            if (cl < 0)   //out of range:  too small
            {
                return -2;
            }
            if (cr > 0)   //out of range: too big
            {
                return -3;
            }
            if (right - left < 2)
            {
                return -1;   //nothing in between 
            }
            int mid = (right - left) / 2 + left;
            int cm = CompareBytes(key, keys[mid].Key, length);
            if (cm == 0)
            {
                return mid;
            }
            if (cm > 0)
            {
                left = mid;
            }
            else
            {
                right = mid;
            }
            return BSearch(keys, key, ref left, ref right, length);  
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
                conn.Close();
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

    }
}
