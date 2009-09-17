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
                    if (0 == string.Compare("WHERE", Qa.NextPart(ref xcmd), true) &&
                    0 == string.Compare("KEY", Qa.NextPart(ref xcmd), true) &&
                    0 == string.Compare("=", Qa.NextPart(ref xcmd), true))
                    {
                        string skey = Qa.NextPart(ref xcmd);
                        if (skey == "-")
                        {
                            string numpart = Qa.NextPart(ref xcmd);
                            skey += numpart;
                        }
                        if (skey.Length == 0)
                        {
                            throw new Exception("RSELECT expects a key value of LONG data type in where clause.");
                        }
                        long key = Int64.Parse(skey);

                        List<KeyValuePair<byte[], string>> mi = conn.mindexes[indexname];                        
                        string chunkname = "";
                        string host = "";
                        if (mi.Count > 0)
                        {
                            int result = -2;
                            int left = 0;
                            int right = mi.Count - 1;
                            UInt64 ukey = (ulong)(key + long.MaxValue + 1);
                            buf[0] = 0; //is null = false on first byte.
                            Utils.Int64ToBytes((Int64)ukey, buf, 1); 
                            result = BSearch(mi, buf, ref left, ref right, 9);
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
                        }
                        conn.OpenSocketRIndex(host);
                        conn.netstm.WriteByte((byte)'s'); //search master index
                        XContent.SendXContent(conn.netstm, indexname);
                        XContent.SendXContent(conn.netstm, buf, 9);
                        XContent.SendXContent(conn.netstm, chunkname);

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

                        return new QaDataReader(this, conn.netstm);
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
    }
}
