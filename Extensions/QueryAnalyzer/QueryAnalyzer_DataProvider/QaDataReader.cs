using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    public class QaDataReader: DbDataReader
    {
        private MetaData[] metadata = null;
        private Dictionary<string, int> ordinals = null;
        private QaCommand cmd = null;
        private System.Net.Sockets.NetworkStream netstm = null;       
        private object[] currow = null;
        private DataTable schemaTable = null;
        private bool isClosed = true;
        private bool eof = true;
        private bool isRindexEnabled = false;
        private bool isFirstRead = true;
        private int allRowsByteCount = 0;
        private int allRowsCurPos = 0;

        public QaDataReader()
        {
        }

        public QaDataReader(QaCommand cm, System.Net.Sockets.NetworkStream ns)
        {
            cmd = cm;
            netstm = ns;
            isClosed = false;
            eof = false;
            isRindexEnabled = cm.conn.isRIndexEnabled();

            //Get metadata
            try
            {
                //column count
                int bc = 0;
                cmd.buf = XContent.ReceiveXBytes(netstm, out bc, cmd.buf);

                if (bc != 4)
                {
                    throw new Exception("Column count expected 4 bytes.");
                }

                int columnCount = Utils.BytesToInt(cmd.buf);
                metadata = new MetaData[columnCount];
                ordinals = new Dictionary<string, int>();

                for (int i = 0; i < columnCount; i++)
                {
                    string cname = XContent.ReceiveXString(netstm, cmd.buf);
                    string ctype = XContent.ReceiveXString(netstm, cmd.buf);
                    cmd.buf = XContent.ReceiveXBytes(netstm, out bc, cmd.buf);
                    if (bc != 4)
                    {
                        throw new Exception("Front bytes expected 4 bytes.");
                    }
                    int frontbytes = Utils.BytesToInt(cmd.buf);
                    cmd.buf = XContent.ReceiveXBytes(netstm, out bc, cmd.buf);
                    if (bc != 4)
                    {
                        throw new Exception("Size expected 4 bytes.");
                    }
                    int csize = Utils.BytesToInt(cmd.buf);
                    cmd.buf = XContent.ReceiveXBytes(netstm, out bc, cmd.buf);
                    if (bc != 4)
                    {
                        throw new Exception("Back bytes expected 4 bytes.");
                    }
                    int backbytes = Utils.BytesToInt(cmd.buf);

                    MetaData md = MetaData.Prepare(cname, Type.GetType(ctype), csize, frontbytes, backbytes);
                    metadata[i] = md;

                    ordinals[cname.ToLower()] = i;
                }

                currow = new object[columnCount];
            }
            catch
            {
                cmd.Abort();
                throw;
            }
        }

        private bool _ReadRIndex()
        {
            if (isFirstRead)
            {
                isFirstRead = false;
                XContent.ReceiveXBytesNoCap(netstm, out allRowsByteCount, ref cmd.buf);
                allRowsCurPos = 0;              
            }
            if (allRowsCurPos >= allRowsByteCount)
            {
                eof = true;
                return false;
            }
            //Read a row.
            for (int i = 0; i < metadata.Length; i++)
            {
                MetaData column = metadata[i];
                int bytesremain = allRowsByteCount - allRowsCurPos;
                if (bytesremain < column.FrontBytes + column.Size + column.BackBytes)
                {
                    throw new Exception("Column value bytes remained has fewer bytes than expected.");
                }

                if (0 != cmd.buf[allRowsCurPos])
                {
                    currow[i] = DBNull.Value;
                }
                else
                {
                    switch (column.Type.FullName)
                    {
                        case "System.Int64":
                            currow[i] = Utils.ToInt64((UInt64)Utils.BytesToLong(cmd.buf, allRowsCurPos + column.FrontBytes));
                            break;
                        default:
                            throw new Exception("Type not supported yet for RIndex.");
                    }
                }
                allRowsCurPos += column.FrontBytes + column.Size + column.BackBytes;
            }
            return true;
        }

        public override bool Read()
        {
            if (isRindexEnabled)
            {
                return _ReadRIndex();
            }

            try
            {
                int ib = netstm.ReadByte();
                if (ib == -1 || ib == (byte)'.')
                {
                    eof = true;
                    return false;
                }

                if (ib != (byte)'+')
                {
                    throw new Exception("Read() handshake is not received.");
                }

                //Read a row.
                for (int i = 0; i < metadata.Length; i++)
                {
                    MetaData column = metadata[i];
                    int bc = 0;
                    cmd.buf = XContent.ReceiveXBytes(netstm, out bc, cmd.buf);
                    if (bc < column.FrontBytes + column.Size + column.BackBytes)
                    {
                        throw new Exception("Column value received from service has fewer bytes than expected.");
                    }

                    if (0 != cmd.buf[0])
                    {
                        //currow[i] = null;
                        currow[i] = DBNull.Value;
                    }
                    else
                    {
                        switch (column.Type.FullName)
                        {
                            case "System.String":
                                {
                                    //trim padding
                                    int stringend = 0;
                                    for (int si = column.FrontBytes + column.Size - 1; si > column.FrontBytes; si = si - 2)
                                    {
                                        if (cmd.buf[si] != 0 || cmd.buf[si - 1] != 0)
                                        {
                                            stringend = si;
                                            break;
                                        }
                                    }
                                    currow[i] = System.Text.Encoding.Unicode.GetString(cmd.buf, column.FrontBytes, stringend - column.FrontBytes + 1);
                                }
                                break;

                            case "System.Int16":
                                currow[i] = Utils.BytesToInt16(cmd.buf, column.FrontBytes);
                                break;

                            case "System.UInt16":
                                currow[i] = Utils.BytesToUInt16(cmd.buf, column.FrontBytes);
                                break;

                            case "System.Int32":
                                currow[i] = Utils.ToInt32((UInt32)Utils.BytesToInt(cmd.buf, column.FrontBytes));
                                break;

                            case "System.UInt32":
                                currow[i] = Utils.BytesToUInt32(cmd.buf, column.FrontBytes);
                                break;

                            case "System.Int64":
                                currow[i] = Utils.ToInt64((UInt64)Utils.BytesToLong(cmd.buf, column.FrontBytes));
                                break;

                            case "System.UInt64":
                                currow[i] = Utils.BytesToULong(cmd.buf, column.FrontBytes);
                                break;

                            case "System.Double":
                                currow[i] = Utils.BytesToDouble(cmd.buf, column.FrontBytes);
                                break;

                        case "System.DateTime":
                            {
                                Int64 ticks = Utils.BytesToLong(cmd.buf, column.FrontBytes);
                                currow[i] = new DateTime(ticks);
                            }                           
                            break;

                            default:
                                throw new Exception("Type not supported yet");
                        }
                    }
                }
                return true;
            }
            catch
            {
                cmd.Abort();
                throw;
            }            
        }

        public override bool NextResult()
        {
            // The sample only returns a single resultset. However,
            // DbDataAdapter expects NextResult to return a value.           
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            return currow[ordinal] == DBNull.Value;
        }

        public override int GetValues(object[] values)
        {
            int i = 0;
            int j = 0;
            for (; i < values.Length && j < metadata.Length; i++, j++)
            {
                values[i] = currow[j];
            }
            return i;
        }

        public override object GetValue(int ordinal)
        {
            return currow[ordinal];
        }

        public override string GetString(int ordinal)
        {
            return (string)currow[ordinal];
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            return (Int32)currow[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (Int64)currow[ordinal];
        }

        public override double GetDouble(int ordinal)
        {
            return (double)currow[ordinal];
        }

        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)currow[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DataTable GetSchemaTable()
        {
            if (isClosed)
            {
                throw new InvalidOperationException("Cannot get schema table when the data reader is closed.");
            }

            if (schemaTable != null)
            {
                return schemaTable;
            }

            schemaTable = new DataTable();
            schemaTable.Columns.Add("ColumnName", typeof(System.String));
            schemaTable.Columns.Add("ColumnOrdinal", typeof(System.Int32));
            schemaTable.Columns.Add("ColumnSize", typeof(System.Int32));
            schemaTable.Columns.Add("DataType", typeof(Type));
            schemaTable.Columns.Add("DataTypeName", typeof(System.String));

            object[] row = new object[5];
            for (int i = 0; i < metadata.Length; i++)
            {
                row[0] = metadata[i].Name;
                row[1] = i;
                row[2] = metadata[i].Size;
                row[3] = metadata[i].Type;
                row[4] = metadata[i].Type.FullName;
                schemaTable.Rows.Add(row);
            }

            return schemaTable;
        }

        public override int GetOrdinal(string name)
        {
            string key = name.ToLower();
            if (ordinals.ContainsKey(key))
            {
                return ordinals[key];
            }
            throw new IndexOutOfRangeException("The name specified is not a valid column name.");
        }

        public override string GetName(int ordinal)
        {
            if (ordinal >= metadata.Length)
            {
                throw new IndexOutOfRangeException("Column ordinal is out of range.");
            }
            return metadata[ordinal].Name;
        }

        public override Type GetFieldType(int ordinal)
        {
            if (ordinal >= metadata.Length)
            {
                throw new IndexOutOfRangeException("Column ordinal is out of range.");
            }
            return metadata[ordinal].Type;
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).FullName;
        }

        public override object this[int ordinal]
        {
            get 
            {
                return currow[ordinal];
            }
        }

        public override object this[string name]
        {
            get 
            {
                return currow[GetOrdinal(name)];
            }
        }

        public override int RecordsAffected
        {
            get 
            { 
                throw new NotImplementedException(); 
            }
        }

        public override int Depth
        {
            get 
            { 
                return 0; 
            }
        }

        public override bool HasRows
        {
            get 
            { 
                throw new NotImplementedException(); 
            }
        }

        public override int FieldCount
        {
            get 
            {
                return metadata.Length;
            }
        }

        public override bool IsClosed
        {
            get 
            {
                return isClosed;
            }
        }

        public override void Close()
        {
            if (isRindexEnabled)
            {
                _CloseRIndex();
                 return;
            }
            if (!eof)
            {
                ReadToEnd();          
            }
            isClosed = true;
            cmd.conn.islocked = false;
        }

        private void _CloseRIndex()
        {
            if (!eof)
            {
                _ReadToEndRIndex();
            }
            cmd.conn.CloseSocketRIndex();
            isClosed = true;
            cmd.conn.islocked = false;
        }

        private void ReadToEnd()
        {
            while (!eof)
            {
                Read();
            }
        }

        private void _ReadToEndRIndex()
        {
            allRowsCurPos = allRowsByteCount;
            eof = true;
        }

        public struct MetaData
        {
            public string Name;
            public Type Type;
            public int Size;
            public int FrontBytes;
            public int BackBytes;

            public static MetaData Prepare(string name, Type type, int size, int frontbytes, int backbytes)
            {
                MetaData md;
                md.Name = name;
                md.Type = type;
                md.Size = size;
                md.FrontBytes = frontbytes;
                md.BackBytes = backbytes;
                return md;
            }
        }
    }
}
