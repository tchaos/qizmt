using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace RDBMS_DBCORE
{
    public struct DbRecordset
    {
        private recordset rs;
        private static byte[] dummy = new byte[1];

        public static DbRecordset Prepare()
        {
            DbRecordset dbrs;
            dbrs.rs = recordset.Prepare();
            return dbrs;            
        }

        public static DbRecordset Prepare(ByteSlice b)
        {
            DbRecordset dbrs;
            dbrs.rs = recordset.Prepare(b);
            return dbrs;   
        }

        public ByteSlice ToByteSlice()
        {
            return rs.ToByteSlice();
        }

        public void PutInt32(Int32 x)
        {
            rs.PutByte(0); //front byte
            UInt32 u = Entry.ToUInt32(x);
            rs.PutUInt32(u);
        }

        public void PutInt(Int32 x)
        {
            PutInt32(x);
        }

        public void PutInt64(Int64 x)
        {
            rs.PutByte(0); //front byte
            UInt64 u = Entry.ToUInt64(x);
            rs.PutUInt64(u);
        }

        public void PutLong(Int64 x)
        {
            PutInt64(x);
        }

        public void PutDouble(double x)
        {
            rs.PutByte(0); //front byte
            rs.PutDouble(x);
        }

        public void PutDateTime(DateTime x)
        {
            rs.PutByte(0); //front byte
            rs.PutDateTime(x);
        }

        public void PutString(mstring s, int length)
        {
            ByteSlice bs = s.ToByteSliceUTF16();
            rs.PutByte(0); //front byte
            int totalsize = length * 2;
            int min = bs.Length < totalsize ? bs.Length : totalsize;

            for (int i = 0; i < min; i++)
            {
                rs.PutByte(bs[i]);
            }
            for (int i = min; i < totalsize; i++)
            {
                rs.PutByte(0);
            }
        }

        public void PutString(string s, int length)
        {
            mstring ms = mstring.Prepare(s);
            PutString(ms, length);
        }

        public int GetInt32()
        {
            rs.GetBytes(dummy, 0, 1);
            UInt32 u = rs.GetUInt32();
            return Entry.ToInt32(u);
        }

        public int GetInt()
        {
            return GetInt32(); 
        }

        public long GetInt64()
        {
            rs.GetBytes(dummy, 0, 1);
            UInt64 u = rs.GetUInt64();
            return Entry.ToInt64(u);
        }

        public long GetLong()
        {
            return GetInt64();
        }

        public double GetDouble()
        {
            rs.GetBytes(dummy, 0, 1);
            return rs.GetDouble();
        }

        public DateTime GetDateTime()
        {
            rs.GetBytes(dummy, 0, 1);
            return rs.GetDateTime();
        }

        public mstring GetString(int length)
        {
            rs.GetBytes(dummy, 0, 1);
            byte[] buf = new byte[length * 2];
            rs.GetBytes(buf, 0, buf.Length);
            string s = System.Text.Encoding.Unicode.GetString(buf);
            mstring ms = mstring.Prepare(s);
            ms = ms.TrimM('\0');
            return ms;
        }
    }
}
