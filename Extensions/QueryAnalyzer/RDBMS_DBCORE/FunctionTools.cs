using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;

namespace RDBMS_DBCORE
{

    public class DbFunctionTools
    {

        public int GetInt(ByteSlice input)
        {
#if DEBUG_FTGET
            if (Types.IsNullValue(input))
            {
                throw new Exception("DEBUG:  GetInt: (Types.IsNullValue(input))");
            }
#endif
            List<byte> buf = AllocBuffer(input.Length - 1);
            ByteSlice.Prepare(input, 1, input.Length - 1).AppendTo(buf);
            Int32 x = Entry.BytesToInt(buf);
            FreeLastBuffer(buf);
            x = (Int32)Entry.ToUInt32(x);
            return x;
        }

        public int GetInt(DbValue input)
        {
            DbType type;
            ByteSlice bs = input.Eval(out type);
            if (DbTypeID.INT != type.ID)
            {
                throw new Exception("Expected INT, not " + type.Name.ToUpper());
            }
            return GetInt(bs);
        }

        public long GetLong(ByteSlice input)
        {
#if DEBUG_FTGET
            if (Types.IsNullValue(input))
            {
                throw new Exception("DEBUG:  GetLong: (Types.IsNullValue(input))");
            }
#endif
            List<byte> buf = AllocBuffer(input.Length - 1);
            ByteSlice.Prepare(input, 1, input.Length - 1).AppendTo(buf);
            Int64 x = Entry.BytesToLong(buf);
            FreeLastBuffer(buf);
            x = (Int64)Entry.ToUInt64(x);
            return x;
        }

        public long GetLong(DbValue input)
        {
            DbType type;
            ByteSlice bs = input.Eval(out type);
            if (DbTypeID.LONG != type.ID)
            {
                throw new Exception("Expected LONG, not " + type.Name.ToUpper());
            }
            return GetLong(bs);
        }

        public DateTime GetDateTime(ByteSlice input)
        {
#if DEBUG_FTGET
            if (Types.IsNullValue(input))
            {
                throw new Exception("DEBUG:  GetDateTime: (Types.IsNullValue(input))");
            }
#endif
            List<byte> buf = AllocBuffer(input.Length - 1);
            ByteSlice.Prepare(input, 1, input.Length - 1).AppendTo(buf);
            Int64 x = Entry.BytesToLong(buf);
            FreeLastBuffer(buf);
            return new DateTime(x);
        }

        public DateTime GetDateTime(DbValue input)
        {
            DbType type;
            ByteSlice bs = input.Eval(out type);
            if (DbTypeID.DATETIME != type.ID)
            {
                throw new Exception("Expected DATETIME, not " + type.Name.ToUpper());
            }
            return GetDateTime(bs);
        }

        public double GetDouble(ByteSlice input)
        {
#if DEBUG_FTGET
            if (Types.IsNullValue(input))
            {
                throw new Exception("DEBUG:  GetDouble: (Types.IsNullValue(input))");
            }
#endif
            recordset rs = recordset.Prepare(ByteSlice.Prepare(input, 1, input.Length - 1));
            double x = rs.GetDouble();
            return x;
        }

        public double GetDouble(DbValue input)
        {
            DbType type;
            ByteSlice bs = input.Eval(out type);
            if (DbTypeID.DOUBLE != type.ID)
            {
                throw new Exception("Expected DOUBLE, not " + type.Name.ToUpper());
            }
            return GetDouble(bs);
        }

        public mstring GetString(ByteSlice input)
        {
#if DEBUG_FTGET
            if (Types.IsNullValue(input))
            {
                throw new Exception("DEBUG:  GetString: (Types.IsNullValue(input))");
            }
#endif
            mstring x = mstring.PrepareUTF16(ByteSlice.Prepare(input, 1, input.Length - 1));
            x = x.TrimM('\0');
            return x;
        }

        public mstring GetString(DbValue input)
        {
            DbType type;
            ByteSlice bs = input.Eval(out type);
            if (DbTypeID.CHARS != type.ID)
            {
                throw new Exception("Expected CHAR(n), not " + type.Name.ToUpper());
            }
            return GetString(bs);
        }


        // RequestCapacity might not be granted exactly. 
        public List<byte> AllocBuffer(int RequestCapacity)
        {
            if (curbuf >= buffers.Count)
            {
                if (RequestCapacity < 0x400 * 2)
                {
                    RequestCapacity = 0x400 * 2;
                }
                List<byte> buf = new List<byte>(RequestCapacity);
                buffers.Add(buf);
                curbuf++;
                return buf;
            }
            // Future: can peek ahead and find buffer with capacity near RequestCapacity and swap.
            return AllocBuffer();
        }

        public List<byte> AllocBuffer()
        {
            if (curbuf >= buffers.Count)
            {
                List<byte> buf = new List<byte>(0x400 * 2);
                buffers.Add(buf);
                curbuf++;
                return buf;
            }
            else
            {
                List<byte> buf = buffers[curbuf++];
                buf.Clear();
                return buf;
            }
        }

        public void FreeLastBuffer(List<byte> buf)
        {
            if (curbuf <= 0 || !object.ReferenceEquals(buffers[curbuf - 1], buf))
            {
                throw new Exception("Mismatched AllocBuffer / FreeLastBuffer");
            }
            curbuf--;
        }


        public DbValue AllocValue(ByteSlice value, DbType type)
        {
            if (curval >= values.Count)
            {
                ImmediateValue val = new ImmediateValue(null, value, type);
                values.Add(val);
                curval++;
                return val;
            }
            else
            {
                ImmediateValue val = values[curval++];
                val._value = value;
                val._type = type;
                return val;
            }
        }

        public ImmediateValue AllocValue(DbTypeID typeID)
        {
            if (curval >= values.Count)
            {
                ImmediateValue val = new ImmediateValue(null, ByteSlice.Prepare(), DbType.Prepare(0, typeID));
                values.Add(val);
                curval++;
                return val;
            }
            else
            {
                ImmediateValue val = values[curval++];
                val._value = ByteSlice.Prepare();
                val._type = DbType.Prepare(0, typeID);
                return val;
            }
        }

        public DbValue AllocValue(ByteSlice value, DbTypeID typeID)
        {
            return AllocValue(value, DbType.Prepare(value.Length, typeID));
        }

        public DbValue AllocValue(Int32 x)
        {
            x = Entry.ToInt32((UInt32)x);
            List<byte> buf = AllocBuffer(1 + 4);
            buf.Add(0); // Nullable; IsNull=false
            Entry.ToBytesAppend(x, buf);
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.INT);
        }

        public DbValue AllocValue(Int64 x)
        {
            x = Entry.ToInt64((UInt64)x);
            List<byte> buf = AllocBuffer(1 + 8);
            buf.Add(0); // Nullable; IsNull=false
            Entry.ToBytesAppend64(x, buf);
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.LONG);
        }

        public DbValue AllocValue(DateTime x)
        {
            long t = x.Ticks;
            List<byte> buf = AllocBuffer(1 + 8);
            buf.Add(0); // Nullable; IsNull=false
            Entry.ToBytesAppend64(t, buf);
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.DATETIME);
        }

        public DbValue AllocValue(double x)
        {
            List<byte> buf = AllocBuffer(1 + 8);
            buf.Add(0); // Nullable; IsNull=false
            Entry.ToBytesAppendDouble(x, buf);
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.DOUBLE);
        }

        public DbValue AllocValue(mstring x, int RowSizeBytes)
        {
            List<byte> buf = AllocBuffer(RowSizeBytes);
            buf.Add(0); // Nullable; IsNull=false
            x.ToByteSliceUTF16().AppendTo(buf);
            for (int i = buf.Count; i < RowSizeBytes; i++)
            {
                buf.Add(0);
            }
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.CHARS);
        }

        public DbValue AllocValue(mstring x)
        {
            List<byte> buf = AllocBuffer(1 + x.Length * 2);
            buf.Add(0); // Nullable; IsNull=false
            x.ToByteSliceUTF16().AppendTo(buf);
            return AllocValue(ByteSlice.Prepare(buf), DbTypeID.CHARS);
        }

        public DbValue AllocNullValue(int RowSizeBytes)
        {
            List<byte> buf = AllocBuffer(RowSizeBytes);
            buf.Add(1); // Nullable, IsNull=true
            for (int i = buf.Count; i < RowSizeBytes; i++)
            {
                buf.Add(0);
            }
            return AllocValue(ByteSlice.Prepare(buf), DbType.PrepareNull(RowSizeBytes));
        }

        public DbValue AllocNullValue()
        {
            return AllocNullValue(1);
        }

        public void FreeLastValue(DbValue val)
        {
            if (curval <= 0 || !object.ReferenceEquals(values[curval - 1], val))
            {
                throw new Exception("Mismatched AllocValue / FreeLastValue");
            }
            curval--;
        }


        public void ResetBuffers()
        {
            curbuf = 0;
            curval = 0;
        }


        static List<List<byte>> buffers = new List<List<byte>>(8);
        static int curbuf = 0;

        static List<ImmediateValue> values = new List<ImmediateValue>(8);
        static int curval = 0;

    }


    public struct DbFunctionArguments
    {

        public DbFunctionArguments(IList<DbValue> args, int startindex, int length)
        {
#if DEBUG
            if (startindex < 0)
            {
                throw new Exception("DEBUG:  DbFunctionArguments: (startindex < 0)");
            }
#endif
#if DEBUG
            if (startindex + length > args.Count)
            {
                throw new Exception("DEBUG:  DbFunctionArguments: (startindex + length > args.Count)");
            }
#endif
            this.args = args;
            this.start = startindex;
            this.len = length;
        }

        public DbFunctionArguments(IList<DbValue> args)
        {
            this.args = args;
            this.start = 0;
            this.len = args.Count;
        }


        public void EnsureCount(string FunctionName, int expected)
        {
            if (this.Length != expected)
            {
                throw new ArgumentException("Function " + FunctionName + ": expected " + expected.ToString() + " arguments, not " + this.Length.ToString());
            }
        }

        public void EnsureMinCount(string FunctionName, int minExpected)
        {
            if (this.Length < minExpected)
            {
                throw new ArgumentException("Function " + FunctionName + ": expected at least " + minExpected.ToString() + " arguments, not " + this.Length.ToString());
            }
        }


        public void Expected(string FunctionName, int argoffset, string what)
        {
            throw new ArgumentException("Function " + FunctionName + ": argument error, expected " + what);
        }


        public DbValue this[int index]
        {
            get
            {
                ValidateIndex(index);
                return args[start + index];
            }
            set
            {
                ValidateIndex(index);
                args[start + index] = value;
            }
        }


        public int Length
        {
            get { return len; }
        }


        void ValidateIndex(int index)
        {
            if (index < 0 || index >= len)
            {
                throw new ArgumentOutOfRangeException("DbFunctionArguments.index");
            }
        }


        IList<DbValue> args;
        int start;
        int len;

    }


    public struct DbAggregatorArguments
    {

        public DbAggregatorArguments(IList<DbFunctionArguments> args, int startindex, int length)
        {
#if DEBUG
            if (startindex < 0)
            {
                throw new Exception("DEBUG:  DbAggregateArguments: (startindex < 0)");
            }
#endif
#if DEBUG
            if (startindex + length > args.Count)
            {
                throw new Exception("DEBUG:  DbAggregateArguments: (startindex + length > args.Count)");
            }
#endif
            this.args = args;
            this.start = startindex;
            this.len = length;
        }

        public DbAggregatorArguments(IList<DbFunctionArguments> args)
        {
            this.args = args;
            this.start = 0;
            this.len = args.Count;
        }


        public DbFunctionArguments this[int index]
        {
            get
            {
                ValidateIndex(index);
                return args[start + index];
            }
            set
            {
                ValidateIndex(index);
                args[start + index] = value;
            }
        }


        public int Length
        {
            get { return len; }
        }


        void ValidateIndex(int index)
        {
            if (index < 0 || index >= len)
            {
                throw new ArgumentOutOfRangeException("DbAggregateArguments.index");
            }
        }


        IList<DbFunctionArguments> args;
        int start;
        int len;

    }


}
