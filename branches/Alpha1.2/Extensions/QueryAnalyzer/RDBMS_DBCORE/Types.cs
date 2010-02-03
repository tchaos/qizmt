using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    public partial class Types
    {

        public static List<byte> NumberLiteralToType(string sNum, DbTypeID typeID)
        {
            return NumberLiteralToTypeBuffer(sNum, typeID, null);
        }

        public static List<byte> NumberLiteralToTypeBuffer(string sNum, DbTypeID typeID, List<byte> buffer)
        {
            if (DbTypeID.INT == typeID)
            {
                Int32 x = Int32.Parse(sNum);
                if (null == buffer)
                {
                    buffer = new List<byte>(1 + 4);
                }
                buffer.Clear();
                buffer.Add(0);
                Entry.ToBytesAppend((Int32)Entry.ToUInt32(x), buffer);
            }
            else if (DbTypeID.LONG == typeID)
            {
                Int64 x = Int64.Parse(sNum);
                if (null == buffer)
                {
                    buffer = new List<byte>(1 + 8);
                }
                buffer.Clear();
                buffer.Add(0);
                Entry.ToBytesAppend64((Int64)Entry.ToUInt64(x), buffer);
            }
            else if (DbTypeID.DOUBLE == typeID)
            {
                double x = double.Parse(sNum);
                if (null == buffer)
                {
                    buffer = new List<byte>(1 + 9);
                }
                buffer.Clear();
                buffer.Add(0);
                recordset rs = recordset.Prepare();
                rs.PutDouble(x);
                for (int id = 0; id < 9; id++)
                {
                    buffer.Add(0);
                }
                rs.GetBytes(buffer, 1, 9);
            }
            else
            {
                // This type isn't comparable with a number!
                if (null == buffer)
                {
                    buffer = new List<byte>(1);
                }
                buffer.Clear();
                buffer.Add(1); // Nullable byte; IsNull=true;
            }
            return buffer;
        }

        public static List<byte> NumberLiteralToBestType(string sNum, out DbTypeID type)
        {
            return NumberLiteralToBestTypeBuffer(sNum, out type, null);
        }

        public static List<byte> NumberLiteralToBestTypeBuffer(string sNum, out DbTypeID type, List<byte> buffer)
        {
            if(0 == sNum.Length)
            {
                type = DbTypeID.NULL;
                return NumberLiteralToTypeBuffer(sNum, DbTypeID.NULL, buffer); // Null it...
            }
            if (-1 != sNum.IndexOf('.'))
            {
                type = DbTypeID.DOUBLE;
                return NumberLiteralToTypeBuffer(sNum, DbTypeID.DOUBLE, buffer);
            }
            long x = long.Parse(sNum);
            if (x < int.MinValue || x > int.MaxValue)
            {
                // Stay long.
                type = DbTypeID.LONG;
                return NumberLiteralToTypeBuffer(sNum, DbTypeID.LONG, buffer);
            }
            else
            {
                // Can be int!
                type = DbTypeID.INT;
                return NumberLiteralToTypeBuffer(sNum, DbTypeID.INT, buffer);
            }
        }
        
        // Returns size.
        public static int NumberLiteralToBestTypeInfo(string sNum, out DbTypeID type)
        {
            if(0 == sNum.Length)
            {
                type = DbTypeID.NULL;
                return 1;
            }
            if (-1 != sNum.IndexOf('.'))
            {
                type = DbTypeID.DOUBLE;
                return 1 + 9;
            }
            long x = long.Parse(sNum);
            if (x < int.MinValue || x > int.MaxValue)
            {
                // Stay long.
                type = DbTypeID.LONG;
                return 1 + 8;
            }
            else
            {
                // Can be int!
                type = DbTypeID.INT;
                return 1 + 4;
            }
        }


        public static bool IsNullValue(ByteSlice x)
        {
#if DEBUG
            if (x.Length < 1 || (0 != x[0] && 1 != x[0]))
            {
                throw new Exception("DEBUG:  IsNullValue: (x.Length < 1 || (0 != x[0] && 1 != x[0]))");
            }
#endif
            return 0 != x[0];
        }

        public static bool IsNullValue(IList<byte> x)
        {
#if DEBUG
            if (x.Count < 1 || (0 != x[0] && 1 != x[0]))
            {
                throw new Exception("DEBUG:  IsNullValue: (x.Count < 1 || (0 != x[0] && 1 != x[0]))");
            }
#endif
            return 0 != x[0];
        }


        public enum ExpressionType
        {
            NONE,
            NUMBER,
            STRING,
            NAME,
            FUNCTION, // The parameters are expressions.
            AS, // AS in CAST.
            NULL, // NULL literal.
        }


        public static string ReadNextBasicExpression(PartReader input, out ExpressionType etype)
        {
            string s;
            s = input.NextPart();
            if (s.Length > 0)
            {
                if ('\'' == s[0])
                {
                    if (s[s.Length - 1] == '\'')
                    {
                        etype = ExpressionType.STRING;
                        return s;
                    }
                    else
                    {
                        throw new Exception("Unterminated string: " + s);
                    }
                }
                else if (char.IsDigit(s[0]))
                {
                    etype = ExpressionType.NUMBER;
                    return s;
                }
                else if ("+" == s || "-" == s)
                {
                    bool positive = "+" == s;
                    s = input.NextPart();
                    if (0 == s.Length)
                    {
                        throw new Exception("Expected number after sign");
                    }
                    etype = ExpressionType.NUMBER;
                    return positive ? s : ("-" + s);
                }
                else if (0 == string.Compare("NULL", s, true))
                {
                    etype = ExpressionType.NULL;
                    return "NULL";
                }
                else if (char.IsLetter(s[0]))
                {
                    string name = s;
                    s = input.PeekPart();
                    if ("(" == s)
                    {
                        input.NextPart(); // Eat the "(".
                        StringBuilder call = new StringBuilder();
                        call.Append(name);
                        call.Append('(');
                        s = input.NextPart();
                        if (")" != s)
                        {
                            bool prevident = false;
                            int nparens = 1;
                            for (; ; )
                            {
                                if ("(" == s)
                                {
                                    nparens++;
                                }
                                bool thisident = (s.Length > 0 && (char.IsLetter(s[0]) || '_' == s[0]));
                                if (prevident && thisident)
                                {
                                    call.Append(' ');
                                }
                                prevident = thisident;
                                call.Append(s);
                                s = input.NextPart();
                                if (")" == s)
                                {
                                    if (0 == --nparens)
                                    {
                                        break;
                                    }
                                }
                                if (0 == s.Length)
                                {
                                    throw new Exception("Expected ) after function");
                                }
                            }
                        }
                        call.Append(')');
                        etype = ExpressionType.FUNCTION;
                        return call.ToString();
                    }
                    else if (0 == string.Compare("AS", name, true))
                    {
                        {
                            StringBuilder sbas = new StringBuilder();
                            int asparens = 0;
                            for (; ; )
                            {
                                //string s =
                                s = input.PeekPart();
                                if (0 == s.Length || "," == s)
                                {
                                    break;
                                }
                                else if ("(" == s)
                                {
                                    asparens++;
                                    sbas.Append(input.NextPart());
                                }
                                else if (")" == s)
                                {
                                    if (0 == asparens)
                                    {
                                        break;
                                    }
                                    asparens--;
                                    sbas.Append(input.NextPart());
                                }
                                else
                                {
                                    sbas.Append(input.NextPart());
                                }
                            }
                            if (0 == sbas.Length)
                            {
                                throw new Exception("Expected type after AS");
                            }

                            etype = ExpressionType.AS;
                            return "'AS " + sbas.Replace("'", "''") + "'";

                        }
                    }
                    else
                    {
                        etype = ExpressionType.NAME;
                        return name;
                    }
                }
                else
                {
#if DEBUG
                    //System.Diagnostics.Debugger.Launch();
#endif
                    throw new Exception("Misunderstood value: " + s);
                }
            }
            etype = ExpressionType.NONE;
            return "";

        }

        public static string ReadNextBasicExpression(PartReader input)
        {
            ExpressionType etype;
            return ReadNextBasicExpression(input, out etype);
        }

        public static ByteSlice LiteralToValue(string literal, out DbTypeID type)
        {
            return LiteralToValueBuffer(literal, out type, null);
        }

        public static ByteSlice LiteralToValueBuffer(string literal, out DbTypeID type, List<byte> buffer)
        {
            if (literal.Length > 0)
            {
                if ('\'' == literal[0])
                {
                    if ('\'' != literal[literal.Length - 1])
                    {
                        throw new Exception("Expected closing quote in string literal");
                    }
                    if (null == buffer)
                    {
                        buffer = new List<byte>(1 + (literal.Length - 2) * 2);
                    }
                    buffer.Clear();
                    buffer.Add(0);
                    for (int i = 1; i <= literal.Length - 2; i++)
                    {
                        int ich = literal[i];
                        //ConvertCodeValueToBytesUTF16
                        buffer.Add((byte)ich);
                        buffer.Add((byte)(ich >> 8));
                        if ('\'' == ich)
                        {
                            i++; // Skip escape quote.
#if DEBUG
                            if ('\'' != literal[i])
                            {
                                throw new Exception("DEBUG:  ('\'' != literal[i])");
                            }
#endif
                        }
                    }
                    type = DbTypeID.CHARS;
#if DEBUG
                    {
                        DbTypeID _xtype;
                        int ltvi = LiteralToValueInfo(literal, out _xtype);
                        if (buffer.Count != ltvi)
                        {
#if DEBUG
                            System.Diagnostics.Debugger.Launch();
#endif
                            throw new Exception("DEBUG:  LiteralToValue: (buffer.Count{" + buffer.Count + "} != LiteralToValueInfo(literal, out _xtype)){" + ltvi + "}");
                        }
                    }
#endif
                    return ByteSlice.Prepare(buffer);
                }
                else if (0 == string.Compare("NULL", literal, true))
                {
                    type = DbTypeID.NULL;
                    if (null == buffer)
                    {
                        buffer = new List<byte>(1);
                    }
                    buffer.Clear();
                    buffer.Add(1); // Nullable, IsNull=true.
                    return ByteSlice.Prepare(buffer);
                }
                else // Assumed numeric.
                {
                    try
                    {
                        return ByteSlice.Prepare(NumberLiteralToBestTypeBuffer(literal, out type, buffer));
                    }
                    catch
                    {
                        // Continue on to null on exception.
                    }
                }
            }
            type = DbTypeID.NULL;
            if (buffer == null)
            {
                buffer = new List<byte>(0);
            }
            buffer.Clear();
            return ByteSlice.Prepare(buffer);

        }

        // Returns size.
        public static int LiteralToValueInfo(string literal, out DbTypeID type)
        {
            if (literal.Length > 0)
            {
                if ('\'' == literal[0])
                {
                    if ('\'' != literal[literal.Length - 1])
                    {
                        throw new Exception("Expected closing quote in string literal");
                    }
                    int strbyteslen = System.Text.Encoding.Unicode.GetByteCount(literal);
                    {
                        strbyteslen -= 2 + 2; // Leading and trailing quote are 2 bytes each.
                        for (int i = 1; i <= literal.Length - 2; i++)
                        {
                            if ('\'' == literal[i])
                            {
                                strbyteslen -= 2; // Un-count 1 quote (2 bytes) of escaped quote ("''" -> "'")
                                i++; // Skip next iteration, it's the other '\''.
#if DEBUG
                                if ('\'' != literal[i])
                                {
                                    throw new Exception("DEBUG:  ('\'' != literal[i])");
                                }
#endif
                            }
                        }
                    }
                    type = DbTypeID.CHARS;
                    return 1 + strbyteslen;
                }
                else if (0 == string.Compare("NULL", literal, true))
                {
                    type = DbTypeID.NULL;
                    return 1;
                }
                else // Assumed numeric.
                {
                    try
                    {
                        return NumberLiteralToBestTypeInfo(literal, out type);
                    }
                    catch
                    {
                        // Continue on to null on exception.
                    }
                }
            }
            type = DbTypeID.NULL;
            return 0;

        }


        public static void UTF16BytesToLower(ref byte b0, ref byte b1)
        {
            char ch = (char)UTFConverter.GetCodeValueUTF16(b0, b1);
            ch = Char.ToLower(ch);
            UTFConverter.ConvertCodeValueToBytesUTF16(ch, ref b0, ref b1);
        }

        public static char UTF16BytesToLowerChar(byte b0, byte b1)
        {
            char ch = (char)UTFConverter.GetCodeValueUTF16(b0, b1);
            return Char.ToLower(ch);
        }

        public static char UTF16BytesToChar(byte b0, byte b1)
        {
            return (char)UTFConverter.GetCodeValueUTF16(b0, b1);
        }


    }


    public class PartReader
    {
        // Must return parts, as from NextPart.
        // Must return empty string only at end of parts stream.
        public virtual string NextPart()
        {
            return "";
        }

        public virtual string PeekPart()
        {
            return "";
        }
    }


    class StringPartReader : PartReader
    {
        public StringPartReader(string str)
        {
            this.str = str;
        }

        public override string NextPart()
        {
            return Qa.NextPart(ref str);
        }

        public override string PeekPart()
        {
            string x = str;
            return Qa.NextPart(ref x);
        }


        public string RemainingString
        {
            get
            {
                return str;
            }
        }

        string str;

    }


    public class StringArrayPartReader : PartReader
    {
        public StringArrayPartReader(string[] Parts, int PartsStartPosition)
        {
            this.aparts = Parts;
            this.curpart = PartsStartPosition;
        }

        public StringArrayPartReader(string[] Parts)
        {
            this.aparts = Parts;
            this.curpart = 0;
        }


        public int CurrentPosition
        {
            get
            {
                return curpart;
            }
        }


        public override string NextPart()
        {
            string op;
            op = (curpart < aparts.Length) ? aparts[curpart++] : "";
            return op;
        }

        public override string PeekPart()
        {
            string op;
            op = (curpart < aparts.Length) ? aparts[curpart] : "";
            return op;
        }


        private int curpart;
        private string[] aparts;
    }


    /// <summary>
    /// Data type in DBCORE.
    /// </summary>
    public struct DbType
    {
        /// <summary>
        /// Name of this DBCORE type.
        /// </summary>
        public string Name;

        /// <summary>
        /// Size of a value of this type.
        /// </summary>
        public int Size; // Includes leading Nullable byte.

        /// <summary>
        /// Type ID.
        /// </summary>
        public DbTypeID ID;


        public static DbType Prepare(string TypeName)
        {
            DbType ret;
            ret.Name = TypeName;
            ret.Size = NameToSize(TypeName);
            ret.ID = NameToID(TypeName);
#if DEBUG
            if (ret.ID == DbTypeID.NULL)
            {
                if (ret.ID != NameToID(NormalizeName(TypeName)))
                {
                    throw new Exception("DEBUG:  DbType.Prepare: type name not normalized: " + TypeName);
                }
            }
#endif
            return ret;
        }


        public static DbType Prepare(string TypeName, int TypeSize)
        {
            DbType ret;
            ret.Name = TypeName;
            ret.Size = TypeSize;
            ret.ID = NameToID(TypeName);
#if DEBUG
            if (ret.ID == DbTypeID.NULL)
            {
                if (ret.ID != NameToID(NormalizeName(TypeName)))
                {
                    throw new Exception("DEBUG:  DbType.Prepare: type name not normalized: " + TypeName);
                }
            }
#endif
            return ret;
        }

        public static DbType Prepare(string TypeName, int TypeSize, DbTypeID TypeID)
        {
            DbType ret;
            ret.Name = TypeName;
            ret.Size = TypeSize;
            ret.ID = TypeID;
#if DEBUG
            if (ret.ID == DbTypeID.NULL)
            {
                if (ret.ID != NameToID(NormalizeName(TypeName)))
                {
                    throw new Exception("DEBUG:  DbType.Prepare: type name not normalized: " + TypeName);
                }
            }
#endif
            return ret;
        }

        public static DbType Prepare(int TypeSize, DbTypeID typeID)
        {
            DbType type;
            switch (typeID)
            {
                case DbTypeID.CHARS:
                    if (0 == TypeSize)
                    {
                        type = DbType.Prepare("char(0)", 0);
                    }
                    else
                    {
#if DEBUG
                        if (0 != ((TypeSize - 1) % 2))
                        {
                            throw new Exception("DEBUG:  DbType.Prepare: case DbTypeID.CHARS: (0 != ((TypeSize - 1) % 2)) (TypeSize=" + TypeSize.ToString() + ")");
                        }
#endif
                        type = DbType.Prepare("char(" + ((TypeSize - 1) / 2).ToString() + ")", TypeSize);
                    }
                    break;

                case DbTypeID.DOUBLE:
                    type = DbType.Prepare("double", TypeSize);
                    break;

                case DbTypeID.INT:
                    type = DbType.Prepare("int", TypeSize);
                    break;

                case DbTypeID.LONG:
                    type = DbType.Prepare("long", TypeSize);
                    break;

                case DbTypeID.DATETIME:
                    type = DbType.Prepare("DateTime", TypeSize);
                    break;

                case DbTypeID.NULL:
                    type = DbType.PrepareNull(TypeSize);
                    break;

                default:
                    throw new Exception("DEBUG:  DbType.Prepare: Unknown type specified");
            }
            return type;
        }

        public static DbType PrepareNull(int TypeSize)
        {
            return DbType.Prepare("NULL", TypeSize, DbTypeID.NULL);
        }

        public static DbType PrepareNull()
        {
            return DbType.PrepareNull(1);
        }


        public static DbTypeID NameToID(string name)
        {
            if ("int" == name)
            {
                return DbTypeID.INT;
            }
            else if ("long" == name)
            {
                return DbTypeID.LONG;
            }
            else if ("double" == name)
            {
                return DbTypeID.DOUBLE;
            }
            else if ("DateTime" == name)
            {
                return DbTypeID.DATETIME;
            }
            else if (name.StartsWith("char("))
            {
                return DbTypeID.CHARS;
            }
            else
            {
                return DbTypeID.NULL;
            }
        }

        public static int NameToSize(string name)
        {
            if ("int" == name)
            {
                return 1 + 4;
            }
            else if ("long" == name)
            {
                return 1 + 8;
            }
            else if ("double" == name)
            {
                return 1 + 9;
            }
            else if ("DateTime" == name)
            {
                return 1 + 8;
            }
            else if (name.StartsWith("char(") && ')' == name[name.Length - 1])
            {
                int nchars = int.Parse(name.Substring(5, name.Length - 5 - 1));
                return 1 + (2 * nchars);
            }
            else
            {
                return 1;
            }
        }

        public static string NormalizeName(string name)
        {
            if (0 == string.Compare("int", name, true))
            {
                return "int";
            }
            else if (0 == string.Compare("long", name, true))
            {
                return "long";
            }
            else if (0 == string.Compare("double", name, true))
            {
                return "double";
            }
            else if (0 == string.Compare("DateTime", name, true))
            {
                return "DateTime";
            }
            else if (name.StartsWith("char(", true, null) && ')' == name[name.Length - 1])
            {
                return "char(" + name.Substring(5, name.Length - 5 - 1).Trim() + ")";
            }
            else
            {
                return name;
            }
        }

    }

    public enum DbTypeID
    {
        NULL,
        INT, // Sortable 1 + 4-byte integer.
        LONG, // Sortable 1 + 8-byte long integer.
        DOUBLE, // 1 + 9-byte sortable double.
        CHARS, // Fixed-length char(n): 1 + (2 * n) bytes.
        DATETIME // Sortable 1 + 8-byte long integer, in ticks.
    }


    public interface IValueContext
    {
        ByteSlice CurrentRow { get; }
        IList<DbColumn> ColumnTypes { get; }
        DbValue ExecDbFunction(string name, DbFunctionArguments args);
        DbFunctionTools Tools { get; }
    }


    public abstract class DbValue
    {
        public DbValue(IValueContext Context)
        {
            this.Context = Context;
        }


        public abstract ByteSlice Eval(out DbType type);


        public ByteSlice Eval()
        {
            DbType type;
            return Eval(out type);
        }


        protected internal IValueContext Context;
    }


    public struct DbColumn
    {
        public DbType Type;
        public int RowOffset; // Starting offset of this column in a row.
        public string ColumnName;


        public static int IndexOf(IList<DbColumn> cols, string name)
        {
            int matchindex = -1;
            for (int i = 0; i < cols.Count; i++)
            {
                string cn = cols[i].ColumnName;
                if (name.Length == cn.Length)
                {
                    bool good = true;
                    for (int j = 0; j < name.Length; j++)
                    {
                        if (Char.ToLower(name[j]) != Char.ToLower(cn[j]))
                        {
                            //if (!(name[j] == '.' && cn[j] == '.'))
                            {
                                good = false;
                                break;
                            }
                        }
                    }
                    if (good)
                    {
                        if (-1 != matchindex)
                        {
                            throw new Exception("Column name " + name + " does not resolve to a single column");
                        }
                        matchindex = i;
                    }
                }
                //
                int cnindex = cn.LastIndexOf('.');
                cnindex++;
                if (cn.Length - cnindex == name.Length)
                {
                    bool good = true;
                    for (int j = 0; j < name.Length; j++, cnindex++)
                    {
                        if (Char.ToLower(name[j]) != Char.ToLower(cn[cnindex]))
                        {
                            good = false;
                            break;
                        }
                    }
                    if (good)
                    {
                        if (-1 != matchindex)
                        {
                            throw new Exception("Column name " + name + " does not resolve to a single column");
                        }
                        matchindex = i;
                    }
                }
            }
            return matchindex;
        }
    }


    public class ColValue : DbValue
    {

        public ColValue(IValueContext Context, DbColumn col)
            : base(Context)
        {
            this.col = col;
        }

        public override ByteSlice Eval(out DbType type)
        {
            type = col.Type;
            return ByteSlice.Prepare(Context.CurrentRow, col.RowOffset, col.Type.Size);
        }

        DbColumn col;

    }

    public class ImmediateValue : DbValue
    {

        public ImmediateValue(IValueContext Context, ByteSlice value, DbType type)
            : base(Context)
        {
            this._value = value;
            this._type = type;
        }

        public override ByteSlice Eval(out DbType type)
        {
            type = this._type;
            return _value;
        }

        public void SetValue(ByteSlice value)
        {
            this._value = value;
            this._type = DbType.Prepare(value.Length, _type.ID);
        }

        internal ByteSlice _value;
        internal DbType _type;
    }

    // Scalar function only.
    public class FuncEvalValue : DbValue
    {
        public FuncEvalValue(IValueContext Context, string FunctionName, IList<DbValue> Arguments)
            : base(Context)
        {
            this.funcname = FunctionName;
            this.args = new DbFunctionArguments(Arguments);
        }

        public FuncEvalValue(IValueContext Context, string FunctionName, DbFunctionArguments Arguments)
            : base(Context)
        {
            this.funcname = FunctionName;
            this.args = Arguments;
        }

        public FuncEvalValue(IValueContext Context, string FunctionName, params DbValue[] Arguments)
            : base(Context)
        {
            this.funcname = FunctionName;
            this.args = new DbFunctionArguments(Arguments);
        }


        public override ByteSlice Eval(out DbType type)
        {
            DbValue ret = Context.ExecDbFunction(funcname, args);
            return ret.Eval(out type);
        }


        string funcname;
        DbFunctionArguments args;
    }

}
