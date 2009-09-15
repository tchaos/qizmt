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
            List<byte> buf;
            if (DbTypeID.INT == typeID)
            {
                Int32 x = Int32.Parse(sNum);
                buf = new List<byte>(1 + 4);
                buf.Add(0);
                Entry.ToBytesAppend((Int32)Entry.ToUInt32(x), buf);
            }
            else if (DbTypeID.LONG == typeID)
            {
                Int64 x = Int64.Parse(sNum);
                buf = new List<byte>(1 + 8);
                buf.Add(0);
                Entry.ToBytesAppend64((Int64)Entry.ToUInt64(x), buf);
            }
            else if (DbTypeID.DOUBLE == typeID)
            {
                double x = double.Parse(sNum);
                buf = new List<byte>(1 + 9);
                buf.Add(0);
                recordset rs = recordset.Prepare();
                rs.PutDouble(x);
                for (int id = 0; id < 9; id++)
                {
                    buf.Add(0);
                }
                rs.GetBytes(buf, 1, 9);
            }
            else
            {
                // This type isn't comparable with a number!
                buf = new List<byte>(1);
                buf.Add(1); // Nullable byte; IsNull=true;
            }
            return buf;
        }


        public static List<byte> NumberLiteralToBestType(string sNum, out DbTypeID type)
        {
            if(0 == sNum.Length)
            {
                type = DbTypeID.NULL;
                return NumberLiteralToType(sNum, DbTypeID.NULL); // Null it...
            }
            if (-1 != sNum.IndexOf('.'))
            {
                type = DbTypeID.DOUBLE;
                return NumberLiteralToType(sNum, DbTypeID.DOUBLE);
            }
            long x = long.Parse(sNum);
            if (x < int.MinValue || x > int.MaxValue)
            {
                // Stay long.
                type = DbTypeID.LONG;
                return NumberLiteralToType(sNum, DbTypeID.LONG);
            }
            else
            {
                // Can be int!
                type = DbTypeID.INT;
                return NumberLiteralToType(sNum, DbTypeID.INT);
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
                            int nparens = 1;
                            for (; ; )
                            {
                                if ("(" == s)
                                {
                                    nparens++;
                                }
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
            if (literal.Length > 0)
            {
                if ('\'' == literal[0])
                {
                    if ('\'' != literal[literal.Length - 1])
                    {
                        throw new Exception("Expected closing quote in string literal");
                    }
                    string str = literal.Substring(1, literal.Length - 2).Replace("''", "'");
                    byte[] strbytes = System.Text.Encoding.Unicode.GetBytes(str);
                    //List<byte> buf = new List<byte>(TypeHint.Size);
                    List<byte> buf = new List<byte>(1 + strbytes.Length);
                    buf.Add(0);
                    buf.AddRange(strbytes);
                    // Pad up the end of the char to be the whole column size.
                    /*while (buf.Count < TypeHint.Size)
                    {
                        buf.Add(0);
                    }*/
                    type = DbTypeID.CHARS;
                    return ByteSlice.Prepare(buf);
                }
                else if (0 == string.Compare("NULL", literal, true))
                {
                    type = DbTypeID.NULL;
                    return ByteSlice.Prepare(new byte[] { 1 }); // Nullable, IsNull=true.
                }
                else // Assumed numeric.
                {
                    try
                    {
                        return ByteSlice.Prepare(NumberLiteralToBestType(literal, out type));
                    }
                    catch
                    {
                        // Continue on to null on exception.
                    }
                }
            }
            type = DbTypeID.NULL;
            return ByteSlice.Prepare(new byte[] { 0 });

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


        public static DbType Prepare(string TypeName, int TypeSize)
        {
            DbType ret;
            ret.Name = TypeName;
            ret.Size = TypeSize;
            ret.ID = NameToID(TypeName);
            return ret;
        }

        public static DbType Prepare(string TypeName, int TypeSize, DbTypeID TypeID)
        {
#if DEBUG
            if (TypeID != NameToID(TypeName))
            {
                throw new Exception("DEBUG:  DbType.Prepare: (TypeID != NameToID(TypeName))");
            }
#endif
            DbType ret;
            ret.Name = TypeName;
            ret.Size = TypeSize;
            ret.ID = TypeID;
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


}
