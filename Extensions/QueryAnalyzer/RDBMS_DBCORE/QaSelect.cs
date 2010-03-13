using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class SelectMap : MapReduce
        {

            string TableName = null;
            string DfsOutputName;
            string SelectWhat;
            string[] awhat = null; // null if selecting all columns, or user-specified list of columns to select.
            List<int> Whats = null; // List of indices in cols, or null.
            long TopCount;
            string Ops;
            string[] aOps;
            string sOptions;
            bool WhatFunctions = false;
            bool GroupBy = false;

            List<int> KeyOn = null; // ORDER BY or GROUP BY key info. List of indices in cols, or null.

            WhereClause filter = null;
            bool DeleteFromFilter = false; // The WhereClause filter is what to DELETE.

            List<UpdateField> Updates = null;

            List<int> OutputColumnSizes;


            void ValidateSettings()
            {

                if (null != Updates)
                {
                    if (null != KeyOn || -1 != TopCount || "^" != SelectWhat)
                    {
                        throw new Exception("Validation failure: Cannot SET");
                    }
                }

                if (GroupBy)
                {
                    if (null == KeyOn)
                    {
                        throw new Exception("GROUP BY expected");
                    }
                }

            }


            List<DbColumn> cols;

            int IndexOfCol(string name)
            {
                return DbColumn.IndexOf(cols, name);
            }

            int IndexOfCol_ensure(string name)
            {
                int result = IndexOfCol(name);
                if (-1 == result)
                {
                    throw new Exception("Column named '" + name + "' not found");
                }
                return result;
            }


            struct UpdateField
            {
                public int FieldIndex; // Index in cols.
                public ByteSlice NewValue;
            }


            // Argument must start with Nullable byte.
            static void CellStringToCaseInsensitiveAppend(ByteSlice x, List<byte> append)
            {
                append.Add(x[0]);
                for (int i = 1; i + 2 <= x.Length; i += 2)
                {
                    byte b0 = x[i + 0];
                    byte b1 = x[i + 1];
                    Types.UTF16BytesToLower(ref b0, ref b1);
                    append.Add(b0);
                    append.Add(b1);
                }
            }


            public class SelectWhereClause : WhereClause
            {
                public SelectWhereClause(IList<DbColumn> RowColTypes, string[] Parts, int PartsStartPosition)
                    : base(RowColTypes)
                {
                    this.aOps = Parts;
                    this.iop = PartsStartPosition;
                }


                public int CurrentPosition
                {
                    get
                    {
                        return iop;
                    }
                }


                public override string NextPart()
                {
                    string op;
                    op = (iop < aOps.Length) ? aOps[iop++] : "";
                    return op;
                }

                public override string PeekPart()
                {
                    string op;
                    op = (iop < aOps.Length) ? aOps[iop] : "";
                    return op;
                }


                private int iop;
                private string[] aOps;
            }


            // _ReadNextLiteral kept for _ReadNextLiteral
            static List<byte> _NumberLiteralToType(string sNum, DbColumn ci)
            {
                List<byte> buf;
                if ("int" == ci.Type.Name)
                {
                    Int32 x = Int32.Parse(sNum);
                    buf = new List<byte>(1 + 4);
                    buf.Add(0);
                    Entry.ToBytesAppend((Int32)Entry.ToUInt32(x), buf);
                }
                else if ("long" == ci.Type.Name)
                {
                    Int64 x = Int64.Parse(sNum);
                    buf = new List<byte>(1 + 8);
                    buf.Add(0);
                    Entry.ToBytesAppend64((Int64)Entry.ToUInt64(x), buf);
                }
                else if ("double" == ci.Type.Name)
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

            // _ReadNextLiteral kept for SET
            static ByteSlice _ReadNextLiteral(PartReader input, DbColumn TypeHint)
            {
                string op;

                op = input.NextPart();
                if (op.Length > 0)
                {
                    if ('\'' == op[0])
                    {
                        if (op[op.Length - 1] == '\'')
                        {
                            if ("DateTime" == TypeHint.Type.Name)
                            {
                                DateTime dt = DateTime.Parse(op.Substring(1, op.Length - 2));
                                Int64 x = dt.Ticks;
                                List<byte> buf = new List<byte>(1 + 8);
                                buf.Add(0);
                                Entry.ToBytesAppend64(x, buf);
                                return ByteSlice.Prepare(buf);
                            }
                            else
                            {
                                string str = op.Substring(1, op.Length - 2).Replace("''", "'");
                                byte[] strbytes = System.Text.Encoding.Unicode.GetBytes(str);
                                //List<byte> buf = new List<byte>(1 + strbytes.Length);
                                List<byte> buf = new List<byte>(TypeHint.Type.Size);
                                buf.Add(0);
                                buf.AddRange(strbytes);
                                // Pad up the end of the char to be the whole column size.
                                while (buf.Count < TypeHint.Type.Size)
                                {
                                    buf.Add(0);
                                }
                                return ByteSlice.Prepare(buf);
                            }
                        }
                        else
                        {
                            throw new Exception("Unterminated string: " + op);
                        }
                    }
                    else if (char.IsDigit(op[0]))
                    {
                        return ByteSlice.Prepare(_NumberLiteralToType(op, TypeHint));
                    }
                    else if ("+" == op || "-" == op)
                    {
                        bool positive = "+" == op;
                        op = input.NextPart();
                        if (0 == op.Length)
                        {
                            throw new Exception("Expected number after sign");
                        }
                        return ByteSlice.Prepare(_NumberLiteralToType(positive ? op : ("-" + op), TypeHint));
                    }
                    else
                    {
                        throw new Exception("Misunderstood value: " + op);
                    }
                }
                return ByteSlice.Prepare(new byte[] { 0 });

            }


            List<byte> orderbuf;

            void InitColTypeInfos(string RowInfo, string DisplayInfo)
            {
                cols = new List<DbColumn>();
                int curoffset = 0;
                string[] rr = RowInfo.Split('\0');
                string[] dd = DisplayInfo.Split(',');
                for (int ix = 0; ix < rr.Length; ix++)
                {
                    string r = rr[ix];
                    string d = dd[ix];
                    DbColumn ci;
                    string tname;
                    int tsize;
                    {
                        int ieq = r.LastIndexOf('=');
                        ci.ColumnName = r.Substring(0, ieq);
                        tsize = int.Parse(r.Substring(ieq + 1));
                    }
                    ci.RowOffset = curoffset;
                    {
                        int ieq = d.LastIndexOf('=');
                        tname = d.Substring(0, ieq);
                        //ci.DisplayWidth = int.Parse(d.Substring(ieq + 1));
                    }
                    ci.Type = DbType.Prepare(tname, tsize);
                    curoffset += tsize;
                    cols.Add(ci);
                }
            }

            void InitOutputColTypeInfos(string OutputRowInfo)
            {
                OutputColumnSizes = new List<int>();
                int curoffset = 0;
                string[] orr = OutputRowInfo.Split('\0');
                //string[] odd = OutputDisplayInfo.Split(',');
                for (int ix = 0; ix < orr.Length; ix++)
                {
                    string r = orr[ix];
                    string tname;
                    int tsize;
                    {
                        int ieq = r.LastIndexOf('=');
                        //tname = r.Substring(0, ieq);
                        {
                            string stsize = r.Substring(ieq + 1);
                            if (!int.TryParse(stsize, out tsize))
                            {
                                throw new FormatException("Input string was not in a correct format: " + stsize);
                            }
                        }
                        //.Add(tname);
                        OutputColumnSizes.Add(tsize);
                    }
                }
            }


            void InitFields()
            {
                orderbuf = new List<byte>(DSpace_KeyLength);
                defkey = DSpace_ProcessID;

                TableName = DSpace_ExecArgs[0];
                DfsOutputName = DSpace_ExecArgs[1];
                string QlArgsSelectWhat = DSpace_ExecArgs[2];
                SelectWhat = QlArgsUnescape(QlArgsSelectWhat);
                TopCount = long.Parse(DSpace_ExecArgs[3]);
                string QlArgsOps = DSpace_ExecArgs[4]; // Still encoded with QlArgsEscape.
                Ops = QlArgsUnescape(QlArgsOps);
                if ("*" == Ops)
                {
                    aOps = new string[] { };
                }
                else
                {
                    aOps = Ops.Split('\0');
                }
                string RowInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[5]);
                string DisplayInfo = DSpace_ExecArgs[6];
                InitColTypeInfos(RowInfo, DisplayInfo);
                string OutputRowInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[7]);
                InitOutputColTypeInfos(OutputRowInfo);
                sOptions = (DSpace_ExecArgs.Length > 8) ? DSpace_ExecArgs[8] : "-";

                WhatFunctions = -1 != sOptions.IndexOf("SFUNC");
                GroupBy = -1 != sOptions.IndexOf("GBY");

                if ("*" != SelectWhat && "^" != SelectWhat)
                {
                    awhat = SelectWhat.Split('\0');
                    Whats = new List<int>(awhat.Length);
                    for (int iww = 0; iww < awhat.Length; iww++)
                    {
                        int WColIndex = IndexOfCol(awhat[iww]); // -1 for function (scalar or aggregate).
                        Whats.Add(WColIndex);
                    }
                }

                {
                    int iop = 0;
                    while (iop < aOps.Length)
                    {
                        string op;
                        op = (iop < aOps.Length) ? aOps[iop++] : "";
                        if (0 == string.Compare(op, "WHERE", true))
                        {
                            if (DeleteFromFilter)
                            {
                                throw new Exception("WHERE specified multiple times");
                            }
                            //DSpace_Log("DEBUG:  WHERE");
                            if (null != filter)
                            {
                                throw new Exception("WHERE specified multiple times");
                            }
                            SelectWhereClause swc = new SelectWhereClause(cols, aOps, iop);
                            filter = swc;
                            swc.Parse();
                            iop = swc.CurrentPosition;
                        }
                        else if (0 == string.Compare(op, "SET", true))
                        {
                            if (null != Updates)
                            {
                                if (DeleteFromFilter)
                                {
                                    throw new Exception("Cannot SET in DELETE");
                                }
                                throw new Exception("SET specified multiple times");
                            }
                            Updates = new List<UpdateField>();
                            StringArrayPartReader upr = new StringArrayPartReader(aOps, iop);
                            for (op = upr.NextPart(); ; op = upr.NextPart())
                            {
                                int FieldIndex = IndexOfCol_ensure(op);
                                if ("=" != upr.NextPart())
                                {
                                    throw new Exception("Expected = after SET field");
                                }
                                if (0 == upr.PeekPart().Length)
                                {
                                    throw new Exception("Expected vaule after SET <field> =");
                                }
                                // Types.ReadNextBasicExpression, etc
                                ByteSlice bsval = _ReadNextLiteral(upr, cols[FieldIndex]);
                                UpdateField uf;
                                uf.FieldIndex = FieldIndex;
                                uf.NewValue = bsval;
                                Updates.Add(uf);
                                if ("," != upr.PeekPart())
                                {
                                    break;
                                }
                                upr.NextPart(); // Eat the ",".
                            }
                            if (0 == Updates.Count)
                            {
                                throw new Exception("SET update expression expected");
                            }
                            iop = upr.CurrentPosition;

                        }
                        else if (0 == string.Compare(op, "D.WHERE", true))
                        {
                            if (null != Updates)
                            {
                                throw new Exception("Cannot SET in DELETE");
                            }
                            DeleteFromFilter = true;
                            if (null != filter)
                            {
                                throw new Exception("WHERE specified multiple times");
                            }
                            SelectWhereClause swc = new SelectWhereClause(cols, aOps, iop);
                            filter = swc;
                            swc.Parse();
                            iop = swc.CurrentPosition;

                        }
                        else if (0 == string.Compare(op, "ORDER", true))
                        {
                            //DSpace_Log("DEBUG:  ORDER");
                            op = (iop < aOps.Length) ? aOps[iop++] : "";
                            if (0 == string.Compare(op, "BY", true))
                            {
                                List<int> xorder = new List<int>();
                                for (; ; )
                                {
                                    op = (iop < aOps.Length) ? aOps[iop++] : "";
                                    if (0 == op.Length || ";" == op)
                                    {
                                        throw new Exception("Expected fields for ORDER BY");
                                    }
                                    int ColIndex = IndexOfCol_ensure(op);
                                    xorder.Add(ColIndex);
                                    op = (iop < aOps.Length) ? aOps[iop++] : "";
                                    if ("," != op)
                                    {
                                        if (0 == op.Length || ";" == op)
                                        {
                                            break;
                                        }
                                        //throw new Exception("Unexpected after ORDER BY ...: " + op);
                                        iop--;
                                        break;
                                    }
                                }
                                if (0 == xorder.Count)
                                {
                                    throw new Exception("Must be at least one column to ORDER BY");
                                }
                                if (GroupBy)
                                {
                                    // GROUP BY happens at earlier phase than ORDER BY,
                                    // so if GROUP BY, ignore ORDER BY and use GROUP BY.
                                    // KeyOn will be set to the GroupBy stuff.
                                    throw new Exception("Unexpected use of ORDER BY with GROUP BY");
                                }
                                else
                                {
                                    KeyOn = xorder;
                                }
                            }
                            else
                            {
                                throw new Exception("Expected BY after ORDER, not " + op);
                            }

                        }
                        else if (0 == string.Compare(op, "GROUP", true))
                        {
                            //DSpace_Log("DEBUG:  GROUP");
                            op = (iop < aOps.Length) ? aOps[iop++] : "";
                            if (0 == string.Compare(op, "BY", true))
                            {
                                if (!GroupBy)
                                {
                                    throw new Exception("Unexpected use of GROUP BY");
                                }
                                List<int> xGROUP = new List<int>();
                                for (; ; )
                                {
                                    op = (iop < aOps.Length) ? aOps[iop++] : "";
                                    if (0 == op.Length || ";" == op)
                                    {
                                        throw new Exception("Expected fields for GROUP BY");
                                    }
                                    int ColIndex = IndexOfCol_ensure(op);
                                    xGROUP.Add(ColIndex);
                                    op = (iop < aOps.Length) ? aOps[iop++] : "";
                                    if ("," != op)
                                    {
                                        if (0 == op.Length || ";" == op)
                                        {
                                            break;
                                        }
                                        //throw new Exception("Unexpected after GROUP BY ...: " + op);
                                        iop--;
                                        break;
                                    }
                                }
                                if (0 == xGROUP.Count)
                                {
                                    throw new Exception("Must be at least one column to GROUP BY");
                                }
                                KeyOn = xGROUP;
                            }
                            else
                            {
                                throw new Exception("Expected BY after GROUP, not " + op);
                            }

                        }
                        else
                        {
                            throw new Exception("Unexpected operation " + op);

                        }

                    }

                }

                ValidateSettings();

            }

            List<byte> updatebuf = new List<byte>();
            List<byte> newfieldsbuf = new List<byte>(); // For when selecting other than *, but not if WhatFunctions
            DbFunctionTools ftools = null; // null until first needed.
            int defkey;
            public override void Map(ByteSlice row, MapOutput output)
            {

                if (null == TableName)
                {
                    InitFields();
                }

                // Operate on values!

                ByteSlice key;

                bool keep = null == filter || filter.TestRow(row);
                if (DeleteFromFilter)
                {
                    keep = !keep;
                }

                if (keep)
                {
                    if (KeyOn != null)
                    {
                        orderbuf.Clear();
                        for (int iob = 0; iob < KeyOn.Count; iob++)
                        {
                            int oi = KeyOn[iob];
                            DbColumn ci = cols[oi];
                            int StartOffset = ci.RowOffset;
                            int Size = ci.Type.Size;
                            ByteSlice cval = ByteSlice.Prepare(row, StartOffset, Size);
                            if (ci.Type.Name.StartsWith("char"))
                            {
                                CellStringToCaseInsensitiveAppend(cval, orderbuf);
                            }
                            else
                            {
                                cval.AppendTo(orderbuf);
                            }
                        }
#if DEBUG
                      string KeyOnStringValue = "<not evaluated yet>";
                      if(0 != ((orderbuf.Count - 1) % 2))
                      {
                          KeyOnStringValue = "<not a string>";
                      }
                      else
                      {
                          //KeyOnStringValue = System.Text.Encoding.Unicode.GetString(ByteSlice.Prepare(orderbuf, 1, orderbuf.Count - 1).ToBytes());
                          {
                              System.Text.Encoding ue = new System.Text.UnicodeEncoding(false, false, true); // throwOnInvalidBytes=true
                              try
                              {
                                    KeyOnStringValue = ue.GetString(ByteSlice.Prepare(orderbuf, 1, orderbuf.Count - 1).ToBytes());
                              }
                              catch
                              {
                                    KeyOnStringValue = "<not a string>";
                              }
                          }
                      }
#endif
                        while (orderbuf.Count < DSpace_KeyLength)
                        {
                            orderbuf.Add(0);
                        }
                        key = ByteSlice.Prepare(orderbuf);
                    }
                    else // Use default key.
                    {
                        orderbuf.Clear();
                        Entry.ToBytesAppend(defkey, orderbuf);
                        key = ByteSlice.Prepare(orderbuf);
                        defkey = unchecked(defkey + DSpace_ProcessCount);
                    }

                    if (null != Updates)
                    {
                        updatebuf.Clear();
                        row.AppendTo(updatebuf);
                        for (int ui = 0; ui < Updates.Count; ui++)
                        {
                            UpdateField uf = Updates[ui];
                            DbColumn ci = cols[uf.FieldIndex];
                            for (int i = 0; i < ci.Type.Size; i++)
                            {
                                updatebuf[ci.RowOffset + i] = uf.NewValue[i];
                            }
                        }
                        row = ByteSlice.Prepare(updatebuf);
                    }

                }
                else
                {

                    if (null == Updates)
                    {
                        return;
                    }

                    key = row;

                }

                // If WhatFunctions, might need all input fields, so keep them here and filter out unwanted stuff in reduce.
                if (null != Whats && !WhatFunctions)
                {
                    newfieldsbuf.Clear();
                    for (int iww = 0; iww < Whats.Count; iww++)
                    {
                        int wi = Whats[iww];
                        if (-1 == wi)
                        {
                            DbTypeID ltype;
                            if (null == ftools)
                            {
                                ftools = new DbFunctionTools();
                            }
                            List<byte> lbuf = ftools.AllocBuffer();
                            ByteSlice cval = Types.LiteralToValueBuffer(awhat[iww], out ltype, lbuf);
                            int Size = cval.Length;
#if DEBUG
                          if(Size != OutputColumnSizes[iww])
                          {
                              throw new Exception("DEBUG:  " + awhat[iww] + ": (Size{" + Size + "} != OutputColumnSizes[iww]{" + OutputColumnSizes[iww] + "})");
                          }
#endif
                            cval.AppendTo(newfieldsbuf);
                        }
                        else
                        {
                            DbColumn ci = cols[wi];
                            int StartOffset = ci.RowOffset;
                            int Size = ci.Type.Size;
#if DEBUG
                          if(Size != OutputColumnSizes[iww])
                          {
                              throw new Exception("DEBUG:  (Size != OutputColumnSizes[iww])");
                          }
#endif
                            ByteSlice cval = ByteSlice.Prepare(row, StartOffset, Size);
                            cval.AppendTo(newfieldsbuf);
                        }
                    }
                    row = ByteSlice.Prepare(newfieldsbuf);
                }

                output.Add(key, row);

            }


            string QlArgsEscape(string args)
            {
                return Qa.QlArgsEscape(args);
            }


            string QlArgsUnescape(string eargs)
            {
                return Qa.QlArgsUnescape(eargs);
            }

            string QlArgsUnescape(string[] eargs)
            {
                return Qa.QlArgsUnescape(eargs);
            }

        }


        public class SelectReduce : MapReduce
        {

            List<string> FieldTypeStrings = null; // Only valid if WhatFunctions.

            public override void ReduceInitialize()
            {
            }


            string TableName = null;
            string sOptions = null;
            string[] awhat = null; // null if selecting all columns, or user-specified list of columns to select.
            List<int> Whats = null; // List of indices in cols, or null.
            bool WhatFunctions = false;
            bool GroupBy = false;

            List<int> OutputColumnSizes;


            List<DbColumn> cols;

            int IndexOfCol(string name)
            {
                return DbColumn.IndexOf(cols, name);
            }

            int IndexOfCol_ensure(string name)
            {
                int result = IndexOfCol(name);
                if (-1 == result)
                {
                    throw new Exception("Column named '" + name + "' not found");
                }
                return result;
            }


            void InitColTypeInfos(string RowInfo, string DisplayInfo)
            {
                cols = new List<DbColumn>();
                int curoffset = 0;
                string[] rr = RowInfo.Split('\0');
                string[] dd = DisplayInfo.Split(',');
                for (int ix = 0; ix < rr.Length; ix++)
                {
                    string r = rr[ix];
                    string d = dd[ix];
                    DbColumn ci;
                    string tname;
                    int tsize;
                    {
                        int ieq = r.LastIndexOf('=');
                        ci.ColumnName = r.Substring(0, ieq);
                        tsize = int.Parse(r.Substring(ieq + 1));
                    }
                    ci.RowOffset = curoffset;
                    {
                        int ieq = d.LastIndexOf('=');
                        tname = d.Substring(0, ieq);
                        //ci.DisplayWidth = int.Parse(d.Substring(ieq + 1));
                    }
                    ci.Type = DbType.Prepare(tname, tsize);
                    curoffset += tsize;
                    cols.Add(ci);
                }
            }

            void InitOutputColTypeInfos(string OutputRowInfo, string[] awhatOutputColumnNames)
            {
                //outputcols = new List<DbColumn>();
                OutputColumnSizes = new List<int>();
                int curoffset = 0;
                string[] orr = OutputRowInfo.Split('\0');
                //string[] odd = OutputDisplayInfo.Split(',');
                int totsize = 0;
                for (int ix = 0; ix < orr.Length; ix++)
                {
                    string r = orr[ix];
                    string tname;
                    int tsize;
                    {
                        int ieq = r.LastIndexOf('=');
                        //tname = r.Substring(0, ieq);
                        tsize = int.Parse(r.Substring(ieq + 1));
                        //.Add(tname);
                        OutputColumnSizes.Add(tsize);
                    }
                    /*DbColumn ci;
                    ci.ColumnName = awhatOutputColumnNames[ix];
                    ci.Type = DbType.Prepare(tname, tsize);
                    ci.RowOffset = totsize;
                    outputcols.Add(ci);*/
                    totsize += tsize;
                }
            }

            void InitFields()
            {
                TableName = DSpace_ExecArgs[0];
                string QlArgsSelectWhat = DSpace_ExecArgs[2];
                string SelectWhat = QlArgsUnescape(QlArgsSelectWhat);
                if ("*" != SelectWhat && "^" != SelectWhat)
                {
                    awhat = SelectWhat.Split('\0');
                }
                string RowInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[5]);
                string DisplayInfo = DSpace_ExecArgs[6];
                InitColTypeInfos(RowInfo, DisplayInfo);
                string OutputRowInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[7]);
                InitOutputColTypeInfos(OutputRowInfo, awhat);

                //sOptions = ... already done.

                if (null != awhat)
                {
                    Whats = new List<int>(awhat.Length);
                    for (int iww = 0; iww < awhat.Length; iww++)
                    {
                        int WColIndex = IndexOfCol(awhat[iww]); // -1 for function (scalar or aggregate).
                        Whats.Add(WColIndex);
                    }
                }
            }


            List<DbFunctionArguments> ffargs = null;
            List<List<DbValue>> fvaluelists;
            DbFunctionTools agtools;
            List<List<byte>> outvalues = new List<List<byte>>();
            List<ByteSlice> rows = new List<ByteSlice>();
            List<SelectClause> sclauses = null;

            public override void Reduce(ByteSlice key, IEnumerator<ByteSlice> values, MapReduceOutput output)
            {
#if DEBUG
                    string KeyStringValue = "<not evaluated yet>";
                    if(0 != ((key.Length - 1) % 2))
                    {
                        KeyStringValue = "<not a string>";
                    }
                    else
                    {
                        //KeyStringValue = System.Text.Encoding.Unicode.GetString(ByteSlice.Prepare(key, 1, key.Length - 1).ToBytes());
                        {
                            System.Text.Encoding ue = new System.Text.UnicodeEncoding(false, false, true); // throwOnInvalidBytes=true
                            try
                            {
                                KeyStringValue = ue.GetString(ByteSlice.Prepare(key, 1, key.Length - 1).ToBytes());
                            }
                            catch
                            {
                                KeyStringValue = "<not a string>";
                            }
                        }
                    }
#endif

                if (null == sOptions)
                {
                    // Done early to detect SFUNC
                    sOptions = (DSpace_ExecArgs.Length > 8) ? DSpace_ExecArgs[8] : "-";

                    WhatFunctions = -1 != sOptions.IndexOf("SFUNC");
                    GroupBy = -1 != sOptions.IndexOf("GBY");
                }

                if (!WhatFunctions)
                {
                    // Normal: no functions (aggregates or scalars).
                    if (GroupBy)
                    {
                        values.Reset();
                        while (values.MoveNext())
                        {
                            output.Add(values.Current);
                            break;
                        }
                    }
                    else
                    {
                        values.Reset();
                        while (values.MoveNext())
                        {
                            output.Add(values.Current);
                        }
                    }
                }
                else //if(WhatFunctions)
                {
                    if (null == TableName)
                    {
                        InitFields(); // Note: only called if functions (aggregates or scalars)!

                        agtools = new DbFunctionTools();
                    }

                    bool GettingFieldTypeStrings = null == FieldTypeStrings;
                    if (GettingFieldTypeStrings)
                    {
                        FieldTypeStrings = new List<string>();
                    }

                    rows.Clear();
                    outvalues.Clear();
                    values.Reset();
                    while (values.MoveNext())
                    {
                        ByteSlice row = values.Current;
                        rows.Add(row);
                        outvalues.Add(agtools.AllocBuffer(row.Length > 256 ? Entry.Round2Power(row.Length) : 256));
                    }

                    //List<SelectClause> sclauses
                    bool NewSelectClauses = null == sclauses;
                    if (NewSelectClauses)
                    {
                        sclauses = new List<SelectClause>(Whats.Count);
                    }
                    for (int iww = 0; iww < Whats.Count; iww++)
                    {
                        int wi = Whats[iww];
                        string Source = awhat[iww];
                        string calls = null;
                        if (NewSelectClauses)
                        {
                            sclauses.Add(new SelectClause(agtools, cols));
                            calls = Source;
                        }
                        SelectClause sclause = sclauses[iww];
                        List<DbValue> results = sclause.ProcessSelectPart(calls, rows);
#if DEBUG
                            if(1 != results.Count && results.Count != outvalues.Count)
                            {
                                throw new Exception("DEBUG:  (WhatFunctions) && (1 != results.Count && results.Count != outvalues.Count)");
                            }
#endif

                        int outvaluesCount = outvalues.Count;
                        DbType rtype = DbType.PrepareNull(); // Just for init.
                        bool JustOneResult = 1 == results.Count;
                        bool DifferentSize = false;
                        for (int ir = 0; ir < outvaluesCount; ir++)
                        {
                            ByteSlice bs;
                            int ix = ir;
                            if (JustOneResult)
                            {
                                ix = 0;
                            }
                            bs = results[ix].Eval(out rtype);
                            bs.AppendTo(outvalues[ir]);
                            for (int vdiff = OutputColumnSizes[iww] - bs.Length; vdiff > 0; vdiff--)
                            {
                                DifferentSize = true;
                                outvalues[ir].Add(0);
                            }
                        }

                        if (GettingFieldTypeStrings)
                        {
                            if (DifferentSize)
                            {
                                rtype = DbType.Prepare(OutputColumnSizes[iww], rtype.ID);
                            }
                            FieldTypeStrings.Add(rtype.Name);
                        }

                    }

                    {
                        int outvaluesCount = outvalues.Count;
                        for (int iv = 0; iv < outvaluesCount; iv++)
                        {
                            output.Add(ByteSlice.Prepare(outvalues[iv]));
                            if (GroupBy)
                            {
                                break;
                            }
                        }
                    }

                    //if(null != agtools)
                    {
                        agtools.ResetBuffers(); // Last. Important!
                    }

                }

            }


            string QlArgsEscape(string args)
            {
                return Qa.QlArgsEscape(args);
            }


            string QlArgsUnescape(string eargs)
            {
                return Qa.QlArgsUnescape(eargs);
            }

            string QlArgsUnescape(string[] eargs)
            {
                return Qa.QlArgsUnescape(eargs);
            }



            public override void ReduceFinalize()
            {

                if (WhatFunctions)
                {
                    string stypes = "";
                    if (null != FieldTypeStrings)
                    {
                        {
                            StringBuilder sbtypes = new StringBuilder();
                            for (int fti = 0; fti < FieldTypeStrings.Count; fti++)
                            {
                                if (fti > 0)
                                {
                                    sbtypes.Append("{264E73F6-E3C9-43de-A3FD-9AC36F905087}");
                                }
                                sbtypes.Append(FieldTypeStrings[fti]);
                            }
                            stypes = sbtypes.ToString();
                        }
                    }
                    // Need to print this even if empty, in case no reduce output.
                    DSpace_Log("BEGIN:{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}" + stypes + "{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}:END"); // OutputDisplayInfo
                }

            }

        }

    }

}