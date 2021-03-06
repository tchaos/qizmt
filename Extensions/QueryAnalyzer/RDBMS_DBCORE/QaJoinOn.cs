﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class JoinOnMap : MapReduce
        {

            delegate ByteSlice DbValueConversion(ByteSlice value, int ResultSize, DbFunctionTools tools);

            ByteSlice Conv_IntToLong(ByteSlice value, int ResultSize, DbFunctionTools tools)
            {
                int x = tools.GetInt(value);
                DbValue v = tools.AllocValue((long)x);
                return v.Eval();
            }

            ByteSlice Conv_IntToDouble(ByteSlice value, int ResultSize, DbFunctionTools tools)
            {
                int x = tools.GetInt(value);
                DbValue v = tools.AllocValue((double)x);
                return v.Eval();
            }

            ByteSlice Conv_LongToDouble(ByteSlice value, int ResultSize, DbFunctionTools tools)
            {
                long x = tools.GetLong(value);
                DbValue v = tools.AllocValue((double)x);
                return v.Eval();
            }

            ByteSlice Conv_String(ByteSlice value, int ResultSize, DbFunctionTools tools)
            {
                mstring x = tools.GetString(value);
                x = x.ToUpperM();
                DbValue v = tools.AllocValue(x, ResultSize);
                return v.Eval();
            }


            enum JoinType
            {
                X,
                INNER_JOIN,
                LEFT_OUTER_JOIN,
                RIGHT_OUTER_JOIN,
            }

            string LeftTableName;
            JoinType type = JoinType.X;
            string RightTableName;
            string On;

            int LeftOffset, LeftSize;
            DbType LeftType;
            DbValueConversion LeftConv = null;

            int RightOffset, RightSize;
            DbType RightType;
            DbValueConversion RightConv = null;

            DbFunctionTools ftools = null;

            DbValueConversion NeedConv(DbType tfrom, DbType tto)
            {
                switch (tfrom.ID)
                {
                    case DbTypeID.INT:
                        switch (tto.ID)
                        {
                            case DbTypeID.LONG:
                                return Conv_IntToLong;
                            case DbTypeID.DOUBLE:
                                return Conv_IntToDouble;
                        }
                        break;
                    case DbTypeID.LONG:
                        switch (tto.ID)
                        {
                            case DbTypeID.DOUBLE:
                                return Conv_LongToDouble;
                        }
                        break;
                    case DbTypeID.CHARS:
                        return Conv_String;
                }
                return null;
            }


            int GetTableID(ByteSlice row)
            {
                return this.MapInputFileIndex;
            }


            public override void Map(ByteSlice row, MapOutput output)
            {
                if (JoinType.X == type)
                {
                    ftools = new DbFunctionTools();

                    string QlLeftTableName = DSpace_ExecArgs[0];
                    LeftTableName = Qa.QlArgsUnescape(QlLeftTableName);
                    string stype = DSpace_ExecArgs[1];
                    string QlRightTableName = DSpace_ExecArgs[2];
                    RightTableName = Qa.QlArgsUnescape(QlRightTableName);
                    string QlOn = DSpace_ExecArgs[3];
                    On = Qa.QlArgsUnescape(QlOn);
                    {
                        string LeftColInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[4]);
                        int ileq = LeftColInfo.LastIndexOf('=');
                        string sLeftType = LeftColInfo.Substring(ileq + 1);
                        LeftType = DbType.Prepare(sLeftType);
                        LeftConv = NeedConv(LeftType, RightType);
                        string[] st = LeftColInfo.Substring(0, ileq).Split(',');
                        LeftOffset = int.Parse(st[0]);
                        LeftSize = int.Parse(st[1]);
                    }
                    {
                        string RightColInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[5]);
                        int ileq = RightColInfo.LastIndexOf('=');
                        string sRightType = RightColInfo.Substring(ileq + 1);
                        RightType = DbType.Prepare(sRightType);
                        RightConv = NeedConv(RightType, LeftType);
                        string[] st = RightColInfo.Substring(0, ileq).Split(',');
                        RightOffset = int.Parse(st[0]);
                        RightSize = int.Parse(st[1]);
                    }
                    if (0 == string.Compare("INNER", stype, true))
                    {
                        type = JoinType.INNER_JOIN;
                    }
                    else if (0 == string.Compare("LEFT_OUTER", stype, true))
                    {
                        type = JoinType.LEFT_OUTER_JOIN;
                    }
                    else if (0 == string.Compare("RIGHT_OUTER", stype, true))
                    {
                        type = JoinType.RIGHT_OUTER_JOIN;
                    }
                    else
                    {
                        throw new NotSupportedException("DEBUG:  JOIN type not supported: " + stype);
                    }

                    string DfsTableFilesInput = DSpace_ExecArgs[6];
                    /*
                    TableFileNames = DfsTableFilesInput.Split(';');
                    if (2 != TableFileNames.Length)
                    {
                        throw new Exception("DEBUG:  Invalid number of tables");
                    }
                    for (int it = 0; it < TableFileNames.Length; it++)
                    {
                        if (TableFileNames[it].StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            TableFileNames[it] = TableFileNames[it].Substring(6);
                        }
                        int iat = TableFileNames[it].IndexOf('@');
                        if (-1 != iat)
                        {
                            TableFileNames[it] = TableFileNames[it].Substring(0, iat);
                        }
                    }
                     * */

                }

                int tableid = GetTableID(row);

                ByteSlice key;
                if (0 == tableid)
                {
                    key = ByteSlice.Prepare(row, LeftOffset, LeftSize);
                    if (null != LeftConv)
                    {
                        key = LeftConv(key, DSpace_KeyLength, ftools);
                    }
                }
                else if (1 == tableid)
                {
                    key = ByteSlice.Prepare(row, RightOffset, RightSize);
                    if (null != RightConv)
                    {
                        key = RightConv(key, DSpace_KeyLength, ftools);
                    }
                }
                else
                {
                    throw new Exception("Map: Unexpected TableID: " + tableid);
                }

                List<byte> valuebuf = ftools.AllocBuffer(1 + 4 + row.Length);
                {
                    DbValue tableiddbvalue = ftools.AllocValue(tableid);
                    tableiddbvalue.Eval().AppendTo(valuebuf);
                }
                row.AppendTo(valuebuf);

                output.Add(key, ByteSlice.Prepare(valuebuf));

                ftools.ResetBuffers();
            }

        }


        public class JoinOnReduce : MapReduce
        {

            DbFunctionTools ftools = null;

            enum JoinType
            {
                X,
                INNER_JOIN,
                LEFT_OUTER_JOIN,
                RIGHT_OUTER_JOIN,
            }
            JoinType type = JoinType.X;

            int GetTableID(ByteSlice row)
            {
                return ftools.GetInt(ByteSlice.Prepare(row, 0, 1 + 4));
            }

            List<ByteSlice> t0, t1;


            public override void ReduceInitialize()
            {
            }

            public override void Reduce(ByteSlice key, IEnumerator<ByteSlice> values, MapReduceOutput output)
            {
                if (JoinType.X == type)
                {
                    ftools = new DbFunctionTools();

                    string stype = DSpace_ExecArgs[1];

                    if (0 == string.Compare("INNER", stype, true))
                    {
                        type = JoinType.INNER_JOIN;
                    }
                    else if (0 == string.Compare("LEFT_OUTER", stype, true))
                    {
                        type = JoinType.LEFT_OUTER_JOIN;
                    }
                    else if (0 == string.Compare("RIGHT_OUTER", stype, true))
                    {
                        type = JoinType.RIGHT_OUTER_JOIN;
                    }
                    else
                    {
                        throw new NotSupportedException("DEBUG:  JOIN type not supported: " + stype);
                    }

                    t0 = new List<ByteSlice>();
                    t1 = new List<ByteSlice>();
                }

                t0.Clear();
                t1.Clear();

                while (values.MoveNext())
                {
                    ByteSlice row = values.Current;

                    int tableid = GetTableID(row);

                    if (0 == tableid)
                    {
                        t0.Add(ByteSlice.Prepare(row, 5, row.Length - 5));
                    }
                    else if (1 == tableid)
                    {
                        t1.Add(ByteSlice.Prepare(row, 5, row.Length - 5));
                    }
                    else
                    {
                        throw new Exception("Reduce: Unexpected TableID: " + tableid);
                    }

                }

                int t0Count = t0.Count;
                int t1Count = t1.Count;

                if (0 == t0Count)
                {
                    if (JoinType.RIGHT_OUTER_JOIN == type)
                    {
                        for (int j = 0; j < t1Count; j++)
                        {
                            ByteSlice z = t1[j];
                            List<byte> valuebuf = ftools.AllocBuffer(DSpace_OutputRecordLength);
                            for (int n = 0; n < DSpace_OutputRecordLength - z.Length; n++)
                            {
                                valuebuf.Add(1); // IsNull=true.
                            }
                            z.AppendTo(valuebuf);
                            output.Add(ByteSlice.Prepare(valuebuf));
                        }

                    }
                }
                else if (0 == t1Count)
                {
                    if (JoinType.LEFT_OUTER_JOIN == type)
                    {
                        for (int i = 0; i < t0Count; i++)
                        {
                            ByteSlice z = t0[i];
                            List<byte> valuebuf = ftools.AllocBuffer(DSpace_OutputRecordLength);
                            z.AppendTo(valuebuf);

                            for (int n = 0; n < DSpace_OutputRecordLength - z.Length; n++)
                            {
                                valuebuf.Add(1); // IsNull=true.
                            }

                            output.Add(ByteSlice.Prepare(valuebuf));
                        }

                    }
                }
                else
                {
                    for (int i = 0; i < t0Count; i++)
                    {
                        for (int j = 0; j < t1Count; j++)
                        {
                            ByteSlice x = t0[i];
                            ByteSlice y = t1[j];
#if DEBUG
                            if (DSpace_OutputRecordLength != x.Length + y.Length)
                            {
                                throw new Exception("DEBUG:  JoinOn.Reduce: (DSpace_OutputRecordLength != x.Length + y.Length).  x.Length=" 
                                    + x.Length.ToString() + "; y.Length=" + y.Length.ToString() + ";DSpace_OutputRecordLength="
                                    + DSpace_OutputRecordLength.ToString());
                            }
#endif
                            List<byte> valuebuf = ftools.AllocBuffer(DSpace_OutputRecordLength);
                            x.AppendTo(valuebuf);
                            y.AppendTo(valuebuf);
                            output.Add(ByteSlice.Prepare(valuebuf));
                        }
                    }
                }

                ftools.ResetBuffers();
            }


            public override void ReduceFinalize()
            {

            }

        }

    }

}