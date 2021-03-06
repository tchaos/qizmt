﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class TopRemote : Remote
        {

            struct ColInfo
            {
                public string Name;
                public int Size;
                public int StartOffset;
                public string Type;
                public int DisplayWidth;
            }
            List<ColInfo> cols;

            string CleanColumnName(string cn)
            {
                int last = 0;
                for (int i = 0; i < cn.Length; i++)
                {
                    if ('.' == cn[i])
                    {
                        last = i + 1;
                    }
                    if ('\'' == cn[i] || '(' == cn[i])
                    {
                        break;
                    }
                }
                if (0 != last)
                {
                    return cn.Substring(last);
                }
                return cn;
            }

            void InitColInfos(string RowInfo, string DisplayInfo)
            {
                cols = new List<ColInfo>();
                int curoffset = 0;
                string[] rr = RowInfo.Split('\0');
                string[] dd = DisplayInfo.Split(',');
                for (int ix = 0; ix < rr.Length; ix++)
                {
                    string r = rr[ix];
                    string d = dd[ix];
                    ColInfo ci;
                    {
                        int ieq = r.LastIndexOf('=');
                        ci.Name = r.Substring(0, ieq);
                        ci.Size = int.Parse(r.Substring(ieq + 1));
                    }
                    ci.StartOffset = curoffset;
                    {
                        int ieq = d.LastIndexOf('=');
                        ci.Type = d.Substring(0, ieq);
                        ci.DisplayWidth = int.Parse(d.Substring(ieq + 1));
                    }
                    curoffset += ci.Size;
                    cols.Add(ci);
                }
            }


            public override void OnRemote(IEnumerator<ByteSlice> input, System.IO.Stream output)
            {

                string TableName = DSpace_ExecArgs[0];
                string DfsOutputName = DSpace_ExecArgs[1]; // Actually the input of this job.
                string RowInfo = Qa.QlArgsUnescape(DSpace_ExecArgs[2]);
                string DisplayInfo = DSpace_ExecArgs[3];
                long TopCount = long.Parse(DSpace_ExecArgs[4]);
                string sOptions = (DSpace_ExecArgs.Length > 5) ? DSpace_ExecArgs[5] : "";
                bool joined = -1 != sOptions.IndexOf("JOINED");

                InitColInfos(RowInfo, DisplayInfo);

                StringBuilder sb = new StringBuilder();

                sb.Length = 0;
                bool ShouldCleanName = !joined;
                foreach (ColInfo ci in cols)
                {
                    string name = ci.Name;
                    if (ShouldCleanName)
                    {
                        name = CleanColumnName(ci.Name);
                    }
                    sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", name);
                }
                string hsep = new string('-', sb.Length);
                DSpace_Log(sb.ToString());
                DSpace_Log(hsep);

                for (ByteSlice rowbuf;
                    (TopCount == -1 || TopCount > 0)
                        && input.MoveNext();
                    )
                {
                    rowbuf = input.Current;

                    sb.Length = 0;
                    foreach (ColInfo ci in cols)
                    {
                        ByteSlice cval = ByteSlice.Prepare(rowbuf, ci.StartOffset, ci.Size);
                        if (0 != cval[0])
                        {
                            sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", "NULL");
                        }
                        else
                        {
                            if (ci.Type.StartsWith("char"))
                            {
                                string charsvalue = System.Text.Encoding.Unicode.GetString(ByteSlice.Prepare(cval, 1, cval.Length - 1).ToBytes());
                                charsvalue = charsvalue.TrimEnd('\0');
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", charsvalue);
                            }
                            else if ("int" == ci.Type)
                            {
                                Int32 x = Entry.BytesToInt(ByteSlice.Prepare(cval, 1, cval.Length - 1).ToBytes());
                                x = Entry.ToInt32((UInt32)x);
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", x);
                            }
                            else if ("long" == ci.Type)
                            {
                                Int64 x = Entry.BytesToLong(ByteSlice.Prepare(cval, 1, cval.Length - 1).ToBytes());
                                x = Entry.ToInt64((UInt64)x);
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", x);
                            }
                            else if ("double" == ci.Type)
                            {
                                recordset rs = recordset.Prepare(ByteSlice.Prepare(cval, 1, cval.Length - 1));
                                double x = rs.GetDouble();
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", x);
                            }
                            else if ("DateTime" == ci.Type)
                            {
                                Int64 x = Entry.BytesToLong(ByteSlice.Prepare(cval, 1, cval.Length - 1).ToBytes());
                                DateTime dt = new DateTime(x);
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ", dt);
                            }
                            else
                            {
                                sb.AppendFormat("{0,-" + ci.DisplayWidth.ToString() + "} ",
                                    "?"); // Not supported yet.
                            }
                        }
                    }
                    DSpace_Log(sb.ToString());
                    if (TopCount != -1)
                    {
                        TopCount--;
                    }

                }

                DSpace_Log(hsep);

            }

        }

    }

}