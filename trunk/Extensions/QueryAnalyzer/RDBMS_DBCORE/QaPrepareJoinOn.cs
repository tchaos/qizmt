using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class PrepareJoinOn : Local
        {

            protected virtual MapReduceCall GetMapReduceCallJoinOn(params string[] InputFiles)
            {
                // Can't QO this yet because of StaticGlobals.DSpace_InputFileName
                if (QOLimit > 0)
                {
#if DEBUG
                    if (InputFiles.Length == 0)
                    {
                        throw new Exception("DEBUG: No input files specified");
                    }
#endif
                    try
                    {
                        long insize = _QOGetFileSizes(InputFiles);
                        if (insize < QOLimit)
                        {
                            return new MapReduceCallInProc("RDBMS_Join", new JoinOnMap(), new JoinOnReduce());
                        }
                    }
                    catch
                    {
                    }
                }
                return new MapReduceCallShellExec("RDBMS_JoinOn", "RDBMS_JoinOn.DBCORE");
            }

            const bool queryresults = true;
            const bool dfsref = false;

            protected override void Run()
            {

                string QlLeftTableName = DSpace_ExecArgs[0];
                string LeftTableName = QlArgsUnescape(QlLeftTableName);
                string stype = DSpace_ExecArgs[1];
                string QlRightTableName = DSpace_ExecArgs[2];
                string RightTableName = QlArgsUnescape(QlRightTableName);
                string QlOn = DSpace_ExecArgs[3];
                string On = QlArgsUnescape(QlOn);

                if (-1 != LeftTableName.IndexOf('\0'))
                {
                    throw new NotSupportedException("Cannot JOIN with multiple left tables: " + LeftTableName);
                }

                if (-1 != RightTableName.IndexOf('\0'))
                {
                    throw new NotSupportedException("Cannot JOIN with multiple right tables: " + RightTableName);
                }

                System.Xml.XmlDocument systables;
                using (GlobalCriticalSection.GetLock())
                {
                    systables = LoadSysTables_unlocked();
                }

                System.Xml.XmlElement xeLeftTable = FindTable(systables, LeftTableName);
                if (null == xeLeftTable)
                {
                    throw new Exception("Table (left) '" + LeftTableName + "' does not exist");
                }
                string sLeftRowSize = xeLeftTable["size"].InnerText;
                int LeftRowSize = int.Parse(sLeftRowSize);
                string LeftDfsTableFilesInput = xeLeftTable["file"].InnerText + "@" + sLeftRowSize;
                if (LeftDfsTableFilesInput.StartsWith("qa://", true, null))
                {
                    throw new Exception("Cannot JOIN with system tables (left)");
                }

                System.Xml.XmlElement xeRightTable = FindTable(systables, RightTableName);
                if (null == xeRightTable)
                {
                    throw new Exception("Table (right) '" + RightTableName + "' does not exist");
                }
                string sRightRowSize = xeRightTable["size"].InnerText;
                int RightRowSize = int.Parse(sRightRowSize);
                string RightDfsTableFilesInput = xeRightTable["file"].InnerText + "@" + sRightRowSize;
                if (RightDfsTableFilesInput.StartsWith("qa://", true, null))
                {
                    throw new Exception("Cannot JOIN with system tables (right)");
                }

                string DfsTableFilesInput = LeftDfsTableFilesInput + ";" + RightDfsTableFilesInput;

                string DfsOutputName = "dfs://RDBMS_JoinOn_" + Guid.NewGuid().ToString() + Qa.DFS_TEMP_FILE_MARKER;

                int DfsOutputRowSize = LeftRowSize + RightRowSize;

                string LeftRowInfo;
                string LeftDisplayInfo; // Display
                List<DbColumn> LeftCols = new List<DbColumn>();
                List<string> LeftColsWidths = new List<string>();
                {
                    StringBuilder sbRowInfo = new StringBuilder();
                    StringBuilder sbDisplayInfo = new StringBuilder(); // Display
                    int totsize = 0;
                    string xtablename = xeLeftTable["name"].InnerText;
                    foreach (System.Xml.XmlNode xn in xeLeftTable.SelectNodes("column"))
                    {
                        if (0 != sbRowInfo.Length)
                        {
                            sbRowInfo.Append('\0');
                            sbDisplayInfo.Append(','); // Display
                        }
                        string stsize = xn["bytes"].InnerText;
                        int tsize = int.Parse(stsize);
                        string RealColName = xn["name"].InnerText;
                        string UserColName = RealColName;
                        string xcolname;
                        if (-1 == UserColName.IndexOf('.'))
                        {
                            xcolname = xtablename + "." + UserColName;
                        }
                        else
                        {
                            xcolname = UserColName;
                        }
                        sbRowInfo.Append(xcolname); // Note: doesn't consider sub-select.
                        sbRowInfo.Append('=');
                        sbRowInfo.Append(stsize);
                        sbDisplayInfo.Append(xn["type"].InnerText); // Display
                        sbDisplayInfo.Append('='); // Display
                        sbDisplayInfo.Append(xn["dw"].InnerText); // Display
                        LeftColsWidths.Add(xn["dw"].InnerText);
                        {
                            DbColumn c;
                            c.Type = DbType.Prepare(xn["type"].InnerText, tsize);
                            c.RowOffset = totsize;
                            c.ColumnName = xcolname;
                            LeftCols.Add(c);
                        }
                        totsize += tsize;
                    }
                    LeftRowInfo = sbRowInfo.ToString();
                    LeftDisplayInfo = sbDisplayInfo.ToString(); // Display
                }

                string RightRowInfo;
                string RightDisplayInfo; // Display
                List<DbColumn> RightCols = new List<DbColumn>();
                List<string> RightColsWidths = new List<string>();
                {
                    StringBuilder sbRowInfo = new StringBuilder();
                    StringBuilder sbDisplayInfo = new StringBuilder(); // Display
                    int totsize = 0;
                    string xtablename = xeRightTable["name"].InnerText;
                    foreach (System.Xml.XmlNode xn in xeRightTable.SelectNodes("column"))
                    {
                        if (0 != sbRowInfo.Length)
                        {
                            sbRowInfo.Append('\0');
                            sbDisplayInfo.Append(','); // Display
                        }
                        string stsize = xn["bytes"].InnerText;
                        int tsize = int.Parse(stsize);
                        string RealColName = xn["name"].InnerText;
                        string UserColName = RealColName;
                        string xcolname;
                        if (-1 == UserColName.IndexOf('.'))
                        {
                            xcolname = xtablename + "." + UserColName;
                        }
                        else
                        {
                            xcolname = UserColName;
                        }
                        sbRowInfo.Append(xcolname); // Note: doesn't consider sub-select.
                        sbRowInfo.Append('=');
                        sbRowInfo.Append(stsize);
                        sbDisplayInfo.Append(xn["type"].InnerText); // Display
                        sbDisplayInfo.Append('='); // Display
                        sbDisplayInfo.Append(xn["dw"].InnerText); // Display
                        RightColsWidths.Add(xn["dw"].InnerText);
                        {
                            DbColumn c;
                            c.Type = DbType.Prepare(xn["type"].InnerText, tsize);
                            c.RowOffset = totsize;
                            c.ColumnName = xcolname;
                            RightCols.Add(c);
                        }
                        totsize += tsize;
                    }
                    RightRowInfo = sbRowInfo.ToString();
                    RightDisplayInfo = sbDisplayInfo.ToString(); // Display
                }

                string on1, onop, on2;
                {
                    string onargs = On;
                    on1 = Qa.NextPart(ref onargs);
                    onop = Qa.NextPart(ref onargs);
                    on2 = Qa.NextPart(ref onargs);
                    if (0 == on1.Length
                        || 0 == onop.Length
                        || 0 == on2.Length
                        || 0 != onargs.Trim().Length)
                    {
                        throw new Exception("Invalid ON expression for JOIN: " + On);
                    }
                }

                bool on1left;
                int on1colindex;
                {
                    int on1colindexother;
                    on1colindex = DbColumn.IndexOf(LeftCols, on1);
                    on1colindexother = DbColumn.IndexOf(RightCols, on1);
                    if (-1 != on1colindex)
                    {
                        if (-1 != on1colindexother)
                        {
                            throw new Exception("Column name " + on1 + " does not resolve to a single column (in left and right tables)");
                        }
                        else
                        {
                            on1left = true;
                        }
                    }
                    else
                    {
                        if (-1 != on1colindexother)
                        {
                            on1colindex = on1colindexother;
                            on1left = false;
                        }
                        else
                        {
                            throw new Exception("No such column named " + on1);
                        }
                    }
                }

                bool on2left;
                int on2colindex;
                {
                    int on2colindexother;
                    on2colindex = DbColumn.IndexOf(LeftCols, on2);
                    on2colindexother = DbColumn.IndexOf(RightCols, on2);
                    if (-1 != on2colindex)
                    {
                        if (-1 != on2colindexother)
                        {
                            throw new Exception("Column name " + on2 + " does not resolve to a single column (in left and right tables)");
                        }
                        else
                        {
                            on2left = true;
                        }
                    }
                    else
                    {
                        if (-1 != on2colindexother)
                        {
                            on2colindex = on2colindexother;
                            on2left = false;
                        }
                        else
                        {
                            throw new Exception("No such column named " + on2);
                        }
                    }
                }

                if ((on1left && on2left)
                    || (!on1left && !on2left))
                {
                    string whicht;
                    if (on1left)
                    {
                        whicht = "(left) " + LeftTableName;
                    }
                    else
                    {
                        whicht = "(right) " + RightTableName;
                    }
                    throw new Exception("ON expression is comparing columns from the same table: "
                        + on1 + " and " + on2 + " are both part of " + whicht);
                }

                // Order ON columns:
                if (!on1left)
                {
                    {
                        string onx = on1;
                        on1 = on2;
                        on2 = onx;
                    }
                    {
                        int onxcolindex = on1colindex;
                        on1colindex = on2colindex;
                        on2colindex = onxcolindex;
                    }
                    {
                        on1left = true;
                        on2left = false;
                    }
                    // Invert the operator..
                    switch (onop)
                    {
                        case "=":
                            //onop = "=";
                            break;
                        case "!=":
                            //onop = "!=";
                            break;
                        case "<":
                            onop = ">";
                            break;
                        case "<=":
                            onop = ">=";
                            break;
                        case ">":
                            onop = "<";
                            break;
                        case ">=":
                            onop = "<=";
                            break;
                        default:
                            throw new Exception("Unhandled ON operation: " + onop);
                    }
                    On = on1 + " " + onop + " " + on2;
                    QlOn = QlArgsEscape(On);
                }

                DbColumn on1col;
                if (on1left)
                {
                    on1col = LeftCols[on1colindex];
                }
                else
                {
                    //on1col = RightCols[on1colindex];
                    throw new Exception("DEBUG:  PrepareJoinOn: (!on1left)");
                }
                if (on1col.Type.Size == 0 || on1col.Type.ID == DbTypeID.NULL)
                {
                    throw new Exception("Invalid column for ON expression: " + on1);
                }

                DbColumn on2col;
                if (on2left)
                {
                    //on2col = LeftCols[on2colindex];
                    throw new Exception("DEBUG:  PrepareJoinOn: (on2left)");
                }
                else
                {
                    on2col = RightCols[on2colindex];
                }
                if (on2col.Type.Size == 0 || on2col.Type.ID == DbTypeID.NULL)
                {
                    throw new Exception("Invalid column for ON expression: " + on2);
                }

                int KeyLength = on1col.Type.Size;
                if (on2col.Type.Size > KeyLength)
                {
                    KeyLength = on2col.Type.Size;
                }

                {
                    MapReduceCall mrc = GetMapReduceCallJoinOn(DfsTableFilesInput);
                    mrc.OverrideOutputMethod = "grouped";
                    mrc.OverrideInput = DfsTableFilesInput;
                    mrc.OverrideOutput = DfsOutputName + "@" + DfsOutputRowSize;
                    mrc.OverrideKeyLength = KeyLength;
                    if (RDBMS_DBCORE.Qa.FaultTolerantExecution)
                    {
                        mrc.OverrideFaultTolerantExecutionMode = "enabled";
                    }
                    mrc.Call("\"" + QlLeftTableName + "\" " + stype + " \"" + QlRightTableName
                        + "\" \"" + QlOn
                        + "\" \"" + on1col.RowOffset + "," + on1col.Type.Size + "=" + QlArgsEscape(on1col.Type.Name)
                        + "\" \"" + on2col.RowOffset + "," + on2col.Type.Size + "=" + QlArgsEscape(on2col.Type.Name)
                        + "\" \"" + DfsTableFilesInput
                        + "\"");
                }

                {
                    PrepareSelect.queryresults qr = new PrepareSelect.queryresults();
                    List<DbColumn> AllCols = new List<DbColumn>(LeftCols.Count + RightCols.Count);
                    AllCols.AddRange(LeftCols);
                    AllCols.AddRange(RightCols);
                    qr.SetFields(AllCols);
                    qr.temptable = DfsOutputName;
                    qr.recordsize = LeftRowSize + RightRowSize;
                    string sPartCount = Shell("dspace countparts \"" + DfsOutputName + "\"").Split('\n')[0].Trim();
                    qr.parts = int.Parse(sPartCount);
                    string sDfsOutputSize = Shell("dspace filesize \"" + DfsOutputName + "\"").Split('\n')[0].Trim();
                    long DfsOutputSize = long.Parse(sDfsOutputSize);
                    long NumRowsOutput = DfsOutputSize / qr.recordsize;
                    qr.recordcount = NumRowsOutput;

                    DSpace_Log(PrepareSelect.SetQueryResults(qr));

                }

            }

        }

    }

}