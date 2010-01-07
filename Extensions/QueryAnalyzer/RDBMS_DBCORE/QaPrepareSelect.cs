using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class PrepareSelect : Local
        {


            public class queryresults
            {
                public string reftable, temptable;
                public bool IsRefTable
                {
                    get { return null != reftable; }
                }
                public bool IsTempTable
                {
                    get { return null != temptable; }
                }

                public class FieldInfo
                {
                    [System.Xml.Serialization.XmlAttribute]
                    public int index;
                    [System.Xml.Serialization.XmlAttribute]
                    public string name;
                    [System.Xml.Serialization.XmlAttribute]
                    public string qatype, cstype;
                    [System.Xml.Serialization.XmlAttribute]
                    public int frontbytes, bytes, backbytes;
                }
                [System.Xml.Serialization.XmlElement("field")]
                public FieldInfo[] fields;

                public long recordcount;
                public int recordsize;
                public int parts;


                public void SetFields(IList<DbColumn> cols)
                {
                    FieldInfo[] f = new FieldInfo[cols.Count];
                    for (int i = 0; i < f.Length; i++)
                    {
                        FieldInfo fi = new FieldInfo();
                        fi.index = i;
                        fi.name = cols[i].ColumnName;
                        string qatype = cols[i].Type.Name;
                        fi.qatype = qatype;
                        fi.cstype = QATypeToCSType(qatype);
                        fi.frontbytes = 1;
                        fi.bytes = cols[i].Type.Size - 1;
                        fi.backbytes = 0;
                        f[i] = fi;
                    }
                    SetFields(f);
                }

                public void SetFields(FieldInfo[] f)
                {
                    this.fields = f;
                }


                public string GetPseudoTableName()
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("'");
                    string tf;
                    if (IsRefTable)
                    {
                        tf = reftable;
                    }
                    else if (IsTempTable)
                    {
                        tf = temptable;
                    }
                    else
                    {
                        throw new InvalidOperationException("GetPseudoTableName: DFSTEMP or DFSREF required");
                    }
                    if (!tf.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        sb.Append("dfs://");
                    }
                    sb.Append(tf);
                    sb.Append('@');
                    sb.Append(recordsize);
                    for (int i = 0; i < fields.Length; i++)
                    {
                        sb.Append(';');
                        sb.Append(fields[i].name);
                        sb.Append('=');
                        sb.Append(fields[i].qatype);
                    }
                    sb.Append('\'');
                    return sb.ToString();
                }

            }

            public static queryresults GetQueryResults(string joboutput)
            {
                {
                    int ixml = joboutput.IndexOf("<?xml");
                    if (ixml > 0)
                    {
                        joboutput = joboutput.Substring(ixml);
                    }
                }
                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(queryresults));
                return (queryresults)xs.Deserialize(new System.IO.StringReader(joboutput));
            }


            public static string SetQueryResults(queryresults qr)
            {
                System.IO.MemoryStream ms = new System.IO.MemoryStream();
                SetQueryResults(qr, ms);
                ms.Position = 0;
                System.IO.StreamReader sr = new System.IO.StreamReader(ms);
                return sr.ReadToEnd();
            }

            public static void SetQueryResults(queryresults qr, System.IO.Stream set)
            {
                System.Xml.Serialization.XmlSerializer xs =
                    new System.Xml.Serialization.XmlSerializer(typeof(PrepareSelect.queryresults));
                xs.Serialize(set, qr);
            }


            List<DbColumn> cols = null;
            List<string> colswidths = null;


            protected override void Run()
            {
#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif

                string TableName = QlArgsUnescape(DSpace_ExecArgs[0]); // nul-delimited.
                string DfsOutputName = DSpace_ExecArgs[1];
                string QlArgsSelectWhat = DSpace_ExecArgs[2];
                long TopCount = long.Parse(DSpace_ExecArgs[3]);
                string QlArgsOps = DSpace_ExecArgs[4]; // Still encoded with QlArgsEscape.
                string sOptions = DSpace_ExecArgs[5]; // "DFSTEMP" or "-", etc
                bool dfstemp = -1 != sOptions.IndexOf("DFSTEMP");
                bool Update = -1 != sOptions.IndexOf("GUPDATE"); // UPDATE (grouped MR).
                bool GroupBy = -1 != sOptions.IndexOf("GBY");
                bool OrderBy = -1 != sOptions.IndexOf("OBY");
                bool Order2By = -1 != sOptions.IndexOf("O2BY");
                bool dfsref = -1 != sOptions.IndexOf("DFSREF");
                bool queryresults = dfstemp || dfsref;
                bool topdfstemp = -1 != sOptions.IndexOf("TOPTEMP");
                bool distinct = -1 != sOptions.IndexOf("DISTINCT");
                bool joined = -1 != sOptions.IndexOf("JOINED");

                string SelectWhat = QlArgsUnescape(QlArgsSelectWhat);
                bool WhatFunctions = -1 != SelectWhat.IndexOf('('); // Select clause has functions (aggregates and/or scalars).
                if (Order2By)
                {
                    WhatFunctions = false;
#if DEBUG
                    //System.Diagnostics.Debugger.Launch();
#endif
                }

                if (OrderBy)
                {
                    if (WhatFunctions || GroupBy)
                    {
                        if ("*" == SelectWhat)
                        {
                            throw new Exception("Invalid query: cannot SELECT * with ORDER BY and GROUP BY");
                        }

                        string sOrderByCols = null; // null, or "foo,bar" from "ORDER BY foo,bar".
                        List<string> OrderByCols = new List<string>();

                        string[] Ops = QlArgsUnescape(QlArgsOps).Split('\0');
                        List<string> NewOps = new List<string>(Ops.Length);
                        for (int iop = 0; iop < Ops.Length; iop++)
                        {
                            if (0 == string.Compare("ORDER", Ops[iop], true))
                            {
                                if (iop + 1 < Ops.Length
                                    && 0 == string.Compare("BY", Ops[iop + 1], true))
                                {
                                    iop++;
                                    int nparens = 0;
                                    StringBuilder sob = new StringBuilder();
                                    StringBuilder curobcol = new StringBuilder();
                                    for (; ; )
                                    {
                                        iop++;
                                        if (iop >= Ops.Length)
                                        {
                                            break;
                                        }
                                        sob.Append(Ops[iop]);
                                        curobcol.Append(Ops[iop]);
                                        if ("(" == Ops[iop])
                                        {
                                            nparens++;
                                        }
                                        else if (nparens > 0)
                                        {
                                            if (")" == Ops[iop])
                                            {
                                                nparens--;
                                            }
                                        }
                                        iop++;
                                        if (iop >= Ops.Length)
                                        {
                                            if (0 != nparens)
                                            {
                                                throw new Exception("Expected ) in ORDER BY");
                                            }
                                            if (curobcol.Length != 0)
                                            {
                                                OrderByCols.Add(curobcol.ToString());
                                                curobcol.Length = 0;
                                            }
                                            break;
                                        }
                                        if (0 == nparens)
                                        {
                                            if (curobcol.Length != 0)
                                            {
                                                OrderByCols.Add(curobcol.ToString());
                                                curobcol.Length = 0;
                                            }
                                            if ("," != Ops[iop])
                                            {
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            curobcol.Append(Ops[iop]);
                                        }
                                        sob.Append(Ops[iop]);
                                    }
                                    sOrderByCols = sob.ToString();
                                    if (iop >= Ops.Length)
                                    {
                                        break;
                                    }
                                }
                            }
                            NewOps.Add(Ops[iop]);
                        }
                        QlArgsOps = QlArgsEscape(string.Join("\0", NewOps.ToArray()));
                        
#if DEBUG
                        //System.Diagnostics.Debugger.Launch();
#endif
                        QueryAnalyzer qa1 = new QueryAnalyzer();
                        string sel1 = qa1.Exec(
                            "SELECT DFSTEMP", QlArgsEscape(sOrderByCols + "," + SelectWhat.Replace('\0', ',')),
                            "FROM", QlArgsEscape(TableName),
                            QlArgsEscape(string.Join(" ", NewOps.ToArray()))
                            );
                        Qa.PrepareSelect.queryresults qr = Qa.PrepareSelect.GetQueryResults(sel1);
                        for (int iobc = 0; iobc < OrderByCols.Count; iobc++)
                        {
                            qr.fields[iobc].name = "~OBY.~" + qr.fields[iobc].name;
                        }
                        try
                        {
                            /*
                            QueryAnalyzer qa2 = new QueryAnalyzer();
                            string sel2 = qa2.Exec(
                                dfstemp ? "SELECT DFSTEMP" : (dfsref ? "SELECT DFSREF" : "SELECT")
                                );
                             * */
                            string newopts = "";
                            if (dfstemp)
                            {
                                newopts += ";DFSTEMP";
                            }
                            if (dfsref)
                            {
                                newopts += ";DFSREF";
                            }
                            if (topdfstemp)
                            {
                                newopts += ";TOPTEMP";
                            }
                            if (distinct)
                            {
                                newopts += ";DISTINCT";
                            }
                            PrepareSelect ps2 = new PrepareSelect();
                            string neworderby;
                            {
                                StringBuilder sbnob = new StringBuilder();
                                //"ORDER\0BY\0" + sOrderByCols.Replace(',', '\0')
                                sbnob.Append("ORDER\0BY");
                                bool firstsbnob = true;
                                for (int iobc = 0; iobc < OrderByCols.Count; iobc++)
                                {
                                    if (!firstsbnob)
                                    {
                                        sbnob.Append("\0,");
                                    }
                                    firstsbnob = false;
                                    sbnob.Append('\0');
                                    sbnob.Append(qr.fields[iobc].name); // Includes "~OBY.~"
                                }
                                neworderby = sbnob.ToString();
                            }
                            newopts += ";O2BY"; // Order-by phase 2.
                            // [QlArgs]SelectWhat might have () but they are NOT evaluated at this point (no SFUNC)
                            string sel2 = ps2.Exec(
                                Qa.QlArgsEscape(qr.GetPseudoTableName()),
                                DfsOutputName,
                                //(OrderByCols.Split(',').Length + 2).ToString() + "-*", // Select N-* where N is after OrderByCols
                                QlArgsSelectWhat,
                                TopCount.ToString(),
                                QlArgsEscape(neworderby),
                                newopts
                                );
                            DSpace_Log(sel2);
                        }
                        finally
                        {
                            if (qr.IsTempTable)
                            {
                                Shell("dspace del \"" + qr.temptable + "\"");
                            }
                        }

                        return;

                    }
                }

                string[] awhat = null;
                string[] UserColumnNames = null;
                if ("*" != SelectWhat && "^" != SelectWhat)
                {
                    awhat = SelectWhat.Split('\0');
                    UserColumnNames = new string[awhat.Length];
                    for (int iww = 0; iww < awhat.Length; iww++)
                    {
                        int ilas = awhat[iww].LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                        if (-1 == ilas)
                        {
                            UserColumnNames[iww] = awhat[iww];
                        }
                        else
                        {
                            string asname = awhat[iww].Substring(ilas + 4);
                            for (int iy = 0; iy < asname.Length; iy++)
                            {
                                if (!char.IsLetterOrDigit(asname[iy])
                                    && '_' != asname[iy])
                                {
                                    asname = null;
                                    break;
                                }
                            }
                            if (!string.IsNullOrEmpty(asname) && !char.IsDigit(asname[0]))
                            {
                                UserColumnNames[iww] = asname;
                                awhat[iww] = awhat[iww].Substring(0, ilas);
                            }
                            else
                            {
                                UserColumnNames[iww] = awhat[iww];
                            }
                        }
                    }
                }

#if DEBUG
                if (Update && !dfstemp)
                {
                    throw new Exception("DEBUG:  (Update && !dfstemp)");
                }
#endif

                System.Xml.XmlDocument systables;
                using (GlobalCriticalSection.GetLock())
                {
                    systables = LoadSysTables_unlocked();
                }

                System.Xml.XmlElement xeTable;

                string DfsTableFilesInput; // Note: can be multiple semicolon-separated input files. Includes record length info (@N).
                string sRowSize;
                int RowSize;

                if (-1 != TableName.IndexOf('\0'))
                {
                    string[] tables = TableName.Split('\0');
                    string tn = tables[0];
                    xeTable = FindTable(systables, tn);
                    if (null == xeTable)
                    {
                        throw new Exception("Table '" + tn + "' does not exist");
                    }
                    sRowSize = xeTable["size"].InnerText;
                    RowSize = int.Parse(sRowSize);
                    DfsTableFilesInput = xeTable["file"].InnerText + "@" + sRowSize;
                    System.Xml.XmlNodeList xnlTableCols = xeTable.SelectNodes("column");
                    int numcols = xnlTableCols.Count;
                    for (int tni = 1; tni < tables.Length; tni++)
                    {
                        tn = tables[tni];
                        System.Xml.XmlElement xett = FindTable(systables, tn);
                        if (null == xett)
                        {
                            throw new Exception("Table '" + tn + "' does not exist");
                        }
                        System.Xml.XmlNodeList xnltcols = xett.SelectNodes("column");
                        bool colsmatch = false;
                        if (xnltcols.Count == numcols)
                        {
                            bool fail = false;
                            for (int ic = 0; ic < numcols; ic++)
                            {
                                if (xnlTableCols[ic]["type"].InnerText != xnltcols[ic]["type"].InnerText)
                                {
                                    fail = true;
                                    break;
                                }
                                if (0 != string.Compare(xnlTableCols[ic]["name"].InnerText, xnltcols[ic]["name"].InnerText, true))
                                {
                                    fail = true;
                                    break;
                                }
                            }
                            colsmatch = !fail;
                        }
                        if (!colsmatch)
                        {
                            throw new Exception("Columns of table " + tn + " do not match columns of table " + xeTable["name"].InnerText);
                        }

                        DfsTableFilesInput += ";" + xett["file"].InnerText + "@" + sRowSize;

                    }

                    if (-1 != DfsTableFilesInput.IndexOf("qa://", StringComparison.OrdinalIgnoreCase))
                    {
                        throw new Exception("Cannot union with a system table"); /////////
                    }

                }
                else
                {
                    xeTable = FindTable(systables, TableName);
                    if (null == xeTable)
                    {
                        throw new Exception("Table '" + TableName + "' does not exist");
                    }
                    sRowSize = xeTable["size"].InnerText;
                    RowSize = int.Parse(sRowSize);
                    DfsTableFilesInput = xeTable["file"].InnerText + "@" + sRowSize;
                }

                if (dfsref)
                {
                    DfsOutputName = xeTable["file"].InnerText;
                }

                if (queryresults)
                {
                    DSpace_Log("<?xml version=\"1.0\"?>");
                    DSpace_Log("  <queryresults>");
                    if (dfsref)
                    {
                        DSpace_Log("    <reftable>" + DfsOutputName + "</reftable>");
                    }
                    else
                    {
                        DSpace_Log("    <temptable>" + DfsOutputName + "</temptable>");
                    }
                }

                string RowInfo;
                string DisplayInfo; // Display
                cols = new List<DbColumn>();
                colswidths = new List<string>();
                {
                    StringBuilder sbRowInfo = new StringBuilder();
                    StringBuilder sbDisplayInfo = new StringBuilder(); // Display
                    int totsize = 0;
                    string xtablename = xeTable["name"].InnerText;
                    foreach (System.Xml.XmlNode xn in xeTable.SelectNodes("column"))
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
                        if (null != awhat)
                        {
                            for (int iww = 0; iww < awhat.Length; iww++)
                            {
                                if (0 == string.Compare(awhat[iww], RealColName, true))
                                {
                                    UserColName = UserColumnNames[iww];
                                    break;
                                }
                            }
                        }
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
                        colswidths.Add(xn["dw"].InnerText);
                        {
                            DbColumn c;
                            c.Type = DbType.Prepare(xn["type"].InnerText, tsize);
                            c.RowOffset = totsize;
                            c.ColumnName = xcolname;
                            cols.Add(c);
                        }
                        totsize += tsize;
                    }
                    RowInfo = sbRowInfo.ToString();
                    DisplayInfo = sbDisplayInfo.ToString(); // Display
                }

                int KeyLength = RowSize; // For now, can null out the extra bytes.

                bool IsSpecialOrdered = -1 != sOptions.IndexOf("SPECIALORDER");

                string DfsInput;
                string DfsTempInputFile = null;
                // FIXME: look for all qa:// and run them through separately,...
                if (DfsTableFilesInput.StartsWith("qa://", true, null))
                {
                    if (dfsref)
                    {
                        throw new Exception("Cannot DFSREF with non-user table");
                    }
                    string SysGenOutputFile;
                    if (IsSpecialOrdered)
                    {
                        SysGenOutputFile = DfsOutputName + "@" + sRowSize;
                        DfsInput = null;
                    }
                    else
                    {
                        SysGenOutputFile = "dfs://RDBMS_QaTemp_" + Guid.NewGuid().ToString() + "@" + sRowSize;
                        DfsTempInputFile = SysGenOutputFile;
                        DfsInput = DfsTempInputFile;
                    }
                    {
                        string qafile = DfsTableFilesInput;
                        int iat = qafile.IndexOf('@');
                        if (-1 != iat)
                        {
                            qafile = qafile.Substring(0, iat);
                        }
                        Shell("dspace exec \"//Job[@Name='RDBMS_SysGen']/IOSettings/DFS_IO/DFSWriter=" + SysGenOutputFile + "\" RDBMS_SysGen.DBCORE \"" + qafile + "\" \"" + Qa.QlArgsEscape(RowInfo) + "\" \"" + DisplayInfo + "\" \"" + TableName + "\"");
                    }
                }
                else
                {
                    DfsInput = DfsTableFilesInput;
                }

#if DEBUG
                if (null != DfsInput)
                {
                    if (-1 == DfsInput.IndexOf('@'))
                    {
                        throw new Exception("Expected @ in DfsInput");
                    }
                }
#endif

                string OutputRowInfo;
                string OutputDisplayInfo;
                long OutputRowSize;
                string OutputsRowSize;
                List<string> outputcolswidths;
                List<DbColumn> outputcols;
                if (null != awhat)
                {
                    outputcols = new List<DbColumn>(awhat.Length);
                    outputcolswidths = new List<string>();
                    StringBuilder sbRowInfo = new StringBuilder();
                    StringBuilder sbDisplayInfo = new StringBuilder();
                    long xRowSize = 0;
                    for (int iww = 0; iww < UserColumnNames.Length; iww++)
                    {
                        string w = UserColumnNames[iww];
                        string sdw; // String display width.
                        DbColumn c = GetDbColumn(w, out sdw);
                        if (c.Type.Size == 0)
                        {
                            throw new Exception("No such column named " + w);
                        }
                        if (0 != sbRowInfo.Length)
                        {
                            sbRowInfo.Append('\0');
                            sbDisplayInfo.Append(',');
                        }
                        {
                            outputcols.Add(c);
                        }
                        //sbRowInfo.Append(c.ColumnName); // Already includes "TableName."
                        sbRowInfo.Append(UserColumnNames[iww]);
                        sbRowInfo.Append('=');
                        sbRowInfo.Append(c.Type.Size);
                        sbDisplayInfo.Append(c.Type.Name);
                        sbDisplayInfo.Append('=');
                        sbDisplayInfo.Append(sdw);
                        xRowSize += c.Type.Size;
                        outputcolswidths.Add(sdw);
                    }
                    OutputRowInfo = sbRowInfo.ToString();
                    OutputDisplayInfo = sbDisplayInfo.ToString();
                    OutputRowSize = xRowSize;
                    OutputsRowSize = xRowSize.ToString();
                }
                else
                {
                    // Same values!
                    outputcols = new List<DbColumn>(cols);
                    OutputRowInfo = RowInfo;
                    OutputDisplayInfo = DisplayInfo;
                    OutputRowSize = RowSize;
                    OutputsRowSize = sRowSize;
                    {
                        outputcolswidths = new List<string>(colswidths.Count);
                        for (int icw = 0; icw < colswidths.Count; icw++)
                        {
                            outputcolswidths.Add(colswidths[icw]);
                        }
                    }
                }

                string QlArgsNewSelectWhat = QlArgsSelectWhat;
                if (null != UserColumnNames)
                {
                    QlArgsNewSelectWhat = QlArgsEscape(string.Join("\0", UserColumnNames));
                }

                string shelloutputSelect1 = "";

                if (!dfsref) // Important! Don't run Select DBCORE if DFSREF.
                {
                    if (!IsSpecialOrdered)
                    {
                        if (WhatFunctions)
                        {
                            string FuncDfsInput = DfsInput;
                            string FuncDfsOutput = "dfs://RDBMS_SelectFunc_" + Guid.NewGuid().ToString();
                            FuncDfsOutput = DfsOutputName; // For now...
                            string FuncArgsOptions = "SFUNC"; // Select clause functions (aggregates and/or scalars).
                            if (GroupBy)
                            {
                                FuncArgsOptions += ";GBY";
                            }
                            string FuncSelectOutput1 = Shell("dspace exec" + " \"//Job[@Name='RDBMS_Select']/IOSettings/OutputMethod=grouped\"" + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + FuncDfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + FuncDfsOutput + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + Qa.QlArgsEscape(RowInfo) + "\" \"" + DisplayInfo + "\" \"" + Qa.QlArgsEscape(OutputRowInfo) + "\" " + FuncArgsOptions).Trim();
                            string[] FuncOutputTypeNames;
                            {
                                const string AOTIBEGINSTRING = "BEGIN:{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}";
                                int aotiBegin = FuncSelectOutput1.IndexOf(AOTIBEGINSTRING);
                                if (-1 == aotiBegin)
                                {
                                    string et = "Function (aggregate and/or scalar) Select MR output invalid (expected begin output type information)";
                                    //#if DEBUG
                                    et += "\r\nMR output:\r\n" + FuncSelectOutput1 + "\r\n";
                                    //#endif
                                    throw new Exception(et);
                                }
                                int aotiEnd = FuncSelectOutput1.IndexOf("{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}:END");
                                if (aotiEnd < aotiBegin)
                                {
                                    throw new Exception("Function (aggregate and/or scalar)  Select MR output invalid (expected end output type information)");
                                }
                                {
                                    string stypes = FuncSelectOutput1.Substring(aotiBegin + AOTIBEGINSTRING.Length, aotiEnd - aotiBegin - AOTIBEGINSTRING.Length);
                                    FuncOutputTypeNames = System.Text.RegularExpressions.Regex.Split(stypes, @"\{264E73F6-E3C9-43de-A3FD-9AC36F905087\}");
                                }
                            }
                            if (FuncOutputTypeNames.Length != outputcolswidths.Count)
                            {
                                throw new Exception("DEBUG:  (FuncOutputTypeNames.Length != outputcolswidths.Count)");
                            }
                            {
                                StringBuilder sbOutputDisplayInfo = new StringBuilder();
                                for (int icw = 0; icw < FuncOutputTypeNames.Length; icw++)
                                {
                                    if (icw > 0)
                                    {
                                        sbOutputDisplayInfo.Append(',');
                                    }
                                    sbOutputDisplayInfo.Append(FuncOutputTypeNames[icw]);
                                    sbOutputDisplayInfo.Append('=');
                                    sbOutputDisplayInfo.Append(outputcolswidths[icw]);
                                }
                                OutputDisplayInfo = sbOutputDisplayInfo.ToString();
                            }
                            if (FuncOutputTypeNames.Length != outputcols.Count)
                            {
                                throw new Exception("DEBUG:  (FuncOutputTypeNames.Length != outputcols.Count)");
                            }
                            {
                                // Fix output type, since I didn't know until the output was generated.
                                for (int oic = 0; oic < FuncOutputTypeNames.Length; oic++)
                                {
                                    DbColumn c = outputcols[oic];
                                    c.Type = DbType.Prepare(FuncOutputTypeNames[oic], c.Type.Size);
                                    outputcols[oic] = c;
                                }
                            }
                            DfsInput = FuncDfsOutput + "@" + OutputsRowSize;
                            {
                                RowInfo = OutputRowInfo;
                                DisplayInfo = OutputDisplayInfo;
                                QlArgsNewSelectWhat = "*";
                            }
                        }
                        else if (GroupBy)
                        {
                            string GByDfsInput = DfsInput;
                            string GByDfsOutput = "dfs://RDBMS_SelectGroupBy_" + Guid.NewGuid().ToString();
                            GByDfsOutput = DfsOutputName; // For now...
                            string GByArgsOptions = "GBY";
                            Shell("dspace exec" + " \"//Job[@Name='RDBMS_Select']/IOSettings/OutputMethod=grouped\"" + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + GByDfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + GByDfsOutput + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + Qa.QlArgsEscape(RowInfo) + "\" \"" + DisplayInfo + "\" \"" + Qa.QlArgsEscape(OutputRowInfo) + "\" " + GByArgsOptions).Trim();
                            DfsInput = GByDfsOutput + "@" + OutputsRowSize;
                            {
                                RowInfo = OutputRowInfo;
                                DisplayInfo = OutputDisplayInfo;
                                QlArgsNewSelectWhat = "*";
                            }
                        }
                        else
                        {
                            string condxpath = ""; // Append to this, but include a space before new xpath.
                            if (Update)
                            {
                                condxpath += " \"//Job[@Name='RDBMS_Select']/IOSettings/OutputMethod=grouped\"";
                            }
                            shelloutputSelect1 = Shell("dspace exec" + condxpath + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + DfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + DfsOutputName + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + Qa.QlArgsEscape(RowInfo) + "\" \"" + DisplayInfo + "\" \"" + Qa.QlArgsEscape(OutputRowInfo) + "\"").Trim();
                        }
                    }
                    else
                    {
                        if (null != awhat)
                        {
                            // This case shouldn't happen anyways.. becuase if custom columns, it's not a even special order command anymore.
                            throw new Exception("Special order commands must select all columns: SELECT * FROM " + TableName + " ...");
                        }
                    }
                }

                if (null != DfsTempInputFile)
                {
                    Shell("dspace del \"" + DfsTempInputFile + "\"");
                }

                if (distinct)
                {
                    string outtablefn = DfsOutputName + "_out_" + Guid.NewGuid().ToString();
                    Shell("dspace exec" + " \"//Job[@Name='RDBMS_Distinct']/IOSettings/DFSInput=" + DfsOutputName + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Distinct']/IOSettings/DFSOutput=" + outtablefn + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Distinct']/IOSettings/KeyLength=" + OutputsRowSize + "\" RDBMS_Distinct.DBCORE \"");
                    string distincttablefn = DfsOutputName + "_distinct_" + Guid.NewGuid().ToString();
                    Shell("dspace rename \"" + DfsOutputName + "\" \"" + distincttablefn + "\"");
                    Shell("dspace rename \"" + outtablefn + "\" \"" + DfsOutputName + "\"");
                    Shell("dspace del \"" + distincttablefn + "\"");
                }

                if (queryresults)
                {

                    {
                        int dtfieldindex = 0;
                        foreach (DbColumn c in outputcols)
                        {
                            LogFieldInfo(dtfieldindex++, c, !joined);
                        }
                    }

                    string sDfsOutputSize = Shell("dspace filesize \"" + DfsOutputName + "\"").Split('\n')[0].Trim();
                    long DfsOutputSize = long.Parse(sDfsOutputSize);
                    long NumRowsOutput = DfsOutputSize / OutputRowSize;
                    if (0 != (DfsOutputSize % OutputRowSize))
                    {
                        throw new Exception("Output file size miscalculation (DfsOutputSize{" + DfsOutputSize + "} % OutputRowSize{" + OutputRowSize + "}) for file: " + DfsOutputName);
                    }
                    long recordcount = NumRowsOutput;
                    if (TopCount >= 0)
                    {
                        if (recordcount > TopCount)
                        {
                            recordcount = TopCount;
                            if (topdfstemp)
                            {
                                string outtablefn = DfsOutputName + "_out_" + Guid.NewGuid().ToString();
                                Shell("dspace exec \"//Job[@Name='RDBMS_WriteTop']/IOSettings/DFS_IO/DFSReader=" + DfsOutputName + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_WriteTop']/IOSettings/DFS_IO/DFSWriter=" + outtablefn + "@" + OutputsRowSize + "\" RDBMS_WriteTop.DBCORE " + TopCount.ToString());
                                string toppingtablefn = DfsOutputName + "_topping_" + Guid.NewGuid().ToString();
                                Shell("dspace rename \"" + DfsOutputName + "\" \"" + toppingtablefn + "\"");
                                Shell("dspace rename \"" + outtablefn + "\" \"" + DfsOutputName + "\"");
                                Shell("dspace del \"" + toppingtablefn + "\"");
                            }
                        }
                    }
                    DSpace_Log("    <recordcount>" + recordcount.ToString() + "</recordcount>");
                    DSpace_Log("    <recordsize>" + OutputsRowSize + "</recordsize>");

                    string sPartCount = Shell("dspace countparts \"" + DfsOutputName + "\"").Split('\n')[0].Trim();
                    DSpace_Log("    <parts>" + sPartCount + "</parts>");

                    DSpace_Log("  </queryresults>");
                }
                else
                {
                    DSpace_Log(shelloutputSelect1);

                    string sTopOptions = "-";
                    if (joined)
                    {
                        sTopOptions += ";JOINED";
                    }
                    DSpace_Log(Shell("dspace exec \"//Job[@Name='RDBMS_Top']/IOSettings/DFS_IO/DFSReader=" + DfsOutputName + "@" + OutputsRowSize + "\" RDBMS_Top.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + Qa.QlArgsEscape(OutputRowInfo) + "\" \"" + OutputDisplayInfo + "\" " + TopCount.ToString() + " " + sTopOptions).Trim());

                    Shell("dspace del \"" + DfsOutputName + "\"");
                }

            }


            DbColumn GetDbColumn(string name)
            {
                string sdw;
                return GetDbColumn(name, out sdw);
            }

            // If not found, the column and type names are empty, and the size is 0.
            // Type ID is NULL and RowOffset is 0 if a function (aggregate or scalar) or literal.
            DbColumn GetDbColumn(string name, out string sdw)
            {
#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif
                return GetDbColumn(new StringPartReader(name), out sdw);
            }

            // If not found, the column and type names are empty, and the size is 0.
            // Type ID is NULL and RowOffset is 0 if a function (aggregate or scalar) or literal.
            DbColumn GetDbColumn(PartReader reader, out string sdw)
            {
                Types.ExpressionType bexprtype;
                string bexpr = Types.ReadNextBasicExpression(reader, out bexprtype);
                switch (bexprtype)
                {
                    case Types.ExpressionType.NAME:
                        {
                            int ci = DbColumn.IndexOf(cols, bexpr);
                            if (-1 != ci)
                            {
                                sdw = colswidths[ci];
                                return cols[ci];
                            }
                        }
                        break;

                    case Types.ExpressionType.NULL:
                        {
                            DbColumn c;
                            c.Type = DbType.PrepareNull();
                            c.RowOffset = 0;
                            c.ColumnName = bexpr;
                            DisplayWidthFromType(c.Type, out sdw);
                            return c;
                        }
                        break;

                    case Types.ExpressionType.NUMBER:
                    case Types.ExpressionType.STRING:
                        {
                            DbColumn c;
                            {
                                DbTypeID typeid;
                                int sz = Types.LiteralToValueInfo(bexpr, out typeid);
                                c.Type = DbType.Prepare(sz, typeid);
                            }
                            c.RowOffset = 0;
                            c.ColumnName = bexpr;
                            DisplayWidthFromType(c.Type, out sdw);
                            return c;
                        }
                        break;

                    case Types.ExpressionType.FUNCTION:
                        {

                            // First see if there's a column named this!
                            {
                                int ci = DbColumn.IndexOf(cols, bexpr);
                                if (-1 != ci)
                                {
                                    sdw = colswidths[ci];
                                    return cols[ci];
                                }
                            }

                            int ip = bexpr.IndexOf('(');
                            if (-1 == ip)
                            {
                                throw new Exception("DEBUG:  Types.ExpressionType.FUNCTION: (-1 == ip)");
                            }
                            int extsize = -1;
                            int tsizenarg = -1; // tsize is only the size of this argument (0-based).
                            string funcname = bexpr.Substring(0, ip);
                            if (0 == string.Compare("AVG", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("STD", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("STD_SAMP", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("VAR_POP", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("VAR_SAMP", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("COUNT", funcname, true))
                            {
                                extsize = 1 + 8; // LONG
                            }
                            else if (0 == string.Compare("COUNTDISTINCT", funcname, true))
                            {
                                extsize = 1 + 8; // LONG
                            }
                            else if (0 == string.Compare("PI", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("LEN", funcname, true))
                            {
                                extsize = 1 + 4; // INT
                            }
                            else if (0 == string.Compare("ATN2", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("SUBSTRING", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("RAND", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("CHARINDEX", funcname, true))
                            {
                                extsize = 1 + 4; // INT
                            }
                            else if (0 == string.Compare("INSTR", funcname, true))
                            {
                                extsize = 1 + 4; // INT
                            }
                            else if (0 == string.Compare("PATINDEX", funcname, true))
                            {
                                extsize = 1 + 4; // INT
                            }
                            else if (0 == string.Compare("SIGN", funcname, true))
                            {
                                extsize = 1 + 4; // INT
                            }
                            else if (0 == string.Compare("ROUND", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("TRUNC", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("RADIANS", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("NVL", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("NVL2", funcname, true))
                            {
                                tsizenarg = 0;
                            }
                            else if (0 == string.Compare("SQRT", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("POWER", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("DEGREES", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("RADIANS", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("SYSDATE", funcname, true))
                            {
                                extsize = 1 + 8; // DateTime
                            }
                            else if (0 == string.Compare("MONTHS_BETWEEN", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("DATEDIFF", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("ADD_MONTHS", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("LOG10", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
                            else if (0 == string.Compare("SQUARE", funcname, true))
                            {
                                extsize = 1 + 9; // DOUBLE
                            }
#if DEBUG
                            else if (0 == string.Compare("CAST", funcname, true))
                            {
#if DEBUG
                                //System.Diagnostics.Debugger.Launch();
#endif
                            }
#endif

                            int totdw = 0;
                            string args = bexpr.Substring(ip + 1, bexpr.Length - ip - 1 - 1);
                            StringPartReader spargs = new StringPartReader(args);
                            int tsize = 1;
                            int narg = -1;
                            for (; ; )
                            {

                                Types.ExpressionType subetype;
                                string sube = Types.ReadNextBasicExpression(spargs, out subetype);
                                if (Types.ExpressionType.NONE == subetype)
                                {
                                    if (1 != tsize)
                                    {
                                        throw new Exception("Expected expression in parameter list");
                                    }
                                    break;
                                }
                                else
                                {
                                    narg++;
                                    if (Types.ExpressionType.AS == subetype)
                                    {
                                        if (0 == string.Compare("CAST", funcname, true))
                                        {
                                            int astsize;
                                            int aswidth;
                                            /*{
                                                string sargwidth;
                                                DbColumn argcol = GetDbColumn(sube, out sargwidth);
                                                int argwidth = int.Parse(sargwidth);
                                                assize = argcol.Type.Size - 1;
                                                aswidth = argwidth;
                                            }*/
                                            {
                                                // Remove "'AS " and "'"
                                                string sastype = sube.Substring(4, sube.Length - 4 - 1).Replace("''", "'");
                                                DbType astype = DbType.Prepare(DbType.NormalizeName(sastype));
                                                if (DbTypeID.NULL == astype.ID)
                                                {
                                                    throw new Exception("Unexpedted AS type: " + sastype);
                                                }
                                                //if (astype.Size - 1 > assize)
                                                {
                                                    astsize = astype.Size - 1;
                                                }
                                                int xdw = DisplayWidthFromType(astype);
                                                //if (xdw > aswidth)
                                                {
                                                    aswidth = xdw;
                                                }
                                            }
                                            //tsize += assize;
                                            //totdw += aswidth;
                                            tsize = astsize;
                                            totdw = aswidth;
                                            break; // Note: ignores anything after this, but that's OK here for CAST.
                                        }
                                        else
                                        {
                                            throw new Exception("AS not expected here");
                                        }
                                    }
                                    else
                                    {
                                        string sargwidth;
                                        DbColumn argcol = GetDbColumn(sube, out sargwidth);
                                        int argwidth = int.Parse(sargwidth);
#if DEBUG
                                        if (0 == argcol.Type.Size)
                                        {
                                            throw new Exception("Argument to function cannot be " + argcol.ColumnName + " (Type.Size=0)");
                                        }
#endif
                                        if (-1 == tsizenarg || narg == tsizenarg)
                                        {
                                            tsize += argcol.Type.Size - 1;
                                            totdw += argwidth;
                                        }

                                    }
                                }

                                string s = spargs.PeekPart();
                                if (s != ",")
                                {
                                    if (0 == string.Compare("AS", s, true))
                                    {
                                        continue;
                                    }
                                    if (0 != s.Length)
                                    {
                                        spargs.NextPart(); // Eat it.
                                        throw new Exception("Unexpected: " + s);
                                    }
                                    break;
                                }
                                spargs.NextPart(); // Eat the ','.

                            }
                            if (-1 != extsize)
                            {
                                tsize = extsize;
                            }
                            DbColumn c;
                            c.Type = DbType.Prepare("DbFunction", tsize, DbTypeID.NULL);  //(string TypeName, int TypeSize, DbTypeID TypeID)
                            c.RowOffset = 0;
                            c.ColumnName = bexpr;
                            sdw = totdw.ToString();
                            return c;
                        }
                        break;

                    default:
                        //throw new InvalidOperationException("Unhandled column: " + bexpr);
                        break;
                }

                {
                    DbColumn c;
                    c.Type = DbType.Prepare("", 0, DbTypeID.NULL);
                    c.RowOffset = 0;
                    c.ColumnName = string.Empty;
                    sdw = "0";
                    return c;
                }

            }


            void LogFieldInfo(int fieldindex, DbColumn c, bool ShouldCleanName)
            {
                string cstype = QATypeToCSType(c.Type.Name);
                string name = c.ColumnName;
                if (ShouldCleanName)
                {
                    name = CleanColumnName(name);
                }
                DSpace_Log("    <field index=\"" + fieldindex.ToString() + "\" name=\"" + name + "\" qatype=\"" + c.Type.Name + "\" cstype=\"" + cstype + "\" frontbytes=\"1\" bytes=\"" + (c.Type.Size - 1).ToString() + "\" backbytes=\"0\" />");
            }


            static string QATypeToCSType(string qatype)
            {
                if (qatype.StartsWith("char", true, null))
                {
                    return "System.String";
                }
                else if (0 == string.Compare(qatype, "DateTime", true))
                {
                    return "System.DateTime";
                }
                else if (0 == string.Compare(qatype, "int", true))
                {
                    return "System.Int32";
                }
                else if (0 == string.Compare(qatype, "long", true))
                {
                    return "System.Int64";
                }
                else if (0 == string.Compare(qatype, "double", true))
                {
                    return "System.Double";
                }
                else
                {
                    return "";
                }
            }


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


        }

    }
}
