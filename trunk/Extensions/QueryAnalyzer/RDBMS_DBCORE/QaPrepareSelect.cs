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

            List<DbColumn> cols = null;
            List<string> colswidths = null;


            protected override void Run()
            {

                string TableName = QlArgsUnescape(DSpace_ExecArgs[0]); // nul-delimited.
                string DfsOutputName = DSpace_ExecArgs[1];
                string QlArgsSelectWhat = DSpace_ExecArgs[2];
                long TopCount = long.Parse(DSpace_ExecArgs[3]);
                string QlArgsOps = DSpace_ExecArgs[4]; // Still encoded with QlArgsEscape.
                string sOptions = DSpace_ExecArgs[5]; // "DFSTEMP" or "-", etc
                bool dfstemp = -1 != sOptions.IndexOf("DFSTEMP");
                bool Update = -1 != sOptions.IndexOf("GUPDATE"); // UPDATE (grouped MR).
                bool GroupBy = -1 != sOptions.IndexOf("GBY");
                bool dfsref = -1 != sOptions.IndexOf("DFSREF");
                bool queryresults = dfstemp || dfsref;
                bool topdfstemp = -1 != sOptions.IndexOf("TOPTEMP");
                bool distinct = -1 != sOptions.IndexOf("DISTINCT");

                string SelectWhat = QlArgsUnescape(QlArgsSelectWhat);
                string[] awhat = null;
                string[] UserColumnNames = null;
                bool WhatFunctions = -1 != SelectWhat.IndexOf('('); // Select clause has functions (aggregates and/or scalars).
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
                    foreach (System.Xml.XmlNode xn in xeTable.SelectNodes("column"))
                    {
                        if (0 != sbRowInfo.Length)
                        {
                            sbRowInfo.Append(',');
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
                        sbRowInfo.Append(TableName + ":" + UserColName); // Note: doesn't consider sub-select.
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
                            c.ColumnName = TableName + ":" + UserColName;
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
                        Shell("dspace exec \"//Job[@Name='RDBMS_SysGen']/IOSettings/DFS_IO/DFSWriter=" + SysGenOutputFile + "\" RDBMS_SysGen.DBCORE \"" + qafile + "\" \"" + RowInfo + "\" \"" + DisplayInfo + "\" \"" + TableName + "\"");
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
                            sbRowInfo.Append(',');
                            sbDisplayInfo.Append(',');
                        }
                        {
                            outputcols.Add(c);
                        }
                        //sbRowInfo.Append(c.ColumnName); // Already includes "TableName:"
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
                            string FuncSelectOutput1 = Shell("dspace exec" + " \"//Job[@Name='RDBMS_Select']/IOSettings/OutputMethod=grouped\"" + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + FuncDfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + FuncDfsOutput + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + RowInfo + "\" \"" + DisplayInfo + "\" \"" + OutputRowInfo + "\" " + FuncArgsOptions).Trim();
                            string[] FuncOutputTypeNames;
                            {
                                const string AOTIBEGINSTRING = "BEGIN:{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}";
                                int aotiBegin = FuncSelectOutput1.IndexOf(AOTIBEGINSTRING);
                                if (-1 == aotiBegin)
                                {
                                    throw new Exception("Function (aggregate and/or scalar) Select MR output invalid (expected begin output type information)");
                                }
                                int aotiEnd = FuncSelectOutput1.IndexOf("{AC596AA3-8E2F-41fa-B9E1-601D92F08AEC}:END");
                                if (aotiEnd < aotiBegin)
                                {
                                    throw new Exception("Function (aggregate and/or scalar)  Select MR output invalid (expected end output type information)");
                                }
                                FuncOutputTypeNames = FuncSelectOutput1.Substring(aotiBegin + AOTIBEGINSTRING.Length, aotiEnd - aotiBegin - AOTIBEGINSTRING.Length).Split(',');
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
                            Shell("dspace exec" + " \"//Job[@Name='RDBMS_Select']/IOSettings/OutputMethod=grouped\"" + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + GByDfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + GByDfsOutput + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + RowInfo + "\" \"" + DisplayInfo + "\" \"" + OutputRowInfo + "\" " + GByArgsOptions).Trim();
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
                            shelloutputSelect1 = Shell("dspace exec" + condxpath + " \"//Job[@Name='RDBMS_Select']/IOSettings/DFSInput=" + DfsInput + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/DFSOutput=" + DfsOutputName + "@" + OutputsRowSize + "\" \"//Job[@Name='RDBMS_Select']/IOSettings/KeyLength=" + KeyLength.ToString() + "\" RDBMS_Select.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + QlArgsNewSelectWhat + "\" " + TopCount.ToString() + " \"" + QlArgsOps + "\" \"" + RowInfo + "\" \"" + DisplayInfo + "\" \"" + OutputRowInfo + "\"").Trim();
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
                            LogFieldInfo(dtfieldindex++, c);
                        }
                    }

                    string sDfsOutputSize = Shell("dspace filesize \"" + DfsOutputName + "\"").Split('\n')[0].Trim();
                    long DfsOutputSize = long.Parse(sDfsOutputSize);
                    long NumRowsOutput = DfsOutputSize / OutputRowSize;
                    if (0 != (DfsOutputSize % OutputRowSize))
                    {
                        throw new Exception("Output file size miscalculation (DfsOutputSize % OutputRowSize) for file: " + DfsOutputName);
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

                    DSpace_Log(Shell("dspace exec \"//Job[@Name='RDBMS_Top']/IOSettings/DFS_IO/DFSReader=" + DfsOutputName + "@" + OutputsRowSize + "\" RDBMS_Top.DBCORE \"" + TableName + "\" \"" + DfsOutputName + "\" \"" + OutputRowInfo + "\" \"" + OutputDisplayInfo + "\" " + TopCount.ToString()).Trim());

                    Shell("dspace del \"" + DfsOutputName + "\"");
                }

            }


            DbColumn GetDbColumn(string name)
            {
                string sdw;
                return GetDbColumn(name, out sdw);
            }

            // If not found, the column and type names are empty, and the size is 0.
            // Type ID is NULL and RowOffset is 0 if a function (aggregate or scalar).
            DbColumn GetDbColumn(string name, out string sdw)
            {
                int ci = DbColumn.IndexOf(cols, name);
                if (-1 != ci)
                {
                    sdw = colswidths[ci];
                    return cols[ci];
                }

                DbColumn c;

                int ip = name.IndexOf('(');
                if (-1 != ip && name.EndsWith(")"))
                {

                    int mintsize = 0;
                    string funcname = name.Substring(0, ip);
                    if (0 == string.Compare("AVG", funcname, true))
                    {
                        mintsize = 1 + 9; // DOUBLE
                    }
                    else if (0 == string.Compare("STD", funcname, true))
                    {
                        mintsize = 1 + 9; // DOUBLE
                    }
                    else if (0 == string.Compare("STD_SAMP", funcname, true))
                    {
                        mintsize = 1 + 9; // DOUBLE
                    }
                    else if (0 == string.Compare("VAR_POP", funcname, true))
                    {
                        mintsize = 1 + 9; // DOUBLE
                    }
                    else if (0 == string.Compare("VAR_SAMP", funcname, true))
                    {
                        mintsize = 1 + 9; // DOUBLE
                    }
                    else if (0 == string.Compare("COUNT", funcname, true))
                    {
                        mintsize = 1 + 8; // LONG
                    }
                    else if (0 == string.Compare("COUNTDISTINCT", funcname, true))
                    {
                        mintsize = 1 + 8; // LONG
                    }

                    int totdw = 0;
                    string pl = name.Substring(ip + 1, name.Length - ip - 1 - 1);
                    int tsize = 1;
                    for (; ; )
                    {
                        string s = Qa.NextPart(ref pl);
                        if (0 == s.Length)
                        {
                            break;
                        }
                        if (s != ",")
                        {
                            int aci = DbColumn.IndexOf(cols, s);
                            if (-1 == aci)
                            {
                                throw new Exception("Column names are required as function (aggregate and/or scalar) arguments, not a valid column name: " + s);
                            }
#if DEBUG
                            if (0 == cols[aci].Type.Size)
                            {
                                throw new Exception("Argument to function cannot be " + cols[aci].ColumnName + " (Type.Size=0)");
                            }
#endif
                            tsize += cols[aci].Type.Size - 1;
                            totdw += int.Parse(colswidths[aci]);
                        }
                    }
                    if (tsize < mintsize)
                    {
                        tsize = mintsize;
                    }
                    // a tsize of (1 + tbasesize) must have an even tbasesize: integral types have even, and char(n) needs even.
                    if (0 != ((tsize - 1) % 2))
                    {
                        tsize++;
                    }
                    c.Type = DbType.Prepare("DbFunction", tsize, DbTypeID.NULL);  //(string TypeName, int TypeSize, DbTypeID TypeID)
                    c.RowOffset = 0;
                    c.ColumnName = name;
                    sdw = totdw.ToString();
                    return c;
                }

                c.Type = DbType.Prepare("", 0, DbTypeID.NULL); //(string TypeName, int TypeSize, DbTypeID TypeID)
                c.RowOffset = 0;
                c.ColumnName = string.Empty;
                sdw = "0";
                return c;

            }


            void LogFieldInfo(int fieldindex, DbColumn c)
            {
                string cstype = QATypeToCSType(c.Type.Name);
                DSpace_Log("    <field index=\"" + fieldindex.ToString() + "\" name=\"" + CleanColumnName(c.ColumnName) + "\" qatype=\"" + c.Type.Name + "\" cstype=\"" + cstype + "\" frontbytes=\"1\" bytes=\"" + (c.Type.Size - 1).ToString() + "\" backbytes=\"0\" />");
            }


            string QATypeToCSType(string qatype)
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
                for (int i = 0; i < cn.Length; i++)
                {
                    if (':' == cn[i])
                    {
                        return cn.Substring(i + 1);
                    }
                    if ('\'' == cn[i])
                    {
                        break;
                    }
                }
                return cn;
            }


        }

    }
}
