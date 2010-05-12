using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class QueryAnalyzer : Local
        {
            protected override void Run()
            {
                string args = QlArgsUnescape(DSpace_ExecArgs);

                //DSpace_Log("DEBUG:  args: " + args);

                try
                {
                    QaExec(args);
                }
                finally
                {
                    dfsclientClose();
                }
            }


            class QaExecException : Exception
            {
                public QaExecException(string msg, Exception innerException)
                    : base(msg, innerException)
                {
                }

                public QaExecException(string msg)
                    : base(msg)
                {
                }
            }


            protected virtual RemoteCall GetRemoteCallBlankTable()
            {
                //return new RemoteCallShellExec("RDBMS_BlankTable", "RDBMS_BlankTable.DBCORE");
                return new RemoteCallInProc("RDBMS_BlankTable", new Remote());
            }


            public void QaExec(string args)
            {
                if (!QlSelect(args)
                    && !QlInsertInto(args)
                    && !QlCreateTable(args)
                    && !QlBatchExecute(args)
                    && !QlShell(args)
                    && !QlSysTablesXml(args)
                    && !QlSysTablesXmlFile(args)
                    && !QlDropTable(args)
                    && !QlTruncateTable(args)
                    && !QlUpdate(args)
                    && !QlDelete(args)
                    )
                {
                    throw new QaExecException("Unknown query: " + args);
                }
            }

            string _QlInSelect(string args, bool ForceDfsTemp, bool DisallowTop, bool Update)
            {
                return _QlInSelect(args, ForceDfsTemp, DisallowTop, Update, false);
            }

            struct JoinInfo
            {
                internal string type; // "INNER"
                internal string table; // Join on this table.
                internal string on; // "ON" clause.
            }

            // args starts after SELECT
            // If Update, implies dfstemp and does grouped job! select.dbcore can select cols to update and set new values.
            string _QlInSelect(string args, bool ForceDfsTemp, bool DisallowTop, bool Update, bool TopDfsTemp)
            {
#if DEBUG
                if (Update && !ForceDfsTemp)
                {
                    throw new Exception("DEBUG:  (Update && !ForceDfsTemp)");
                }
#endif
                string SelectWhat = null;
                bool dfstemp = ForceDfsTemp;
                bool dfsref = false;
                long TopCount = -1;
                string TableName = "";
                string Ops = "*";
                bool GroupBy = false;
                bool OrderBy = false;
                bool distinct = false;
                List<JoinInfo> joins = null;
                //string AfterUnion = null; // e.g. '[ALL] SELECT ...' ; can end with ';'
#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif
                for (string op = Qa.NextPart(ref args);
                    0 != op.Length && ";" != op;
                    op = Qa.NextPart(ref args))
                {
                    if (0 == string.Compare("DFSTEMP", op, true))
                    {
                        if (Update)
                        {
                            throw new Exception("User: Cannot use DFSTEMP with this operation");
                        }
                        dfstemp = true;
                    }
                    else if (0 == string.Compare("DFSREF", op, true))
                    {
                        if (Update)
                        {
                            throw new Exception("User: Cannot use DFSREF with this operation");
                        }
                        dfsref = true;
                    }
                    else if (0 == string.Compare("TOP", op, true))
                    {
                        if (DisallowTop)
                        {
                            throw new Exception("TOP cannot be used with this operation at this time");
                        }
                        TopCount = long.Parse(Qa.NextPart(ref args));
                    }
                    else if (0 == string.Compare("DISTINCT", op, true))
                    {
                        distinct = true;
                    }
                    else if (0 == string.Compare("FROM", op, true))
                    {
                        if (null == SelectWhat)
                        {
                            throw new Exception("Expected which columns to select, not FROM");
                        }
                        TableName = Qa.NextPart(ref args);
                        if (TableName == "(")
                        {
                            TableName = "";
                            for (; ; )
                            {
                                if (0 != string.Compare("SELECT", Qa.NextPart(ref args), true))
                                {
                                    throw new Exception("Expected SELECT for sub select");
                                }
                                if ("*" != Qa.NextPart(ref args))
                                {
                                    throw new Exception("Expected selecting of * in sub select");
                                }
                                if (0 != string.Compare("FROM", Qa.NextPart(ref args), true))
                                {
                                    throw new Exception("Expected FROM in sub select");
                                }
                                string ttn = Qa.NextPart(ref args);
                                if (0 == TableName.Length)
                                {
                                    TableName = ttn;
                                }
                                else
                                {
                                    TableName += "\0" + ttn;
                                }
                                string s = Qa.NextPart(ref args);
                                if (0 == string.Compare("UNION", s, true))
                                {
                                    // If not ALL, could set a flag for sOptions.
                                    if (0 != string.Compare("ALL", Qa.NextPart(ref args), true))
                                    {
                                        throw new Exception("UNION not supported; expected UNION ALL");
                                    }
                                    continue;
                                }
                                else if (")" == s)
                                {
                                    break;
                                }
                                else
                                {
                                    throw new Exception("Expected ) for sub select");
                                }

                            }

                        }
                    }
                    else if (0 == string.Compare("WHERE", op, true)
                        || 0 == string.Compare("D.WHERE", op, true)
                        || 0 == string.Compare("ORDER", op, true)
                        || 0 == string.Compare("SET", op, true)
                        || 0 == string.Compare("GROUP", op, true)
                        || 0 == string.Compare("INNER", op, true)
                        || 0 == string.Compare("LEFT", op, true)
                        || 0 == string.Compare("RIGHT", op, true)
                        )
                    {
                        {
                            StringBuilder sbOps = new StringBuilder();
                            for (string s = op; ; s = Qa.NextPart(ref args))
                            {
                                if (0 == s.Length || ";" == s)
                                {
                                    break;
                                }
                                if (0 != sbOps.Length)
                                {
                                    sbOps.Append('\0');
                                }
                                if (0 == string.Compare("UNION", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("ALL", xs, true)
                                        || 0 == string.Compare("SELECT", xs, true))
                                    {
                                        // It's a UNION and not something else (like a table named union).
                                        //AfterUnion = args;
                                        //args = "";
                                        //break;
                                        throw new Exception("Invalid placement of UNION");
                                    }
                                }
                                else if (0 == string.Compare("GROUP", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("BY", xs, true))
                                    {
                                        GroupBy = true;
                                    }
                                }
                                else if (0 == string.Compare("ORDER", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("BY", xs, true))
                                    {
                                        if (distinct)
                                        {
                                            throw new Exception("ORDER BY is not supported with SELECT DISTINCT");
                                        }
                                        OrderBy = true;
                                    }
                                }
                                else if (0 == string.Compare("INNER", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("JOIN", xs, true))
                                    {
                                        args = x;
                                        JoinInfo ji = _ParseJoin("INNER", ref args);
                                        if (null == joins)
                                        {
                                            joins = new List<JoinInfo>();
                                        }
                                        joins.Add(ji);
                                        continue;
                                    }
                                }
                                else if (0 == string.Compare("LEFT", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("OUTER", xs, true))
                                    {
                                        args = x;
                                        s = Qa.NextPart(ref args);
                                        if (0 == string.Compare("JOIN", s, true))
                                        {
                                            JoinInfo ji = _ParseJoin("LEFT_OUTER", ref args);
                                            if (null == joins)
                                            {
                                                joins = new List<JoinInfo>();
                                            }
                                            joins.Add(ji);
                                        }
                                        else
                                        {
                                            throw new Exception("Expected JOIN after LEFT OUTER, not " + s);
                                        }
                                        continue;
                                    }
                                    else if (0 == string.Compare("JOIN", xs, true))
                                    {
                                        throw new Exception("Expected OUTER between LEFT and JOIN");
                                    }
                                }
                                else if (0 == string.Compare("RIGHT", s, true))
                                {
                                    string x = args;
                                    string xs = Qa.NextPart(ref x);
                                    if (0 == string.Compare("OUTER", xs, true))
                                    {
                                        args = x;
                                        s = Qa.NextPart(ref args);
                                        if (0 == string.Compare("JOIN", s, true))
                                        {
                                            JoinInfo ji = _ParseJoin("RIGHT_OUTER", ref args);
                                            if (null == joins)
                                            {
                                                joins = new List<JoinInfo>();
                                            }
                                            joins.Add(ji);
                                        }
                                        else
                                        {
                                            throw new Exception("Expected JOIN after RIGHT OUTER, not " + s);
                                        }
                                        continue;
                                    }
                                    else if (0 == string.Compare("JOIN", xs, true))
                                    {
                                        throw new Exception("Expected OUTER between RIGHT and JOIN");
                                    }
                                }
                                sbOps.Append(s);
                            }
                            if (sbOps.Length > 0)
                            {
                                Ops = sbOps.ToString();
                            }
                        }
                    }
                    else if (0 == string.Compare("UNION", op, true))
                    {
                        //AfterUnion = args;
                        //args = "";
                        //break;
                        throw new Exception("Invalid placement of UNION");
                    }
                    else
                    {
                        if ("-" == op || "+" == op)
                        {
                            op += Qa.NextPart(ref args);
                        }
                        if (null != SelectWhat)
                        {
                            throw new Exception("Unexpected: " + op);
                        }
                        StringBuilder swsb = new StringBuilder();
                        swsb.Append(op);
                        for (; ; )
                        {
                            string xargs = args;
                            string np = Qa.NextPart(ref xargs);
                            if ("," == np)
                            {
                                args = xargs; // Keep.
                                string swx = Qa.NextPart(ref args);
                                if ("-" == swx || "+" == swx)
                                {
                                    swx += Qa.NextPart(ref args);
                                }
                                if (0 == swx.Length)
                                {
                                    throw new Exception("Unexpected ,");
                                }
                                swsb.Append('\0');
                                swsb.Append(swx);
                            }
                            else if ("(" == np)
                            {
                                args = xargs; // Keep.
                                swsb.Append('(');
                                bool prevident = false;
                                int nparens = 1;
                                for (; ; )
                                {
                                    string slp = Qa.NextPart(ref args);
                                    if (0 == slp.Length)
                                    {
                                        throw new Exception("Expected )");
                                    }
                                    bool thisident = (char.IsLetterOrDigit(slp[0]) || '_' == slp[0]);
                                    if (prevident && thisident)
                                    {
                                        swsb.Append(' ');
                                    }
                                    prevident = thisident;
                                    swsb.Append(slp);
                                    if (")" == slp)
                                    {
                                        if (--nparens == 0)
                                        {
                                            break;
                                        }
                                    }
                                    else if ("(" == slp)
                                    {
                                        nparens++;
                                    }
                                }
                            }
                            else if (0 == string.Compare("AS", np, true))
                            {
                                args = xargs; // Keep.
                                string asname = Qa.NextPart(ref args);
                                if (0 == asname.Length)
                                {
                                    throw new Exception("Expected new column name after AS");
                                }
                                // TO-DO: validate asname.
                                swsb.Append(" AS ");
                                swsb.Append(asname);
                            }
                            else
                            {
                                break;
                            }
                        }
                        SelectWhat = swsb.ToString();
                    }
                }

                if (string.IsNullOrEmpty(TableName))
                {
                    throw new Exception("Expected FROM <table_name>");
                }
                if (string.IsNullOrEmpty(SelectWhat))
                {
                    throw new Exception("Expected which columns to select");
                }

                if (0 == string.Compare(TableName, "sys.help", true))
                {
                    if (null != joins)
                    {
                        throw new Exception("Cannot JOIN with sys.help");
                    }
                    return _QlSysHelp(dfstemp);
                }
                else if (0 == string.Compare(TableName, "sys.indexes", true))
                {
                    if (null != joins)
                    {
                        throw new Exception("Cannot JOIN with sys.indexes");
                    }
                    return _QlSysIndexes(dfstemp);
                }

                string DeleteDfsInputFile = null; // Delete this input file after the input has been used.
                bool joined = false;
                try
                {

                    if (null != joins)
                    {
                        foreach (JoinInfo ji in joins)
                        {
#if DEBUG
                            //System.Diagnostics.Debugger.Launch();
#endif

                            string joinonresult = (new PrepareJoinOn()).Exec(
                                QlArgsEscape(TableName),
                                ji.type,
                                QlArgsEscape(ji.table),
                                QlArgsEscape(ji.on)
                                );
                            PrepareSelect.queryresults qr = PrepareSelect.GetQueryResults(joinonresult);

                            if (null != DeleteDfsInputFile)
                            {
                                try
                                {
                                    dfsclient.DeleteFile(DeleteDfsInputFile);
                                }
                                catch
                                {
                                }
                                DeleteDfsInputFile = null;
                            }
                            DeleteDfsInputFile = qr.temptable;

                            TableName = qr.GetPseudoTableName();

                        }
                        joined = true;
                    }

                    string _dontn = SafeTextDfsPath(TableName.Replace('\0', '-'));
                    if (_dontn.Length > 100)
                    {
                        _dontn = _dontn.Substring(0, 100);
                    }
                    string DfsOutputName = "dfs://RDBMS_Select_" + _dontn + "{" + Guid.NewGuid().ToString() + "}";
                    if (dfstemp)
                    {
                        DfsOutputName += DFS_TEMP_FILE_MARKER;
                    }
                    else if (dfsref)
                    {
                        DfsOutputName = "<dfsref>";
                    }

                    if (dfstemp && dfsref)
                    {
                        throw new Exception("Cannot have DFSTEMP and DFSREF");
                    }

                    string sOptions = "";
                    if (dfstemp)
                    {
                        sOptions += ";DFSTEMP";
                        if (TopDfsTemp && TopCount > -1)
                        {
                            sOptions += ";TOPTEMP";
                        }
                    }
                    if (dfsref)
                    {
                        sOptions += ";DFSREF";
                    }
                    if (Update)
                    {
                        // Grouped update!
                        sOptions += ";GUPDATE;DFSTEMP";
                    }
                    if (GroupBy)
                    {
                        sOptions += ";GBY";
                    }
                    if (OrderBy)
                    {
                        sOptions += ";OBY";
                    }
                    if (distinct)
                    {
                        sOptions += ";DISTINCT";
                    }
                    if (joined)
                    {
                        sOptions += ";JOINED";
                    }
                    if (0 == sOptions.Length)
                    {
                        sOptions = "-";
                    }
                    return (new PrepareSelect()).Exec(QlArgsEscape(TableName), DfsOutputName, QlArgsEscape(SelectWhat), TopCount.ToString(), QlArgsEscape(Ops), sOptions);

                }
                finally
                {
                    if (null != DeleteDfsInputFile)
                    {
                        try
                        {
                            dfsclient.DeleteFile(DeleteDfsInputFile);
                        }
                        catch
                        {
                        }
                        DeleteDfsInputFile = null;
                    }
                }

            }

            JoinInfo _ParseJoin(string stype, ref string AfterJOIN)
            {
                string args = AfterJOIN;
                JoinInfo ji;
                ji.type = stype;
                ji.table = Qa.NextPart(ref args);
                if (string.IsNullOrEmpty(ji.table))
                {
                    throw new Exception("Table name expected for JOIN");
                }
                if (0 != string.Compare("ON", Qa.NextPart(ref args), true))
                {
                    throw new Exception("ON expected for JOIN " + ji.table);
                }
                {
                    string on1, onop, on2;
                    on1 = Qa.NextPart(ref args);
                    onop = Qa.NextPart(ref args);
                    on2 = Qa.NextPart(ref args);
                    if (0 == on1.Length
                        || 0 == onop.Length
                        || 0 == on2.Length)
                    {
                        throw new Exception("Invalid ON expression for JOIN " + ji.table);
                    }
                    if ("=" != onop)
                    {
                        throw new NotImplementedException("ON expression must compare equality with =");
                    }
                    ji.on = on1 + " " + onop + " " + on2;
                }
                AfterJOIN = args;
                return ji;
            }


            bool QlUpdate(string args)
            {
                if (!(0 == string.Compare("UPDATE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string TableName = Qa.NextPart(ref args);
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }

                switch (Qa.NextPart(ref args).ToUpper())
                {
                    case "SET":
                        {

                            System.Xml.XmlDocument systables;
                            using (GlobalCriticalSection.GetLock())
                            {
                                systables = LoadSysTables_unlocked();
                            }

                            System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                            if (null == xeTable)
                            {
                                throw new Exception("Table '" + TableName + "' does not exist");
                            }

                            string inselargs = "^ FROM " + TableName + " SET " + args;
                            string outputSelect1 = _QlInSelect(inselargs, true, true, true); // ForceDfsTemp=true, DisallowTop=true, Update=true
                            System.Xml.XmlDocument seloutxd = new System.Xml.XmlDocument();
                            // queryresults < temptable, recordcount, recordsize, parts, field"index, name, qatype, cstype, frontbytes, bytes, backbytes" >
                            seloutxd.LoadXml(outputSelect1);
                            System.Xml.XmlElement queryresults = seloutxd["queryresults"];

                            string DfsOutputName = queryresults["temptable"].InnerText;
                            string UpdateDfsName = xeTable["file"].InnerText;

                            // Do NOT make oldtablefn a temp (killall) file, because DfsOutputName is, and we don't want to lose both. 
                            string oldtablefn = UpdateDfsName + "_updating_" + Guid.NewGuid().ToString();
                            Shell("dspace rename \"" + UpdateDfsName + "\" \"" + oldtablefn + "\"");
                            Shell("dspace rename \"" + DfsOutputName + "\" \"" + UpdateDfsName + "\"");
                            Shell("dspace del \"" + oldtablefn + "\"");

                        }
                        break;

                    default:
                        throw new Exception("Expected SET after UPDATE <table_name>");
                }

                return true;
            }


            bool QlDelete(string args)
            {
                if (!(0 == string.Compare("DELETE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                if (0 != string.Compare("FROM", Qa.NextPart(ref args), true))
                {
                    throw new Exception("Expected FROM after DELETE");
                }

                string TableName = Qa.NextPart(ref args);
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }

                switch (Qa.NextPart(ref args).ToUpper())
                {
                    case "WHERE":
                        {

                            System.Xml.XmlDocument systables;
                            using (GlobalCriticalSection.GetLock())
                            {
                                systables = LoadSysTables_unlocked();
                            }

                            System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                            if (null == xeTable)
                            {
                                throw new Exception("Table '" + TableName + "' does not exist");
                            }

                            string inselargs = "^ FROM " + TableName + " D.WHERE " + args;
                            string outputSelect1 = _QlInSelect(inselargs, true, true, true); // ForceDfsTemp=true, DisallowTop=true, Update=true
                            System.Xml.XmlDocument seloutxd = new System.Xml.XmlDocument();
                            // queryresults < temptable, recordcount, recordsize, parts, field"index, name, qatype, cstype, frontbytes, bytes, backbytes" >
                            seloutxd.LoadXml(outputSelect1);
                            System.Xml.XmlElement queryresults = seloutxd["queryresults"];

                            string DfsOutputName = queryresults["temptable"].InnerText;
                            string UpdateDfsName = xeTable["file"].InnerText;

                            // Do NOT make oldtablefn a temp (killall) file, because DfsOutputName is, and we don't want to lose both. 
                            string oldtablefn = UpdateDfsName + "_deleting_" + Guid.NewGuid().ToString();
                            Shell("dspace rename \"" + UpdateDfsName + "\" \"" + oldtablefn + "\"");
                            Shell("dspace rename \"" + DfsOutputName + "\" \"" + UpdateDfsName + "\"");
                            Shell("dspace del \"" + oldtablefn + "\"");

                        }
                        break;

                    default:
                        throw new Exception("Expected WHERE after DELETE FROM <table_name>");
                }

                return true;
            }


            bool QlSelect(string args)
            {
                if (!(0 == string.Compare("SELECT", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                DSpace_Log(_QlInSelect(args, false, false, false).Trim()); // ForceDfsTemp=false, DisallowTop=false, Update=false

                return true;
            }


            bool QlInsertInto(string args)
            {
                if (!(0 == string.Compare("INSERT", Qa.NextPart(ref args), true)
                    && 0 == string.Compare("INTO", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string TableName = Qa.NextPart(ref args);
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }

                switch (Qa.NextPart(ref args).ToUpper())
                {

                    case "(":
                        throw new Exception("Expected VALUES");

                    case "VALUES":
                        {
                            string Vals;
                            {
                                StringBuilder sbVals = new StringBuilder();
                                for (; ; )
                                {
                                    string s = Qa.NextPart(ref args);
                                    if (0 == s.Length || ";" == s)
                                    {
                                        break;
                                    }
                                    if (0 != sbVals.Length)
                                    {
                                        sbVals.Append('\0');
                                    }
                                    sbVals.Append(s);
                                }
                                Vals = sbVals.ToString();
                            }
                            string OutputDfsFile = "dfs://RDBMS_InsertValues_" + TableName + "{" + Guid.NewGuid().ToString() + "}";
                            DSpace_Log((new PrepareInsertValues()).Exec(TableName, OutputDfsFile, QlArgsEscape(Vals)).Trim());
                        }
                        break;

                    case "SELECT":
                        {

                            System.Xml.XmlDocument systables;
                            using (GlobalCriticalSection.GetLock())
                            {
                                systables = LoadSysTables_unlocked();
                            }

                            System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                            if (null == xeTable)
                            {
                                throw new Exception("Table '" + TableName + "' does not exist");
                            }

                            string outputSelect1 = _QlInSelect(args, true, false, false, true); // ForceDfsTemp=true, DisallowTop=false, Update=false, TopDfsTemp=true
                            System.Xml.XmlDocument seloutxd = new System.Xml.XmlDocument();
                            // queryresults < temptable, recordcount, recordsize, parts, field"index, name, qatype, cstype, frontbytes, bytes, backbytes" >
                            seloutxd.LoadXml(outputSelect1);
                            System.Xml.XmlElement queryresults = seloutxd["queryresults"];

                            // Ensure tables have the same column types!
                            bool typesmatch = false;
                            if (queryresults["recordsize"].InnerText == xeTable["size"].InnerText)
                            {
                                System.Xml.XmlNodeList InsertTableCols = xeTable.SelectNodes("column");
                                System.Xml.XmlNodeList ResultsTableCols = queryresults.SelectNodes("field");
                                if (InsertTableCols.Count == ResultsTableCols.Count)
                                {
                                    int numcols = InsertTableCols.Count;
                                    bool fail = false;
                                    for (int it = 0; it < numcols; it++)
                                    {
                                        System.Xml.XmlNode xeInsert = InsertTableCols[it];
                                        System.Xml.XmlNode xeResults = ResultsTableCols[it];
                                        if (xeInsert["type"].InnerText != xeResults.Attributes["qatype"].Value)
                                        {
                                            fail = true;
                                            break;
                                        }
                                    }
                                    typesmatch = !fail;
                                }
                            }
                            if (!typesmatch)
                            {
                                StringBuilder sbexpect = new StringBuilder();
                                StringBuilder sbunexp = new StringBuilder();
                                try
                                {
                                    {
                                        System.Xml.XmlNodeList InsertTableCols = xeTable.SelectNodes("column");
                                        for (int it = 0; it < InsertTableCols.Count; it++)
                                        {
                                            System.Xml.XmlNode xeInsert = InsertTableCols[it];
                                            if (0 != it)
                                            {
                                                sbexpect.Append(',');
                                            }
                                            sbexpect.Append(xeInsert["type"].InnerText);
                                        }
                                    }
                                    {
                                        System.Xml.XmlNodeList ResultsTableCols = queryresults.SelectNodes("field");
                                        for (int it = 0; it < ResultsTableCols.Count; it++)
                                        {
                                            System.Xml.XmlNode xeResults = ResultsTableCols[it];
                                            if (0 != it)
                                            {
                                                sbunexp.Append(',');
                                            }
                                            sbunexp.Append(xeResults.Attributes["qatype"].Value);
                                        }
                                    }
                                }
                                catch
                                {
                                }
                                throw new Exception("Column types do not match",
                                    new InvalidOperationException("Record mismatch; expected "
                                        + xeTable["size"].InnerText + " byte records of " + sbexpect.ToString()
                                        + ", not " + queryresults["recordsize"].InnerText + " byte records of " + sbunexp.ToString()));
                            }

                            string InsertDfsName = xeTable["file"].InnerText;
                            string DfsOutputName = queryresults["temptable"].InnerText;

                            Shell("dspace combine \"" + DfsOutputName + "\" \"" + InsertDfsName + "\"");

                        }
                        break;

                    case "BIND":
                        {
                            string BindDfsFile;
                            {
                                string qidf = Qa.NextPart(ref args);
                                if (0 == qidf.Length)
                                {
                                    throw new Exception("Expected bind dfs file name");
                                }
                                if (qidf.Length <= 2 || qidf[0] != '\'' || qidf[qidf.Length - 1] != '\'')
                                {
                                    throw new Exception("Expected single-quoted MR.DFS file name to bind");
                                }
                                BindDfsFile = qidf.Substring(1, qidf.Length - 2).Replace("''", "'");
                            }
                            if (BindDfsFile.StartsWith("RDBMS_", true, null) || BindDfsFile.StartsWith("dfs://RDBMS_", true, null))
                            {
                                throw new Exception("Cannot bind a RDBMS file into a table: " + BindDfsFile);
                            }
                            try
                            {
                                Shell("dspace combine \"" + BindDfsFile + "\" \"RDBMS_Table_" + TableName + "\"");
                            }
                            catch (Exception e)
                            {
                                System.Xml.XmlDocument systables;
                                using (GlobalCriticalSection.GetLock())
                                {
                                    systables = LoadSysTables_unlocked();
                                }

                                System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                                if (null == xeTable)
                                {
                                    throw new Exception("Table '" + TableName + "' does not exist");
                                }

                                if (xeTable["file"].InnerText.StartsWith("qa://", true, null))
                                {
                                    throw new Exception("Cannot BIND into a system table: " + xeTable["name"].InnerText);
                                }

                                int BindDfsFileRecordLength = GetDfsFileRecordLength(BindDfsFile);
                                if (BindDfsFileRecordLength < 1)
                                {
                                    throw new Exception("Bind file '" + BindDfsFile + "' not found or has incorrect type");
                                }

                                int ExpectedBindRowLength = int.Parse(xeTable["size"].InnerText);
                                if (BindDfsFileRecordLength != ExpectedBindRowLength)
                                {
                                    throw new Exception("Record length mismatch of bind file '" + BindDfsFile + "', expected " + ExpectedBindRowLength.ToString() + " byte records");
                                }

                                throw;

                            }
                        }
                        break;

                    case "IMPORT":
                        {
                            // m/r over import dfs file which has record size not including Nullable bytes.
                            string ImportDfsFile;
                            {
                                string qidf = Qa.NextPart(ref args);
                                if (0 == qidf.Length)
                                {
                                    throw new Exception("Expected import dfs file name");
                                }
                                if (qidf.Length <= 2 || qidf[0] != '\'' || qidf[qidf.Length - 1] != '\'')
                                {
                                    throw new Exception("Expected single-quoted MR.DFS file name to import");
                                }
                                ImportDfsFile = qidf.Substring(1, qidf.Length - 2).Replace("''", "'");
                            }
                            string OutputDfsFile = "dfs://RDBMS_Import_" + TableName + "{" + Guid.NewGuid().ToString() + "}";
                            DSpace_Log((new PrepareImport()).Exec(TableName, ImportDfsFile, OutputDfsFile).Trim());
                        }
                        break;

                    case "IMPORTLINES":
                        {
                            string ImportDfsFile;
                            {
                                string qidf = Qa.NextPart(ref args);
                                if (0 == qidf.Length)
                                {
                                    throw new Exception("Expected import dfs file name");
                                }
                                if (qidf.Length <= 2 || qidf[0] != '\'' || qidf[qidf.Length - 1] != '\'')
                                {
                                    throw new Exception("Expected single-quoted MR.DFS file name to import");
                                }
                                ImportDfsFile = qidf.Substring(1, qidf.Length - 2).Replace("''", "'");
                            }
                            string OutputDfsFile = "dfs://RDBMS_ImportLines_" + TableName + "{" + Guid.NewGuid().ToString() + "}";
                            string StrDelim = "','";
                            if ("DELIMITER" == Qa.NextPart(ref args))
                            {
                                StrDelim = Qa.NextPart(ref args);
                            }
                            if (StrDelim.Length <= 2 || StrDelim[0] != '\'' || StrDelim[StrDelim.Length - 1] != '\'')
                            {
                                throw new Exception("Invalid delimiter, expected one character string");
                            }
                            DSpace_Log((new PrepareImportLines()).Exec(TableName, ImportDfsFile, OutputDfsFile, QlArgsEscape(StrDelim)).Trim());
                        }
                        break;

                    default:
                        throw new Exception("INSERT INTO error, expected VALUES, IMPORT or IMPORTLINES");
                }

                return true;
            }


            bool QlDropTable(string args)
            {
                int PinForSecs = 0;
                bool PinForEnabled = RDBMS_DBCORE.Qa.HasPinFor(args, ref PinForSecs, ref args);

                if (!(0 == string.Compare("DROP", Qa.NextPart(ref args), true)
                    && 0 == string.Compare("TABLE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string TableName = Qa.NextPart(ref args);
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }

                System.Xml.XmlDocument systables;
                using (GlobalCriticalSection.GetLock())
                {
                    systables = LoadSysTables_unlocked();
                }

                System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                if (null == xeTable)
                {
                    throw new Exception("Table '" + TableName + "' does not exist");
                }

                string DfsTableFile = xeTable["file"].InnerText;
                if (!DfsTableFile.StartsWith("dfs://", true, null))
                {
                    throw new Exception("This table cannot be deleted");
                }

                string DfsTableFileTemp = null;
                string bulkgetlistfilepath = null;
                if (PinForEnabled)
                {
                    DfsTableFileTemp = DfsTableFile + "_Drop_" + Guid.NewGuid().ToString() + DFS_TEMP_FILE_MARKER;
                    bulkgetlistfilepath = IOUtils.GetTempDirectory() + @"\BgDrop_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                }

                using (GlobalCriticalSection.GetLock())
                {
                    if (PinForEnabled)
                    {
                        Shell("dspace bulkget \"" + bulkgetlistfilepath + "\" \"" + DfsTableFile + "\"");
                    }

                    systables = LoadSysTables_unlocked(); // Reload in case of intermediate changes.
                    xeTable = FindTable(systables, TableName);
                    if (null != xeTable)
                    {
                        xeTable.ParentNode.RemoveChild(xeTable);
                        UpdateSysTables_unlocked(systables);
                    }

                    if (PinForEnabled)
                    {
                        Shell("dspace rename \"" + DfsTableFile + "\" \"" + DfsTableFileTemp + "\"");
                    }
                }

                string KeepFileName = null;
                if (PinForEnabled)
                {
                    string FirstPartName;
                    using (System.IO.StreamReader sr = new System.IO.StreamReader(bulkgetlistfilepath))
                    {
                        string bpline = sr.ReadLine();
                        if (string.IsNullOrEmpty(bpline))
                        {
                            // The table is empty...
                            FirstPartName = null;
                        }
                        else
                        {
                            string[] bparts = bpline.Split(' ');
                            FirstPartName = bparts[1];
                        }
                    }
                    System.IO.File.Delete(bulkgetlistfilepath);
                    if (null != FirstPartName)
                    {
                        KeepFileName = FirstPartName + ".keep";
                    }
                }

                if (null != KeepFileName)
                {
#if DEBUG
                    if (PinForSecs < 1)
                    {
                        throw new Exception("DEBUG:  (null != KeepFileName) && (PinForSecs < 1)");
                    }
#endif

                    // Gather hosts and MR.DFS directories...
                    //List<string> chosts;
                    List<string> cpaths;
                    {
                        string htnppartsoutput = Shell("dspace slaveinstalls -healthy");
                        {
                            string[] lines = htnppartsoutput
                                .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            //chosts = new List<string>(lines.Length);
                            cpaths = new List<string>(lines.Length);
                            for (int ip = 0; ip < lines.Length; ip++)
                            {
                                string line = lines[ip];
                                int isp = line.IndexOf(' ');
                                //string host = line.Substring(0, isp);
                                string netpath = line.Substring(isp + 1);
                                //chosts.Add(host);
                                cpaths.Add(netpath);
                            }
                        }
                    }

                    // Wait a little bit to give another client a chance to write a Keeper file after their successful BulkGet.
                    System.Threading.Thread.Sleep(1000 * 3);

                    // Check for Keepers!
                    int cpathsCount = cpaths.Count;
                    for (bool keepchecking = true; ; )
                    {
                        keepchecking = false;
                        int ic = 0;
                        for (; ic < cpathsCount; ic++)
                        {
                            string KeepFilePath = cpaths[ic] + @"\" + KeepFileName;
                            try
                            {
                                if (System.IO.File.Exists(KeepFilePath))
                                {
                                    keepchecking = true;
                                    break;
                                }
                            }
                            catch
                            {
                            }
                        }
                        if (!keepchecking)
                        {
                            break;
                        }
                        // Keep file(s) present; delete 'em!..
                        for (; ic < cpathsCount; ic++)
                        {
                            string KeepFilePath = cpaths[ic] + @"\" + KeepFileName;
                            try
                            {
                                System.IO.File.Delete(KeepFilePath);
                            }
                            catch
                            {
                            }
                        }
                        System.Threading.Thread.Sleep(1000 * PinForSecs);
                    }

                }

                if (PinForEnabled)
                {
                    try
                    {
                        dfsclient.DeleteFile(DfsTableFileTemp);
                    }
                    catch
                    {
                    }
                }
                else
                {
                    try
                    {
                        dfsclient.DeleteFile(DfsTableFile);
                    }
                    catch
                    {
                    }
                }

                return true;
            }


            bool QlTruncateTable(string args)
            {
                if (!(0 == string.Compare("TRUNCATE", Qa.NextPart(ref args), true)
                    && 0 == string.Compare("TABLE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string TableName = Qa.NextPart(ref args);
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }

                System.Xml.XmlDocument systables;
                using (GlobalCriticalSection.GetLock())
                {
                    systables = LoadSysTables_unlocked();
                }

                System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                if (null == xeTable)
                {
                    throw new Exception("Table '" + TableName + "' does not exist");
                }

                string DfsTableFile = xeTable["file"].InnerText;
                if (!DfsTableFile.StartsWith("dfs://", true, null))
                {
                    throw new Exception("This table cannot be truncated");
                }
                string sRowSize = xeTable["size"].InnerText;

                try
                {
                    dfsclient.DeleteFile(DfsTableFile);
                }
                catch
                {
                }
                {
                    RemoteCall rc = GetRemoteCallBlankTable();
                    rc.OverrideOutput = DfsTableFile + "@" + sRowSize;
                    rc.Call();
                }

                return true;
            }


            bool QlCreateTable(string args)
            {
                if (!(0 == string.Compare("CREATE", Qa.NextPart(ref args), true)
                    && 0 == string.Compare("TABLE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string TableName = Qa.NextPart(ref args); // To-do: validate table name.
                if (0 == TableName.Length)
                {
                    throw new Exception("Table name expected");
                }
                if (TableName.Length > 100)
                {
                    throw new Exception("Table name too long; cannot be longer than 100 characters");
                }

                List<ColInfo> colinfo = new List<ColInfo>();
                {
                    string scols = args.Trim();
                    if (!scols.StartsWith("("))
                    {
                        throw new Exception("Column information expected for new table " + TableName);
                    }
                    {
                        int ilrp = scols.LastIndexOf(')');
                        if (-1 == ilrp)
                        {
                            throw new Exception("Expected ) after table columns");
                        }
                        scols = scols.Substring(1, ilrp - 1);
                    }
                    foreach (string _col in scols.Split(','))
                    {
                        string col = _col;
                        string colname = Qa.NextPart(ref col); // To-do: validate column name.
                        if (0 == colname.Length)
                        {
                            throw new Exception("Column name expected");
                        }
                        string coltype = col.Trim(); // To-do: validate type name.
                        if (0 == coltype.Length)
                        {
                            throw new Exception("Type expected for column '" + colname + "'");
                        }
                        {
                            ColInfo ci;
                            ci.Name = colname;
                            ci.Type = coltype;
                            colinfo.Add(ci);
                        }
                    }
                }

                int totsize = 0; // Size of a full record.
                using (GlobalCriticalSection.GetLock())
                {
                    System.Xml.XmlDocument systables = null;
                    try
                    {
                        systables = LoadSysTables_unlocked();
                    }
                    catch (System.IO.FileNotFoundException e)
                    {
                        // No tables exist yet.
                        systables = new System.Xml.XmlDocument();
                        systables.LoadXml(@"<tables></tables>");
                    }

                    {
                        if (null != FindTable(systables, TableName))
                        {
                            throw new Exception("Table with name '" + TableName + "' already exists");
                        }
                    }

                    System.Xml.XmlElement xeTables = (System.Xml.XmlElement)systables.SelectSingleNode("tables");

                    {
                        System.Xml.XmlElement xeNewTable = systables.CreateElement("table");

                        xeNewTable.AppendChild(NewElement(systables, "name", TableName));
                        xeNewTable.AppendChild(NewElement(systables, "file", "dfs://RDBMS_Table_" + TableName)); // Rectangular binary file name in DFS.
                        checked
                        {
                            foreach (ColInfo ci in colinfo)
                            {
                                string cleantype;
                                int tsize; // Size of type.
                                int dw; // Display width.
                                string justify;
                                if (ci.Type.StartsWith("char", true, null))
                                {
                                    justify = CharNJustify();
                                    string sz = ci.Type.Substring(4).Trim();
                                    if (!sz.StartsWith("(") || !sz.EndsWith(")"))
                                    {
                                        throw new Exception("Invalid char syntax '" + ci.Type + "' for column '" + ci.Name + "': expected char(length)");
                                    }
                                    sz = sz.Substring(1, sz.Length - 2).Trim();
                                    try
                                    {
                                        tsize = int.Parse(sz);
                                        if (tsize < 1)
                                        {
                                            throw new Exception("char length must be positive");
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        throw new Exception("Invalid char syntax '" + ci.Type + "' for column '" + ci.Name + "': " + e.Message);
                                    }
                                    cleantype = "char(" + sz + ")";
                                    dw = tsize;
                                    tsize *= 2; // UTF16.
                                }
                                else if (0 == string.Compare("int", ci.Type, true))
                                {
                                    justify = IntJustify();
                                    tsize = 4;
                                    cleantype = "int";
                                    dw = 10;
                                }
                                else if (0 == string.Compare("long", ci.Type, true))
                                {
                                    justify = LongJustify();
                                    tsize = 8;
                                    cleantype = "long";
                                    dw = 20;
                                }
                                else if (0 == string.Compare("DateTime", ci.Type, true))
                                {
                                    justify = LongJustify();
                                    tsize = 8;
                                    cleantype = "DateTime";
                                    dw = 22;
                                }
                                else if (0 == string.Compare("double", ci.Type, true))
                                {
                                    justify = DoubleJustify();
                                    tsize = 9;
                                    cleantype = "double";
                                    dw = 20; // Precision...
                                }
                                else
                                {
                                    throw new Exception("Unknown type '" + ci.Type + "' for column '" + ci.Name + "'");
                                }
                                if (dw < 4)
                                {
                                    dw = 4; // Enough room for NULL.
                                }
                                tsize += 1; // Include the Nullable byte.
                                System.Xml.XmlElement col = systables.CreateElement("column");
                                col.AppendChild(NewElement(systables, "name", ci.Name));
                                col.AppendChild(NewElement(systables, "type", cleantype));
                                col.AppendChild(NewElement(systables, "bytes", tsize.ToString()));
                                col.AppendChild(NewElement(systables, "dw", dw.ToString())); // Display width.
                                col.AppendChild(NewElement(systables, "justify", justify));
                                xeNewTable.AppendChild(col);
                                totsize += tsize;
                            }
                            xeNewTable.AppendChild(NewElement(systables, "size", totsize.ToString())); // Size of a full record.
                        }

                        xeTables.AppendChild(xeNewTable);
                    }

                    UpdateSysTables_unlocked(systables);

                }

                string dfstablefile = "dfs://RDBMS_Table_" + TableName;
                try
                {
                    dfsclient.DeleteFile(dfstablefile);
                }
                catch
                {
                }
                {
                    RemoteCall rc = GetRemoteCallBlankTable();
                    rc.OverrideOutput = dfstablefile + "@" + totsize.ToString();
                    rc.Call();
                }

                DSpace_Log("Created table '" + TableName + "' with " + colinfo.Count.ToString() + " columns");

                return true;
            }


            bool QlBatchExecute(string args)
            {
                if (!(0 == string.Compare("BATCHEXECUTE", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string infp = Qa.NextPart(ref args);
                if (0 == infp.Length
                    || infp.Length <= 2
                    || '\'' != infp[0]
                    || '\'' != infp[infp.Length - 1]
                    )
                {
                    throw new Exception("Expected string containing path to BATCHEXECUTE input file");
                }
                infp = infp.Substring(1, infp.Length - 2).Replace("''", "'");

                // Note: errors with query abort the rest of the queries.

                string insertfilepath = null;
                System.IO.FileStream insertfile = null;
                try
                {
                    // Queries delimited by win-newline, nul, win-newline
                    using (System.IO.StreamReader sr = System.IO.File.OpenText(infp))
                    {
                        StringBuilder sb = new StringBuilder(500);
                        string insertfilename = null; // Only valid if insertfilepath is set.
                        string InsertTableName = ""; // Current table name for the insertfile.
                        string DfsTableFile = null; // Only valid when insertfile open.
                        int[] RowTypeSizes; // Only valid when insertfile open.
                        string[] RowTypes; // Only valid when insertfile open.
                        int OutputRowLength = 0; // Only valid when insertfile open.
                        for (; ; )
                        {
                            sb.Length = 0;
                            int endstate = 0;
                            string query = null;
                            for (; ; )
                            {
                                int ich = sr.Read();
                                if (-1 == ich)
                                {
                                    break;
                                }
                                char ch = (char)ich;
                                sb.Append(ch);
                                switch (endstate)
                                {
                                    case 0:
                                        if ('\r' == ch)
                                        {
                                            endstate = 1;
                                        }
                                        break;
                                    case 1:
                                        endstate = 0;
                                        if ('\n' == ch)
                                        {
                                            endstate = 2;
                                        }
                                        break;
                                    case 2:
                                        endstate = 0;
                                        if ('\0' == ch)
                                        {
                                            endstate = 3;
                                        }
                                        break;
                                    case 3:
                                        endstate = 0;
                                        if ('\r' == ch)
                                        {
                                            endstate = 4;
                                        }
                                        break;
                                    case 4:
                                        endstate = 0;
                                        if ('\n' == ch)
                                        {
                                            endstate = 5;
                                        }
                                        break;
                                }
                                if (5 == endstate)
                                {
                                    sb.Length = sb.Length - 5; // Chop off delimiter.
                                    query = sb.ToString();
                                    break;
                                }
                            }

                            if (null == query)
                            {
                                if (null != insertfile)
                                {
                                    insertfile.Close();
                                    insertfile = null;
                                    {
                                        string fn = insertfilepath;
                                        string tn = InsertTableName;
                                        insertfilepath = null;
                                        InsertTableName = "";
                                        _FinishInsertFile(fn, insertfilename, OutputRowLength, DfsTableFile);
                                    }
                                }
                                break;
                            }

                            {
                                // Check query!
                                string qargs = query;
                                bool handled = false;
                                if (0 == string.Compare("INSERT", Qa.NextPart(ref qargs), true)
                                    && 0 == string.Compare("INTO", Qa.NextPart(ref qargs), true))
                                {
                                    string TableName = Qa.NextPart(ref qargs);
                                    if (0 == TableName.Length)
                                    {
                                        throw new Exception("Table name expected");
                                    }
                                    switch (Qa.NextPart(ref qargs).ToUpper())
                                    {

                                        case "(":
                                            throw new Exception("Expected VALUES");

                                        case "VALUES":
                                            handled = true;
                                            {
                                                if (0 != string.Compare(InsertTableName, TableName, true))
                                                {
                                                    // New table, so flush current one.
                                                    if (null != insertfile)
                                                    {
                                                        insertfile.Close();
                                                        insertfile = null;
                                                        {
                                                            string fn = insertfilepath;
                                                            string tn = InsertTableName;
                                                            insertfilepath = null;
                                                            InsertTableName = "";
                                                            _FinishInsertFile(fn, insertfilename, OutputRowLength, DfsTableFile);
                                                        }
                                                    }
                                                }

                                                if (null == insertfile)
                                                {
                                                    InsertTableName = TableName;
                                                    //insertfilename = "batchinsert-" + InsertTableName + "-" + Guid.NewGuid().ToString() + "-" + DFS_TEMP_FILE_MARKER + ".bin"; // Too long.
                                                    insertfilename = "BI-" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-').Replace("+", "_").Trim(new char[] { '=' }) + ".bin";
                                                    insertfilepath = IOUtils.GetTempDirectory() + @"\" + insertfilename;
                                                    insertfile = System.IO.File.Create(insertfilepath);
                                                }

                                                System.Xml.XmlDocument systables;
                                                using (GlobalCriticalSection.GetLock())
                                                {
                                                    systables = LoadSysTables_unlocked();
                                                }

                                                System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                                                if (null == xeTable)
                                                {
                                                    throw new Exception("Table '" + TableName + "' does not exist");
                                                }

                                                DfsTableFile = xeTable["file"].InnerText;

                                                {
                                                    string RowInfo;
                                                    string TypeInfo; // Type
                                                    OutputRowLength = 0;
                                                    {
                                                        StringBuilder sbRowInfo = new StringBuilder();
                                                        StringBuilder sbTypeInfo = new StringBuilder(); // Type
                                                        foreach (System.Xml.XmlNode xn in xeTable.SelectNodes("column"))
                                                        {
                                                            if (0 != sbRowInfo.Length)
                                                            {
                                                                sbRowInfo.Append(',');
                                                                sbTypeInfo.Append(','); // Type
                                                            }
                                                            string stsize = xn["bytes"].InnerText;
                                                            int tsize = int.Parse(stsize);
                                                            OutputRowLength += tsize;
                                                            sbRowInfo.Append(stsize);
                                                            sbTypeInfo.Append(xn["type"].InnerText); // Type
                                                        }
                                                        RowInfo = sbRowInfo.ToString();
                                                        TypeInfo = sbTypeInfo.ToString(); // Type
                                                    }

                                                    string[] sRowTypeSizes = RowInfo.Split(',');
                                                    RowTypeSizes = new int[sRowTypeSizes.Length];
                                                    for (int i = 0; i < RowTypeSizes.Length; i++)
                                                    {
                                                        RowTypeSizes[i] = int.Parse(sRowTypeSizes[i]);
                                                    }

                                                    RowTypes = TypeInfo.Split(',');
                                                }

                                                string[] valtoks;
                                                {
                                                    List<string> lvt = new List<string>();
                                                    for (; ; )
                                                    {
                                                        string s = Qa.NextPart(ref qargs);
                                                        if (0 == s.Length)
                                                        {
                                                            break;
                                                        }
                                                        lvt.Add(s);
                                                    }
                                                    valtoks = lvt.ToArray();
                                                }

                                                // Note: most of this code is duplicated in RDBMS_InsertValues.DBCORE
                                                {
                                                    int ivtok = 0;
                                                    string vtok = "";

                                                    vtok = (ivtok < valtoks.Length) ? valtoks[ivtok++] : "";
                                                    if ("(" != vtok)
                                                    {
                                                        throw new Exception("Expected (...) after VALUES");
                                                    }

                                                    string[] uservalues = new string[RowTypes.Length];
                                                    int nuservals = 0;
                                                    bool expectcomma = false;
                                                    for (; ; )
                                                    {
                                                        vtok = (ivtok < valtoks.Length) ? valtoks[ivtok++] : "";
                                                        if ("," == vtok)
                                                        {
                                                            if (!expectcomma)
                                                            {
                                                                throw new Exception("Unexpected comma in VALUES (...)");
                                                            }
                                                            expectcomma = false;
                                                            continue;
                                                        }
                                                        {
                                                            // Expect value or ")"!
                                                            if ("" == vtok)
                                                            {
                                                                if (nuservals == uservalues.Length)
                                                                {
                                                                    throw new Exception("Expected ) after VALUES(...");
                                                                }
                                                                else
                                                                {
                                                                    throw new Exception("Expected " + nuservals.ToString() + " values and ) after VALUES(...");
                                                                }
                                                            }
                                                            if (")" == vtok)
                                                            {
                                                                if (nuservals != uservalues.Length)
                                                                {
                                                                    throw new Exception("Expected " + nuservals.ToString() + " values in VALUES(...");
                                                                }
                                                                break;
                                                            }
                                                            if (expectcomma)
                                                            {
                                                                throw new Exception("Expected comma in VALUES (...)");
                                                            }
                                                            if (nuservals >= uservalues.Length)
                                                            {
                                                                throw new Exception("Too many values in VALUES(...)");
                                                            }
                                                            string val = vtok;
                                                            if ("+" == val || "-" == val)
                                                            {
                                                                vtok = (ivtok < valtoks.Length) ? valtoks[ivtok++] : "";
                                                                if (0 == vtok.Length)
                                                                {
                                                                    throw new Exception("Expected number after sign");
                                                                }
                                                                val = val + vtok;
                                                            }
                                                            uservalues[nuservals++] = val;
                                                        }
                                                        expectcomma = true;
                                                    }

                                                    List<byte> valuebuf = new List<byte>();
                                                    //valuebuf.Clear();
                                                    int icol = 0;
                                                    try
                                                    {
                                                        for (; icol < RowTypes.Length; icol++)
                                                        {
                                                            string uval = uservalues[icol];
                                                            string type = RowTypes[icol];
                                                            int tsize = RowTypeSizes[icol];
                                                            if (0 == string.Compare("NULL", uval, true))
                                                            {
                                                                valuebuf.Add(1); // Nullable, IsNull=true!
                                                                for (int remain = (tsize - 1); remain > 0; remain--)
                                                                {
                                                                    valuebuf.Add(0); // Padding to column size.
                                                                }
                                                            }
                                                            else
                                                            {
                                                                if (type.StartsWith("char"))
                                                                {
                                                                    if (uval.Length <= 2 || '\'' != uval[0] || '\'' != uval[uval.Length - 1])
                                                                    {
                                                                        throw new Exception("Invalid string: " + uval);
                                                                    }
                                                                    string x = uval.Substring(1, uval.Length - 2).Replace("''", "'");
                                                                    byte[] bx = System.Text.Encoding.Unicode.GetBytes(x);
                                                                    if (bx.Length > tsize - 1)
                                                                    {
                                                                        throw new Exception("String too large for " + type);
                                                                    }
                                                                    valuebuf.Add(0); // Not null.
                                                                    valuebuf.AddRange(bx);
                                                                    for (int remain = (tsize - 1) - bx.Length; remain > 0; remain--)
                                                                    {
                                                                        valuebuf.Add(0); // Padding to char column size.
                                                                    }
                                                                }
                                                                else if ("int" == type)
                                                                {
                                                                    int x = int.Parse(uval);
                                                                    recordset rsx = recordset.Prepare();
                                                                    rsx.PutInt32((Int32)Entry.ToUInt32(x));
                                                                    ByteSlice bsx = rsx.ToByteSlice();
                                                                    valuebuf.Add(0); // Not null.
                                                                    bsx.AppendTo(valuebuf);
                                                                }
                                                                else if ("long" == type)
                                                                {
                                                                    long x = long.Parse(uval);
                                                                    recordset rsx = recordset.Prepare();
                                                                    rsx.PutInt64((Int64)Entry.ToUInt64(x));
                                                                    ByteSlice bsx = rsx.ToByteSlice();
                                                                    valuebuf.Add(0); // Not null.
                                                                    bsx.AppendTo(valuebuf);
                                                                }
                                                                else if ("DateTime" == type)
                                                                {

                                                                    if (uval.Length <= 2 || '\'' != uval[0] || '\'' != uval[uval.Length - 1])
                                                                    {
                                                                        throw new Exception("Invalid string: " + uval);
                                                                    }
                                                                    string xs = uval.Substring(1, uval.Length - 2).Replace("''", "'");

                                                                    DateTime xdt = DateTime.Parse(xs);

                                                                    long x = xdt.Ticks;
                                                                    recordset rsx = recordset.Prepare();
                                                                    rsx.PutInt64(x);
                                                                    ByteSlice bsx = rsx.ToByteSlice();
                                                                    valuebuf.Add(0); // Not null.
                                                                    bsx.AppendTo(valuebuf);
                                                                }
                                                                else if ("double" == type)
                                                                {
                                                                    double x = double.Parse(uval);
                                                                    recordset rsx = recordset.Prepare();
                                                                    rsx.PutDouble(x);
                                                                    ByteSlice bsx = rsx.ToByteSlice();
                                                                    valuebuf.Add(0); // Not null.
                                                                    bsx.AppendTo(valuebuf);
                                                                }
                                                                else
                                                                {
                                                                    throw new Exception("Unhandled data type " + type);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        throw new Exception("Problem with value " + uservalues[icol] + " of type " + RowTypes[icol] + ": " + e.Message, e);
                                                    }

                                                    if (valuebuf.Count != OutputRowLength)
                                                    {
                                                        throw new Exception("Record length mismatch (" + valuebuf.Count + " != " + OutputRowLength.ToString() + ") [#8129]");
                                                    }
                                                    else
                                                    {
                                                        // write out valuebuf!
                                                        for (int iv = 0; iv < OutputRowLength; iv++)
                                                        {
                                                            insertfile.WriteByte(valuebuf[iv]);
                                                        }
                                                    }

                                                }

                                            }
                                            break;
                                    }
                                }
                                if (!handled)
                                {
                                    if (null != insertfile)
                                    {
                                        insertfile.Close();
                                        insertfile = null;
                                        {
                                            string fn = insertfilepath;
                                            string tn = InsertTableName;
                                            insertfilepath = null;
                                            InsertTableName = "";
                                            _FinishInsertFile(fn, insertfilename, OutputRowLength, DfsTableFile);
                                        }
                                    }
                                    QaExec(query);
                                }
                            }

                        }

                    }
                }
                finally
                {
                    if (null != insertfilepath)
                    {
                        if (null != insertfile)
                        {
                            insertfile.Close();
                            insertfile = null;
                        }
                        System.IO.File.Delete(insertfilepath);
                        insertfilepath = null;
                    }

                    //System.IO.File.Delete(infp); // Caller owns the file.
                }

                return true;
            }

            void _FinishInsertFile(string insertfilepath, string insertfilename, int OutputRowLength, string DfsTableFile)
            {
                string dfsfn = DFS_TEMP_FILE_MARKER + "-" + insertfilename;
                Shell("dspace put \"" + insertfilepath + "\" \"" + dfsfn + "@" + OutputRowLength.ToString() + "\"");
                System.IO.File.Delete(insertfilepath);
                Shell("dspace combine \"" + dfsfn + "\" \"" + DfsTableFile + "\"");
            }


            bool QlShell(string args)
            {
                if (!(0 == string.Compare("SHELL", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string shellcmdraw = Qa.NextPart(ref args);
                bool dfstemp = false;
                if (0 == string.Compare(shellcmdraw, "DFSTEMP", true))
                {
                    dfstemp = true;
                    shellcmdraw = Qa.NextPart(ref args);
                }
                if (shellcmdraw.Length <= 2 || '\'' != shellcmdraw[0] || '\'' != shellcmdraw[shellcmdraw.Length - 1])
                {
                    throw new Exception("Expected SHELL command string");
                }
                //shellcmd = shellcmdraw.Substring(1, shellcmdraw.Length - 2).Replace("''", "'");

                //DSpace_Log(Shell(shellcmdraw));

                string TableName = "Sys.Shell(" + shellcmdraw + ")";

                string DfsOutputName = "dfs://RDBMS_SYSSHELL" + "{" + Guid.NewGuid().ToString() + "}";
                if (dfstemp)
                {
                    DfsOutputName += DFS_TEMP_FILE_MARKER;
                }
                DSpace_Log((new PrepareSelect()).Exec(QlArgsEscape(TableName), DfsOutputName, "*", "-1", "*", ("SPECIALORDER" + (dfstemp ? ";DFSTEMP" : ""))));

                return true;
            }


            bool QlSysTablesXml(string args)
            {
                if (!(0 == string.Compare("SysTablesXml", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                bool dfstemp = false;
                if (0 == string.Compare("DFSTEMP", Qa.NextPart(ref args), true))
                {
                    dfstemp = true;
                }

                string SysTablesXml;
                using (GlobalCriticalSection.GetLock())
                {
                    SysTablesXml = LoadSysTablesXml_unlocked();
                }

                //DSpace_Log(SysTablesXml);

                string TableName = "Sys.TablesXML";

                string DfsOutputName = "dfs://RDBMS_SYSTABLESXML" + "{" + Guid.NewGuid().ToString() + "}";
                if (dfstemp)
                {
                    DfsOutputName += DFS_TEMP_FILE_MARKER;
                }
                DSpace_Log((new PrepareSelect()).Exec(QlArgsEscape(TableName), DfsOutputName, "*", "-1", "*", ("SPECIALORDER" + (dfstemp ? ";DFSTEMP" : ""))));

                return true;
            }

            string _QlSysHelp(bool dfstemp)
            {
                string TableName = "Sys.Help";

                string DfsOutputName = "dfs://RDBMS_SYSSHELP" + "{" + Guid.NewGuid().ToString() + "}";
                if (dfstemp)
                {
                    DfsOutputName += DFS_TEMP_FILE_MARKER;
                }
                return (new PrepareSelect()).Exec(QlArgsEscape(TableName), DfsOutputName, "*", "-1", "*", ("SPECIALORDER" + (dfstemp ? ";DFSTEMP" : "")));
            }

            string _QlSysIndexes(bool dfstemp)
            {
                string TableName = "Sys.Indexes(" + @"\\" + System.Net.Dns.GetHostName() + @"\" + System.Environment.CurrentDirectory.Replace(':', '$') + ")";

                string DfsOutputName = "dfs://RDBMS_SYSINDEXES" + "{" + Guid.NewGuid().ToString() + "}";
                if (dfstemp)
                {
                    DfsOutputName += DFS_TEMP_FILE_MARKER;
                }
                return (new PrepareSelect()).Exec(QlArgsEscape(TableName), DfsOutputName, "*", "-1", "*", (dfstemp ? ";DFSTEMP" : ""));
            }

            bool QlSysTablesXmlFile(string args)
            {
                if (!(0 == string.Compare("SysTablesXmlFile", Qa.NextPart(ref args), true)))
                {
                    return false;
                }

                string SysTablesXml;
                using (GlobalCriticalSection.GetLock())
                {
                    SysTablesXml = LoadSysTablesXml_unlocked();
                }

                string fp = Qa.NextPart(ref args);
                if (fp.Length < 2 || '\'' != fp[0] || '\'' != fp[fp.Length - 1])
                {
                    throw new Exception("Destination file name expected");
                }
                fp = fp.Substring(1, fp.Length - 2);

                System.IO.File.WriteAllText(fp, SysTablesXml);

                DSpace_Log("<SysTablesXmlFile>" + fp + "</SysTablesXmlFile>");

                return true;
            }


            public struct ColInfo
            {
                public string Name;
                public string Type;
            }
        
        }


    }
}
