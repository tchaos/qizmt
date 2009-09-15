using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        // Returns the next part and updates text to exclude it.
        // Returns empty string at end.
        public static string NextPart(ref string text)
        {
            string s = text.TrimStart(' ', '\t', '\r', '\n');
            if (s.Length > 0)
            {
                if (s[0] == '(' || s[0] == ')' || s[0] == ',' || s[0] == ';')
                {
                    string result = s.Substring(0, 1);
                    text = s.Substring(1);
                    return result;
                }
                if (s[0] == '\'')
                {
                    bool prevsquot = false;
                    for (int i = 1; ; i++)
                    {
                        if (i >= s.Length)
                        {
                            if (prevsquot)
                            {
                                string result = s;
                                text = "";
                                return result;
                            }
                            throw new Exception("Expected terminating single quote: " + s);
                        }
                        if (s[i] == '\'')
                        {
                            if (prevsquot)
                            {
                                prevsquot = false;
                            }
                            else
                            {
                                prevsquot = true;
                            }
                        }
                        else if (prevsquot)
                        {
                            if (s[i] == ' ')
                            {
                                string result = s.Substring(0, i);
                                text = s.Substring(i + 1);
                                return result;
                            }
                            else // Text directly after.
                            {
                                string result = s.Substring(0, i);
                                text = s.Substring(i);
                                return result;
                            }
                        }
                    }
                }
            }
            for (int i = 0; ; i++)
            {
                if (i >= s.Length)
                {
                    string result = s;
                    text = string.Empty;
                    return result;
                }
                if (char.IsWhiteSpace(s[i]))
                {
                    string result = s.Substring(0, i);
                    text = s.Substring(i + 1);
                    return result;
                }
                if (!char.IsLetterOrDigit(s[i]) && '_' != s[i] && '.' != s[i])
                {
                    if (i > 0)
                    {
                        string result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                    {
                        i++; // Return this symbol.
                        string result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                }


            }
        }


        public static void IntToDbBytesAppend(Int32 x, List<byte> append)
        {
            append.Add(0); // Nullable; not null.
            Entry.ToBytesAppend((Int32)Entry.ToUInt32(x), append);
        }

        public static void LongToDbBytesAppend(Int64 x, List<byte> append)
        {
            append.Add(0); // Nullable; not null.
            Entry.ToBytesAppend64((Int64)Entry.ToUInt64(x), append);
        }

        public static void DateTimeToDbBytesAppend(DateTime x, List<byte> append)
        {
            append.Add(0); // Nullable; not null.
            Entry.ToBytesAppend64(x.Ticks, append);
        }

        public static void DoubleToDbBytesAppend(Double x, List<byte> append)
        {
            append.Add(0); // Nullable; not null.
            Entry.ToBytesAppendDouble(x, append);
        }

        public static void StringToDbBytesAppend(string x, List<byte> append, int RowByteCount)
        {
            byte[] bx = System.Text.Encoding.Unicode.GetBytes(x);
            append.Add(0); // Not null.
            append.AddRange(bx);
            for (int i = 1 + bx.Length; i < RowByteCount; i++)
            {
                append.Add(0);
            }
        }

        public static void StringToDbBytesAppend(string x, List<byte> append)
        {
            StringToDbBytesAppend(x, append, 0);
        }


        public static string QlArgsEscape(string args)
        {
            return args.Replace("&", "&amp;").Replace("\r", "&#13;").Replace("\n", "&#10;").Replace("\"", "&quot;").Replace("'", "&apos;").Replace("\0", "&#0;").Replace(">", "&gt;").Replace("<", "&lt;");
        }


        public static string QlArgsUnescape(string eargs)
        {
            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
            xd.LoadXml("<x></x>");
            xd["x"].InnerXml = eargs;
            return xd["x"].InnerText;
        }

        public static string QlArgsUnescape(string[] eargs)
        {
            return QlArgsUnescape(string.Join(" ", eargs));
        }


        public abstract class Local
        {
            public string Exec(params string[] args)
            {
                DSpace_ExecArgs = args;
                Run();
                return ReadToEnd();
            }

            protected abstract void Run();

            protected string[] DSpace_ExecArgs;

            protected void DSpace_Log(string line)
            {
                _log.AppendLine(line);
            }

            StringBuilder _log = new StringBuilder();

            public string ReadToEnd()
            {
                string result = _log.ToString();
                _log.Length = 0;
                return result;
            }
        }


        internal static string Shell(string cmdline, bool suppresserrors)
        {
            return Exec.Shell(cmdline, suppresserrors);
        }

        internal static string Shell(string cmdline)
        {
            return Exec.Shell(cmdline);
        }


        public static bool CanUseDfsRef(string xcmd)
        {
            if (0 == string.Compare("SELECT", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true)
                && "*" == RDBMS_DBCORE.Qa.NextPart(ref xcmd)
                && 0 == string.Compare("FROM", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true)
                )
            {
                string xTableName = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                if (!xTableName.StartsWith("sys.", true, null) // User tables only!
                    && 0 == RDBMS_DBCORE.Qa.NextPart(ref xcmd).Length)
                {
                    return true;
                }
            }
            return false;
        }

        public static bool IsPortionCmd(string xcmd, ref int portion, ref int totalPortion, ref string cleanedCmd)
        {
            if (0 == string.Compare("SELECT", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true)
                && 0 == string.Compare("PORTION", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true))
            {
                string sportion = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                string stotPortion = "";

                if (0 == string.Compare("OF", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true))
                {
                    stotPortion = RDBMS_DBCORE.Qa.NextPart(ref xcmd);

                    if (0 == string.Compare("*", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true)
                        && 0 == string.Compare("FROM", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true))
                    {
                        string tablename = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                        if (!tablename.StartsWith("sys.", true, null) // User tables only!
                            && 0 == RDBMS_DBCORE.Qa.NextPart(ref xcmd).Length)
                        {
                            try
                            {
                                portion = Int32.Parse(sportion);
                                totalPortion = Int32.Parse(stotPortion);
                            }
                            catch
                            {
                                throw new Exception("PORTION must be valid integers.");
                            }
                            if (portion <= 0 || totalPortion <= 0)
                            {
                                throw new Exception("PORTION must be bigger than zero.");
                            }
                            if (portion > totalPortion)
                            {
                                throw new Exception("PORTION OF must be smaller than or equal to the total number of portions");
                            }
                            cleanedCmd = "SELECT * FROM " + tablename;
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public static bool HasPinFor(string xcmd, ref int PinFor, ref string resulting_xcmd)
        {
            string cmd = xcmd;
            string querystart = RDBMS_DBCORE.Qa.NextPart(ref cmd);
            if (0 == string.Compare("SELECT", querystart, true)
                || 0 == string.Compare("DROP", querystart, true))
            {
                if (0 == string.Compare("PIN", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                    && 0 == string.Compare("FOR", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
                {
                    string sPinFor = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                    if (0 == sPinFor.Length)
                    {
                        throw new Exception("Expected integer after PIN FOR");
                    }
                    try
                    {
                        PinFor = int.Parse(sPinFor);
                        if (PinFor < 1)
                        {
                            throw new FormatException("Expected positive integer");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Invalid integer for PIN FOR", e);
                    }
                    resulting_xcmd = querystart + " " + cmd;
                    return true;
                }
            }
            return false;
        }


        public const string DFS_TEMP_FILE_MARKER = "{14D3C051-6E33-4e24-9CB7-C7E085AAA877}";


        public static string SafeTextDfsPath(string s)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char ch in s)
            {
                if (sb.Length >= 150)
                {
                    sb.Append('-');
                    sb.Append(s.GetHashCode());
                    break;
                }
                if ('.' == ch)
                {
                    if (0 == ch)
                    {
                        sb.Append("-2E");
                        continue;
                    }
                }
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch)
                {
                    sb.Append('-');
                    if (ch > 0xFF)
                    {
                        sb.Append('u');
                        sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                    }
                    else
                    {
                        sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (0 == sb.Length)
            {
                return "_";
            }
            return sb.ToString();
        }


        static int GetDfsFileRecordLength(string dfsfile)
        {
            string output = Shell("dspace ls \"" + dfsfile + "\"");
            for (int i = 0;
                i < output.Length
                    && '\n' != output[i]
                    && '\r' != output[i];
                i++)
            {
                if ('@' == output[i])
                {
                    string s = output.Substring(i + 1);
                    for (int j = 0; ; j++)
                    {
                        if (j == s.Length || !char.IsDigit(s[j]))
                        {
                            return int.Parse(s.Substring(0, j));
                        }
                    }
                }
            }
            return 0;
        }


        // Note: if a parametered table, there must be no spacing around the parentheses at this point.
        static System.Xml.XmlElement FindTable(System.Xml.XmlDocument xd, string TableName)
        {
            System.Xml.XmlElement xeTables = xd.SelectSingleNode("/tables") as System.Xml.XmlElement;
            if (null == xeTables)
            {
                throw new Exception("SysTables format critical failure");
            }
            foreach (System.Xml.XmlNode xn in xeTables.ChildNodes)
            {
                if (0 == string.Compare(TableName, xn["name"].InnerText, true))
                {
                    return xn as System.Xml.XmlElement;
                }
            }
            {
                if (0 == string.Compare(TableName, "sys.tables", true))
                {
                    System.Xml.XmlElement xeNewTable = xd.CreateElement("table");
                    xeNewTable.AppendChild(NewElement(xd, "name", "Sys.Tables"));
                    xeNewTable.AppendChild(NewElement(xd, "file", "qa://Sys.Tables"));
                    int totsize = 0;
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "Table";
                        string cleantype = "char(100)";
                        string ciType = cleantype;
                        int nchars = 100;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "File";
                        string cleantype = "char(120)";
                        string ciType = cleantype;
                        int nchars = 120;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    xeNewTable.AppendChild(NewElement(xd, "size", totsize.ToString())); // Size of a full record.
                    return xeNewTable;
                }
                else if (0 == string.Compare(TableName, "sys.tablesXML", true))
                {
                    System.Xml.XmlElement xeNewTable = xd.CreateElement("table");
                    xeNewTable.AppendChild(NewElement(xd, "name", "Sys.TablesXML"));
                    xeNewTable.AppendChild(NewElement(xd, "file", "qa://Sys.TablesXML"));
                    int totsize = 0;
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "Line";
                        string cleantype = "char(200)";
                        string ciType = cleantype;
                        int nchars = 200;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    xeNewTable.AppendChild(NewElement(xd, "size", totsize.ToString())); // Size of a full record.
                    return xeNewTable;
                }
                else if (TableName.StartsWith("sys.shell(", true, null))
                {
                    System.Xml.XmlElement xeNewTable = xd.CreateElement("table");
                    xeNewTable.AppendChild(NewElement(xd, "name", "Sys.Shell"));
                    xeNewTable.AppendChild(NewElement(xd, "file", "qa://Sys.Shell"));
                    int totsize = 0;
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "Line";
                        string cleantype = "char(200)";
                        string ciType = cleantype;
                        int nchars = 200;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    xeNewTable.AppendChild(NewElement(xd, "size", totsize.ToString())); // Size of a full record.
                    return xeNewTable;
                }
                else if (0 == string.Compare(TableName, "sys.help", true))
                {
                    System.Xml.XmlElement xeNewTable = xd.CreateElement("table");
                    xeNewTable.AppendChild(NewElement(xd, "name", "Sys.Help"));
                    xeNewTable.AppendChild(NewElement(xd, "file", "qa://Sys.Help"));
                    int totsize = 0;
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "Line";
                        string cleantype = "char(1000)";
                        string ciType = cleantype;
                        int nchars = 1000;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    xeNewTable.AppendChild(NewElement(xd, "size", totsize.ToString())); // Size of a full record.
                    return xeNewTable;
                }
                else if(TableName.StartsWith("sys.indexes(", StringComparison.OrdinalIgnoreCase))
                {
                    System.Xml.XmlElement xeNewTable = xd.CreateElement("table");
                    xeNewTable.AppendChild(NewElement(xd, "name", "Sys.Indexes"));
                    xeNewTable.AppendChild(NewElement(xd, "file", "qa://Sys.Indexes"));
                    int totsize = 0;
                    {
                        System.Xml.XmlElement col = xd.CreateElement("column");
                        string ciName = "IndexName";
                        string cleantype = "char(1000)";
                        string ciType = cleantype;
                        int nchars = 1000;
                        int dw = nchars;
                        int tsize = 1 + (nchars * 2);
                        string justify = CharNJustify();
                        col.AppendChild(NewElement(xd, "name", ciName));
                        col.AppendChild(NewElement(xd, "type", cleantype));
                        col.AppendChild(NewElement(xd, "bytes", tsize.ToString()));
                        col.AppendChild(NewElement(xd, "dw", dw.ToString())); // Display width.
                        col.AppendChild(NewElement(xd, "justify", justify));
                        xeNewTable.AppendChild(col);
                        totsize += tsize;
                    }
                    xeNewTable.AppendChild(NewElement(xd, "size", totsize.ToString())); // Size of a full record.
                    return xeNewTable;
                }
            }
            return null;
        }

        static System.Xml.XmlElement NewElement(System.Xml.XmlDocument xd, string name, string InnerText)
        {
            System.Xml.XmlElement xe = xd.CreateElement(name);
            xe.InnerText = InnerText;
            return xe;
        }

        static string CharNJustify() { return "left"; }
        static string IntJustify() { return "left"; }
        static string LongJustify() { return "left"; }
        static string DoubleJustify() { return "left"; }


        const string SYSTABLES_FILENAME = "RDBMS_SysTables";


        static string _GetSysTablesFile_unlocked()
        {
            string systablesfp = IOUtils.GetTempDirectory() + @"\" + Guid.NewGuid() + SYSTABLES_FILENAME;
            try
            {
                Shell("dspace get " + SYSTABLES_FILENAME + " \"" + systablesfp + "\"");
            }
            catch (Exception e)
            {
                if (-1 != e.Message.IndexOf("Error:  The specified file '" + SYSTABLES_FILENAME + "' does not exist in DFS"))
                {
                    throw new System.IO.FileNotFoundException("Unable to load " + SYSTABLES_FILENAME + ": " + e.Message, e);
                }
                throw;
            }
            return systablesfp;
        }

        static void _FinishGetSysTablesFile_unlocked(string systablesfp)
        {
            System.IO.File.Delete(systablesfp);
        }


        static System.Xml.XmlDocument LoadSysTables_unlocked()
        {
            string systablesfp = _GetSysTablesFile_unlocked();
            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
            xd.Load(systablesfp);
            _FinishGetSysTablesFile_unlocked(systablesfp);
            return xd;
        }

        static string LoadSysTablesXml_unlocked()
        {
            string systablesfp = _GetSysTablesFile_unlocked();
            string result = System.IO.File.ReadAllText(systablesfp);
            _FinishGetSysTablesFile_unlocked(systablesfp);
            return result;
        }


        static void UpdateSysTables_unlocked(System.Xml.XmlDocument xd)
        {
            string systablesfp = IOUtils.GetTempDirectory() + @"\" + Guid.NewGuid() + SYSTABLES_FILENAME;

            xd.Save(systablesfp);

            try
            {
                Shell("dspace del " + SYSTABLES_FILENAME + ".temp");
            }
            catch
            {
            }
            Shell("dspace put \"" + systablesfp + "\" " + SYSTABLES_FILENAME + ".temp");

            try
            {
                Shell("dspace del " + SYSTABLES_FILENAME);
            }
            catch
            {
            }
            Shell("dspace rename " + SYSTABLES_FILENAME + ".temp " + SYSTABLES_FILENAME);

        }


    }
}
