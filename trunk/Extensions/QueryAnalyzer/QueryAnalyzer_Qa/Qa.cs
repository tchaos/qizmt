using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;

namespace RDBMS_qa
{
    public partial class Qa : Form
    {
        private string filename = "";
        private string connstr = "";
        private string firsthost = "";

        public Qa()
        {
            InitializeComponent();
        }

        private void Qa_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
            {
                RunQuery();
            }
        }

        public string QueryText
        {
            get
            {
                return txtQuery.Text;
            }
            set
            {
                txtQuery.Text = value;
            }
        }

        public string FileName
        {
            get
            {
                return filename;
            }
            set
            {
                filename = value;
                this.Text = filename;
            }
        }

        public string ConnectionString
        {
            get
            {
                return connstr;
            }
            set
            {
                if (value == null || value.Length == 0)
                {
                    MessageBox.Show("Connection string cannot be empty.");
                    return;
                }
                connstr = value;
                SetFirstHost();
            }
        }

        private void SetFirstHost()
        {
            string[] parts = connstr.Split(';');
            foreach (string part in parts)
            {
                string[] keyval = part.Split('=');
                string key = keyval[0].Trim();
                if (string.Compare(key, "data source", true) == 0)
                {
                    firsthost = keyval[1].Split(',')[0];
                    break;
                }
            }
        }

        public void Copy()
        {
            if (dgvResults.Focused)
            {
                DataObject obj = dgvResults.GetClipboardContent();
                if (obj != null)
                {
                    Clipboard.SetDataObject(dgvResults.GetClipboardContent());
                }
                else
                {
                    Clipboard.Clear();
                }                            
            }
            else
            {
                txtQuery.Clipboard.Copy();
            }
        }

        public void Paste()
        {
            txtQuery.Clipboard.Paste();
        }

        public void SelectAll()
        {
            if (dgvResults.Focused)
            {
                dgvResults.SelectAll();
            }
            else
            {
                txtQuery.Selection.SelectAll();
            }
        }

        private void ShowStatus(string status)
        {            
            this.toolStripStatusLabel1.Text = status;
            this.statusStrip1.Refresh();
        }

        private void EnableEditMenutItems(bool enabled)
        {
            copyCtrlCToolStripMenuItem.Enabled = enabled;
            selectAllCtrlAToolStripMenuItem.Enabled = enabled;
        }

        static string _CleanQuery(string query, string newlinestring)
        {
            //Remove newlines from non-string-literal.
            //Identify all string literals.
            Regex regx = new Regex("'[^']*'");
            MatchCollection lits = regx.Matches(query);

            //Find all newlines that are not in the string literals.
            int prevIndex = 0;
            string cleanedQuery = "";
            foreach (Match lit in lits)
            {
                string substr = query.Substring(prevIndex, lit.Index - prevIndex);

                //Replace newline with a space.
                substr = substr.Replace("\r", "").Replace("\n", newlinestring);
                cleanedQuery += substr + lit.Value;

                prevIndex = lit.Index + lit.Length;
            }

            if (prevIndex < query.Length)
            {
                cleanedQuery += query.Substring(prevIndex).Replace("\r", "").Replace("\n", newlinestring);
            }

            return cleanedQuery;
        }

        private bool IsRIndexQuery(string query)
        {
            string _query = query.Trim();
            return (_query.StartsWith("rselect", StringComparison.OrdinalIgnoreCase) ||
                _query.StartsWith("rinsert", StringComparison.OrdinalIgnoreCase) ||
                _query.StartsWith("rdelete", StringComparison.OrdinalIgnoreCase) ||
                _query.StartsWith("rcreate", StringComparison.OrdinalIgnoreCase) ||
                _query.StartsWith("rdrop", StringComparison.OrdinalIgnoreCase));            
        }

        private void TSafe(Action act)
        {
            button1.Invoke(act);
        }

        public void RunQuery()
        {
            ClearResults();

            string fquery = txtQuery.Text.Trim();
            bool hasgo = false;
            if (fquery.StartsWith("GO", StringComparison.OrdinalIgnoreCase))
            {
                for (int i = 2; ; i++)
                {
                    if (i >= fquery.Length)
                    {
                        //hasgo = false;
                        break;
                    }
                    if ('\n' == fquery[i])
                    {
                        hasgo = true;
                        fquery = fquery.Substring(i + 1).TrimStart();
                        break;
                    }
                    if (!char.IsWhiteSpace(fquery[i]))
                    {
                        //hasgo = false;
                        break;
                    }
                }
            }

            List<string> queries = new List<string>();
            if (hasgo)
            {
                string nsqueries = _CleanQuery(fquery, "\0");
                queries.AddRange(nsqueries.Split(new char[] { '\0' },
                    StringSplitOptions.RemoveEmptyEntries));
                if (0 == queries.Count)
                {
                    MessageBox.Show("Expected query after GO");
                    return;
                }
            }
            else
            {
                string cleanedQuery = _CleanQuery(fquery, " ");
                if (cleanedQuery.Length == 0)
                {
                    MessageBox.Show("Query is empty.");
                    return;
                }
                queries.Add(cleanedQuery);
            }

            button1.Enabled = false;

            ShowStatus("Executing query...");

            this.Update();

            System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                {
                    string status = "";
                    DbConnection conn = null;
                    DateTime starttime = DateTime.MinValue;
                    bool completed = false;
                    bool needsaycompleted = true;
                    try
                    {
                        System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");
                        conn = fact.CreateConnection();
                        if (IsRIndexQuery(queries[0])) //just check the first query
                        {
                            conn.ConnectionString = Utils.GetRIndexConnStr(firsthost);
                        }
                        else
                        {
                            conn.ConnectionString = ConnectionString;
                        }
                        conn.Open();
                        DbCommand cmd = conn.CreateCommand();
                        starttime = DateTime.Now;
                        for (int onquery = 0; onquery < queries.Count; onquery++)
                        {
                            string thisquery = queries[onquery].Trim();
                            if (0 == thisquery.Length)
                            {
                                continue;
                            }
                            if (onquery > 0)
                            {
                                TSafe(new Action(delegate()
                                    {
                                        ShowStatus("Executing next query...");
                                    }));
                            }
                            TSafe(new Action(delegate()
                                {
                                    this.Update();
                                }));
                            cmd.CommandText = thisquery;

                            if (thisquery.StartsWith("select", StringComparison.OrdinalIgnoreCase) ||
                                thisquery.StartsWith("shell", StringComparison.OrdinalIgnoreCase) ||
                                thisquery.StartsWith("rselect", StringComparison.OrdinalIgnoreCase))
                            {
                                needsaycompleted = false;
                                DbDataReader reader = cmd.ExecuteReader();
                                TSafe(new Action(delegate()
                                    {
                                        ClearResults();
                                    }));

                                DataSet ds = new DataSet();
                                DataTable dt = new DataTable();

                                //Get column meta data.
                                int columnCount = reader.FieldCount;
                                for (int i = 0; i < columnCount; i++)
                                {
                                    dt.Columns.Add(reader.GetName(i));
                                }

#if DEBUG
                                try
                                {
                                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                                    if (computer_name == "MAPDCMILLER"
                                        || computer_name == "MAPDDRULE1")
                                    {
                                        StringBuilder sblastschema = new StringBuilder();
                                        for (int i = 0; i < columnCount; i++)
                                        {
                                            if (sblastschema.Length != 0)
                                            {
                                                sblastschema.Append(", ");
                                            }
                                            sblastschema.AppendFormat("{0} {1}",
                                                reader.GetDataTypeName(i), reader.GetName(i));
                                        }
                                        sblastschema.AppendLine();
                                        System.IO.File.AppendAllText(@"C:\temp\lastschema.txt", sblastschema.ToString());
                                    }
                                }
                                catch
                                {
                                }
#endif

                                ds.Tables.Add(dt);

                                while (reader.Read())
                                {
                                    object[] row = new object[columnCount];
                                    reader.GetValues(row);
                                    for (int ir = 0; ir < row.Length; ir++)
                                    {
                                        if (DBNull.Value == row[ir])
                                        {
                                            row[ir] = "NULL";
                                        }
                                    }
                                    dt.Rows.Add(row);
                                }

                                //Get result table schema before closing the reader.
                                DataTable schema = reader.GetSchemaTable();
                                reader.Close();
                                TSafe(new Action(delegate()
                                    {
                                        dgvResults.DataSource = ds.Tables[0];
                                        SetGridWidths(schema);
                                    }));
                            }
                            else
                            {
                                cmd.ExecuteNonQuery();
                                TSafe(new Action(delegate()
                                    {
                                        ClearResults();
                                        needsaycompleted = true;
                                    }));
                            }
                        }
                        completed = true;
                    }
                    catch (Exception e)
                    {
                        TSafe(new Action(delegate()
                            {
                                AddRow("Exception Details", e.ToString());
                            }));                        
                        status = "Exceptions occurred";
                    }
                    finally
                    {
                        try
                        {
                            conn.Close();
                        }
                        catch (Exception e)
                        {
                            completed = false;
                            TSafe(new Action(delegate()
                            {
                                AddRow("Exception Details", e.ToString());
                            }));
                            status = "Exceptions occurred";
                        }
                    }

                    if (completed)
                    {
                        if (needsaycompleted)
                        {
                            AddRow("Query results", "Completed");
                        }
                        long tsecs = (long)(DateTime.Now - starttime).TotalSeconds;
                        StringBuilder sb = new StringBuilder();
                        if (tsecs >= 60 * 60 * 24)
                        {
                            long x = tsecs / (60 * 60 * 24);
                            sb.AppendFormat(" {0} {1}", x, 1 == x ? "day" : "days");
                            tsecs %= 60 * 60 * 24;
                        }
                        if (tsecs >= 60 * 60)
                        {
                            long x = tsecs / (60 * 60);
                            sb.AppendFormat(" {0} {1}", x, 1 == x ? "hour" : "hours");
                            tsecs %= 60 * 60;
                        }
                        if (tsecs >= 60)
                        {
                            long x = tsecs / 60;
                            sb.AppendFormat(" {0} {1}", x, 1 == x ? "minute" : "minutes");
                            tsecs %= 60;
                        }
                        if (tsecs > 0 || 0 == sb.Length)
                        {
                            long x = tsecs / 1;
                            sb.AppendFormat(" {0} {1}", x, 1 == x ? "second" : "seconds");
                            tsecs %= 1;
                        }
                        status = "Execution completed in" + sb.ToString();
                    }

                    TSafe(new Action(delegate()
                        {
                            EnableEditMenutItems(true);
                            ShowStatus(status);
                            button1.Enabled = true;                           
                        }));                   
                }));

            thd.Start();            
        }

        private void SetGridWidths(DataTable schema)
        {
            foreach (DataRow row in schema.Rows)
            {
                string ctype = (string)row["DataTypeName"];
                int cordinal = (int)row["ColumnOrdinal"];
                int charcount = 0;

                switch (ctype)
                {
                    case "System.String":
                        {
                            int csize = (int)row["ColumnSize"];
                            charcount = (csize / 2) + 1;
                        }
                        break;

                    case "System.Int32":
                        charcount = 11;
                        break;

                    case "System.Int64":
                        charcount = 20;
                        break;

                    case "System.Double":
                        charcount = 20;
                        break;

                    case "System.DateTime":
                        charcount = 22;
                        break;

                    default:
                        break;
                }

                if (charcount > 50)
                {
                    charcount = 50;
                }

                //Set width of column.
                dgvResults.Columns[cordinal].Width = charcount * 8; //8 pixel per character.
            }
        }

        private void AddRow(string columnName, string columnValue)
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            dt.Columns.Add(columnName);
            ds.Tables.Add(dt);
            dt.Rows.Add(new object[] { columnValue });
            dgvResults.DataSource = ds.Tables[0];
            dgvResults.Columns[0].Width = dgvResults.Width - 50;
        }

        public void Save()
        {
            if (FileName != null && FileName.Length > 0)
            {
                System.IO.File.WriteAllText(FileName, txtQuery.Text);
            }
            else
            {
                SaveAs();
            }
        }

        public void SaveAs()
        {
            if (this.saveFileDialog1.ShowDialog() == DialogResult.OK)
            {
                FileName = this.saveFileDialog1.FileName;
                System.IO.File.WriteAllText(FileName, txtQuery.Text);
            }
        }

        public void OpenFile(string fn)
        {
            FileName = fn;
            string query = System.IO.File.ReadAllText(FileName);
            txtQuery.Text = query;
        }

        private void ClearResults()
        {           
            dgvResults.DataSource = null;
            dgvResults.Rows.Clear();
            dgvResults.Refresh();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            button1.Enabled = false;
            RunQuery();
        }

        int oldheight;

        private void Qa_Load(object sender, EventArgs e)
        {
            oldheight = this.Height;

            /* Keywords:
                STATEMENTS         0
                DATA_TYPES         1
                SYSTEM_TABLES      2
                GLOBAL_VARIABLES   3
                FUNCTIONS          4
                STORED_PROCEDURES  5
                OPERATORS          6
             * */
            txtQuery.Lexing.Keywords[0] = "GO AND OR UNION ALL INNERJOIN INPUT OUTPUT INNER JOIN LEFT RIGHT OUTER OUTERJOIN LIKE HELP CREATE TABLE INSERT INTO IMPORT IMPORTLINES VALUES SELECT TOP FROM WHERE ORDER BY UPDATE SET TRUNCATE DROP SHELL DELETE DISTINCT GROUP ON".ToLower();
            txtQuery.Lexing.Keywords[1] = "INT LONG DOUBLE CHAR DATETIME".ToLower();
#if DEBUG
            txtQuery.Lexing.Keywords[4] = "UPPER LOWER ROUND RAND MIN MAX".ToLower();
#endif

            //txtQuery.Styles[(int)MssqlStyles.IDENTIFIER].ForeColor = 
            txtQuery.Styles[(int)MssqlStyles.STRING].ForeColor = Color.Brown;
            //txtQuery.Styles[(int)MssqlStyles.NUMBER].ForeColor = Color.Black;
            txtQuery.Styles[(int)MssqlStyles.LINE_COMMENT].ForeColor = Color.DarkGreen;
            txtQuery.Styles[(int)MssqlStyles.STATEMENT].ForeColor = Color.Blue;
            txtQuery.Styles[(int)MssqlStyles.DATATYPE].ForeColor = Color.Navy;
            txtQuery.Styles[(int)MssqlStyles.FUNCTION].ForeColor = Color.CadetBlue;

        }

        private void copyCtrlCToolStripMenuItem_Click(object sender, EventArgs e)
        {
            dgvResults.Focus();
            this.Copy();
        }

        private void selectAllCtrlAToolStripMenuItem_Click(object sender, EventArgs e)
        {
            dgvResults.Focus();
            this.SelectAll();
        }

        private void Qa_Resize(object sender, EventArgs e)
        {
            int diff = this.Height - oldheight;
            txtQuery.Height += diff / 2;
            oldheight = this.Height;
        }
    }


    enum MssqlStyles
    {
        DEFAULT,
        COMMENT,
        LINE_COMMENT,
        NUMBER,
        STRING,
        OPERATOR,
        IDENTIFIER,
        VARIABLE,
        COLUMN_NAME,
        STATEMENT,
        DATATYPE,
        SYSTABLE,
        GLOBAL_VARIABLE,
        FUNCTION,
        STORED_PROCEDURE,
        DEFAULT_PREF_DATATYPE,
        COLUMN_NAME_2,

        _END
    }

}
