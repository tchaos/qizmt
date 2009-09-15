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

        public void RunQuery()
        {
            ClearResults();

            string query = txtQuery.Text.Trim();

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
                substr = substr.Replace(Environment.NewLine, " ");
                cleanedQuery += substr + lit.Value;

                prevIndex = lit.Index + lit.Length;
            }

            if (prevIndex < query.Length)
            {
                cleanedQuery += query.Substring(prevIndex).Replace(Environment.NewLine, " ");
            }

            if (cleanedQuery.Length == 0)
            {
                MessageBox.Show("Query is empty.");
                return;
            }

            button1.Enabled = false;          

            ShowStatus("Executing query...");

            string status = "";
            DbConnection conn = null;
            try
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                conn = fact.CreateConnection();
                conn.ConnectionString = ConnectionString;
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = cleanedQuery;

                if (cleanedQuery.StartsWith("select", StringComparison.OrdinalIgnoreCase) ||
                    cleanedQuery.StartsWith("shell", StringComparison.OrdinalIgnoreCase)||
                    cleanedQuery.StartsWith("rselect", StringComparison.OrdinalIgnoreCase))
                {
                    DbDataReader reader = cmd.ExecuteReader();

                    DataSet ds = new DataSet();
                    DataTable dt = new DataTable();

                    //Get column meta data.
                    int columnCount = reader.FieldCount;
                    for (int i = 0; i < columnCount; i++)
                    {
                        dt.Columns.Add(reader.GetName(i));
                    }

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
                    dgvResults.DataSource = ds.Tables[0];
                    SetGridWidths(schema);
                }
                else
                {
                    cmd.ExecuteNonQuery();
                    AddRow("Query results", "Completed");
                }
                status = "Execution completed";   
            }
            catch (Exception e)
            {
                AddRow("Exception Details", e.ToString());
                status = "Exceptions occurred";
            }
            finally
            {
                try
                {
                    conn.Close();  
                }
                catch(Exception e)
                {
                    AddRow("Exception Details", e.ToString());
                    status = "Exceptions occurred";
                }
            }

            EnableEditMenutItems(true);
            ShowStatus(status);
            button1.Enabled = true;
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
            button1.Enabled = true;
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
            txtQuery.Lexing.Keywords[0] = "AND OR UNION ALL INNERJOIN INPUT OUTPUT INNER JOIN LEFT OUTERJOIN LIKE HELP CREATE TABLE INSERT INTO IMPORT IMPORTLINES VALUES SELECT TOP FROM WHERE ORDER BY UPDATE SET TRUNCATE DROP INT LONG DOUBLE CHAR SHELL DELETE".ToLower();
            
            //txtQuery.Styles[(int)MssqlStyles.IDENTIFIER].ForeColor = 
            txtQuery.Styles[(int)MssqlStyles.STRING].ForeColor = Color.Brown;
            //txtQuery.Styles[(int)MssqlStyles.NUMBER].ForeColor = Color.Black;
            txtQuery.Styles[(int)MssqlStyles.LINE_COMMENT].ForeColor = Color.DarkGreen;
            txtQuery.Styles[(int)MssqlStyles.STATEMENT].ForeColor = Color.Blue;

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
