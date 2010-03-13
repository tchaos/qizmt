using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Xml;
using System.Windows.Forms;

namespace RDBMS_qa
{
    public partial class Form1 : Form
    {
        internal string connstr = "";
        private string datasource = "";
        private Dictionary<string, Dictionary<string, string>> tableStatements;
        private Dictionary<string, string> sysStatements;
        private string tab = "    ";
        const int defaultTreeViewWidth = 300;
        private XmlNodeList cmdNodes = null;
        private string[] cmdKeys = null;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            this.WindowState = FormWindowState.Maximized;
            sysStatements = new Dictionary<string, string>();
            sysStatements["SYS.Tables"] = "SELECT *\r\nFROM Sys.Tables";
            sysStatements["SYS.Help"] = "SELECT *\r\nFROM Sys.Help";
            sysStatements["SYS.Indexes"] = "SELECT *\r\nFROM Sys.Indexes";
            sysStatements["MRDFS.Help"] = "SHELL 'DSpace'";
            sysStatements["MRDFS.Users"] = "SHELL 'DSpace who'";
            sysStatements["MRDFS.CPU"] = "SHELL 'DSpace perfmon cputime'";
            sysStatements["MRDFS.DiskIO"] = "SHELL 'DSpace perfmon diskio'";
            sysStatements["MRDFS.Network"] = "SHELL 'DSpace perfmon network'";
            sysStatements["MRDFS.Memory"] = "SHELL 'DSpace perfmon availablememory'";
            sysStatements["MRDFS.DistributedFiles"] = "SHELL 'DSpace dir'";
            sysStatements["MRDFS.RunningJobs"] = "SHELL 'DSpace ps'";
            sysStatements["MRDFS.History"] = "SHELL 'DSpace history 1000'";
            sysStatements["MRDFS.Info"] = "SHELL 'DSpace info'";
            sysStatements["MRDFS.Health"] = "SHELL 'DSpace health'";
            sysStatements["MRDFS.InstallDir"] = "SHELL 'DSpace listinstalldir'";
            sysStatements["MRDFS.Temperature"] = "SHELL 'DSpace cputemp'";
            sysStatements["MRDFS.Ghost"] = "SHELL 'DSpace ghost'";
            sysStatements["MRDFS.Log"] = "SHELL 'DSpace viewlog'";
            
            //uncomment to display a MDI sub window without connecting
            //ShowNewQaForm("SELECT TOP 10 * FROM foo WHERE base='all' ORDER BY bar;");
        }

        private void ShowStatus(string status)
        {
            this.toolStripStatusLabel2.Text = status;
            this.statusStrip1.Refresh();
        }

        private void LoadTreeView()
        {
            //ShowStatus("Loading table information...");
            ShowStatus("Connecting to database...");
            tvTables.Nodes.Clear();

            string rootName = datasource;
            if (rootName.Length > 10)
            {
                rootName = rootName.Substring(0, 10) + "...";
            }

            TreeNode root = tvTables.Nodes.Add(rootName);
            root.ImageIndex = 0;
            root.SelectedImageIndex = 0;
            root.ContextMenuStrip = cmsConn;

            TreeNode tableFolder = root.Nodes.Add("Tables");
            tableFolder.ImageIndex = 1;
            tableFolder.SelectedImageIndex = 1;
            tableFolder.ContextMenuStrip = cmsTables;

            DbConnection conn = null;
            try
            {
                //Query all table information.
                string sxml = "";
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("Qizmt_DataProvider");
                conn = fact.CreateConnection();
                conn.ConnectionString = connstr;
                conn.Open();
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SYSTABLESXML";
                DbDataReader reader = cmd.ExecuteReader();  
                while (reader.Read())
                {
                    sxml += reader.GetString(0).Trim();
                }
                reader.Close();

                if (sxml.Length == 0)
                {
                    MessageBox.Show("No table information is returned.");
                }
                else
                {
                    XmlDocument doc = new XmlDocument();
                    doc.LoadXml(sxml);

                    tableStatements = new Dictionary<string, Dictionary<string, string>>();

                    //Get all tables
                    XmlNodeList tables = doc.SelectNodes("//table");
                    foreach (XmlNode table in tables)
                    {
                        string tname = table["name"].InnerText;

                        //Get all columns of this table.
                        XmlNodeList columns = table.SelectNodes("column");

                        //Generate sample statements for this table.
                        Dictionary<string, string> statements = new Dictionary<string, string>();

                        Random rnd = new Random();

                        {
                            string st = "INSERT INTO " + tname + "\r\nVALUES\r\n(\r\n";
                            for (int ni = 0; ni < columns.Count; ni++)
                            {
                                XmlNode column = columns[ni];
                                string cname = column["name"].InnerText;
                                string ctype = column["type"].InnerText;
                                st += tab + GenerateColumnValue(rnd, ctype);
                                if (ni < columns.Count - 1)
                                {
                                    st += ",";
                                }
                                st += "\r\n";
                            }
                            st += ")";
                            statements["INSERT INTO"] = st;
                        }

                        statements["INSERT IMPORT"] = "INSERT INTO " + tname + "\r\nIMPORT 'dfs://data.txt'";
                        statements["INSERT BIND"] = "INSERT INTO " + tname + "\r\nBIND 'dfs://DbData'";
                        statements["INSERT IMPORTLINES"] = "INSERT INTO " + tname + "\r\nIMPORTLINES 'dfs://data.txt' DELIMITER '/'";
                        
                        {
                            string st = "SELECT *\r\nFROM " + tname + "\r\nWHERE ";

                            //filter by the first column
                            string cname = columns[0]["name"].InnerText;
                            string ctype = columns[0]["type"].InnerText;
                            st += cname + " = " + GenerateColumnValue(rnd, ctype) + "\r\n";

                            statements["SELECT WHERE"] = st;
                        }

                        statements["INSERT INTO SELECT"] = "INSERT INTO " + tname + "\r\nSELECT TOP 2 * FROM " + tname;
                        statements["SELECT ORDER BY"] = "SELECT * \r\nFROM " + tname + "\r\nORDER BY " + columns[0]["name"].InnerText;
                        statements["SELECT FROM"] = "SELECT * \r\nFROM " + tname;
                        statements["SELECT TOP"] = "SELECT TOP 10 * \r\nFROM " + tname; 

                        //UPDATE
                        {
                            string st = "UPDATE " + tname + "\r\nSET ";

                            //update the second column
                            XmlNode updatecolumn = columns.Count > 1 ? columns[1] : columns[0];
                            string cname = updatecolumn["name"].InnerText;
                            string ctype = updatecolumn["type"].InnerText;
                            st += cname + " = " + GenerateColumnValue(rnd, ctype);
                            statements["UPDATE SET"] = st;

                            //filter by the first column
                            XmlNode filtercolumn = columns[0];
                            cname = filtercolumn["name"].InnerText;
                            ctype = filtercolumn["type"].InnerText;
                            st += "\r\nWHERE " + cname + " = " + GenerateColumnValue(rnd, ctype);                            
                            statements["UPDATE WHERE"] = st;
                        }

                        // GROUP BY
                        {
                            string cnameaggr = columns[0]["name"].InnerText;
                            string cnamegby;
                            string selstr;
                            if (columns.Count > 1)
                            {
                                cnamegby = columns[1]["name"].InnerText;
                                selstr = "MIN(" + cnameaggr + ")," + cnamegby;
                            }
                            else
                            {
                                cnamegby = cnameaggr;
                                selstr = cnamegby;
                            }
                            statements["GROUP BY"] = "SELECT TOP 10\r\n" + selstr + " \r\n" + "FROM " + tname + "\r\n" + "GROUP BY " + cnamegby;
                        }

                        {
                            string st = "DELETE FROM " + tname + "\r\nWHERE ";                           
                            st += columns[0]["name"].InnerText + " = " + GenerateColumnValue(rnd, columns[0]["type"].InnerText);
                            statements["DELETE FROM"] = st;
                        }

                        //statements["CREATE TABLE"] = "CREATE TABLE colors\r\n(\r\n" + tab + "color CHAR(50),\r\n" + tab + "red INT,\r\n" + tab + "green INT,\r\n" + tab + "blue INT\r\n)";
                        {
                            StringBuilder ct = new StringBuilder();
                            ct.Append("CREATE TABLE ");
                            ct.Append(tname);
                            ct.Append("\r\n(");
                            int ncol = 0;
                            foreach (XmlNode xn in columns)
                            {
                                if (0 != ncol++)
                                {
                                    ct.Append(',');
                                }
                                ct.Append("\r\n");
                                ct.Append(tab);
                                ct.Append(xn["name"].InnerText);
                                ct.Append(' ');
                                ct.Append(xn["type"].InnerText.ToUpper());
                            }
                            ct.Append("\r\n)");
                            statements["CREATE TABLE"] = ct.ToString();
                        }
                        statements["TRUNCATE TABLE"] = "TRUNCATE TABLE " + tname;
                        statements["DROP TABLE"] = "DROP TABLE " + tname;

                        tableStatements[tname] = statements;
                        TreeNode treenode = tableFolder.Nodes.Add(tname);
                        treenode.ContextMenuStrip = cmsTable;
                    }

                    //Load help information.
                    {
                        string xml = "";
                        DbCommand hcmd = conn.CreateCommand();
                        hcmd.CommandText = "select * from sys.help";
                        DbDataReader hreader = hcmd.ExecuteReader();
                        while (hreader.Read())
                        {
                            xml += hreader.GetString(0);
                        }
                        hreader.Close();

                        XmlDocument usageXml = new XmlDocument();
                        usageXml.LoadXml(xml);
                        cmdNodes = usageXml.SelectNodes("//command");

                        XmlNodeList cmds = usageXml.SelectNodes("//command/keyword");
                        cmdKeys = new string[cmds.Count];
                        for (int i = 0; i < cmdKeys.Length; i++)
                        {
                            cmdKeys[i] = cmds[i].InnerText;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                MessageBox.Show("Error while loading table information: " + e.ToString());
            }
            finally
            {
                conn.Close();
            }

            TreeNode sysFolder = root.Nodes.Add("System");
            sysFolder.ImageIndex = 1;
            sysFolder.SelectedImageIndex = 1;

            foreach (string key in sysStatements.Keys)
            {
                TreeNode systable = sysFolder.Nodes.Add(key);
                systable.ContextMenuStrip = cmsSystem;
            }

            tvTables.ExpandAll();
            ShowStatus("");
        }

        private string GenerateColumnValue(Random rnd, string columnTypeName)
        {            
            string type = columnTypeName.ToLower();
            if (type.StartsWith("char("))
            {
                type = "char";
            }

            string columnValue = "";
            switch (type)
            {
                case "int":
                    columnValue = rnd.Next().ToString();
                    break;

                case "char":
                    {
                        int ichar = rnd.Next(97, 122);
                        columnValue = "'" + (char)ichar + "'";
                    }                    
                    break;

                case "datetime":                    
                    columnValue = "'" + DateTime.Now.ToString() + "'";                    
                    break;

                case "double":
                    columnValue = rnd.NextDouble().ToString();
                    break;

                case "long":
                    columnValue = DateTime.Now.Ticks.ToString();
                    break;

                default:
                    //throw new Exception("Column type not supported.");
                    columnValue = "???";
                    break;
            }
            return columnValue;
        }

        private void tvTables_NodeMouseClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            tvTables.SelectedNode = e.Node;
        }

        private void cmsTable_ItemClicked(object sender, ToolStripItemClickedEventArgs e)
        {
            string selectedTableName = tvTables.SelectedNode.Text;            
            string itemText = e.ClickedItem.Text.ToUpper();
            string query = tableStatements[selectedTableName][itemText];           
            ShowNewQaForm(query);            
        }

        private Qa ShowNewQaForm(string query)
        {
            Qa newform = new Qa();
            newform.Icon = this.Icon;
            newform.FormClosed += new FormClosedEventHandler(newform_FormClosed);
            newform.ConnectionString = connstr;
            newform.QueryText = query;           
            newform.MdiParent = this;
            newform.Show();
            EnableSaveMenuItems(true);
            EnableEditMenuItems(true);
            return newform;
        }

        void newform_FormClosed(object sender, FormClosedEventArgs e)
        {
            if (MdiChildren.Length == 1)
            {
                EnableSaveMenuItems(false);
                EnableEditMenuItems(false);
            }            
        }

        private void EnableSaveMenuItems(bool enabled)
        {
            saveAsToolStripMenuItem.Enabled = enabled;
            saveToolStripMenuItem.Enabled = enabled;
            runF5ToolStripMenuItem.Enabled = enabled;
        }

        private void openToolStripMenuItem_Click(object sender, EventArgs e)
        {
            frmConnection frm = new frmConnection();
            frm.Owner = this;
            frm.Icon = this.Icon;
            if (connstr.Length > 0)
            {
                frm.ConnectionString = connstr;
            }            

            if (frm.ShowDialog() == DialogResult.OK)
            {
                connstr = frm.ConnectionString;               

                //Get Data Source names.
                datasource = "";
                string[] parts = connstr.Trim().Trim(';').Split(';');
                foreach (string part in parts)
                {
                    int del = part.IndexOf('=');
                    string name = "";
                    string val = "";
                    if (del > -1)
                    {
                        name = part.Substring(0, del).Trim();
                        val = part.Substring(del + 1).Trim();
                    }
                    if (name.ToLower() == "data source")
                    {
                        datasource = val;
                    }
                }
                LoadTreeView();

                //Close all open query analyzers since they are still pointing to the old connection string.
                foreach (Form child in MdiChildren)
                {
                    child.Close();
                }

                //Open a blank query analyzer.
               ShowNewQaForm("");
            }

            bool enabled = connstr.Length > 0;
            this.newToolStripMenuItem.Enabled = enabled;
            this.openToolStripMenuItem1.Enabled = enabled;
            this.refreshToolStripMenuItem1.Enabled = enabled;
            searchToolStripMenuItem.Enabled = enabled;
            browseToolStripMenuItem.Enabled = enabled;
        }

        private void newToolStripMenuItem_Click(object sender, EventArgs e)
        {
            ShowNewQaForm("");
        }

        private void openToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            if (this.openFileDialog1.ShowDialog() == DialogResult.OK)
            {
                Qa newform = ShowNewQaForm("");               
                newform.OpenFile(this.openFileDialog1.FileName);
            }
        }

        private void refreshToolStripMenuItem_Click(object sender, EventArgs e)
        {
            LoadTreeView();
        }

        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa activeqa = (Qa)this.ActiveMdiChild;
            activeqa.Save();
        }

        private void saveAsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa activeqa = (Qa)this.ActiveMdiChild;
            activeqa.SaveAs();
        }

        private void runF5ToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa activeqa = (Qa)this.ActiveMdiChild;
            activeqa.RunQuery();
        }

        private void tvTables_NodeMouseHover(object sender, TreeNodeMouseHoverEventArgs e)
        {
            if (e.Node.Level == 0)
            {
                e.Node.ToolTipText = datasource;
            }
        }

        private void sELECTFROMToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            string selectedTableName = tvTables.SelectedNode.Text;
            string query = sysStatements[selectedTableName];
            ShowNewQaForm(query);     
        }

        private void createTableToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            ShowNewQaForm("CREATE TABLE colors\r\n(\r\n" + tab + "color CHAR(50),\r\n" + tab + "red INT,\r\n" + tab + "green INT,\r\n" + tab + "blue INT\r\n)");   
        }

        private void copyCtrlCToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa frm = (Qa)ActiveMdiChild;
            frm.Copy();
        }

        private void EnableEditMenuItems(bool enabled)
        {
            copyCtrlCToolStripMenuItem.Enabled = enabled;
            pasteCtrlvToolStripMenuItem.Enabled = enabled;
            selectAllCtrlAToolStripMenuItem.Enabled = enabled;
        }

        private void pasteCtrlvToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa frm = (Qa)ActiveMdiChild;
            frm.Paste();
        }

        private void selectAllCtrlAToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Qa frm = (Qa)ActiveMdiChild;
            frm.SelectAll();
        }

        private void searchToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Help helpForm = new Help(cmdNodes, cmdKeys);
            helpForm.Show();
            helpForm.Focus();
        }

        private void browseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            HelpBrowser helpBrowser = new HelpBrowser(cmdNodes);
            helpBrowser.Show();
            helpBrowser.Focus();
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }
    }
}
