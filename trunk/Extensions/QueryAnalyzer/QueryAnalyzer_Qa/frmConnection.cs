using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace RDBMS_qa
{
    public partial class frmConnection : Form
    {
        private string connstr = "";

        public frmConnection()
        {
            InitializeComponent();
        }

        private void Connection_Load(object sender, EventArgs e)
        {
            if (ConnectionString.Length == 0)
            {
                ConnectionString = "Data Source = localhost," + System.Net.Dns.GetHostName() + "; Batch Size = 64MB; MR.DFS Block Size = 16MB";
            }
            txtConnStr.Text = ConnectionString;
        }

        private bool TestConnection(string cs, out string reason)
        {
            reason = "";
            try
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                conn.ConnectionString = txtConnStr.Text.Trim();
                conn.Open();
                conn.Close();
                return true;
            }
            catch(Exception e)
            {
                reason = e.ToString();
                return false;
            }
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            if (!CheckConnstr(txtConnStr.Text))
            {
                this.DialogResult = DialogResult.Cancel;
                MessageBox.Show("Connection string cannot be empty.");                
                return;
            }

            string reason = "";
            if (TestConnection(txtConnStr.Text, out reason))
            {
                ConnectionString = txtConnStr.Text.Trim();
                Close();
            }
            else
            {
                this.DialogResult = DialogResult.Cancel;    
                MessageBox.Show("Failed to connect.");                            
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
                if (CheckConnstr(value))
                {
                    connstr = value;
                }
                else
                {
                    MessageBox.Show("Connection string cannot be empty.");
                }
            }
        }

        private bool CheckConnstr(string cs)
        {
            return (cs != null && cs.Trim().Length > 0);
        }

        private void btnTest_Click(object sender, EventArgs e)
        {
            if (!CheckConnstr(txtConnStr.Text))
            {
                MessageBox.Show("Connection string cannot be empty.");
                return;
            }

            string reason = "";
            if (TestConnection(txtConnStr.Text, out reason))
            {
                MessageBox.Show("Connection tested successfully.");
            }
            else
            {
                MessageBox.Show("Failed to connect.");
            }
        }   
    }
}
