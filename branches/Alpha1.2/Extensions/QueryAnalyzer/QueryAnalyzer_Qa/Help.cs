using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Xml;

namespace RDBMS_qa
{
    public partial class Help : Form
    {
        private XmlNodeList cmdNodes = null;
        private string[] cmdKeys = null;

        public Help(XmlNodeList commandNodes, string[] commandKeys)
        {
            InitializeComponent();
            cmdNodes = commandNodes;
            cmdKeys = commandKeys;
        }

        private void btnSearch_Click(object sender, EventArgs e)
        {
            richTextBox1.Text = "";                        
            string txt = txtSearch.Text.Trim();
            if (txt.Length > 0)
            {
                for (int i = 0; i < cmdKeys.Length; i++)
                {
                    if (cmdKeys[i].IndexOf(txt, StringComparison.OrdinalIgnoreCase) > -1)
                    {
                        ShowManual(i);
                    }
                }
            }
        }

        private void ShowManual(int cmdIndex)
        {
            string cmdName = cmdKeys[cmdIndex];
            XmlNode cmdNode = cmdNodes[cmdIndex];
            string desc = cmdNode["description"].InnerText;
            string usage = cmdNode["usage"].InnerText;
            string ex1 = cmdNode["example1"].InnerText;
            string ex2 = cmdNode["example2"].InnerText;
            string tab = "       ";
            string fontfamily = "Arial";
            float fontsize = 8.25f;

            {
                Font font = new Font(fontfamily, fontsize + 2f, FontStyle.Bold);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = cmdName;
            }

            richTextBox1.SelectionColor = Color.Black;
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + "Description:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + tab + desc;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + "Usage:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + tab + usage;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + "Example 1:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + tab + ex1;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + "Example 2:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                richTextBox1.SelectionFont = font;
                richTextBox1.SelectedText = Environment.NewLine + tab + ex2 + Environment.NewLine + Environment.NewLine;
            }
        }

        private void txtSearch_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter && txtSearch.Text.Trim().Length > 0)
            {
                btnSearch_Click(sender, e);
            }
        }

        private void txtSearch_TextChanged(object sender, EventArgs e)
        {
            btnSearch.Enabled = (txtSearch.Text.Trim().Length > 0);
        }
    }
}
