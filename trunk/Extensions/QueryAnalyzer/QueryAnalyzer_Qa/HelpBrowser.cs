using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Xml;
using System.Windows.Forms;

namespace RDBMS_qa
{
    public partial class HelpBrowser : Form
    {
        private XmlNodeList cmdNodes = null;

        public HelpBrowser(XmlNodeList commandNodes)
        {
            InitializeComponent();
            cmdNodes = commandNodes;
            LoadTree();
        }

        private void LoadTree()
        {
            TreeNode root = tvTopic.Nodes.Add("Topic");
            for (int i = 1; i < cmdNodes.Count; i++)
            {
                XmlNode node = cmdNodes[i];
                string keyword = node["keyword"].InnerText;               
                TreeNode topic = root.Nodes.Add(keyword);
            }
        }

        private void tvTopic_NodeMouseClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Node.Level == 0)
            {
                return;
            }
            txtInfo.Text = "";
            string cmdName = e.Node.Text;            
            XmlNode cmdNode = cmdNodes[e.Node.Index + 1];
            string desc = cmdNode["description"].InnerText;
            string usage = cmdNode["usage"].InnerText;
            string ex1 = cmdNode["example1"].InnerText;
            string ex2 = cmdNode["example2"].InnerText;
            string tab = "       ";
            string fontfamily = "Arial";
            float fontsize = 8.25f;

            {
                Font font = new Font(fontfamily, fontsize + 2f, FontStyle.Bold);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = cmdName;
            }

            txtInfo.SelectionColor = Color.Black;
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + "Description:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + tab + desc;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + "Usage:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + tab + usage;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + "Example 1:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + tab + ex1;
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Bold);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + "Example 2:";
            }
            {
                Font font = new Font(fontfamily, fontsize, FontStyle.Regular);
                txtInfo.SelectionFont = font;
                txtInfo.SelectedText = Environment.NewLine + tab + ex2 + Environment.NewLine + Environment.NewLine;
            }
        }
    }
}
