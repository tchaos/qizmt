using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MySpace.DataMining.DistributedObjects.SeqFileView
{
    public partial class SelOpenDialog : Form
    {
        public SelOpenDialog()
        {
            InitializeComponent();
        }


        private void ButtonBrowse_Click(object sender, EventArgs e)
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.Filter = "All Files (*.*)|*.*|DistributedObjects Files|*.txt;*.tx_;*._xt;*.zb;*.processed;*.exception";
            if (DialogResult.OK == ofd.ShowDialog(this))
            {
                TextBoxFile.Text = ofd.FileName;
            }
        }

        private void ButtonOpen_Click(object sender, EventArgs e)
        {
            if (!System.IO.File.Exists(TextBoxFile.Text))
            {
                MessageBox.Show("File does not exist");
                return;
            }
            DialogResult = DialogResult.OK;
        }

    }
}
