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
    public partial class CharactersForm : Form
    {
        public CharactersForm()
        {
            InitializeComponent();
        }

        private void pic_Click(object sender, EventArgs e)
        {
            Hide();
        }

        private void CharactersForm_Deactivate(object sender, EventArgs e)
        {
            Hide();
        }

        private void CharactersForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            e.Cancel = true;
            Hide();
        }
    }
}
