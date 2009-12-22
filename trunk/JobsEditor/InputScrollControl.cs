using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MySpace.DataMining.AELight
{
    public partial class InputScrollControl : UserControl
    {
        ToolTip tt;


        public InputScrollControl()
        {
            tt = new ToolTip();
            InitializeComponent();
            InputScroll.MouseDown += new MouseEventHandler(InputScroll_MouseDown);
        }

        void InputScroll_MouseDown(object sender, MouseEventArgs e)
        {
            InputScroll.Select();
        }

        private void InputScroll_Scroll(object sender, ScrollEventArgs e)
        {
            //InputScroll
        }

        private void CurrentInputBox_TextChanged(object sender, EventArgs e)
        {
            tt.SetToolTip(CurrentInputBox, "Input: " + CurrentInputBox.Text);
        }

        private void InputScrollControl_Load(object sender, EventArgs e)
        {

        }
    }
}
