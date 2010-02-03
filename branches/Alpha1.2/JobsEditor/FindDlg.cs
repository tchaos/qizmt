/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MySpace.DataMining.AELight
{
    public partial class FindDlg : Form
    {
        public FindDlg()
        {
            InitializeComponent();
        }


        public void DismissFindDlg()
        {
            if (null != Owner)
            {
                Owner.Activate();
            }
            Hide();
        }


        private void CancelBtn_Click(object sender, EventArgs e)
        {
            DismissFindDlg();
        }

        private void FindDlg_Load(object sender, EventArgs e)
        {

        }

        private void FindDlg_FormClosing(object sender, FormClosingEventArgs e)
        {
            e.Cancel = true;
            DismissFindDlg();
        }
    }
}
