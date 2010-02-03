using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace Graphing
{
	public partial class Message : Form
	{
		public Message()
		{
			InitializeComponent();
		}

		public void SetMessage(string msg)
		{
			lblMsg.Text = msg;
		}
	}
}
