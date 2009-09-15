using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace Graphing
{
	static class Program
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main(string[] args)
		{
			Application.EnableVisualStyles();
			Application.SetCompatibleTextRenderingDefault(false);
			string filePath = "" ;
			int gridCount = 10;
			bool suppressInitialCentroids = false;			
			string msg = null;

			if (args != null && args.Length > 0)
			{
				filePath = args[0];

				for (int i = 1; i < args.Length; i++)
				{
					if (args[i] == "-s")
						suppressInitialCentroids = true;
					else
					{
						if (args[i] == "-g")
						{
							if (i == args.Length - 1)
								msg = "Grid count is expected.";
							else
								gridCount = Convert.ToInt32(args[i + 1]);							
						}
					}
				}				
			}
			else
			{
				msg = Graph.GetUsage();					
			}

			if(msg == null)
				Application.Run(new Graph(filePath, gridCount, suppressInitialCentroids));
			else
			{
				Graphing.Message frmMsg = new Graphing.Message();
				frmMsg.SetMessage(msg);
				Application.Run(frmMsg);
			}
		}
	}
}
