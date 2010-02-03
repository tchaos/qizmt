using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace MySpace.DataMining.DistributedObjects.SeqFileView
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
            SeqFileViewForm sfvf = new SeqFileViewForm();
            sfvf.Show();
            if (args.Length > 0)
            {
                sfvf.AskOpenFile(args[0]);
            }
            Application.Run(sfvf);
        }
    }
}
