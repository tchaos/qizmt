using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace RDBMS_qa
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
#if DEBUG
            AutoConnectAtStartup = (args.Length > 0 && "/c" == args[0]);
#endif
            try
            {
                _FixSciLexDLL();
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message, "Editor Init Failure", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }
            Application.Run(new Form1());
        }


#if DEBUG
        internal static bool AutoConnectAtStartup = false;
#endif


        static void _FixSciLexDLL()
        {
            if (IntPtr.Size > 4)
            {
                string thisdir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                string dll = thisdir + @"\SciLexer.dll";
                string dll64 = thisdir + @"\SciLexer64.dll";
                string dll32 = thisdir + @"\SciLexer32.dll";
                if (System.IO.File.Exists(dll64))
                {
                    if (System.IO.File.Exists(dll32))
                    {
                        try
                        {
                            System.IO.File.Delete(dll);
                        }
                        catch
                        {
                        }
                    }
                    else
                    {
                        System.IO.File.Move(dll, dll32);
                    }
                    System.IO.File.Move(dll64, dll);
                }
            }
        }

    }
}
