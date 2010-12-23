using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MySpace.DataMining.AELight.ConfigEditor
{
    public partial class ConfigEditor : Form
    {
        public ConfigEditor(string PrettyFile)
        {
#if DEBUG
            System.Threading.Thread.CurrentThread.Name = "ConfigEditor";
#endif

            Application.EnableVisualStyles();
            Application.DoEvents();

            this.PrettyFile = PrettyFile;

            InitializeComponent();

            this.Text = PrettyFile + " - Config Editor";

            string FileContents = null;
            try
            {
                System.IO.Stream dfss = new MySpace.DataMining.DistributedObjects.DfsStream(PrettyFile);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfss);
                FileContents = sr.ReadToEnd();
                sr.Close();
                dfss.Close();
            }
            catch (System.IO.FileNotFoundException)
            {
                IsNewFile = true;
            }

            if (IsNewFile)
            {
                string ctemp = "";
                {
                    string jobname = PrettyFile;
                    {
                        int ilslash = jobname.LastIndexOf('/'); // Cuts off dfs:// or a dir.
                        if (-1 != ilslash)
                        {
                            jobname = jobname.Substring(ilslash + 1);
                        }
                    }
                    {
                        int ildot = jobname.LastIndexOf('.');
                        if (-1 != ildot)
                        {
                            jobname = jobname.Substring(0, ildot);
                        }
                    }
                    ctemp = "";
                }
                Doc.Text = ctemp;
                Doc.Modified = true; // !

                SetStatus("New Jobs");
            }
            else
            {
                Doc.Text = FileContents;
                Doc.Modified = false; // !

                if (IsReadOnly)
                {
                    //Doc.IsReadOnly = IsReadOnly;
                    Doc.IsReadOnly = true;

                    SaveBtn.Enabled = false;

                    SetStatus("Read-only", 300);
                }
                else
                {
                    SetStatus("Open", 300);
                }
            }

            Doc.UndoRedo.EmptyUndoBuffer(); // !

        }

        public string PrettyFile;
        const bool IsReadOnly = false;
        bool IsNewFile = false;


        Timer stmr;

        protected void SetStatus(string s, int duration)
        {
            if (status.Visible)
            {
                status.Text = s;
            }
            else
            {
                //toolStripStatusLabel1.Text = s;
            }

            if (null != stmr)
            {
                stmr.Stop();
                stmr = null;
            }
            stmr = new Timer();
            stmr.Interval = duration;
            stmr.Tick += new EventHandler(statustmr_Tick);
            stmr.Start();
        }

        protected void SetStatus(string s)
        {
            SetStatus(s, 2000);
        }

        void statustmr_Tick(object sender, EventArgs e)
        {
            if (status.Visible)
            {
                status.Text = "";
            }
            else
            {
                //toolStripStatusLabel1.Text = "";
            }
            stmr.Stop();
            stmr = null;
        }


        private void ConfigEditor_Load(object sender, EventArgs e)
        {
            Doc.Select();
        }


        private void ConfigEditor_FormClosing(object sender, FormClosingEventArgs e)
        {

            if (IsReadOnly)
            {
                return;
            }

            if (DialogResult.Abort == DialogResult)
            {
                return;
            }

            if (Doc.Modified)
            {
                switch (MessageBox.Show(this, "This file was modified; save it before closing?", "Save?", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question))
                {
                    case DialogResult.Yes:
                        if (!DoSave())
                        {
                            // Don't close if it failed to save..
                            e.Cancel = true;
                        }
                        break;

                    case DialogResult.No:
                        break;

                    //case DialogResult.Cancel:
                    default:
                        e.Cancel = true;
                        return;
                }
            }
        }

        private void SaveBtn_Click(object sender, EventArgs e)
        {
            if (!DoSave())
            {
                // Don't close if it failed to save..
                return;
            }
            DialogResult = DialogResult.OK;
        }


        bool DoSave()
        {
            if (IsReadOnly)
            {
                SetStatus("Cannot save read-only");
                return false;
            }
            
            string DocText = Doc.Text;

            {
                try
                {
                    string fn = PrettyFile;
                    if (fn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        fn = fn.Substring(6);
                    }
                    dfs dc = Surrogate.ReadMasterDfsConfig();
                    string[] slaves = dc.Slaves.SlaveList.Split(';');
                    if (0 == slaves.Length || dc.Slaves.SlaveList.Length == 0)
                    {
                        throw new Exception("DFS SlaveList error (machines)");
                    }
                    Random rnd = new Random(DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    string newactualfilehost = slaves[rnd.Next() % slaves.Length];
                    string newactualfilename = dfs.GenerateZdFileDataNodeName(fn);
                    string myActualFile = Surrogate.NetworkPathForHost(newactualfilehost) + @"\" + newactualfilename;
                    {

                        byte[] smallbuf = new byte[4];
                        MySpace.DataMining.DistributedObjects.Entry.ToBytes(4, smallbuf, 0);
                        using (System.IO.FileStream fs = new System.IO.FileStream(myActualFile, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write))
                        {
                            fs.Write(smallbuf, 0, 4);
                            byte[] buf = Encoding.UTF8.GetBytes(Doc.Text);
                            fs.Write(buf, 0, buf.Length);
                        }
                        if (IsNewFile)
                        {
                            Console.Write(MySpace.DataMining.DistributedObjects.Exec.Shell(
                                "DSpace -dfsbind \"" + newactualfilehost + "\" \"" + newactualfilename + "\" \"" + fn + "\" " + DfsFileTypes.NORMAL
                                + " -h4"));
                        }
                        else
                        {
                            string tempdfsfile = fn + Guid.NewGuid().ToString() + dfs.TEMP_FILE_MARKER;
                            Console.Write(MySpace.DataMining.DistributedObjects.Exec.Shell(
                                "DSpace -dfsbind \"" + newactualfilehost + "\" \"" + newactualfilename + "\" \"" + tempdfsfile + "\" " + DfsFileTypes.NORMAL
                                + " -h4"));
                            MySpace.DataMining.DistributedObjects.Exec.Shell("DSpace swap \"" + tempdfsfile + "\" \"" + fn + "\"");
                            MySpace.DataMining.DistributedObjects.Exec.Shell("DSpace delete \"" + tempdfsfile + "\"", true); // suppresserrors=true
                        }
                    }
                    //ActualFile = myActualFile; // Only update this when fully committed to DFS!
                    IsNewFile = false;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    MessageBox.Show(this, "Unable to save new file to DFS:\r\n\r\n" + e.Message, "Save-New Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return false; // Important! don't continue with any of the rest if this fails!
                }
            }

            Doc.Modified = false; // !
            return true;
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DoSave();
        }

        private void aboutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            string atxt;
            try
            {
                atxt = System.IO.File.ReadAllText(
                    System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
                    + @"\licenses_and_attributions.txt");
            }
            catch (Exception aex)
            {
                MessageBox.Show(this, aex.Message, "Unable to load about-information!", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            Form af = new Form();
            TextBox atb = new TextBox();

            af.Text = "About";
            af.Icon = this.Icon;
            af.Size = new Size(760, 520);
            af.StartPosition = FormStartPosition.CenterParent;
            af.MinimizeBox = false;
            af.MaximizeBox = false;
            af.Load += new EventHandler(delegate(object sender2, EventArgs e2) { atb.Select(0, 0); });

            atb.Parent = af;
            atb.Font = new Font(FontFamily.GenericMonospace, 10);
            atb.ScrollBars = ScrollBars.Vertical;
            atb.Dock = DockStyle.Fill;
            atb.ReadOnly = true;
            atb.Multiline = true;
            atb.Text = atxt;

            af.ShowDialog(this);
            af.Dispose();
        }


    }


    public class ConfigEdit
    {

        public static string RealUserName = Environment.UserName;


        public static bool CheckConfigFileSize(string PrettyFile, long FileSize)
        {
            if (FileSize >= 0x400 * 0x400 * 50)
            {
                if (DialogResult.Yes != MessageBox.Show("DFS file '" + PrettyFile + "' is " + Surrogate.GetFriendlyByteSize(FileSize)
                    + Environment.NewLine + "Do you still wish to load it?", "Large config file", MessageBoxButtons.YesNo))
                {
                    return false;
                }
            }
            return true;
        }


        public static void RunConfigEditor(string PrettyFile)
        {
            ConfigEditor cedit = new ConfigEditor(PrettyFile);
            cedit.ShowDialog();
            cedit.Dispose();
        }

    }

}
