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

namespace QizmtEC2
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();

            addbrowsetextboxes(this);
        }


        Button _curbrowsebtn;
        TextBox _curbrowsebtntextbox;

        void _browsebtnFileSystem_gotfocus(Object sender, EventArgs ea)
        {
            if (null != _curbrowsebtn)
            {
                _curbrowsebtn.Dispose();
                _curbrowsebtn = null;
            }
            TextBox tb = (TextBox)sender;
            _curbrowsebtntextbox = tb;
            _curbrowsebtn = new Button();
            _curbrowsebtn.Text = "...";
            _curbrowsebtn.TabIndex = tb.TabIndex + 1;
            _curbrowsebtn.Bounds = new Rectangle(tb.Right + 2, tb.Top, 24, tb.Height);
            _curbrowsebtn.Parent = tb.Parent;
            _curbrowsebtn.Click += new EventHandler(_browsebtnFileSystem_browse);
        }

        void _browsebtnFileSystem_browse(Object sender, EventArgs ea)
        {
            OpenFileDialog fd = new OpenFileDialog();
            {
                Label label = _curbrowsebtntextbox.Parent.GetNextControl(_curbrowsebtntextbox, false) as Label;
                if (null != label)
                {
                    fd.Title = label.Text;
                }
            }
            fd.FileName = _curbrowsebtntextbox.Text;
            if (DialogResult.OK == fd.ShowDialog(this))
            {
                _curbrowsebtntextbox.Text = fd.FileName;
            }
        }

        void _browsebtnFileSystemDirectories_gotfocus(Object sender, EventArgs ea)
        {
            if (null != _curbrowsebtn)
            {
                _curbrowsebtn.Dispose();
                _curbrowsebtn = null;
            }
            TextBox tb = (TextBox)sender;
            _curbrowsebtntextbox = tb;
            _curbrowsebtn = new Button();
            _curbrowsebtn.Text = "...";
            _curbrowsebtn.TabIndex = tb.TabIndex + 1;
            _curbrowsebtn.Bounds = new Rectangle(tb.Right + 2, tb.Top, 24, tb.Height);
            _curbrowsebtn.Parent = tb.Parent;
            _curbrowsebtn.Click += new EventHandler(_browsebtnFileSystemDirectories_browse);
        }

        void _browsebtnFileSystemDirectories_browse(Object sender, EventArgs ea)
        {
            FolderBrowserDialog fd = new FolderBrowserDialog();
            {
                Label label = _curbrowsebtntextbox.Parent.GetNextControl(_curbrowsebtntextbox, false) as Label;
                if (null != label)
                {
                    fd.Description = label.Text;
                }
            }
            fd.ShowNewFolderButton = true;
            fd.SelectedPath = _curbrowsebtntextbox.Text;
            if (DialogResult.OK == fd.ShowDialog(this))
            {
                _curbrowsebtntextbox.Text = fd.SelectedPath;
            }
        }

        void addbrowsetextboxes(Control cparent)
        {
            foreach (Control ctrl in cparent.Controls)
            {
                if (ctrl.HasChildren)
                {
                    addbrowsetextboxes(ctrl);
                }
                else
                {
                    TextBox tb = ctrl as TextBox;
                    if (null != tb)
                    {
                        if (tb.AutoCompleteMode != AutoCompleteMode.None)
                        {
                            switch (tb.AutoCompleteSource)
                            {
                                case AutoCompleteSource.FileSystem:
                                    {
                                        tb.GotFocus += new EventHandler(_browsebtnFileSystem_gotfocus);
                                    }
                                    break;
                                case AutoCompleteSource.FileSystemDirectories:
                                    {
                                        tb.GotFocus += new EventHandler(_browsebtnFileSystemDirectories_gotfocus);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }
        }


        private void PrevButton_Click(object sender, EventArgs e)
        {
            int selidx = this.tc.SelectedIndex;
            if (selidx > 0)
            {
                selidx--;
                this.tc.SelectedIndex = selidx;
            }
        }

        private void NextButton_Click(object sender, EventArgs e)
        {
            int selidx = this.tc.SelectedIndex;
            int lastidx = this.tc.TabPages.Count - 1;
            if (selidx < lastidx)
            {
                selidx++;
                this.tc.SelectedIndex = selidx;
            }
        }

        void FixNextPrevButtons()
        {
            int selidx = this.tc.SelectedIndex;
            int lastidx = this.tc.TabPages.Count - 1;
            
            PrevButton.Enabled = selidx > 0;
            NextButton.Enabled = selidx < lastidx;
        }

        private void tc_SelectedIndexChanged(object sender, EventArgs e)
        {
            FixNextPrevButtons();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            /*if (this.AmiCombo.Items.Count > 0)
            {
                this.AmiCombo.SelectedIndex = 0;
            }*/

            /*if (this.AvailabilityZoneCombo.Items.Count > 0)
            {
                this.AvailabilityZoneCombo.SelectedIndex = 0;
            }*/

            {
                string sv = Environment.GetEnvironmentVariable("JAVA_HOME");
                if (!string.IsNullOrEmpty(sv))
                {
                    JavaHomeBox.Text = sv;
                }
            }
            {
                string sv = Environment.GetEnvironmentVariable("EC2_HOME");
                if (!string.IsNullOrEmpty(sv))
                {
                    Ec2HomeBox.Text = sv;
                }
            }
            {
                string sv = Environment.GetEnvironmentVariable("EC2_CERT");
                if (!string.IsNullOrEmpty(sv))
                {
                    Ec2CertBox.Text = sv;
                }
            }
            {
                string sv = Environment.GetEnvironmentVariable("EC2_PRIVATE_KEY");
                if (!string.IsNullOrEmpty(sv))
                {
                    Ec2PrivateKeyBox.Text = sv;
                }
            }
        }

        private void KeyPairBox_DropDown(object sender, EventArgs e)
        {
            object[] oldkeypairs = null;
            if (KeyPairBox.Items.Count > 0)
            {
                oldkeypairs = new string[KeyPairBox.Items.Count];
                for (int i = 0; i < oldkeypairs.Length; i++)
                {
                    oldkeypairs[i] = KeyPairBox.Items[i];
                }
            }

            Info info = GetInfo(false); // interactive=false
            // Note info.valid doesn't need to be true here,
            // just enough of the fields need to be set.
            // They will be checked in CallEc2Command.
            {
                KeyPairBox.Items.Clear();
                KeyPairBox.Items.Add("Loading...");
                System.Threading.Thread thd = new System.Threading.Thread(
                    new System.Threading.ThreadStart(
                    delegate()
                    {
                        try
                        {
                            string[] keypairoutputs = CallEc2Command(info, "ec2-describe-keypairs.cmd")
                                .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            this.Invoke(
                                new Action(delegate()
                                {
                                    KeyPairBox.Items.Clear();
                                    for (int i = 0; i < keypairoutputs.Length; i++)
                                    {
                                        string s = keypairoutputs[i];
                                        const string lead = "KEYPAIR";
                                        if (s.Length > lead.Length
                                            && s.StartsWith(lead)
                                            && (' ' == s[lead.Length] || '\t' == s[lead.Length]))
                                        {
                                            s = s.Substring(lead.Length + 1);
                                            int j = s.IndexOfAny(new char[] { ' ', '\t' });
                                            if (-1 != j)
                                            {
                                                s = s.Substring(0, j);
                                            }
                                            KeyPairBox.Items.Add(s);
                                        }
                                    }
                                }));
                        }
                        catch
                        {
                            this.Invoke(
                                new Action(delegate()
                                {
                                    KeyPairBox.Items.Clear();
                                    if (oldkeypairs != null)
                                    {
                                        for (int i = 0; i < oldkeypairs.Length; i++)
                                        {
                                            KeyPairBox.Items.Add(oldkeypairs[i]);
                                        }
                                    }
                                }));
                        }
                    }));
                thd.IsBackground = true;
                thd.Start();
            }
        }


        public string JavaHome
        {
            get
            {
                return Environment.ExpandEnvironmentVariables(JavaHomeBox.Text);
            }
        }

        public string Ec2Home
        {
            get
            {
                return Environment.ExpandEnvironmentVariables(Ec2HomeBox.Text);
            }
        }

        public string Ec2Cert
        {
            get
            {
                return Environment.ExpandEnvironmentVariables(Ec2CertBox.Text);
            }
        }

        public string Ec2PrivateKey
        {
            get
            {
                return Environment.ExpandEnvironmentVariables(Ec2PrivateKeyBox.Text);
            }
        }

        public string KeyPairPrivateKeyFile
        {
            get
            {
                return Environment.ExpandEnvironmentVariables(KeyPairPrivateKeyFileBox.Text);
            }
        }


        bool errcontinue(string msg)
        {
            return DialogResult.Yes == MessageBox.Show(this, msg
                + Environment.NewLine + Environment.NewLine + "Proceed anyway?",
                this.Text, MessageBoxButtons.YesNo, MessageBoxIcon.Warning);
        }

        void validatecertificatefile(string fp)
        {
            if (!System.IO.File.ReadAllText(fp).Trim()
                       .StartsWith("-----BEGIN CERTIFICATE-----"))
            {
                throw new Exception("File does not contain a valid certificate");
            }
        }

        void validateprivatekeyfile(string fp)
        {
            if (!System.IO.File.ReadAllText(fp).Trim()
                       .StartsWith("-----BEGIN PRIVATE KEY-----"))
            {
                throw new Exception("File does not contain a valid private key");
            }
        }

        void validatersaprivatekeyfile(string fp)
        {
            if (!System.IO.File.ReadAllText(fp).Trim()
                       .StartsWith("-----BEGIN RSA PRIVATE KEY-----"))
            {
                throw new Exception("File does not contain a valid RSA private key");
            }
        }


        static bool _findingcontrol(Control parent, Control find)
        {
            foreach (Control c in parent.Controls)
            {
                if (c == find)
                {
                    return true;
                }
                if (c.HasChildren)
                {
                    if (_findingcontrol(c, find))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        int TabIndexFromControl(Control ctrl)
        {
            int ntabs = this.tc.TabPages.Count;
            for (int itab = 0; itab < ntabs; itab++)
            {
                if (_findingcontrol(this.tc.TabPages[itab], ctrl))
                {
                    return itab;
                }
            }
            return -1;
        }

        void SelectControlInTabs(Control ctrl)
        {
            int ntabs = this.tc.TabPages.Count;
            for (int itab = 0; itab < ntabs; itab++)
            {
                if (_findingcontrol(this.tc.TabPages[itab], ctrl))
                {
                    this.tc.SelectedIndex = itab;
                    break;
                }
            }
            ctrl.Select();
        }

        void BadControlValue(Control ctrl)
        {
            ctrl.BackColor = Color.Maroon;
            ctrl.ForeColor = Color.WhiteSmoke;
            {
                Timer t = new Timer();
                t.Interval = 1000 * 10;
                t.Tick += new EventHandler(
                    delegate(Object sender, EventArgs ea)
                    {
                        ctrl.ResetForeColor();
                        ctrl.ResetBackColor();
                        t.Stop();
                    });
                t.Start();
            }
            SelectControlInTabs(ctrl);
        }


        static Random _rnd = new Random(unchecked(DateTime.Now.Millisecond
            + System.Diagnostics.Process.GetCurrentProcess().Id));

        static int rand()
        {
            return _rnd.Next();
        }


        void showpassword(string headmsg, int instnum)
        {
            string head = "";
            if (!string.IsNullOrEmpty(headmsg))
            {
                head = headmsg + Environment.NewLine + Environment.NewLine;
            }
            string mypasswd = masterpasswd;
            string formachinemsg = "";
            Ec2Instance einst = null;
            if (instances.Count > 0)
            {
                if (instnum < 0)
                {
                    einst = instances[rand() % instances.Count];
                }
                else
                {
                    einst = instances[instnum % instances.Count];
                }
                formachinemsg = " for machine " + einst.ipaddr;
                if (!string.IsNullOrEmpty(einst.passwd))
                {
                    mypasswd = einst.passwd;
                }
            }
            Clipboard.SetText(mypasswd);
            MessageBox.Show(this, head + "Administrator password" + formachinemsg + " is:"
                + Environment.NewLine + "    " + mypasswd
                + Environment.NewLine + "(copied to clipboard)",
                this.Text,
                MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        void showpassword()
        {
            showpassword(null, -1);
        }

        private void GetPasswordButton_Click(object sender, EventArgs e)
        {
            showpassword();
        }


        static string set(string s)
        {
            byte[] inbuf = Encoding.UTF8.GetBytes(s);
            byte[] buf = new byte[inbuf.Length];
            System.Security.Cryptography.MD5CryptoServiceProvider cp =
                new System.Security.Cryptography.MD5CryptoServiceProvider();
            byte[] kv = cp.ComputeHash(
                Encoding.ASCII.GetBytes("StringBuilder sb = new StringBuilder();"
                + inbuf.Length).Reverse().ToArray());
            int ik = inbuf.Length % kv.Length;
            for (int i = 0; i < inbuf.Length; i++)
            {
                buf[i] = (byte)((inbuf[i] + (byte)i) ^ kv[ik]);
                if (++ik == kv.Length)
                {
                    ik = 0;
                }
            }
            return Convert.ToBase64String(buf);
        }


        class Ec2Instance
        {
            public string instanceID,
                ipaddr,
                ipaddrInternal,
                passwd;

            public string GetHostnameInternal()
            {
                if (string.IsNullOrEmpty(ipaddrInternal))
                {
                    throw new Exception("Ec2Instance.GetHostnameInternal: ipaddrInternal is null or empty");
                }
                System.Net.IPAddress addr = System.Net.IPAddress.Parse(ipaddrInternal);
                byte[] bytes = addr.GetAddressBytes();
                int laddr = BitConverter.ToInt32(bytes, 0);
                laddr = System.Net.IPAddress.NetworkToHostOrder(laddr);
                return "ip-" + laddr.ToString("X8");
            }

            public Ec2Instance(string instanceID)
            {
                this.instanceID = instanceID;
            }
        }

        List<Ec2Instance> instances;

        Ec2Instance GetInstanceByID(string instanceID)
        {
            int instancesCount = instances.Count;
            for (int i = 0; i < instancesCount; i++)
            {
                if (0 == string.Compare(instanceID, instances[i].instanceID,
                    StringComparison.OrdinalIgnoreCase))
                {
                    return instances[i];
                }
            }
            return null;
        }


        long t1 = DateTime.Now.Ticks;
        string masterpasswd;

        private void StartClusterButton_Click(object sender, EventArgs e)
        {
            try
            {

                Info info = GetInfo(true); // interactive=true
                if (!info.valid)
                {
                    return;
                }

                masterpasswd = AdminPasswdBox.Text;
                if (masterpasswd != AdminPasswdRetypeBox.Text)
                {
                    BadControlValue(AdminPasswdRetypeBox);
                    BadControlValue(AdminPasswdBox);
                    MessageBox.Show(this, "Passwords do not match", this.Text,
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                    AdminPasswdRetypeBox.Clear();
                    AdminPasswdBox.Clear();
                    return;
                }

                if (masterpasswd.Length > 0 && masterpasswd.Length <= 5)
                {
                    BadControlValue(AdminPasswdRetypeBox);
                    BadControlValue(AdminPasswdBox);
                    MessageBox.Show(this, "Please enter a longer password", this.Text,
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return;
                }

                if (DialogResult.OK != MessageBox.Show(this, "About to start " + info.numberofmachines + " machine Qizmt cluster...",
                    this.Text, MessageBoxButtons.OKCancel, MessageBoxIcon.Information))
                {
                    return;
                }

                bool masterpasswordgenerated = false;
                if (string.IsNullOrEmpty(masterpasswd))
                {
                    masterpasswordgenerated = true;
                    masterpasswd = GenPassword(2144804780);
                }

                string surrogatepassword;
                if (masterpasswd.Length > 0)
                {
                    surrogatepassword = masterpasswd;
                }
                else
                {
                    surrogatepassword = GenPassword(711076331);
                }

                string cmdline = "ec2-run-instances.cmd";
                cmdline += " " + info.amiID;
                if (!string.IsNullOrEmpty(info.securitygroups))
                {
                    string[] groups = info.securitygroups.Split(
                        new char[] { ' ', ',', ';' },
                        StringSplitOptions.RemoveEmptyEntries);
                    foreach (string g in groups)
                    {
                        cmdline += " --group " + g;
                    }
                }
                cmdline += " --key " + info.keypairname;
                if (!string.IsNullOrEmpty(info.instancetype))
                {
                    cmdline += " --instance-type \"" + info.instancetype + "\"";
                }
                if (!string.IsNullOrEmpty(info.availabilityzone))
                {
                    cmdline += " --availability-zone \"" + info.availabilityzone + "\"";
                }

                QizmtClusterTopPanel.Enabled = false;
                StartClusterButton.Enabled = false;
                {
                    int thistab = TabIndexFromControl(StartClusterButton);
                    if (-1 != thistab)
                    {
                        for (int itab = 0; itab < this.tc.TabPages.Count; itab++)
                        {
                            if (itab != thistab)
                            {
                                this.tc.TabPages[itab].Enabled = false;
                            }
                        }
                    }
                }
                OutputBox.Visible = true;
                AdminPasswdBox.Clear();
                AdminPasswdRetypeBox.Clear();

                //MessageBox.Show("DEBUG: " + cmdline);

                Application.DoEvents();

                instances = new List<Ec2Instance>(info.numberofmachines);
                if (info.numberofmachines > 1)
                {
                    // Non-surrogate workers:
                    string mycmdline = cmdline + " --instance-count " + (info.numberofmachines - 1);
                    string mysetpasswd = ""; // Can stay blank.
                    if (!string.IsNullOrEmpty(masterpasswd))
                    {
                        mysetpasswd = masterpasswd;
                    }
                    string extradata = "@WORKER:\tPASSWORD:" + mysetpasswd + "\t";
                    mycmdline += " --user-data \"" + set(extradata) + "\"";
#if DEBUG
                    //MessageBox.Show("NSWORKERS: " + mycmdline);
#endif
                    {
                        string[] output = CallEc2Command(info, mycmdline)
                            .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (string ln in output)
                        {
                            string[] parts = ln.Split('\t');
                            if (parts.Length > 4 && "INSTANCE" == parts[0])
                            {
                                string instanceID = string.Intern(parts[1]);
                                instances.Add(new Ec2Instance(instanceID));
                                OutputBox.AppendText(Environment.NewLine
                                    + "Machine " + instances.Count + " of " + info.numberofmachines
                                    + " initializing: " + instanceID);
                            }
                        }
                        Application.DoEvents();
                    }

                    {
                        int numhostsfound = 0;
                        for (int outeritries = 0; ; )
                        {
                            for (int itries = 0; itries < 90; itries++)
                            {
                                for (int i = 0; i < 15; i++)
                                {
                                    System.Threading.Thread.Sleep(200);
                                    Application.DoEvents();
                                }
                                string[] output = CallEc2Command(info, "ec2-describe-instances.cmd")
                                    .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (string ln in output)
                                {
                                    string[] parts = ln.Split('\t');
                                    if (parts.Length > 17 && "INSTANCE" == parts[0])
                                    {
                                        string instanceID = string.Intern(parts[1]);
                                        Ec2Instance einst = GetInstanceByID(instanceID);
                                        if (null != einst && null == einst.ipaddrInternal)
                                        {
                                            string instanceIPaddr = parts[16];
                                            string instanceIPaddrInternal = parts[17];
                                            if (instanceIPaddrInternal.Length > 0)
                                            {
                                                einst.ipaddr = instanceIPaddr;
                                                einst.ipaddrInternal = instanceIPaddrInternal;
                                                if (!string.IsNullOrEmpty(masterpasswd))
                                                {
                                                    einst.passwd = masterpasswd;
                                                }
                                                numhostsfound++;
                                            }
                                        }
                                    }
                                }
                                Application.DoEvents();
                                if (numhostsfound == instances.Count)
                                {
                                    break;
                                }
                            }
                            if (numhostsfound != instances.Count)
                            {
                                if (++outeritries == 5)
                                {
                                    throw new Exception("Machine instances lost; only found " + numhostsfound + " of " + instances.Count);
                                }
                                continue;
                            }
                            break;
                        }
                    }

                    if (string.IsNullOrEmpty(masterpasswd))
                    {
                        OutputBox.AppendText(Environment.NewLine + "Please wait...");
                        Application.DoEvents();
                        // Wait for their passwords!
                        // "Join" loop:
                        foreach (Ec2Instance einst in instances)
                        {
                            WaitForEc2PasswordReadyDoEvents(info, einst.instanceID);
                            einst.passwd = GetEc2InstancePassword(info, einst.instanceID);
                        }
                    }

                    {
                        // Give non-surrogate workers a head start.
                        const int iwaitsecs = 30;
                        for (int iwait = 0; iwait < iwaitsecs * 5; iwait++)
                        {
                            System.Threading.Thread.Sleep(200);
                            Application.DoEvents();
                        }
                    }

                }
                if (info.numberofmachines > 0)
                {
                    // Surrogate:
                    string mycmdline = cmdline + " --instance-count 1";
                    StringBuilder sbworkers = new StringBuilder(100);
                    foreach (Ec2Instance einst in instances)
                    {
                        if (0 != sbworkers.Length)
                        {
                            sbworkers.Append((char)1);
                        }
                        sbworkers.Append(einst.ipaddrInternal);
                        sbworkers.Append('=');
                        if (null != einst.passwd)
                        {
                            sbworkers.Append(einst.passwd);
                        }
                        else
                        {
                            sbworkers.Append(masterpasswd);
                        }
                    }
                    string extradata = "@SURROGATEWORKER:" + sbworkers
                        + "\tPASSWORD:" + surrogatepassword + "\t";
                    mycmdline += " --user-data \"" + set(extradata) + "\"";
#if DEBUG
                    Clipboard.SetText(mycmdline);
                    //MessageBox.Show("SURROGATEWORKER: " + mycmdline);
#endif
                    string machineinstance = "";
                    {
                        string[] output = CallEc2Command(info, mycmdline)
                            .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (string ln in output)
                        {
                            string[] parts = ln.Split('\t');
                            if (parts.Length > 4 && "INSTANCE" == parts[0])
                            {
                                string instanceID = string.Intern(parts[1]);
                                machineinstance = instanceID;
                                Ec2Instance einst = new Ec2Instance(instanceID);
                                einst.passwd = surrogatepassword; // Set surrogate password!
                                instances.Add(einst);
                                OutputBox.AppendText(Environment.NewLine
                                    + "Machine " + info.numberofmachines + " of " + info.numberofmachines
                                    + " initializing: " + instanceID);
                            }
                        }
                        Application.DoEvents();
                    }

                    {
                        bool foundminstance = false;
                        for (int outeritries = 0; ; )
                        {
                            for (int itries = 0; itries < 90; itries++)
                            {
                                for (int i = 0; i < 15; i++)
                                {
                                    System.Threading.Thread.Sleep(200);
                                    Application.DoEvents();
                                }
                                string[] output = CallEc2Command(info, "ec2-describe-instances.cmd")
                                    .Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (string ln in output)
                                {
                                    string[] parts = ln.Split('\t');
                                    if (parts.Length > 17 && "INSTANCE" == parts[0])
                                    {
                                        string instanceID = string.Intern(parts[1]);
                                        if (machineinstance == instanceID)
                                        {
                                            Ec2Instance einst = GetInstanceByID(instanceID);
                                            if (null == einst || null != einst.ipaddrInternal)
                                            {
                                                throw new Exception("Problem with instance " + instanceID
                                                    + " - ec2-describe-instances.cmd output not consistent");
                                            }
                                            string instanceIPaddr = parts[16];
                                            string instanceIPaddrInternal = parts[17];
                                            if (instanceIPaddrInternal.Length > 0)
                                            {
                                                einst.ipaddr = instanceIPaddr;
                                                einst.ipaddrInternal = instanceIPaddrInternal;
                                                foundminstance = true;
                                            }
                                        }
                                    }
                                }
                                Application.DoEvents();
                                if (foundminstance)
                                {
                                    break;
                                }
                            }
                            if (!foundminstance)
                            {
                                if (++outeritries == 5)
                                {
                                    throw new Exception("Machine instances lost; only found " + (info.numberofmachines - 1) + " of " + info.numberofmachines);
                                }
                                continue;
                            }
                            break;
                        }
                    }

                    OutputBox.AppendText(Environment.NewLine + "Machines:");
                    foreach (Ec2Instance einst in instances)
                    {
                        OutputBox.AppendText(Environment.NewLine + "\t" + einst.ipaddr);
                    }

                    OutputBox.AppendText(Environment.NewLine + "Please wait...");
                    Application.DoEvents();
                    // Wait for all instances to be ready!
                    // "Join" loop:
                    foreach (Ec2Instance einst in instances)
                    {
                        WaitForEc2InstanceReadyDoEvents(info, einst.instanceID);
                    }
                    Application.DoEvents();
                    {
                        // Give a bit of time for the surrogate to setup.
                        OutputBox.AppendText(".");
                        int iwaitsecs = 60 + instances.Count;
                        for (int iwait = 0; iwait < iwaitsecs * 5; iwait++)
                        {
                            System.Threading.Thread.Sleep(200);
                            Application.DoEvents();
                        }
                    }
                    OutputBox.AppendText(Environment.NewLine + "Qizmt cluster now ready!");
                    if (masterpasswordgenerated)
                    {
                        GetPasswordButton.Visible = true;
                    }
                    int instnum = rand();
                    if (launchRdpCheck.Checked)
                    {
                        LaunchRDP(instnum);
                    }
                    launchRdpCheck.Visible = false;
                    const string rdy = "Qizmt cluster now ready!";
                    if (masterpasswd == null)
                    {
                        showpassword(rdy, instnum);
                    }
                    else
                    {
                        MessageBox.Show(this, rdy, this.Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
                    }
                    LaunchRdpButton.Visible = true;
                    TerminateClusterButton.Visible = true;

                }

            }
            catch (Exception e4234)
            {
                MessageBox.Show(this, e4234.ToString(), "Fatal Error - " + this.Text,
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                Environment.Exit(4234);
            }

        }


        void LaunchRDP(int instnum)
        {
            string rdpserver = instances[instnum % instances.Count].ipaddr;
            // mstsc.exe /v:server
            System.Diagnostics.Process rdpproc = System.Diagnostics
                .Process.Start("mstsc.exe", "/v:" + rdpserver);
            rdpproc.Dispose();
        }


        private void LaunchRdpButton_Click(object sender, EventArgs e)
        {
            int instnum = rand();
            LaunchRDP(instnum);
            if (masterpasswd == null)
            {
                showpassword(null, instnum);
            }
        }


        private void TerminateClusterButton_Click(object sender, EventArgs e)
        {
            this.Close();
        }


        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (!StartClusterButton.Enabled)
            {
                string terminstances = "";
                bool ready = instances.Count > 0;
                foreach (Ec2Instance einst in instances)
                {
                    if (string.IsNullOrEmpty(einst.instanceID))
                    {
                        ready = false;
                        break;
                    }
                    terminstances += " " + einst.instanceID;
                }
                if (!ready)
                {
                    if (DialogResult.Yes != MessageBox.Show(this, "ERROR:"
                        + Environment.NewLine + "The cluster hasn't initialized; quit now and leave machines running?",
                        this.Text, MessageBoxButtons.YesNo, MessageBoxIcon.Error))
                    {
                        e.Cancel = true;
                        return;
                    }
                }
                else
                {
                    if (DialogResult.Yes != MessageBox.Show(this, "WARNING:"
                        + Environment.NewLine + "Are you sure you want to TERMINATE this Qizmt cluster?",
                        this.Text, MessageBoxButtons.YesNo, MessageBoxIcon.Warning))
                    {
                        e.Cancel = true;
                        return;
                    }

                    this.Text = "TERMINATING: " + this.Text;
                    this.Enabled = false;
                    Application.DoEvents();

                    Info info = GetInfo(true);

                    this.Enabled = true;
                    string termoutput = CallEc2Command(info, "ec2-terminate-instances.cmd" + terminstances);
                    MessageBox.Show(this, "Cluster terminated"
                        + Environment.NewLine + Environment.NewLine + termoutput,
                        this.Text, MessageBoxButtons.OK);

                }
            }
            StopWaiting = true;
        }


        string GenPassword(int randomness)
        {
            string passwd = "";
            long t2 = DateTime.Now.Ticks;
            Random rnd = new Random(unchecked(
                System.Diagnostics.Process.GetCurrentProcess().Id
                + System.Threading.Thread.CurrentThread.ManagedThreadId
                + (int)(t1 * t2)
                + (int)(t2 - t1)
                + randomness
                ));
            char[] rc = new char[4];
            string specialchars = "!#$%&+;.,=@_-~";
            for (int i = 0; i < rnd.Next(9, 15 + 1); i++)
            {
                rc[0] = (char)rnd.Next('A', 'Z' + 1);
                rc[1] = (char)rnd.Next('a', 'z' + 1);
                rc[2] = (char)rnd.Next('0', '1' + 1);
                rc[3] = specialchars[rnd.Next(0, specialchars.Length)];
                char ch = rc[rnd.Next(0, rc.Length)];
                passwd += ch;
            }
            return passwd;
        }


        static bool StopWaiting = false;


        // Ready password doesn't mean you should log in yet, but you can get the admin password.
        bool IsEc2PasswordReady(Info info, string instanceID)
        {
            string output = CallEc2Command(info, "ec2-get-console-output.cmd " + instanceID);
            if (-1 != output.IndexOf("</Password>"))
            {
                return true;
            }
            return false;
        }

        void WaitForEc2PasswordReady(Info info, string instanceID)
        {
            while (!IsEc2PasswordReady(info, instanceID))
            {
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
                const int numsecs = 10;
                System.Threading.Thread.Sleep(1000 * numsecs);
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
            }
        }

        void WaitForEc2PasswordReadyDoEvents(Info info, string instanceID)
        {
            while (!IsEc2PasswordReady(info, instanceID))
            {
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
                const int numsecs = 10;
                for (int i = 0; i < numsecs * 5; i++)
                {
                    System.Threading.Thread.Sleep(200);
                    Application.DoEvents();
                }
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
            }
        }


        // Ready to be logged into.
        bool IsEc2InstanceReady(Info info, string instanceID)
        {
            // Try a few ways in case the output changes.
            string output = CallEc2Command(info, "ec2-get-console-output.cmd " + instanceID);
            int index = output.IndexOf("</RDPCERTIFICATE>");
            if (-1 != index)
            {
                if (-1 != output.IndexOf("Windows is Ready", index, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            else
            {
                if (-1 != output.IndexOf(": Message: Windows is Ready to use"))
                {
                    return true;
                }
            }
            return false;
        }

        void WaitForEc2InstanceReady(Info info, string instanceID)
        {
            while (!IsEc2InstanceReady(info, instanceID))
            {
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
                const int numsecs = 10;
                System.Threading.Thread.Sleep(1000 * numsecs);
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
            }
        }

        void WaitForEc2InstanceReadyDoEvents(Info info, string instanceID)
        {
            while (!IsEc2InstanceReady(info, instanceID))
            {
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
                const int numsecs = 10;
                for (int i = 0; i < numsecs * 5; i++)
                {
                    System.Threading.Thread.Sleep(200);
                    Application.DoEvents();
                }
                if (StopWaiting)
                {
                    throw new System.Threading.ThreadInterruptedException("Canceled");
                }
            }
        }


        string GetEc2InstancePassword(Info info, string instanceID)
        {
            if (!info.valid)
            {
                throw new InvalidOperationException("All fields must be valid to get EC2 instance password");
            }
            return CallEc2Command(info, "ec2-get-password.cmd " + instanceID
                + " -k " + info.keypairprivatekeyfile).Trim();
        }


        Info GetInfo(bool interactive)
        {
            Info info = new Info();

            string javahome = JavaHome;
            if (!System.IO.File.Exists(javahome + @"\bin\java.exe"))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(JavaHomeBox);
                if (!errcontinue("Java not found at " + javahome))
                {
                    return info;
                }
            }
            info.javahome = javahome;

            string ec2home = Ec2Home;
            if (!System.IO.File.Exists(ec2home + @"\bin\ec2-run-instances.cmd"))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(Ec2HomeBox);
                if (!errcontinue("EC2 command-line tools not found at " + ec2home))
                {
                    return info;
                }
            }
            info.ec2home = ec2home;

            string ec2cert = Ec2Cert;
            if (!System.IO.File.Exists(ec2cert))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(Ec2CertBox);
                if (!errcontinue("EC2 certificate file not found at " + ec2cert))
                {
                    return info;
                }
            }
            try
            {
                validatecertificatefile(ec2cert);
            }
            catch (Exception ee)
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(Ec2CertBox);
                if (!errcontinue("EC2 certificate file error: " + ee.Message))
                {
                    return info;
                }
            }
            info.ec2cert = ec2cert;

            string ec2privatekey = Ec2PrivateKey;
            if (!System.IO.File.Exists(ec2privatekey))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(Ec2PrivateKeyBox);
                if (!errcontinue("EC2 private key file not found at " + ec2privatekey))
                {
                    return info;
                }
            }
            try
            {
                validateprivatekeyfile(ec2privatekey);
            }
            catch (Exception ee)
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(Ec2PrivateKeyBox);
                if (!errcontinue("EC2 private key file error: " + ee.Message))
                {
                    return info;
                }
            }
            info.ec2privatekey = ec2privatekey;

            string amiID = AmiCombo.Text.Trim();
            if (!amiID.StartsWith("ami-", StringComparison.OrdinalIgnoreCase))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(AmiCombo);
                //MessageBox.Show(this, "Invalid AMI ID",
                //    this.Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
                //return;
                if (!errcontinue("Invalid AMI ID"))
                {
                    return info;
                }
            }
            for (int i = 4; i < amiID.Length; i++)
            {
                if (' ' == amiID[i])
                {
                    amiID = amiID.Substring(0, i);
                    break;
                }
            }
            info.amiID = amiID;

            string keypairname = KeyPairBox.Text.Trim();
            if (0 == keypairname.Length || "Loading..." == keypairname)
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(KeyPairBox);
                MessageBox.Show(this, "KeyPair Name not provided",
                    this.Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
                return info;
            }
            info.keypairname = keypairname;

            string keypairprivatekeyfile = KeyPairPrivateKeyFile;
            if (!System.IO.File.Exists(keypairprivatekeyfile))
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(KeyPairPrivateKeyFileBox);
                if (!errcontinue("KeyPair RSA private key file not found at " + keypairprivatekeyfile))
                {
                    return info;
                }
            }
            try
            {
                validatersaprivatekeyfile(keypairprivatekeyfile);
            }
            catch (Exception ee)
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(KeyPairPrivateKeyFileBox);
                if (!errcontinue("KeyPair RSA private key file error: " + ee.Message))
                {
                    return info;
                }
            }
            info.keypairprivatekeyfile = keypairprivatekeyfile;

            string instancetype = InstanceTypeBox.Text.Trim();
            {
                int isp = instancetype.IndexOf(' ');
                if (-1 != isp)
                {
                    instancetype = instancetype.Substring(0, isp);
                }
            }
            if (0 == instancetype.Length)
            {
                BadControlValue(InstanceTypeBox);
                MessageBox.Show(this, "Must specify an instance type",
                    this.Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
                return info;
            }
            info.instancetype = instancetype;

            string availabilityzone = AvailabilityZoneCombo.Text.Trim();
            info.availabilityzone = availabilityzone;

            string securitygroups = SecurityGroupsBox.Text.Trim();
            info.securitygroups = securitygroups;

            int numberofmachines;
            if (!int.TryParse(NumberOfMachinesBox.Text.Trim(), out numberofmachines)
                || numberofmachines < 1)
            {
                if (!interactive)
                {
                    return info;
                }
                BadControlValue(NumberOfMachinesBox);
                MessageBox.Show(this, "Invalid number of machines specified for Qizmt cluster",
                    this.Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
                return info;
            }
            info.numberofmachines = numberofmachines;

            info.valid = true;
            return info;
        }


        class Info
        {
            public bool valid = false;
            public string
                javahome,
                ec2home,
                ec2cert,
                ec2privatekey,
                amiID,
                keypairname,
                keypairprivatekeyfile,
                instancetype,
                availabilityzone,
                securitygroups;
            public int numberofmachines;
        }


        static string CallEc2Command(Info info, string cmdline)
        {
            return CallEc2Command(info, cmdline, false);
        }

        static string CallEc2Command(Info info, string cmdline, bool suppresserrors)
        {
            if (string.IsNullOrEmpty(info.javahome))
            {
                throw new Exception("Invalid JAVA_HOME");
            }
            Environment.SetEnvironmentVariable("JAVA_HOME", info.javahome);

            if (string.IsNullOrEmpty(info.ec2home))
            {
                throw new Exception("Invalid EC2_HOME");
            }
            Environment.SetEnvironmentVariable("EC2_HOME", info.ec2home);

            if (string.IsNullOrEmpty(info.ec2privatekey))
            {
                throw new Exception("Invalid EC2_PRIVATE_KEY");
            }
            Environment.SetEnvironmentVariable("EC2_PRIVATE_KEY", info.ec2privatekey);

            if (string.IsNullOrEmpty(info.ec2cert))
            {
                throw new Exception("Invalid EC2_CERT");
            }
            Environment.SetEnvironmentVariable("EC2_CERT", info.ec2cert);

            return Exec.Shell(info.ec2home + @"\bin\" + cmdline, suppresserrors);
        }


    }
}
