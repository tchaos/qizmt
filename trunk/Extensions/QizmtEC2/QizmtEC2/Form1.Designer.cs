namespace QizmtEC2
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.tc = new System.Windows.Forms.TabControl();
            this.SetupTab = new System.Windows.Forms.TabPage();
            this.Ec2PrivateKeyBox = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.Ec2CertBox = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.Ec2HomeBox = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.JavaHomeBox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.Ec2MachinesTab = new System.Windows.Forms.TabPage();
            this.InstanceTypeBox = new System.Windows.Forms.ComboBox();
            this.SecurityGroupsBox = new System.Windows.Forms.TextBox();
            this.label10 = new System.Windows.Forms.Label();
            this.AvailabilityZoneCombo = new System.Windows.Forms.ComboBox();
            this.label9 = new System.Windows.Forms.Label();
            this.KeyPairPrivateKeyFileBox = new System.Windows.Forms.TextBox();
            this.label8 = new System.Windows.Forms.Label();
            this.KeyPairBox = new System.Windows.Forms.ComboBox();
            this.label7 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.AmiCombo = new System.Windows.Forms.ComboBox();
            this.label5 = new System.Windows.Forms.Label();
            this.QizmtClusterTab = new System.Windows.Forms.TabPage();
            this.TerminateClusterButton = new System.Windows.Forms.Button();
            this.OutputBox = new System.Windows.Forms.TextBox();
            this.LaunchRdpButton = new System.Windows.Forms.Button();
            this.launchRdpCheck = new System.Windows.Forms.CheckBox();
            this.QizmtClusterTopPanel = new System.Windows.Forms.Panel();
            this.label14 = new System.Windows.Forms.Label();
            this.AdminPasswdRetypeBox = new System.Windows.Forms.TextBox();
            this.label13 = new System.Windows.Forms.Label();
            this.AdminPasswdBox = new System.Windows.Forms.TextBox();
            this.label12 = new System.Windows.Forms.Label();
            this.NumberOfMachinesBox = new System.Windows.Forms.TextBox();
            this.label11 = new System.Windows.Forms.Label();
            this.StartClusterButton = new System.Windows.Forms.Button();
            this.BottomPanel = new System.Windows.Forms.Panel();
            this.GetPasswordButton = new System.Windows.Forms.Button();
            this.PrevButton = new System.Windows.Forms.Button();
            this.NextButton = new System.Windows.Forms.Button();
            this.tc.SuspendLayout();
            this.SetupTab.SuspendLayout();
            this.Ec2MachinesTab.SuspendLayout();
            this.QizmtClusterTab.SuspendLayout();
            this.QizmtClusterTopPanel.SuspendLayout();
            this.BottomPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // tc
            // 
            this.tc.Controls.Add(this.SetupTab);
            this.tc.Controls.Add(this.Ec2MachinesTab);
            this.tc.Controls.Add(this.QizmtClusterTab);
            this.tc.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tc.Location = new System.Drawing.Point(0, 0);
            this.tc.Name = "tc";
            this.tc.SelectedIndex = 0;
            this.tc.Size = new System.Drawing.Size(522, 276);
            this.tc.TabIndex = 0;
            this.tc.SelectedIndexChanged += new System.EventHandler(this.tc_SelectedIndexChanged);
            // 
            // SetupTab
            // 
            this.SetupTab.Controls.Add(this.Ec2PrivateKeyBox);
            this.SetupTab.Controls.Add(this.label4);
            this.SetupTab.Controls.Add(this.Ec2CertBox);
            this.SetupTab.Controls.Add(this.label3);
            this.SetupTab.Controls.Add(this.Ec2HomeBox);
            this.SetupTab.Controls.Add(this.label2);
            this.SetupTab.Controls.Add(this.JavaHomeBox);
            this.SetupTab.Controls.Add(this.label1);
            this.SetupTab.Location = new System.Drawing.Point(4, 22);
            this.SetupTab.Name = "SetupTab";
            this.SetupTab.Padding = new System.Windows.Forms.Padding(3);
            this.SetupTab.Size = new System.Drawing.Size(514, 250);
            this.SetupTab.TabIndex = 0;
            this.SetupTab.Text = "Setup";
            this.SetupTab.UseVisualStyleBackColor = true;
            // 
            // Ec2PrivateKeyBox
            // 
            this.Ec2PrivateKeyBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.Ec2PrivateKeyBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.FileSystem;
            this.Ec2PrivateKeyBox.Location = new System.Drawing.Point(20, 176);
            this.Ec2PrivateKeyBox.Name = "Ec2PrivateKeyBox";
            this.Ec2PrivateKeyBox.Size = new System.Drawing.Size(466, 20);
            this.Ec2PrivateKeyBox.TabIndex = 7;
            this.Ec2PrivateKeyBox.Text = "%UserProfile%\\ec2-keys\\pk-*.pem";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(17, 160);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(206, 13);
            this.label4.TabIndex = 6;
            this.label4.Text = "EC2 private key file (EC2_PRIVATE_KEY)";
            // 
            // Ec2CertBox
            // 
            this.Ec2CertBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.Ec2CertBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.FileSystem;
            this.Ec2CertBox.Location = new System.Drawing.Point(20, 128);
            this.Ec2CertBox.Name = "Ec2CertBox";
            this.Ec2CertBox.Size = new System.Drawing.Size(466, 20);
            this.Ec2CertBox.TabIndex = 5;
            this.Ec2CertBox.Text = "%UserProfile%\\ec2-keys\\cert-*.pem";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(17, 112);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(187, 13);
            this.label3.TabIndex = 4;
            this.label3.Text = "EC2 X.509 certificate file (EC2_CERT)";
            // 
            // Ec2HomeBox
            // 
            this.Ec2HomeBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.Ec2HomeBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.FileSystemDirectories;
            this.Ec2HomeBox.Location = new System.Drawing.Point(20, 79);
            this.Ec2HomeBox.Name = "Ec2HomeBox";
            this.Ec2HomeBox.Size = new System.Drawing.Size(466, 20);
            this.Ec2HomeBox.TabIndex = 3;
            this.Ec2HomeBox.Text = "C:\\ec2-api-tools";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(17, 63);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(245, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "EC2 command-line tools home folder (EC2_HOME)";
            // 
            // JavaHomeBox
            // 
            this.JavaHomeBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.JavaHomeBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.FileSystemDirectories;
            this.JavaHomeBox.Location = new System.Drawing.Point(20, 29);
            this.JavaHomeBox.Name = "JavaHomeBox";
            this.JavaHomeBox.Size = new System.Drawing.Size(466, 20);
            this.JavaHomeBox.TabIndex = 1;
            this.JavaHomeBox.Text = "C:\\Program Files\\Java\\jre6";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(17, 13);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(161, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Java home folder (JAVA_HOME)";
            // 
            // Ec2MachinesTab
            // 
            this.Ec2MachinesTab.Controls.Add(this.InstanceTypeBox);
            this.Ec2MachinesTab.Controls.Add(this.SecurityGroupsBox);
            this.Ec2MachinesTab.Controls.Add(this.label10);
            this.Ec2MachinesTab.Controls.Add(this.AvailabilityZoneCombo);
            this.Ec2MachinesTab.Controls.Add(this.label9);
            this.Ec2MachinesTab.Controls.Add(this.KeyPairPrivateKeyFileBox);
            this.Ec2MachinesTab.Controls.Add(this.label8);
            this.Ec2MachinesTab.Controls.Add(this.KeyPairBox);
            this.Ec2MachinesTab.Controls.Add(this.label7);
            this.Ec2MachinesTab.Controls.Add(this.label6);
            this.Ec2MachinesTab.Controls.Add(this.AmiCombo);
            this.Ec2MachinesTab.Controls.Add(this.label5);
            this.Ec2MachinesTab.Location = new System.Drawing.Point(4, 22);
            this.Ec2MachinesTab.Name = "Ec2MachinesTab";
            this.Ec2MachinesTab.Size = new System.Drawing.Size(514, 250);
            this.Ec2MachinesTab.TabIndex = 2;
            this.Ec2MachinesTab.Text = "EC2 Machines";
            this.Ec2MachinesTab.UseVisualStyleBackColor = true;
            // 
            // InstanceTypeBox
            // 
            this.InstanceTypeBox.FormattingEnabled = true;
            this.InstanceTypeBox.Items.AddRange(new object[] {
            "t1.micro - x86/x86_64 - Micro/Micro",
            "m1.small - x86 - Standard/Small",
            "m1.large - x86_64 - Standard/Large",
            "m1.xlarge - x86_64 - Standard/Extra-Large",
            "m2.xlarge - x86_64 - High-Memory/Extra-Large",
            "m2.2xlarge - x86_64 - High-Memory/Double-Extra-Large",
            "m2.4xlarge - x86_64 - High-Memory/Quadruple-Extra-Large",
            "c1.medium - x86 - High-CPU/Medium",
            "c1.xlarge - x86_64 - High-CPU/Extra-Large"});
            this.InstanceTypeBox.Location = new System.Drawing.Point(165, 36);
            this.InstanceTypeBox.Name = "InstanceTypeBox";
            this.InstanceTypeBox.Size = new System.Drawing.Size(322, 21);
            this.InstanceTypeBox.TabIndex = 3;
            // 
            // SecurityGroupsBox
            // 
            this.SecurityGroupsBox.Location = new System.Drawing.Point(165, 143);
            this.SecurityGroupsBox.Name = "SecurityGroupsBox";
            this.SecurityGroupsBox.Size = new System.Drawing.Size(322, 20);
            this.SecurityGroupsBox.TabIndex = 11;
            this.SecurityGroupsBox.Text = "default ";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(20, 146);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(85, 13);
            this.label10.TabIndex = 10;
            this.label10.Text = "Security Groups:";
            // 
            // AvailabilityZoneCombo
            // 
            this.AvailabilityZoneCombo.FormattingEnabled = true;
            this.AvailabilityZoneCombo.Items.AddRange(new object[] {
            "us-east-1a",
            "us-east-1b",
            "us-east-1c",
            "us-east-1d"});
            this.AvailabilityZoneCombo.Location = new System.Drawing.Point(165, 116);
            this.AvailabilityZoneCombo.Name = "AvailabilityZoneCombo";
            this.AvailabilityZoneCombo.Size = new System.Drawing.Size(322, 21);
            this.AvailabilityZoneCombo.TabIndex = 9;
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(20, 119);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(87, 13);
            this.label9.TabIndex = 8;
            this.label9.Text = "Availability Zone:";
            // 
            // KeyPairPrivateKeyFileBox
            // 
            this.KeyPairPrivateKeyFileBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.KeyPairPrivateKeyFileBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.FileSystem;
            this.KeyPairPrivateKeyFileBox.Location = new System.Drawing.Point(165, 90);
            this.KeyPairPrivateKeyFileBox.Name = "KeyPairPrivateKeyFileBox";
            this.KeyPairPrivateKeyFileBox.Size = new System.Drawing.Size(322, 20);
            this.KeyPairPrivateKeyFileBox.TabIndex = 7;
            this.KeyPairPrivateKeyFileBox.Text = "%UserProfile%\\ec2-keys\\pkcs8-KeyPairName.pem";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(20, 93);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(117, 13);
            this.label8.TabIndex = 6;
            this.label8.Text = "KeyPair private key file:";
            // 
            // KeyPairBox
            // 
            this.KeyPairBox.FormattingEnabled = true;
            this.KeyPairBox.Location = new System.Drawing.Point(165, 63);
            this.KeyPairBox.Name = "KeyPairBox";
            this.KeyPairBox.Size = new System.Drawing.Size(322, 21);
            this.KeyPairBox.TabIndex = 5;
            this.KeyPairBox.DropDown += new System.EventHandler(this.KeyPairBox_DropDown);
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(20, 66);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(77, 13);
            this.label7.TabIndex = 4;
            this.label7.Text = "KeyPair Name:";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(20, 40);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(78, 13);
            this.label6.TabIndex = 2;
            this.label6.Text = "Instance Type:";
            // 
            // AmiCombo
            // 
            this.AmiCombo.FormattingEnabled = true;
            this.AmiCombo.Items.AddRange(new object[] {
            "ami-b8c42cd1 - MySpace Qizmt x86 EBS - Windows-Server2008-i386-Base-v103",
            "ami-76bb4a1f - MySpace Qizmt x86_64 instance-store - Server2003r2-x86_64-Win-v1.0" +
                "7"});
            this.AmiCombo.Location = new System.Drawing.Point(98, 10);
            this.AmiCombo.Name = "AmiCombo";
            this.AmiCombo.Size = new System.Drawing.Size(389, 21);
            this.AmiCombo.TabIndex = 1;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(20, 13);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(43, 13);
            this.label5.TabIndex = 0;
            this.label5.Text = "AMI ID:";
            // 
            // QizmtClusterTab
            // 
            this.QizmtClusterTab.Controls.Add(this.TerminateClusterButton);
            this.QizmtClusterTab.Controls.Add(this.OutputBox);
            this.QizmtClusterTab.Controls.Add(this.LaunchRdpButton);
            this.QizmtClusterTab.Controls.Add(this.launchRdpCheck);
            this.QizmtClusterTab.Controls.Add(this.QizmtClusterTopPanel);
            this.QizmtClusterTab.Controls.Add(this.StartClusterButton);
            this.QizmtClusterTab.Location = new System.Drawing.Point(4, 22);
            this.QizmtClusterTab.Name = "QizmtClusterTab";
            this.QizmtClusterTab.Size = new System.Drawing.Size(514, 250);
            this.QizmtClusterTab.TabIndex = 1;
            this.QizmtClusterTab.Text = "Qizmt Cluster";
            this.QizmtClusterTab.UseVisualStyleBackColor = true;
            // 
            // TerminateClusterButton
            // 
            this.TerminateClusterButton.Location = new System.Drawing.Point(376, 96);
            this.TerminateClusterButton.Name = "TerminateClusterButton";
            this.TerminateClusterButton.Size = new System.Drawing.Size(120, 23);
            this.TerminateClusterButton.TabIndex = 10;
            this.TerminateClusterButton.Text = "Terminate Cluster";
            this.TerminateClusterButton.UseVisualStyleBackColor = true;
            this.TerminateClusterButton.Visible = false;
            this.TerminateClusterButton.Click += new System.EventHandler(this.TerminateClusterButton_Click);
            // 
            // OutputBox
            // 
            this.OutputBox.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.OutputBox.Location = new System.Drawing.Point(0, 125);
            this.OutputBox.Multiline = true;
            this.OutputBox.Name = "OutputBox";
            this.OutputBox.ReadOnly = true;
            this.OutputBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.OutputBox.Size = new System.Drawing.Size(514, 125);
            this.OutputBox.TabIndex = 9;
            this.OutputBox.Text = "Initializing Qizmt Cluster...";
            this.OutputBox.Visible = false;
            // 
            // LaunchRdpButton
            // 
            this.LaunchRdpButton.Location = new System.Drawing.Point(187, 96);
            this.LaunchRdpButton.Name = "LaunchRdpButton";
            this.LaunchRdpButton.Size = new System.Drawing.Size(156, 23);
            this.LaunchRdpButton.TabIndex = 9;
            this.LaunchRdpButton.Text = "Launch Remote Desktop";
            this.LaunchRdpButton.UseVisualStyleBackColor = true;
            this.LaunchRdpButton.Visible = false;
            this.LaunchRdpButton.Click += new System.EventHandler(this.LaunchRdpButton_Click);
            // 
            // launchRdpCheck
            // 
            this.launchRdpCheck.AutoSize = true;
            this.launchRdpCheck.Checked = true;
            this.launchRdpCheck.CheckState = System.Windows.Forms.CheckState.Checked;
            this.launchRdpCheck.Location = new System.Drawing.Point(180, 100);
            this.launchRdpCheck.Name = "launchRdpCheck";
            this.launchRdpCheck.Size = new System.Drawing.Size(247, 17);
            this.launchRdpCheck.TabIndex = 12;
            this.launchRdpCheck.Text = "Launch Remote Desktop when cluster is ready";
            this.launchRdpCheck.UseVisualStyleBackColor = true;
            // 
            // QizmtClusterTopPanel
            // 
            this.QizmtClusterTopPanel.Controls.Add(this.label14);
            this.QizmtClusterTopPanel.Controls.Add(this.AdminPasswdRetypeBox);
            this.QizmtClusterTopPanel.Controls.Add(this.label13);
            this.QizmtClusterTopPanel.Controls.Add(this.AdminPasswdBox);
            this.QizmtClusterTopPanel.Controls.Add(this.label12);
            this.QizmtClusterTopPanel.Controls.Add(this.NumberOfMachinesBox);
            this.QizmtClusterTopPanel.Controls.Add(this.label11);
            this.QizmtClusterTopPanel.Location = new System.Drawing.Point(3, 0);
            this.QizmtClusterTopPanel.Name = "QizmtClusterTopPanel";
            this.QizmtClusterTopPanel.Size = new System.Drawing.Size(511, 94);
            this.QizmtClusterTopPanel.TabIndex = 8;
            // 
            // label14
            // 
            this.label14.AutoSize = true;
            this.label14.Location = new System.Drawing.Point(372, 44);
            this.label14.Name = "label14";
            this.label14.Size = new System.Drawing.Size(50, 13);
            this.label14.TabIndex = 4;
            this.label14.Text = "(optional)";
            // 
            // AdminPasswdRetypeBox
            // 
            this.AdminPasswdRetypeBox.Location = new System.Drawing.Point(178, 67);
            this.AdminPasswdRetypeBox.Name = "AdminPasswdRetypeBox";
            this.AdminPasswdRetypeBox.Size = new System.Drawing.Size(188, 20);
            this.AdminPasswdRetypeBox.TabIndex = 6;
            this.AdminPasswdRetypeBox.UseSystemPasswordChar = true;
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Location = new System.Drawing.Point(108, 70);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(44, 13);
            this.label13.TabIndex = 5;
            this.label13.Text = "Retype:";
            // 
            // AdminPasswdBox
            // 
            this.AdminPasswdBox.Location = new System.Drawing.Point(178, 41);
            this.AdminPasswdBox.Name = "AdminPasswdBox";
            this.AdminPasswdBox.Size = new System.Drawing.Size(188, 20);
            this.AdminPasswdBox.TabIndex = 3;
            this.AdminPasswdBox.UseSystemPasswordChar = true;
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(14, 44);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(138, 13);
            this.label12.TabIndex = 2;
            this.label12.Text = "Set Administrator Password:";
            // 
            // NumberOfMachinesBox
            // 
            this.NumberOfMachinesBox.Location = new System.Drawing.Point(253, 15);
            this.NumberOfMachinesBox.Name = "NumberOfMachinesBox";
            this.NumberOfMachinesBox.Size = new System.Drawing.Size(52, 20);
            this.NumberOfMachinesBox.TabIndex = 1;
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Location = new System.Drawing.Point(14, 18);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(224, 13);
            this.label11.TabIndex = 0;
            this.label11.Text = "Number of Machines to Start for Qizmt Cluster:";
            // 
            // StartClusterButton
            // 
            this.StartClusterButton.Location = new System.Drawing.Point(18, 96);
            this.StartClusterButton.Name = "StartClusterButton";
            this.StartClusterButton.Size = new System.Drawing.Size(131, 23);
            this.StartClusterButton.TabIndex = 11;
            this.StartClusterButton.Text = "&Start Qizmt Cluster";
            this.StartClusterButton.UseVisualStyleBackColor = true;
            this.StartClusterButton.Click += new System.EventHandler(this.StartClusterButton_Click);
            // 
            // BottomPanel
            // 
            this.BottomPanel.Controls.Add(this.GetPasswordButton);
            this.BottomPanel.Controls.Add(this.PrevButton);
            this.BottomPanel.Controls.Add(this.NextButton);
            this.BottomPanel.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.BottomPanel.Location = new System.Drawing.Point(0, 276);
            this.BottomPanel.Name = "BottomPanel";
            this.BottomPanel.Size = new System.Drawing.Size(522, 37);
            this.BottomPanel.TabIndex = 1;
            // 
            // GetPasswordButton
            // 
            this.GetPasswordButton.Location = new System.Drawing.Point(12, 6);
            this.GetPasswordButton.Name = "GetPasswordButton";
            this.GetPasswordButton.Size = new System.Drawing.Size(107, 23);
            this.GetPasswordButton.TabIndex = 2;
            this.GetPasswordButton.Text = "Get Password";
            this.GetPasswordButton.UseVisualStyleBackColor = true;
            this.GetPasswordButton.Visible = false;
            this.GetPasswordButton.Click += new System.EventHandler(this.GetPasswordButton_Click);
            // 
            // PrevButton
            // 
            this.PrevButton.Enabled = false;
            this.PrevButton.Location = new System.Drawing.Point(354, 6);
            this.PrevButton.Name = "PrevButton";
            this.PrevButton.Size = new System.Drawing.Size(75, 23);
            this.PrevButton.TabIndex = 1;
            this.PrevButton.Text = "&Previous";
            this.PrevButton.UseVisualStyleBackColor = true;
            this.PrevButton.Click += new System.EventHandler(this.PrevButton_Click);
            // 
            // NextButton
            // 
            this.NextButton.Location = new System.Drawing.Point(435, 6);
            this.NextButton.Name = "NextButton";
            this.NextButton.Size = new System.Drawing.Size(75, 23);
            this.NextButton.TabIndex = 0;
            this.NextButton.Text = "&Next";
            this.NextButton.UseVisualStyleBackColor = true;
            this.NextButton.Click += new System.EventHandler(this.NextButton_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(522, 313);
            this.Controls.Add(this.tc);
            this.Controls.Add(this.BottomPanel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.Name = "Form1";
            this.Text = "Qizmt on EC2";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.Form1_FormClosing);
            this.Load += new System.EventHandler(this.Form1_Load);
            this.tc.ResumeLayout(false);
            this.SetupTab.ResumeLayout(false);
            this.SetupTab.PerformLayout();
            this.Ec2MachinesTab.ResumeLayout(false);
            this.Ec2MachinesTab.PerformLayout();
            this.QizmtClusterTab.ResumeLayout(false);
            this.QizmtClusterTab.PerformLayout();
            this.QizmtClusterTopPanel.ResumeLayout(false);
            this.QizmtClusterTopPanel.PerformLayout();
            this.BottomPanel.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabControl tc;
        private System.Windows.Forms.TabPage SetupTab;
        private System.Windows.Forms.Panel BottomPanel;
        private System.Windows.Forms.Button PrevButton;
        private System.Windows.Forms.Button NextButton;
        private System.Windows.Forms.TextBox JavaHomeBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox Ec2HomeBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox Ec2CertBox;
        private System.Windows.Forms.TextBox Ec2PrivateKeyBox;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TabPage QizmtClusterTab;
        private System.Windows.Forms.TabPage Ec2MachinesTab;
        private System.Windows.Forms.ComboBox AmiCombo;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.ComboBox KeyPairBox;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.TextBox KeyPairPrivateKeyFileBox;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.ComboBox AvailabilityZoneCombo;
        private System.Windows.Forms.TextBox SecurityGroupsBox;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Panel QizmtClusterTopPanel;
        private System.Windows.Forms.Label label14;
        private System.Windows.Forms.TextBox AdminPasswdRetypeBox;
        private System.Windows.Forms.Label label13;
        private System.Windows.Forms.TextBox AdminPasswdBox;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.TextBox NumberOfMachinesBox;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.TextBox OutputBox;
        private System.Windows.Forms.Button GetPasswordButton;
        private System.Windows.Forms.Button TerminateClusterButton;
        private System.Windows.Forms.Button LaunchRdpButton;
        private System.Windows.Forms.CheckBox launchRdpCheck;
        private System.Windows.Forms.Button StartClusterButton;
        private System.Windows.Forms.ComboBox InstanceTypeBox;
    }
}

