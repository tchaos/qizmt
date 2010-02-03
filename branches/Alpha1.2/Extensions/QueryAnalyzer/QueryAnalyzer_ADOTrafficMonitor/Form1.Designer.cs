namespace QueryAnalyzer_ADOTrafficMonitor
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
            this.txtHosts = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.txtClients = new System.Windows.Forms.TextBox();
            this.txtSQL = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.btnStartMon = new System.Windows.Forms.Button();
            this.btnStopMon = new System.Windows.Forms.Button();
            this.btnStartQuery = new System.Windows.Forms.Button();
            this.btnStopQuery = new System.Windows.Forms.Button();
            this.progressBar1 = new System.Windows.Forms.ProgressBar();
            this.label4 = new System.Windows.Forms.Label();
            this.lblIndex0 = new System.Windows.Forms.Label();
            this.lblIndex1 = new System.Windows.Forms.Label();
            this.lblIndex2 = new System.Windows.Forms.Label();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.radRobin = new System.Windows.Forms.RadioButton();
            this.radRandom = new System.Windows.Forms.RadioButton();
            this.btnKillall = new System.Windows.Forms.Button();
            this.txtStatus = new System.Windows.Forms.TextBox();
            this.statusStrip1 = new System.Windows.Forms.StatusStrip();
            this.toolStripStatusLabel1 = new System.Windows.Forms.ToolStripStatusLabel();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.ConnStrOtherBox = new System.Windows.Forms.TextBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.radReader = new System.Windows.Forms.RadioButton();
            this.radNonquery = new System.Windows.Forms.RadioButton();
            this.lblThreadCount = new System.Windows.Forms.Label();
            this.groupBox1.SuspendLayout();
            this.statusStrip1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.SuspendLayout();
            // 
            // txtHosts
            // 
            this.txtHosts.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.txtHosts.Location = new System.Drawing.Point(25, 40);
            this.txtHosts.Multiline = true;
            this.txtHosts.Name = "txtHosts";
            this.txtHosts.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtHosts.Size = new System.Drawing.Size(871, 146);
            this.txtHosts.TabIndex = 0;
            this.txtHosts.TextChanged += new System.EventHandler(this.txtHosts_TextChanged);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(25, 21);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(45, 13);
            this.label1.TabIndex = 1;
            this.label1.Text = "Monitor:";
            // 
            // txtClients
            // 
            this.txtClients.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.txtClients.Location = new System.Drawing.Point(25, 440);
            this.txtClients.Multiline = true;
            this.txtClients.Name = "txtClients";
            this.txtClients.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtClients.Size = new System.Drawing.Size(871, 117);
            this.txtClients.TabIndex = 2;
            this.txtClients.TextChanged += new System.EventHandler(this.txtClients_TextChanged);
            // 
            // txtSQL
            // 
            this.txtSQL.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.txtSQL.Location = new System.Drawing.Point(25, 602);
            this.txtSQL.Multiline = true;
            this.txtSQL.Name = "txtSQL";
            this.txtSQL.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtSQL.Size = new System.Drawing.Size(871, 119);
            this.txtSQL.TabIndex = 3;
            this.txtSQL.TextChanged += new System.EventHandler(this.txtSQL_TextChanged);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(25, 421);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(92, 13);
            this.label2.TabIndex = 4;
            this.label2.Text = "ADO.NET Clients:";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(25, 583);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(31, 13);
            this.label3.TabIndex = 5;
            this.label3.Text = "SQL:";
            // 
            // btnStartMon
            // 
            this.btnStartMon.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnStartMon.Enabled = false;
            this.btnStartMon.Location = new System.Drawing.Point(739, 316);
            this.btnStartMon.Name = "btnStartMon";
            this.btnStartMon.Size = new System.Drawing.Size(75, 23);
            this.btnStartMon.TabIndex = 6;
            this.btnStartMon.Text = "Start";
            this.btnStartMon.UseVisualStyleBackColor = true;
            this.btnStartMon.Click += new System.EventHandler(this.btnStartMon_Click);
            // 
            // btnStopMon
            // 
            this.btnStopMon.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnStopMon.Enabled = false;
            this.btnStopMon.Location = new System.Drawing.Point(821, 316);
            this.btnStopMon.Name = "btnStopMon";
            this.btnStopMon.Size = new System.Drawing.Size(75, 23);
            this.btnStopMon.TabIndex = 7;
            this.btnStopMon.Text = "Stop";
            this.btnStopMon.UseVisualStyleBackColor = true;
            this.btnStopMon.Click += new System.EventHandler(this.btnStopMon_Click);
            // 
            // btnStartQuery
            // 
            this.btnStartQuery.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnStartQuery.Enabled = false;
            this.btnStartQuery.Location = new System.Drawing.Point(739, 750);
            this.btnStartQuery.Name = "btnStartQuery";
            this.btnStartQuery.Size = new System.Drawing.Size(75, 23);
            this.btnStartQuery.TabIndex = 8;
            this.btnStartQuery.Text = "Start";
            this.btnStartQuery.UseVisualStyleBackColor = true;
            this.btnStartQuery.Click += new System.EventHandler(this.btnStartQuery_Click);
            // 
            // btnStopQuery
            // 
            this.btnStopQuery.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnStopQuery.Enabled = false;
            this.btnStopQuery.Location = new System.Drawing.Point(821, 749);
            this.btnStopQuery.Name = "btnStopQuery";
            this.btnStopQuery.Size = new System.Drawing.Size(75, 23);
            this.btnStopQuery.TabIndex = 9;
            this.btnStopQuery.Text = "Stop";
            this.btnStopQuery.UseVisualStyleBackColor = true;
            this.btnStopQuery.Click += new System.EventHandler(this.btnStopQuery_Click);
            // 
            // progressBar1
            // 
            this.progressBar1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.progressBar1.Location = new System.Drawing.Point(25, 219);
            this.progressBar1.Maximum = 20971520;
            this.progressBar1.Name = "progressBar1";
            this.progressBar1.Size = new System.Drawing.Size(871, 28);
            this.progressBar1.TabIndex = 10;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(25, 284);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(0, 13);
            this.label4.TabIndex = 11;
            // 
            // lblIndex0
            // 
            this.lblIndex0.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.lblIndex0.AutoSize = true;
            this.lblIndex0.Location = new System.Drawing.Point(25, 254);
            this.lblIndex0.Name = "lblIndex0";
            this.lblIndex0.Size = new System.Drawing.Size(40, 13);
            this.lblIndex0.TabIndex = 13;
            this.lblIndex0.Text = "0 Gb/s";
            // 
            // lblIndex1
            // 
            this.lblIndex1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.lblIndex1.AutoSize = true;
            this.lblIndex1.Location = new System.Drawing.Point(394, 254);
            this.lblIndex1.Name = "lblIndex1";
            this.lblIndex1.Size = new System.Drawing.Size(40, 13);
            this.lblIndex1.TabIndex = 14;
            this.lblIndex1.Text = "5 Gb/s";
            // 
            // lblIndex2
            // 
            this.lblIndex2.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.lblIndex2.AutoSize = true;
            this.lblIndex2.Location = new System.Drawing.Point(850, 254);
            this.lblIndex2.Name = "lblIndex2";
            this.lblIndex2.Size = new System.Drawing.Size(46, 13);
            this.lblIndex2.TabIndex = 15;
            this.lblIndex2.Text = "10 Gb/s";
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.radRobin);
            this.groupBox1.Controls.Add(this.radRandom);
            this.groupBox1.Location = new System.Drawing.Point(25, 316);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(119, 81);
            this.groupBox1.TabIndex = 16;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Data Source";
            // 
            // radRobin
            // 
            this.radRobin.AutoSize = true;
            this.radRobin.Location = new System.Drawing.Point(10, 50);
            this.radRobin.Name = "radRobin";
            this.radRobin.Size = new System.Drawing.Size(88, 17);
            this.radRobin.TabIndex = 1;
            this.radRobin.Text = "Round Robin";
            this.radRobin.UseVisualStyleBackColor = true;
            // 
            // radRandom
            // 
            this.radRandom.AutoSize = true;
            this.radRandom.Checked = true;
            this.radRandom.Location = new System.Drawing.Point(10, 24);
            this.radRandom.Name = "radRandom";
            this.radRandom.Size = new System.Drawing.Size(65, 17);
            this.radRandom.TabIndex = 0;
            this.radRandom.TabStop = true;
            this.radRandom.Text = "Random";
            this.radRandom.UseVisualStyleBackColor = true;
            // 
            // btnKillall
            // 
            this.btnKillall.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnKillall.Enabled = false;
            this.btnKillall.Location = new System.Drawing.Point(802, 363);
            this.btnKillall.Name = "btnKillall";
            this.btnKillall.Size = new System.Drawing.Size(94, 23);
            this.btnKillall.TabIndex = 17;
            this.btnKillall.Text = "Kill All Protocol";
            this.btnKillall.UseVisualStyleBackColor = true;
            this.btnKillall.Click += new System.EventHandler(this.btnKillall_Click);
            // 
            // txtStatus
            // 
            this.txtStatus.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.txtStatus.Location = new System.Drawing.Point(28, 799);
            this.txtStatus.Multiline = true;
            this.txtStatus.Name = "txtStatus";
            this.txtStatus.ReadOnly = true;
            this.txtStatus.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtStatus.Size = new System.Drawing.Size(868, 82);
            this.txtStatus.TabIndex = 18;
            // 
            // statusStrip1
            // 
            this.statusStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripStatusLabel1});
            this.statusStrip1.Location = new System.Drawing.Point(0, 926);
            this.statusStrip1.Name = "statusStrip1";
            this.statusStrip1.Size = new System.Drawing.Size(927, 22);
            this.statusStrip1.TabIndex = 19;
            this.statusStrip1.Text = "statusStrip1";
            // 
            // toolStripStatusLabel1
            // 
            this.toolStripStatusLabel1.Name = "toolStripStatusLabel1";
            this.toolStripStatusLabel1.Size = new System.Drawing.Size(109, 17);
            this.toolStripStatusLabel1.Text = "toolStripStatusLabel1";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.ConnStrOtherBox);
            this.groupBox2.Location = new System.Drawing.Point(339, 316);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(373, 51);
            this.groupBox2.TabIndex = 20;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Other connection string settings";
            // 
            // ConnStrOtherBox
            // 
            this.ConnStrOtherBox.Location = new System.Drawing.Point(16, 19);
            this.ConnStrOtherBox.Name = "ConnStrOtherBox";
            this.ConnStrOtherBox.Size = new System.Drawing.Size(340, 20);
            this.ConnStrOtherBox.TabIndex = 0;
            this.ConnStrOtherBox.Text = "Batch Size = 64MB; MR.DFS Block Size = 16MB";
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.radReader);
            this.groupBox3.Controls.Add(this.radNonquery);
            this.groupBox3.Location = new System.Drawing.Point(181, 316);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(119, 81);
            this.groupBox3.TabIndex = 21;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Mode";
            // 
            // radReader
            // 
            this.radReader.AutoSize = true;
            this.radReader.Location = new System.Drawing.Point(17, 50);
            this.radReader.Name = "radReader";
            this.radReader.Size = new System.Drawing.Size(60, 17);
            this.radReader.TabIndex = 1;
            this.radReader.TabStop = true;
            this.radReader.Text = "Reader";
            this.radReader.UseVisualStyleBackColor = true;
            // 
            // radNonquery
            // 
            this.radNonquery.AutoSize = true;
            this.radNonquery.Checked = true;
            this.radNonquery.Location = new System.Drawing.Point(17, 24);
            this.radNonquery.Name = "radNonquery";
            this.radNonquery.Size = new System.Drawing.Size(74, 17);
            this.radNonquery.TabIndex = 0;
            this.radNonquery.TabStop = true;
            this.radNonquery.Text = "Non-query";
            this.radNonquery.UseVisualStyleBackColor = true;
            // 
            // lblThreadCount
            // 
            this.lblThreadCount.AutoSize = true;
            this.lblThreadCount.Location = new System.Drawing.Point(25, 728);
            this.lblThreadCount.Name = "lblThreadCount";
            this.lblThreadCount.Size = new System.Drawing.Size(35, 13);
            this.lblThreadCount.TabIndex = 22;
            this.lblThreadCount.Text = "label5";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(927, 948);
            this.Controls.Add(this.lblThreadCount);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.statusStrip1);
            this.Controls.Add(this.txtStatus);
            this.Controls.Add(this.btnKillall);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.lblIndex2);
            this.Controls.Add(this.lblIndex1);
            this.Controls.Add(this.lblIndex0);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.progressBar1);
            this.Controls.Add(this.btnStopQuery);
            this.Controls.Add(this.btnStartQuery);
            this.Controls.Add(this.btnStopMon);
            this.Controls.Add(this.btnStartMon);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.txtSQL);
            this.Controls.Add(this.txtClients);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.txtHosts);
            this.Name = "Form1";
            this.Text = "ADO.NET Performance Monitor";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.Resize += new System.EventHandler(this.Form1_Resize);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.statusStrip1.ResumeLayout(false);
            this.statusStrip1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtHosts;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtClients;
        private System.Windows.Forms.TextBox txtSQL;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Button btnStartMon;
        private System.Windows.Forms.Button btnStopMon;
        private System.Windows.Forms.Button btnStartQuery;
        private System.Windows.Forms.Button btnStopQuery;
        private System.Windows.Forms.ProgressBar progressBar1;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label lblIndex0;
        private System.Windows.Forms.Label lblIndex1;
        private System.Windows.Forms.Label lblIndex2;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.RadioButton radRobin;
        private System.Windows.Forms.RadioButton radRandom;
        private System.Windows.Forms.Button btnKillall;
        private System.Windows.Forms.TextBox txtStatus;
        private System.Windows.Forms.StatusStrip statusStrip1;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.TextBox ConnStrOtherBox;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.RadioButton radReader;
        private System.Windows.Forms.RadioButton radNonquery;
        private System.Windows.Forms.Label lblThreadCount;
    }
}

