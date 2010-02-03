namespace MySpace.DataMining.AELight
{
    partial class FindDlg
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
            this.label1 = new System.Windows.Forms.Label();
            this.FindBox = new System.Windows.Forms.TextBox();
            this.FindNextBtn = new System.Windows.Forms.Button();
            this.MatchCaseCheck = new System.Windows.Forms.CheckBox();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.DirDownRadio = new System.Windows.Forms.RadioButton();
            this.DirUpRadio = new System.Windows.Forms.RadioButton();
            this.CancelBtn = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(56, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Fi&nd what:";
            // 
            // FindBox
            // 
            this.FindBox.Location = new System.Drawing.Point(74, 6);
            this.FindBox.Name = "FindBox";
            this.FindBox.Size = new System.Drawing.Size(185, 20);
            this.FindBox.TabIndex = 1;
            // 
            // FindNextBtn
            // 
            this.FindNextBtn.Location = new System.Drawing.Point(268, 4);
            this.FindNextBtn.Name = "FindNextBtn";
            this.FindNextBtn.Size = new System.Drawing.Size(75, 23);
            this.FindNextBtn.TabIndex = 4;
            this.FindNextBtn.Text = "&Find Next";
            this.FindNextBtn.UseVisualStyleBackColor = true;
            // 
            // MatchCaseCheck
            // 
            this.MatchCaseCheck.AutoSize = true;
            this.MatchCaseCheck.Location = new System.Drawing.Point(15, 71);
            this.MatchCaseCheck.Name = "MatchCaseCheck";
            this.MatchCaseCheck.Size = new System.Drawing.Size(82, 17);
            this.MatchCaseCheck.TabIndex = 2;
            this.MatchCaseCheck.Text = "Match &case";
            this.MatchCaseCheck.UseVisualStyleBackColor = true;
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.DirDownRadio);
            this.groupBox1.Controls.Add(this.DirUpRadio);
            this.groupBox1.Location = new System.Drawing.Point(137, 44);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(121, 43);
            this.groupBox1.TabIndex = 3;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Direction";
            this.groupBox1.Visible = false;
            // 
            // DirDownRadio
            // 
            this.DirDownRadio.AutoSize = true;
            this.DirDownRadio.Checked = true;
            this.DirDownRadio.Location = new System.Drawing.Point(60, 19);
            this.DirDownRadio.Name = "DirDownRadio";
            this.DirDownRadio.Size = new System.Drawing.Size(53, 17);
            this.DirDownRadio.TabIndex = 1;
            this.DirDownRadio.TabStop = true;
            this.DirDownRadio.Text = "&Down";
            this.DirDownRadio.UseVisualStyleBackColor = true;
            // 
            // DirUpRadio
            // 
            this.DirUpRadio.AutoSize = true;
            this.DirUpRadio.Location = new System.Drawing.Point(15, 19);
            this.DirUpRadio.Name = "DirUpRadio";
            this.DirUpRadio.Size = new System.Drawing.Size(39, 17);
            this.DirUpRadio.TabIndex = 0;
            this.DirUpRadio.Text = "&Up";
            this.DirUpRadio.UseVisualStyleBackColor = true;
            // 
            // CancelBtn
            // 
            this.CancelBtn.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.CancelBtn.Location = new System.Drawing.Point(268, 33);
            this.CancelBtn.Name = "CancelBtn";
            this.CancelBtn.Size = new System.Drawing.Size(75, 23);
            this.CancelBtn.TabIndex = 5;
            this.CancelBtn.Text = "Cancel";
            this.CancelBtn.UseVisualStyleBackColor = true;
            this.CancelBtn.Click += new System.EventHandler(this.CancelBtn_Click);
            // 
            // FindDlg
            // 
            this.AcceptButton = this.FindNextBtn;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.CancelBtn;
            this.ClientSize = new System.Drawing.Size(355, 100);
            this.Controls.Add(this.CancelBtn);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.MatchCaseCheck);
            this.Controls.Add(this.FindBox);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.FindNextBtn);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "FindDlg";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.Manual;
            this.Text = "Find";
            this.Load += new System.EventHandler(this.FindDlg_Load);
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FindDlg_FormClosing);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button CancelBtn;
        public System.Windows.Forms.TextBox FindBox;
        public System.Windows.Forms.CheckBox MatchCaseCheck;
        public System.Windows.Forms.RadioButton DirDownRadio;
        public System.Windows.Forms.RadioButton DirUpRadio;
        public System.Windows.Forms.Button FindNextBtn;
    }
}