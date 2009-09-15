namespace MySpace.DataMining.DistributedObjects.SeqFileView
{
    partial class SelOpenDialog
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
            this.TextBoxFile = new System.Windows.Forms.TextBox();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.RadioZblockMode = new System.Windows.Forms.RadioButton();
            this.RadioLineMode = new System.Windows.Forms.RadioButton();
            this.RadioHexMode = new System.Windows.Forms.RadioButton();
            this.ButtonBrowse = new System.Windows.Forms.Button();
            this.ButtonOpen = new System.Windows.Forms.Button();
            this.ButtonCancel = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 15);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(26, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "File:";
            // 
            // TextBoxFile
            // 
            this.TextBoxFile.Location = new System.Drawing.Point(55, 12);
            this.TextBoxFile.Name = "TextBoxFile";
            this.TextBoxFile.Size = new System.Drawing.Size(202, 20);
            this.TextBoxFile.TabIndex = 1;
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.RadioZblockMode);
            this.groupBox1.Controls.Add(this.RadioLineMode);
            this.groupBox1.Controls.Add(this.RadioHexMode);
            this.groupBox1.Location = new System.Drawing.Point(15, 38);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(327, 94);
            this.groupBox1.TabIndex = 3;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Open As";
            // 
            // RadioZblockMode
            // 
            this.RadioZblockMode.AutoSize = true;
            this.RadioZblockMode.Location = new System.Drawing.Point(17, 65);
            this.RadioZblockMode.Name = "RadioZblockMode";
            this.RadioZblockMode.Size = new System.Drawing.Size(202, 17);
            this.RadioZblockMode.TabIndex = 2;
            this.RadioZblockMode.TabStop = true;
            this.RadioZblockMode.Text = "zBlock mode - 2 binary Int32s per row";
            this.RadioZblockMode.UseVisualStyleBackColor = true;
            // 
            // RadioLineMode
            // 
            this.RadioLineMode.AutoSize = true;
            this.RadioLineMode.Location = new System.Drawing.Point(17, 42);
            this.RadioLineMode.Name = "RadioLineMode";
            this.RadioLineMode.Size = new System.Drawing.Size(188, 17);
            this.RadioLineMode.TabIndex = 1;
            this.RadioLineMode.TabStop = true;
            this.RadioLineMode.Text = "Line mode - one ASCII line per row";
            this.RadioLineMode.UseVisualStyleBackColor = true;
            // 
            // RadioHexMode
            // 
            this.RadioHexMode.AutoSize = true;
            this.RadioHexMode.Checked = true;
            this.RadioHexMode.Location = new System.Drawing.Point(17, 19);
            this.RadioHexMode.Name = "RadioHexMode";
            this.RadioHexMode.Size = new System.Drawing.Size(198, 17);
            this.RadioHexMode.TabIndex = 0;
            this.RadioHexMode.TabStop = true;
            this.RadioHexMode.Text = "Generic hex mode - 16 bytes per row";
            this.RadioHexMode.UseVisualStyleBackColor = true;
            // 
            // ButtonBrowse
            // 
            this.ButtonBrowse.Location = new System.Drawing.Point(267, 10);
            this.ButtonBrowse.Name = "ButtonBrowse";
            this.ButtonBrowse.Size = new System.Drawing.Size(75, 23);
            this.ButtonBrowse.TabIndex = 2;
            this.ButtonBrowse.Text = "&Browse...";
            this.ButtonBrowse.UseVisualStyleBackColor = true;
            this.ButtonBrowse.Click += new System.EventHandler(this.ButtonBrowse_Click);
            // 
            // ButtonOpen
            // 
            this.ButtonOpen.Location = new System.Drawing.Point(182, 143);
            this.ButtonOpen.Name = "ButtonOpen";
            this.ButtonOpen.Size = new System.Drawing.Size(75, 23);
            this.ButtonOpen.TabIndex = 4;
            this.ButtonOpen.Text = "&Open";
            this.ButtonOpen.UseVisualStyleBackColor = true;
            this.ButtonOpen.Click += new System.EventHandler(this.ButtonOpen_Click);
            // 
            // ButtonCancel
            // 
            this.ButtonCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.ButtonCancel.Location = new System.Drawing.Point(267, 143);
            this.ButtonCancel.Name = "ButtonCancel";
            this.ButtonCancel.Size = new System.Drawing.Size(75, 23);
            this.ButtonCancel.TabIndex = 5;
            this.ButtonCancel.Text = "Cancel";
            this.ButtonCancel.UseVisualStyleBackColor = true;
            // 
            // SelOpenDialog
            // 
            this.AcceptButton = this.ButtonOpen;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.ButtonCancel;
            this.ClientSize = new System.Drawing.Size(354, 178);
            this.Controls.Add(this.ButtonCancel);
            this.Controls.Add(this.ButtonOpen);
            this.Controls.Add(this.ButtonBrowse);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.TextBoxFile);
            this.Controls.Add(this.label1);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "SelOpenDialog";
            this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Open File";
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button ButtonBrowse;
        private System.Windows.Forms.Button ButtonOpen;
        private System.Windows.Forms.Button ButtonCancel;
        public System.Windows.Forms.TextBox TextBoxFile;
        public System.Windows.Forms.RadioButton RadioZblockMode;
        public System.Windows.Forms.RadioButton RadioLineMode;
        public System.Windows.Forms.RadioButton RadioHexMode;
    }
}