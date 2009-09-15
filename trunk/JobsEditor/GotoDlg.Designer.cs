namespace MySpace.DataMining.AELight
{
    partial class GotoDlg
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
            this.LineNumBox = new System.Windows.Forms.TextBox();
            this.GotoBtn = new System.Windows.Forms.Button();
            this.CancelBtn = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(30, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "&Line:";
            // 
            // LineNumBox
            // 
            this.LineNumBox.Location = new System.Drawing.Point(48, 6);
            this.LineNumBox.MaxLength = 30;
            this.LineNumBox.Name = "LineNumBox";
            this.LineNumBox.Size = new System.Drawing.Size(60, 20);
            this.LineNumBox.TabIndex = 1;
            this.LineNumBox.Validating += new System.ComponentModel.CancelEventHandler(this.LineNumBox_Validating);
            // 
            // GotoBtn
            // 
            this.GotoBtn.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.GotoBtn.Location = new System.Drawing.Point(114, 6);
            this.GotoBtn.Name = "GotoBtn";
            this.GotoBtn.Size = new System.Drawing.Size(75, 23);
            this.GotoBtn.TabIndex = 2;
            this.GotoBtn.Text = "&Go To";
            this.GotoBtn.UseVisualStyleBackColor = true;
            this.GotoBtn.Click += new System.EventHandler(this.GotoBtn_Click);
            // 
            // CancelBtn
            // 
            this.CancelBtn.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.CancelBtn.Location = new System.Drawing.Point(114, 35);
            this.CancelBtn.Name = "CancelBtn";
            this.CancelBtn.Size = new System.Drawing.Size(75, 23);
            this.CancelBtn.TabIndex = 3;
            this.CancelBtn.Text = "Cancel";
            this.CancelBtn.UseVisualStyleBackColor = true;
            // 
            // GotoDlg
            // 
            this.AcceptButton = this.GotoBtn;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.CancelBtn;
            this.ClientSize = new System.Drawing.Size(197, 67);
            this.Controls.Add(this.CancelBtn);
            this.Controls.Add(this.GotoBtn);
            this.Controls.Add(this.LineNumBox);
            this.Controls.Add(this.label1);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "GotoDlg";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Go To";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button GotoBtn;
        private System.Windows.Forms.Button CancelBtn;
        public System.Windows.Forms.TextBox LineNumBox;
    }
}