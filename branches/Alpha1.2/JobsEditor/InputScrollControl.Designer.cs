namespace MySpace.DataMining.AELight
{
    partial class InputScrollControl
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.CurrentInputBox = new System.Windows.Forms.TextBox();
            this.InputScroll = new System.Windows.Forms.VScrollBar();
            this.label2 = new System.Windows.Forms.Label();
            this.NudgeBack = new System.Windows.Forms.Button();
            this.NudgeForward = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 10);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(34, 13);
            this.label1.TabIndex = 4;
            this.label1.Text = "Input:";
            // 
            // CurrentInputBox
            // 
            this.CurrentInputBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.CurrentInputBox.BackColor = System.Drawing.SystemColors.Control;
            this.CurrentInputBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.CurrentInputBox.Location = new System.Drawing.Point(15, 26);
            this.CurrentInputBox.MaxLength = 1073741824;
            this.CurrentInputBox.Name = "CurrentInputBox";
            this.CurrentInputBox.Size = new System.Drawing.Size(206, 13);
            this.CurrentInputBox.TabIndex = 5;
            this.CurrentInputBox.TextChanged += new System.EventHandler(this.CurrentInputBox_TextChanged);
            // 
            // InputScroll
            // 
            this.InputScroll.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.InputScroll.LargeChange = 1;
            this.InputScroll.Location = new System.Drawing.Point(199, 10);
            this.InputScroll.Maximum = 0;
            this.InputScroll.Name = "InputScroll";
            this.InputScroll.Size = new System.Drawing.Size(22, 379);
            this.InputScroll.TabIndex = 0;
            this.InputScroll.TabStop = true;
            this.InputScroll.Scroll += new System.Windows.Forms.ScrollEventHandler(this.InputScroll_Scroll);
            // 
            // label2
            // 
            this.label2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(41, 373);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(39, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "Nudge";
            // 
            // NudgeBack
            // 
            this.NudgeBack.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.NudgeBack.Font = new System.Drawing.Font("Verdana", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.NudgeBack.Location = new System.Drawing.Point(15, 369);
            this.NudgeBack.Name = "NudgeBack";
            this.NudgeBack.Size = new System.Drawing.Size(20, 20);
            this.NudgeBack.TabIndex = 1;
            this.NudgeBack.Text = "<";
            this.NudgeBack.UseVisualStyleBackColor = true;
            // 
            // NudgeForward
            // 
            this.NudgeForward.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.NudgeForward.Font = new System.Drawing.Font("Verdana", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.NudgeForward.Location = new System.Drawing.Point(86, 369);
            this.NudgeForward.Name = "NudgeForward";
            this.NudgeForward.Size = new System.Drawing.Size(20, 20);
            this.NudgeForward.TabIndex = 3;
            this.NudgeForward.Text = ">";
            this.NudgeForward.UseVisualStyleBackColor = true;
            // 
            // InputScrollControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.NudgeForward);
            this.Controls.Add(this.NudgeBack);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.InputScroll);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.CurrentInputBox);
            this.MinimumSize = new System.Drawing.Size(150, 150);
            this.Name = "InputScrollControl";
            this.Size = new System.Drawing.Size(235, 402);
            this.Load += new System.EventHandler(this.InputScrollControl_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        public System.Windows.Forms.TextBox CurrentInputBox;
        public System.Windows.Forms.VScrollBar InputScroll;
        private System.Windows.Forms.Label label2;
        public System.Windows.Forms.Button NudgeBack;
        public System.Windows.Forms.Button NudgeForward;
    }
}
