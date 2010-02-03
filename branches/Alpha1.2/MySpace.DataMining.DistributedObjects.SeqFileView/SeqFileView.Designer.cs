namespace MySpace.DataMining.DistributedObjects.SeqFileView
{
    partial class SeqFileViewForm
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
            this.ButtonOpen = new System.Windows.Forms.Button();
            this.ButtonDown = new System.Windows.Forms.Button();
            this.Display = new System.Windows.Forms.RichTextBox();
            this.LabelLocation = new System.Windows.Forms.Label();
            this.LabelHex = new System.Windows.Forms.Label();
            this.LabelAscii = new System.Windows.Forms.Label();
            this.LabelAnsiShow = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // ButtonOpen
            // 
            this.ButtonOpen.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.ButtonOpen.Location = new System.Drawing.Point(12, 388);
            this.ButtonOpen.Name = "ButtonOpen";
            this.ButtonOpen.Size = new System.Drawing.Size(75, 23);
            this.ButtonOpen.TabIndex = 0;
            this.ButtonOpen.Text = "&Open...";
            this.ButtonOpen.UseVisualStyleBackColor = true;
            this.ButtonOpen.Click += new System.EventHandler(this.ButtonOpen_Click);
            // 
            // ButtonDown
            // 
            this.ButtonDown.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.ButtonDown.Enabled = false;
            this.ButtonDown.Location = new System.Drawing.Point(542, 388);
            this.ButtonDown.Name = "ButtonDown";
            this.ButtonDown.Size = new System.Drawing.Size(88, 23);
            this.ButtonDown.TabIndex = 1;
            this.ButtonDown.Text = "Page &Down";
            this.ButtonDown.UseVisualStyleBackColor = true;
            this.ButtonDown.Click += new System.EventHandler(this.ButtonDown_Click);
            // 
            // Display
            // 
            this.Display.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
                        | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.Display.BackColor = System.Drawing.SystemColors.Control;
            this.Display.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.Display.Font = new System.Drawing.Font("Courier New", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Display.ForeColor = System.Drawing.SystemColors.ControlText;
            this.Display.Location = new System.Drawing.Point(12, 25);
            this.Display.Name = "Display";
            this.Display.ReadOnly = true;
            this.Display.ScrollBars = System.Windows.Forms.RichTextBoxScrollBars.Vertical;
            this.Display.Size = new System.Drawing.Size(618, 357);
            this.Display.TabIndex = 2;
            this.Display.Text = "";
            this.Display.WordWrap = false;
            // 
            // LabelLocation
            // 
            this.LabelLocation.AutoSize = true;
            this.LabelLocation.Location = new System.Drawing.Point(12, 9);
            this.LabelLocation.Name = "LabelLocation";
            this.LabelLocation.Size = new System.Drawing.Size(0, 13);
            this.LabelLocation.TabIndex = 3;
            // 
            // LabelHex
            // 
            this.LabelHex.AutoSize = true;
            this.LabelHex.Location = new System.Drawing.Point(87, 9);
            this.LabelHex.Name = "LabelHex";
            this.LabelHex.Size = new System.Drawing.Size(0, 13);
            this.LabelHex.TabIndex = 4;
            // 
            // LabelAscii
            // 
            this.LabelAscii.AutoSize = true;
            this.LabelAscii.Location = new System.Drawing.Point(463, 9);
            this.LabelAscii.Name = "LabelAscii";
            this.LabelAscii.Size = new System.Drawing.Size(0, 13);
            this.LabelAscii.TabIndex = 5;
            // 
            // LabelAnsiShow
            // 
            this.LabelAnsiShow.Anchor = System.Windows.Forms.AnchorStyles.Bottom;
            this.LabelAnsiShow.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(255)))), ((int)(((byte)(128)))));
            this.LabelAnsiShow.ForeColor = System.Drawing.Color.Black;
            this.LabelAnsiShow.Location = new System.Drawing.Point(280, 388);
            this.LabelAnsiShow.Name = "LabelAnsiShow";
            this.LabelAnsiShow.Size = new System.Drawing.Size(83, 23);
            this.LabelAnsiShow.TabIndex = 6;
            this.LabelAnsiShow.Text = "Characters";
            this.LabelAnsiShow.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.LabelAnsiShow.MouseDown += new System.Windows.Forms.MouseEventHandler(this.LabelAnsiShow_MouseDown);
            this.LabelAnsiShow.MouseHover += new System.EventHandler(this.LabelAnsiShow_MouseHover);
            // 
            // SeqFileViewForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(642, 423);
            this.Controls.Add(this.LabelAnsiShow);
            this.Controls.Add(this.LabelAscii);
            this.Controls.Add(this.LabelHex);
            this.Controls.Add(this.LabelLocation);
            this.Controls.Add(this.Display);
            this.Controls.Add(this.ButtonDown);
            this.Controls.Add(this.ButtonOpen);
            this.MinimumSize = new System.Drawing.Size(650, 400);
            this.Name = "SeqFileViewForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.WindowsDefaultBounds;
            this.Text = "SeqFileView";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button ButtonOpen;
        private System.Windows.Forms.Button ButtonDown;
        private System.Windows.Forms.RichTextBox Display;
        private System.Windows.Forms.Label LabelLocation;
        private System.Windows.Forms.Label LabelHex;
        private System.Windows.Forms.Label LabelAscii;
        private System.Windows.Forms.Label LabelAnsiShow;
    }
}

