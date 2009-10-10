namespace MySpace.DataMining.AELight
{
    partial class JobsEditor
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
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(JobsEditor));
            this.DocContextMenu = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.undoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.redoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripSeparator();
            this.cutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.pasteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.deleteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem2 = new System.Windows.Forms.ToolStripSeparator();
            this.selectAllToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.panel1 = new System.Windows.Forms.Panel();
            this.CancelBtn = new System.Windows.Forms.Button();
            this.SaveBtn = new System.Windows.Forms.Button();
            this.status = new System.Windows.Forms.Label();
            this.TopMenu = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.saveToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem5 = new System.Windows.Forms.ToolStripSeparator();
            this.exitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.findToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.findNextToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.replaceToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.goToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.debugToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.startDebuggingToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.stopDebuggingToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem3 = new System.Windows.Forms.ToolStripSeparator();
            this.stepIntoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.stepOverToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.stepOutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem6 = new System.Windows.Forms.ToolStripSeparator();
            this.toggleBreakpointToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.deleteAllBreakpointsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem4 = new System.Windows.Forms.ToolStripSeparator();
            this.skipToReduceToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.autoCompleteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.debugByProxyToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.debugShellExecToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.showToolbarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.helpToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.aboutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.Doc = new ScintillaNet.Scintilla();
            this.BottomSplit = new System.Windows.Forms.Splitter();
            this.BottomTabs = new System.Windows.Forms.TabControl();
            this.Tab1 = new System.Windows.Forms.TabPage();
            this.debuglocalssplit = new System.Windows.Forms.Splitter();
            this.DbgCallsList = new System.Windows.Forms.ListBox();
            this.DbgVars = new System.Windows.Forms.TreeView();
            this.Tab2 = new System.Windows.Forms.TabPage();
            this.DbgInput = new System.Windows.Forms.TextBox();
            this.DbgOutputContextMenu = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.clearToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.statusStrip1 = new System.Windows.Forms.StatusStrip();
            this.toolStripStatusLabel1 = new System.Windows.Forms.ToolStripStatusLabel();
            this.toolStrip1 = new System.Windows.Forms.ToolStrip();
            this.SaveStripButton = new System.Windows.Forms.ToolStripButton();
            this.FindStripButton = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.DebugStripButton = new System.Windows.Forms.ToolStripButton();
            this.DebugStopStripButton = new System.Windows.Forms.ToolStripButton();
            this.DebugStepIntoStripButton = new System.Windows.Forms.ToolStripButton();
            this.DebugStepOverStripButton = new System.Windows.Forms.ToolStripButton();
            this.DebugStepOutStripButton = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.DebugSkipToReduceStripButton = new System.Windows.Forms.ToolStripButton();
            this.DocContextMenu.SuspendLayout();
            this.panel1.SuspendLayout();
            this.TopMenu.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Doc)).BeginInit();
            this.BottomTabs.SuspendLayout();
            this.Tab1.SuspendLayout();
            this.Tab2.SuspendLayout();
            this.DbgOutputContextMenu.SuspendLayout();
            this.statusStrip1.SuspendLayout();
            this.toolStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // DocContextMenu
            // 
            this.DocContextMenu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.undoToolStripMenuItem,
            this.redoToolStripMenuItem,
            this.toolStripMenuItem1,
            this.cutToolStripMenuItem,
            this.copyToolStripMenuItem,
            this.pasteToolStripMenuItem,
            this.deleteToolStripMenuItem,
            this.toolStripMenuItem2,
            this.selectAllToolStripMenuItem});
            this.DocContextMenu.Name = "DocContextMenu";
            this.DocContextMenu.Size = new System.Drawing.Size(165, 170);
            this.DocContextMenu.Opening += new System.ComponentModel.CancelEventHandler(this.DocContextMenu_Opening);
            // 
            // undoToolStripMenuItem
            // 
            this.undoToolStripMenuItem.Name = "undoToolStripMenuItem";
            this.undoToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Z)));
            this.undoToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.undoToolStripMenuItem.Text = "&Undo";
            this.undoToolStripMenuItem.Click += new System.EventHandler(this.undoToolStripMenuItem_Click);
            // 
            // redoToolStripMenuItem
            // 
            this.redoToolStripMenuItem.Name = "redoToolStripMenuItem";
            this.redoToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Y)));
            this.redoToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.redoToolStripMenuItem.Text = "&Redo";
            this.redoToolStripMenuItem.Click += new System.EventHandler(this.redoToolStripMenuItem_Click);
            // 
            // toolStripMenuItem1
            // 
            this.toolStripMenuItem1.Name = "toolStripMenuItem1";
            this.toolStripMenuItem1.Size = new System.Drawing.Size(161, 6);
            // 
            // cutToolStripMenuItem
            // 
            this.cutToolStripMenuItem.Name = "cutToolStripMenuItem";
            this.cutToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.X)));
            this.cutToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.cutToolStripMenuItem.Text = "Cu&t";
            this.cutToolStripMenuItem.Click += new System.EventHandler(this.cutToolStripMenuItem_Click);
            // 
            // copyToolStripMenuItem
            // 
            this.copyToolStripMenuItem.Name = "copyToolStripMenuItem";
            this.copyToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.C)));
            this.copyToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.copyToolStripMenuItem.Text = "&Copy";
            this.copyToolStripMenuItem.Click += new System.EventHandler(this.copyToolStripMenuItem_Click);
            // 
            // pasteToolStripMenuItem
            // 
            this.pasteToolStripMenuItem.Name = "pasteToolStripMenuItem";
            this.pasteToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.V)));
            this.pasteToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.pasteToolStripMenuItem.Text = "&Paste";
            this.pasteToolStripMenuItem.Click += new System.EventHandler(this.pasteToolStripMenuItem_Click);
            // 
            // deleteToolStripMenuItem
            // 
            this.deleteToolStripMenuItem.Name = "deleteToolStripMenuItem";
            this.deleteToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.deleteToolStripMenuItem.Text = "&Delete";
            this.deleteToolStripMenuItem.Click += new System.EventHandler(this.deleteToolStripMenuItem_Click);
            // 
            // toolStripMenuItem2
            // 
            this.toolStripMenuItem2.Name = "toolStripMenuItem2";
            this.toolStripMenuItem2.Size = new System.Drawing.Size(161, 6);
            // 
            // selectAllToolStripMenuItem
            // 
            this.selectAllToolStripMenuItem.Name = "selectAllToolStripMenuItem";
            this.selectAllToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.A)));
            this.selectAllToolStripMenuItem.Size = new System.Drawing.Size(164, 22);
            this.selectAllToolStripMenuItem.Text = "Select &All";
            this.selectAllToolStripMenuItem.Click += new System.EventHandler(this.selectAllToolStripMenuItem_Click);
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.CancelBtn);
            this.panel1.Controls.Add(this.SaveBtn);
            this.panel1.Controls.Add(this.status);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel1.Location = new System.Drawing.Point(0, 662);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(960, 33);
            this.panel1.TabIndex = 1;
            // 
            // CancelBtn
            // 
            this.CancelBtn.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.CancelBtn.Location = new System.Drawing.Point(873, 6);
            this.CancelBtn.Name = "CancelBtn";
            this.CancelBtn.Size = new System.Drawing.Size(75, 23);
            this.CancelBtn.TabIndex = 1;
            this.CancelBtn.Text = "Cancel";
            this.CancelBtn.UseVisualStyleBackColor = true;
            this.CancelBtn.Click += new System.EventHandler(this.CancelBtn_Click);
            // 
            // SaveBtn
            // 
            this.SaveBtn.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.SaveBtn.Location = new System.Drawing.Point(792, 6);
            this.SaveBtn.Name = "SaveBtn";
            this.SaveBtn.Size = new System.Drawing.Size(75, 23);
            this.SaveBtn.TabIndex = 0;
            this.SaveBtn.Text = "OK";
            this.SaveBtn.UseVisualStyleBackColor = true;
            this.SaveBtn.Click += new System.EventHandler(this.SaveBtn_Click);
            this.SaveBtn.MouseDown += new System.Windows.Forms.MouseEventHandler(this.SaveBtn_MouseDown);
            // 
            // status
            // 
            this.status.AutoSize = true;
            this.status.Location = new System.Drawing.Point(12, 11);
            this.status.Name = "status";
            this.status.Size = new System.Drawing.Size(0, 13);
            this.status.TabIndex = 2;
            // 
            // TopMenu
            // 
            this.TopMenu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem,
            this.editToolStripMenuItem,
            this.debugToolStripMenuItem,
            this.toolsToolStripMenuItem,
            this.helpToolStripMenuItem});
            this.TopMenu.Location = new System.Drawing.Point(0, 0);
            this.TopMenu.Name = "TopMenu";
            this.TopMenu.Size = new System.Drawing.Size(960, 24);
            this.TopMenu.TabIndex = 2;
            this.TopMenu.Text = "menuStrip1";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.saveToolStripMenuItem,
            this.toolStripMenuItem5,
            this.exitToolStripMenuItem});
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileToolStripMenuItem.Text = "&File";
            // 
            // saveToolStripMenuItem
            // 
            this.saveToolStripMenuItem.Name = "saveToolStripMenuItem";
            this.saveToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.S)));
            this.saveToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.saveToolStripMenuItem.Text = "&Save";
            this.saveToolStripMenuItem.Click += new System.EventHandler(this.saveToolStripMenuItem_Click);
            // 
            // toolStripMenuItem5
            // 
            this.toolStripMenuItem5.Name = "toolStripMenuItem5";
            this.toolStripMenuItem5.Size = new System.Drawing.Size(135, 6);
            // 
            // exitToolStripMenuItem
            // 
            this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";
            this.exitToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.W)));
            this.exitToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.exitToolStripMenuItem.Text = "E&xit";
            this.exitToolStripMenuItem.Click += new System.EventHandler(this.exitToolStripMenuItem_Click);
            // 
            // editToolStripMenuItem
            // 
            this.editToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.findToolStripMenuItem,
            this.findNextToolStripMenuItem,
            this.replaceToolStripMenuItem,
            this.goToToolStripMenuItem});
            this.editToolStripMenuItem.Name = "editToolStripMenuItem";
            this.editToolStripMenuItem.Size = new System.Drawing.Size(39, 20);
            this.editToolStripMenuItem.Text = "&Edit";
            this.editToolStripMenuItem.Click += new System.EventHandler(this.editToolStripMenuItem_Click);
            // 
            // findToolStripMenuItem
            // 
            this.findToolStripMenuItem.Name = "findToolStripMenuItem";
            this.findToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.F)));
            this.findToolStripMenuItem.Size = new System.Drawing.Size(167, 22);
            this.findToolStripMenuItem.Text = "&Find...";
            this.findToolStripMenuItem.Click += new System.EventHandler(this.findToolStripMenuItem_Click);
            // 
            // findNextToolStripMenuItem
            // 
            this.findNextToolStripMenuItem.Name = "findNextToolStripMenuItem";
            this.findNextToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F3;
            this.findNextToolStripMenuItem.Size = new System.Drawing.Size(167, 22);
            this.findNextToolStripMenuItem.Text = "Find &Next";
            this.findNextToolStripMenuItem.Click += new System.EventHandler(this.findNextToolStripMenuItem_Click);
            // 
            // replaceToolStripMenuItem
            // 
            this.replaceToolStripMenuItem.Name = "replaceToolStripMenuItem";
            this.replaceToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.H)));
            this.replaceToolStripMenuItem.Size = new System.Drawing.Size(167, 22);
            this.replaceToolStripMenuItem.Text = "&Replace...";
            this.replaceToolStripMenuItem.Click += new System.EventHandler(this.replaceToolStripMenuItem_Click);
            // 
            // goToToolStripMenuItem
            // 
            this.goToToolStripMenuItem.Name = "goToToolStripMenuItem";
            this.goToToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.G)));
            this.goToToolStripMenuItem.Size = new System.Drawing.Size(167, 22);
            this.goToToolStripMenuItem.Text = "&Go to...";
            this.goToToolStripMenuItem.Click += new System.EventHandler(this.goToToolStripMenuItem_Click);
            // 
            // debugToolStripMenuItem
            // 
            this.debugToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.startDebuggingToolStripMenuItem,
            this.stopDebuggingToolStripMenuItem,
            this.toolStripMenuItem3,
            this.stepIntoToolStripMenuItem,
            this.stepOverToolStripMenuItem,
            this.stepOutToolStripMenuItem,
            this.toolStripMenuItem6,
            this.toggleBreakpointToolStripMenuItem,
            this.deleteAllBreakpointsToolStripMenuItem,
            this.toolStripMenuItem4,
            this.skipToReduceToolStripMenuItem});
            this.debugToolStripMenuItem.Name = "debugToolStripMenuItem";
            this.debugToolStripMenuItem.Size = new System.Drawing.Size(54, 20);
            this.debugToolStripMenuItem.Text = "&Debug";
            // 
            // startDebuggingToolStripMenuItem
            // 
            this.startDebuggingToolStripMenuItem.Name = "startDebuggingToolStripMenuItem";
            this.startDebuggingToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F5;
            this.startDebuggingToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.startDebuggingToolStripMenuItem.Text = "&Start Debugging";
            this.startDebuggingToolStripMenuItem.Click += new System.EventHandler(this.startDebuggingToolStripMenuItem_Click);
            // 
            // stopDebuggingToolStripMenuItem
            // 
            this.stopDebuggingToolStripMenuItem.Enabled = false;
            this.stopDebuggingToolStripMenuItem.Name = "stopDebuggingToolStripMenuItem";
            this.stopDebuggingToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F12;
            this.stopDebuggingToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.stopDebuggingToolStripMenuItem.Text = "Stop Debugging";
            this.stopDebuggingToolStripMenuItem.Click += new System.EventHandler(this.stopDebuggingToolStripMenuItem_Click);
            // 
            // toolStripMenuItem3
            // 
            this.toolStripMenuItem3.Name = "toolStripMenuItem3";
            this.toolStripMenuItem3.Size = new System.Drawing.Size(264, 6);
            // 
            // stepIntoToolStripMenuItem
            // 
            this.stepIntoToolStripMenuItem.Enabled = false;
            this.stepIntoToolStripMenuItem.Name = "stepIntoToolStripMenuItem";
            this.stepIntoToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F11;
            this.stepIntoToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.stepIntoToolStripMenuItem.Text = "Step &Into";
            this.stepIntoToolStripMenuItem.Click += new System.EventHandler(this.stepIntoToolStripMenuItem_Click);
            // 
            // stepOverToolStripMenuItem
            // 
            this.stepOverToolStripMenuItem.Enabled = false;
            this.stepOverToolStripMenuItem.Name = "stepOverToolStripMenuItem";
            this.stepOverToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F10;
            this.stepOverToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.stepOverToolStripMenuItem.Text = "Step &Over";
            this.stepOverToolStripMenuItem.Click += new System.EventHandler(this.stepOverToolStripMenuItem_Click);
            // 
            // stepOutToolStripMenuItem
            // 
            this.stepOutToolStripMenuItem.Enabled = false;
            this.stepOutToolStripMenuItem.Name = "stepOutToolStripMenuItem";
            this.stepOutToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Shift | System.Windows.Forms.Keys.F11)));
            this.stepOutToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.stepOutToolStripMenuItem.Text = "Step Out";
            this.stepOutToolStripMenuItem.Click += new System.EventHandler(this.stepOutToolStripMenuItem_Click);
            // 
            // toolStripMenuItem6
            // 
            this.toolStripMenuItem6.Name = "toolStripMenuItem6";
            this.toolStripMenuItem6.Size = new System.Drawing.Size(264, 6);
            // 
            // toggleBreakpointToolStripMenuItem
            // 
            this.toggleBreakpointToolStripMenuItem.Name = "toggleBreakpointToolStripMenuItem";
            this.toggleBreakpointToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F9;
            this.toggleBreakpointToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.toggleBreakpointToolStripMenuItem.Text = "To&ggle Breakpoint";
            this.toggleBreakpointToolStripMenuItem.Click += new System.EventHandler(this.toggleBreakpointToolStripMenuItem_Click);
            // 
            // deleteAllBreakpointsToolStripMenuItem
            // 
            this.deleteAllBreakpointsToolStripMenuItem.Name = "deleteAllBreakpointsToolStripMenuItem";
            this.deleteAllBreakpointsToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)(((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Shift)
                        | System.Windows.Forms.Keys.F9)));
            this.deleteAllBreakpointsToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.deleteAllBreakpointsToolStripMenuItem.Text = "Delete All Breakpoints";
            this.deleteAllBreakpointsToolStripMenuItem.Click += new System.EventHandler(this.deleteAllBreakpointsToolStripMenuItem_Click);
            // 
            // toolStripMenuItem4
            // 
            this.toolStripMenuItem4.Name = "toolStripMenuItem4";
            this.toolStripMenuItem4.Size = new System.Drawing.Size(264, 6);
            // 
            // skipToReduceToolStripMenuItem
            // 
            this.skipToReduceToolStripMenuItem.Enabled = false;
            this.skipToReduceToolStripMenuItem.Name = "skipToReduceToolStripMenuItem";
            this.skipToReduceToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Shift | System.Windows.Forms.Keys.F12)));
            this.skipToReduceToolStripMenuItem.Size = new System.Drawing.Size(267, 22);
            this.skipToReduceToolStripMenuItem.Text = "Skip to Reduce";
            this.skipToReduceToolStripMenuItem.Click += new System.EventHandler(this.skipToReduceToolStripMenuItem_Click);
            // 
            // toolsToolStripMenuItem
            // 
            this.toolsToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.autoCompleteToolStripMenuItem,
            this.debugByProxyToolStripMenuItem,
            this.debugShellExecToolStripMenuItem,
            this.showToolbarToolStripMenuItem});
            this.toolsToolStripMenuItem.Name = "toolsToolStripMenuItem";
            this.toolsToolStripMenuItem.Size = new System.Drawing.Size(48, 20);
            this.toolsToolStripMenuItem.Text = "&Tools";
            // 
            // autoCompleteToolStripMenuItem
            // 
            this.autoCompleteToolStripMenuItem.Checked = true;
            this.autoCompleteToolStripMenuItem.CheckState = System.Windows.Forms.CheckState.Checked;
            this.autoCompleteToolStripMenuItem.Name = "autoCompleteToolStripMenuItem";
            this.autoCompleteToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.autoCompleteToolStripMenuItem.Text = "Auto-Complete";
            this.autoCompleteToolStripMenuItem.Click += new System.EventHandler(this.autoCompleteToolStripMenuItem_Click);
            // 
            // debugByProxyToolStripMenuItem
            // 
            this.debugByProxyToolStripMenuItem.Name = "debugByProxyToolStripMenuItem";
            this.debugByProxyToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.debugByProxyToolStripMenuItem.Text = "Debug by &Proxy";
            this.debugByProxyToolStripMenuItem.Click += new System.EventHandler(this.debugByProxyToolStripMenuItem_Click);
            // 
            // debugShellExecToolStripMenuItem
            // 
            this.debugShellExecToolStripMenuItem.Name = "debugShellExecToolStripMenuItem";
            this.debugShellExecToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.debugShellExecToolStripMenuItem.Text = "Debug Shell Exec";
            this.debugShellExecToolStripMenuItem.Click += new System.EventHandler(this.debugShellExecToolStripMenuItem_Click);
            // 
            // showToolbarToolStripMenuItem
            // 
            this.showToolbarToolStripMenuItem.Name = "showToolbarToolStripMenuItem";
            this.showToolbarToolStripMenuItem.Size = new System.Drawing.Size(163, 22);
            this.showToolbarToolStripMenuItem.Text = "Show Toolbar";
            this.showToolbarToolStripMenuItem.Click += new System.EventHandler(this.showToolbarToolStripMenuItem_Click);
            // 
            // helpToolStripMenuItem
            // 
            this.helpToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.aboutToolStripMenuItem});
            this.helpToolStripMenuItem.Name = "helpToolStripMenuItem";
            this.helpToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.helpToolStripMenuItem.Text = "&Help";
            // 
            // aboutToolStripMenuItem
            // 
            this.aboutToolStripMenuItem.Name = "aboutToolStripMenuItem";
            this.aboutToolStripMenuItem.Size = new System.Drawing.Size(107, 22);
            this.aboutToolStripMenuItem.Text = "&About";
            this.aboutToolStripMenuItem.Click += new System.EventHandler(this.aboutToolStripMenuItem_Click);
            // 
            // Doc
            // 
            this.Doc.AutoComplete.CancelAtStart = false;
            this.Doc.AutoComplete.IsCaseSensitive = false;
            this.Doc.AutoComplete.ListString = "";
            this.Doc.AutoComplete.MaxWidth = 300;
            this.Doc.BackColor = System.Drawing.Color.White;
            this.Doc.ContextMenuStrip = this.DocContextMenu;
            this.Doc.Dock = System.Windows.Forms.DockStyle.Fill;
            this.Doc.Font = new System.Drawing.Font("Verdana", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Doc.ForeColor = System.Drawing.Color.Black;
            this.Doc.Indentation.IndentWidth = 4;
            this.Doc.Indentation.SmartIndentType = ScintillaNet.SmartIndent.CPP;
            this.Doc.Indentation.TabWidth = 4;
            this.Doc.Indentation.UseTabs = false;
            this.Doc.IsBraceMatching = true;
            this.Doc.Lexing.Lexer = ScintillaNet.Lexer.Null;
            this.Doc.Lexing.LexerName = "null";
            this.Doc.Lexing.LineCommentPrefix = "//";
            this.Doc.Lexing.StreamCommentPrefix = "/*";
            this.Doc.Lexing.StreamCommentSufix = "*/";
            this.Doc.LineWrap.Mode = ScintillaNet.WrapMode.Word;
            this.Doc.Location = new System.Drawing.Point(0, 24);
            this.Doc.Margin = new System.Windows.Forms.Padding(0);
            this.Doc.Margins.Margin0.Width = 10;
            this.Doc.Margins.Margin1.IsClickable = true;
            this.Doc.Margins.Margin1.Width = 14;
            this.Doc.Name = "Doc";
            this.Doc.Size = new System.Drawing.Size(960, 424);
            this.Doc.TabIndex = 3;
            this.Doc.Zoom = 1;
            this.Doc.Scroll += new System.EventHandler<System.Windows.Forms.ScrollEventArgs>(this.Doc_Scroll);
            this.Doc.Resize += new System.EventHandler(this.Doc_Resize);
            this.Doc.KeyUp += new System.Windows.Forms.KeyEventHandler(this.Doc_KeyUp);
            this.Doc.KeyDown += new System.Windows.Forms.KeyEventHandler(this.Doc_KeyDown);
            // 
            // BottomSplit
            // 
            this.BottomSplit.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.BottomSplit.Location = new System.Drawing.Point(0, 448);
            this.BottomSplit.Name = "BottomSplit";
            this.BottomSplit.Size = new System.Drawing.Size(960, 3);
            this.BottomSplit.TabIndex = 5;
            this.BottomSplit.TabStop = false;
            this.BottomSplit.Visible = false;
            // 
            // BottomTabs
            // 
            this.BottomTabs.Alignment = System.Windows.Forms.TabAlignment.Bottom;
            this.BottomTabs.Controls.Add(this.Tab1);
            this.BottomTabs.Controls.Add(this.Tab2);
            this.BottomTabs.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.BottomTabs.Location = new System.Drawing.Point(0, 451);
            this.BottomTabs.Name = "BottomTabs";
            this.BottomTabs.SelectedIndex = 0;
            this.BottomTabs.Size = new System.Drawing.Size(960, 211);
            this.BottomTabs.TabIndex = 6;
            this.BottomTabs.Visible = false;
            // 
            // Tab1
            // 
            this.Tab1.Controls.Add(this.debuglocalssplit);
            this.Tab1.Controls.Add(this.DbgCallsList);
            this.Tab1.Controls.Add(this.DbgVars);
            this.Tab1.Location = new System.Drawing.Point(4, 4);
            this.Tab1.Name = "Tab1";
            this.Tab1.Padding = new System.Windows.Forms.Padding(3);
            this.Tab1.Size = new System.Drawing.Size(952, 185);
            this.Tab1.TabIndex = 0;
            this.Tab1.Text = "Locals / Call Stack";
            this.Tab1.UseVisualStyleBackColor = true;
            // 
            // debuglocalssplit
            // 
            this.debuglocalssplit.Location = new System.Drawing.Point(474, 3);
            this.debuglocalssplit.Name = "debuglocalssplit";
            this.debuglocalssplit.Size = new System.Drawing.Size(3, 179);
            this.debuglocalssplit.TabIndex = 7;
            this.debuglocalssplit.TabStop = false;
            // 
            // DbgCallsList
            // 
            this.DbgCallsList.Dock = System.Windows.Forms.DockStyle.Fill;
            this.DbgCallsList.FormattingEnabled = true;
            this.DbgCallsList.IntegralHeight = false;
            this.DbgCallsList.Location = new System.Drawing.Point(474, 3);
            this.DbgCallsList.Name = "DbgCallsList";
            this.DbgCallsList.Size = new System.Drawing.Size(475, 179);
            this.DbgCallsList.TabIndex = 6;
            // 
            // DbgVars
            // 
            this.DbgVars.Dock = System.Windows.Forms.DockStyle.Left;
            this.DbgVars.Location = new System.Drawing.Point(3, 3);
            this.DbgVars.Name = "DbgVars";
            this.DbgVars.Size = new System.Drawing.Size(471, 179);
            this.DbgVars.TabIndex = 5;
            // 
            // Tab2
            // 
            this.Tab2.Controls.Add(this.DbgInput);
            this.Tab2.Location = new System.Drawing.Point(4, 4);
            this.Tab2.Name = "Tab2";
            this.Tab2.Size = new System.Drawing.Size(952, 185);
            this.Tab2.TabIndex = 1;
            this.Tab2.Text = "Output / Debug";
            this.Tab2.UseVisualStyleBackColor = true;
            // 
            // DbgInput
            // 
            this.DbgInput.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.DbgInput.HideSelection = false;
            this.DbgInput.Location = new System.Drawing.Point(0, 165);
            this.DbgInput.Name = "DbgInput";
            this.DbgInput.Size = new System.Drawing.Size(952, 20);
            this.DbgInput.TabIndex = 0;
            this.DbgInput.KeyDown += new System.Windows.Forms.KeyEventHandler(this.DbgInput_KeyDown);
            // 
            // DbgOutputContextMenu
            // 
            this.DbgOutputContextMenu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.clearToolStripMenuItem});
            this.DbgOutputContextMenu.Name = "DebugOutputContextMenu";
            this.DbgOutputContextMenu.Size = new System.Drawing.Size(102, 26);
            // 
            // clearToolStripMenuItem
            // 
            this.clearToolStripMenuItem.Name = "clearToolStripMenuItem";
            this.clearToolStripMenuItem.Size = new System.Drawing.Size(101, 22);
            this.clearToolStripMenuItem.Text = "Clear";
            this.clearToolStripMenuItem.Click += new System.EventHandler(this.clearToolStripMenuItem_Click);
            // 
            // statusStrip1
            // 
            this.statusStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripStatusLabel1});
            this.statusStrip1.Location = new System.Drawing.Point(0, 673);
            this.statusStrip1.Name = "statusStrip1";
            this.statusStrip1.Size = new System.Drawing.Size(960, 22);
            this.statusStrip1.SizingGrip = false;
            this.statusStrip1.TabIndex = 7;
            this.statusStrip1.Visible = false;
            // 
            // toolStripStatusLabel1
            // 
            this.toolStripStatusLabel1.Name = "toolStripStatusLabel1";
            this.toolStripStatusLabel1.Size = new System.Drawing.Size(0, 17);
            // 
            // toolStrip1
            // 
            this.toolStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.SaveStripButton,
            this.FindStripButton,
            this.toolStripSeparator1,
            this.DebugStripButton,
            this.DebugStopStripButton,
            this.DebugStepIntoStripButton,
            this.DebugStepOverStripButton,
            this.DebugStepOutStripButton,
            this.toolStripSeparator2,
            this.DebugSkipToReduceStripButton});
            this.toolStrip1.Location = new System.Drawing.Point(0, 24);
            this.toolStrip1.Name = "toolStrip1";
            this.toolStrip1.Size = new System.Drawing.Size(960, 25);
            this.toolStrip1.TabIndex = 0;
            this.toolStrip1.Text = "toolStrip1";
            this.toolStrip1.Visible = false;
            // 
            // SaveStripButton
            // 
            this.SaveStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.SaveStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.Save;
            this.SaveStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.SaveStripButton.Name = "SaveStripButton";
            this.SaveStripButton.Size = new System.Drawing.Size(23, 22);
            this.SaveStripButton.Tag = "";
            this.SaveStripButton.ToolTipText = "Save (Ctrl+S)";
            this.SaveStripButton.Click += new System.EventHandler(this.SaveStripButton_Click);
            // 
            // FindStripButton
            // 
            this.FindStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.FindStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.Find;
            this.FindStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.FindStripButton.Name = "FindStripButton";
            this.FindStripButton.Size = new System.Drawing.Size(23, 22);
            this.FindStripButton.Tag = "";
            this.FindStripButton.ToolTipText = "Find (Ctrl+F)";
            this.FindStripButton.Click += new System.EventHandler(this.FindStripButton_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(6, 25);
            this.toolStripSeparator1.Tag = "Dbg_1ff9";
            // 
            // DebugStripButton
            // 
            this.DebugStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.Debug;
            this.DebugStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugStripButton.Name = "DebugStripButton";
            this.DebugStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugStripButton.Tag = "Dbg_1ff9";
            this.DebugStripButton.ToolTipText = "Debug (F5)";
            this.DebugStripButton.Click += new System.EventHandler(this.DebugStripButton_Click);
            // 
            // DebugStopStripButton
            // 
            this.DebugStopStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugStopStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.DebugStop;
            this.DebugStopStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugStopStripButton.Name = "DebugStopStripButton";
            this.DebugStopStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugStopStripButton.Tag = "DbgOnly_1ff9";
            this.DebugStopStripButton.ToolTipText = "Stop Debugging (F12)";
            this.DebugStopStripButton.Visible = false;
            this.DebugStopStripButton.Click += new System.EventHandler(this.DebugStopStripButton_Click);
            // 
            // DebugStepIntoStripButton
            // 
            this.DebugStepIntoStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugStepIntoStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.DebugStepInto;
            this.DebugStepIntoStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugStepIntoStripButton.Name = "DebugStepIntoStripButton";
            this.DebugStepIntoStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugStepIntoStripButton.Tag = "DbgOnly_1ff9";
            this.DebugStepIntoStripButton.ToolTipText = "Step Into (F11)";
            this.DebugStepIntoStripButton.Visible = false;
            this.DebugStepIntoStripButton.Click += new System.EventHandler(this.DebugStepIntoStripButton_Click);
            // 
            // DebugStepOverStripButton
            // 
            this.DebugStepOverStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugStepOverStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.DebugStepOver;
            this.DebugStepOverStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugStepOverStripButton.Name = "DebugStepOverStripButton";
            this.DebugStepOverStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugStepOverStripButton.Tag = "DbgOnly_1ff9";
            this.DebugStepOverStripButton.ToolTipText = "Step Over (F10)";
            this.DebugStepOverStripButton.Visible = false;
            this.DebugStepOverStripButton.Click += new System.EventHandler(this.DebugStepOverStripButton_Click);
            // 
            // DebugStepOutStripButton
            // 
            this.DebugStepOutStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugStepOutStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.DebugStepOut;
            this.DebugStepOutStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugStepOutStripButton.Name = "DebugStepOutStripButton";
            this.DebugStepOutStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugStepOutStripButton.Tag = "DbgOnly_1ff9";
            this.DebugStepOutStripButton.ToolTipText = "Step Out (Shift+F11)";
            this.DebugStepOutStripButton.Visible = false;
            this.DebugStepOutStripButton.Click += new System.EventHandler(this.DebugStepOutStripButton_Click);
            // 
            // toolStripSeparator2
            // 
            this.toolStripSeparator2.Name = "toolStripSeparator2";
            this.toolStripSeparator2.Size = new System.Drawing.Size(6, 25);
            this.toolStripSeparator2.Tag = "DbgOnly_1ff9";
            this.toolStripSeparator2.Visible = false;
            // 
            // DebugSkipToReduceStripButton
            // 
            this.DebugSkipToReduceStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.DebugSkipToReduceStripButton.Enabled = false;
            this.DebugSkipToReduceStripButton.Image = global::MySpace.DataMining.AELight.Properties.Resources.DebugSkipToReduce;
            this.DebugSkipToReduceStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.DebugSkipToReduceStripButton.Name = "DebugSkipToReduceStripButton";
            this.DebugSkipToReduceStripButton.Size = new System.Drawing.Size(23, 22);
            this.DebugSkipToReduceStripButton.Tag = "DbgOnly_1ff9";
            this.DebugSkipToReduceStripButton.ToolTipText = "Skip to Reduce (Shift+F12)";
            this.DebugSkipToReduceStripButton.Visible = false;
            this.DebugSkipToReduceStripButton.Click += new System.EventHandler(this.DebugSkipToReduceStripButton_Click);
            // 
            // JobsEditor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(960, 695);
            this.Controls.Add(this.Doc);
            this.Controls.Add(this.BottomSplit);
            this.Controls.Add(this.BottomTabs);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.statusStrip1);
            this.Controls.Add(this.toolStrip1);
            this.Controls.Add(this.TopMenu);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.TopMenu;
            this.Name = "JobsEditor";
            this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "JobsEditor";
            this.Load += new System.EventHandler(this.JobsEditor_Load);
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.JobsEditor_FormClosing);
            this.DocContextMenu.ResumeLayout(false);
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.TopMenu.ResumeLayout(false);
            this.TopMenu.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Doc)).EndInit();
            this.BottomTabs.ResumeLayout(false);
            this.Tab1.ResumeLayout(false);
            this.Tab2.ResumeLayout(false);
            this.Tab2.PerformLayout();
            this.DbgOutputContextMenu.ResumeLayout(false);
            this.statusStrip1.ResumeLayout(false);
            this.statusStrip1.PerformLayout();
            this.toolStrip1.ResumeLayout(false);
            this.toolStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Button SaveBtn;
        private System.Windows.Forms.Button CancelBtn;
        private System.Windows.Forms.MenuStrip TopMenu;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem findToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem goToToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem findNextToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip DocContextMenu;
        private System.Windows.Forms.ToolStripMenuItem undoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem redoToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem cutToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem pasteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem deleteToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem2;
        private System.Windows.Forms.ToolStripMenuItem selectAllToolStripMenuItem;
        private System.Windows.Forms.Label status;
        private ScintillaNet.Scintilla Doc;
        private System.Windows.Forms.ToolStripMenuItem toolsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem autoCompleteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem debugToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem startDebuggingToolStripMenuItem;
        private System.Windows.Forms.Splitter BottomSplit;
        private System.Windows.Forms.ToolStripMenuItem stepIntoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem stepOverToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem stepOutToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem stopDebuggingToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem3;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem4;
        private System.Windows.Forms.ToolStripMenuItem skipToReduceToolStripMenuItem;
        private System.Windows.Forms.TabControl BottomTabs;
        private System.Windows.Forms.TabPage Tab1;
        private System.Windows.Forms.TreeView DbgVars;
        private System.Windows.Forms.Splitter debuglocalssplit;
        private System.Windows.Forms.ListBox DbgCallsList;
        private System.Windows.Forms.TabPage Tab2;
        private System.Windows.Forms.TextBox DbgInput;
        private System.Windows.Forms.ContextMenuStrip DbgOutputContextMenu;
        private System.Windows.Forms.ToolStripMenuItem clearToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem saveToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem5;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem6;
        private System.Windows.Forms.ToolStripMenuItem toggleBreakpointToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem replaceToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem debugByProxyToolStripMenuItem;
        private System.Windows.Forms.StatusStrip statusStrip1;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel1;
        private System.Windows.Forms.ToolStrip toolStrip1;
        private System.Windows.Forms.ToolStripButton SaveStripButton;
        private System.Windows.Forms.ToolStripButton FindStripButton;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStripButton DebugStripButton;
        private System.Windows.Forms.ToolStripButton DebugStopStripButton;
        private System.Windows.Forms.ToolStripButton DebugStepIntoStripButton;
        private System.Windows.Forms.ToolStripButton DebugStepOverStripButton;
        private System.Windows.Forms.ToolStripButton DebugStepOutStripButton;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator2;
        private System.Windows.Forms.ToolStripButton DebugSkipToReduceStripButton;
        private System.Windows.Forms.ToolStripMenuItem showToolbarToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem debugShellExecToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem deleteAllBreakpointsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem aboutToolStripMenuItem;
    }
}