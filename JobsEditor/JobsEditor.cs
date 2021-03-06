﻿/**************************************************************************************
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


//#define DBGOUTPUT_RTB

//#define SAVE_DEBUG_BY_PROXY


#if DEBUG

//#define DEBUGform
#define DEBUGout

//#define DEBUG_SLOW_DEBUGGER

#endif


using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.DirectoryServices;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using System.Runtime.InteropServices;


namespace MySpace.DataMining.AELight
{
    public partial class JobsEditor : Form
    {
        public string ActualFile, PrettyFile;
        public bool IsReadOnly;


        UserConfig config;
        Dictionary<string, object> settings;

        string backupdir = null;

#if DBGOUTPUT_RTB
        RichTextBox DbgOutput;
#else
        TextBox DbgOutput;
#endif

        bool DebugSwitch = false;
        bool DebugStepSwitch = false;

        string[] ExecArgs;

        IList<string> SourceCodeXPathSets = null;


        void InitJobsEditor(string ActualFile, string PrettyFile, Dictionary<string, object> settings)
        {
#if DEBUG
            System.Threading.Thread.CurrentThread.Name = "JobsEditor";
#endif

            Application.EnableVisualStyles();
            Application.DoEvents();

            this.DebugSwitch = settings.ContainsKey("DebugSwitch");
            if (DebugSwitch)
            {
                if (string.IsNullOrEmpty(ActualFile))
                {
                    throw new Exception("Cannot debug, jobs file does not exist: " + PrettyFile);
                }
            }

            this.DebugStepSwitch = settings.ContainsKey("DebugStepSwitch");

            if (settings.ContainsKey("ExecArgs"))
            {
                ExecArgs = (string[])settings["ExecArgs"];
            }
            else
            {
                ExecArgs = new string[0];
            }

            if (settings.ContainsKey("SourceCodeXPathSets"))
            {
                SourceCodeXPathSets = (IList<string>)settings["SourceCodeXPathSets"];
            }

            InitializeComponent();

            {
#if DBGOUTPUT_RTB
                DbgOutput = new RichTextBox();
                this.DbgOutput.BackColor = System.Drawing.Color.White;
                this.DbgOutput.ContextMenuStrip = this.DbgOutputContextMenu;
                this.DbgOutput.DetectUrls = false;
                this.DbgOutput.Dock = System.Windows.Forms.DockStyle.Fill;
                this.DbgOutput.ForeColor = System.Drawing.Color.Black;
                this.DbgOutput.Location = new System.Drawing.Point(0, 0);
                this.DbgOutput.Name = "DbgOutput";
                this.DbgOutput.ReadOnly = true;
                this.DbgOutput.ScrollBars = System.Windows.Forms.RichTextBoxScrollBars.Vertical;
                this.DbgOutput.Size = new System.Drawing.Size(952, 165);
                this.DbgOutput.TabIndex = 1;
                this.DbgOutput.Text = "";
                this.Tab2.Controls.Add(DbgOutput);
#else
                DbgOutput = new TextBox();
                this.DbgOutput.Multiline = true;
                this.DbgOutput.HideSelection = false;
                this.DbgOutput.BackColor = System.Drawing.Color.White;
                this.DbgOutput.ContextMenuStrip = this.DbgOutputContextMenu;
                this.DbgOutput.Dock = System.Windows.Forms.DockStyle.Fill;
                this.DbgOutput.ForeColor = System.Drawing.Color.Black;
                this.DbgOutput.Location = new System.Drawing.Point(0, 0);
                this.DbgOutput.Name = "DbgOutput";
                this.DbgOutput.ReadOnly = true;
                this.DbgOutput.ScrollBars = ScrollBars.Vertical;
                this.DbgOutput.Size = new System.Drawing.Size(952, 165);
                this.DbgOutput.TabIndex = 1;
                this.DbgOutput.Text = "";
#endif
                this.Tab2.Controls.Add(DbgOutput);
                this.DbgOutput.BringToFront();
                this.BottomTabs.SelectedIndexChanged += new EventHandler(BottomTabs_SelectedIndexChanged);
            }

            UiSyncLock = DbgOutput;

            this.settings = settings;

            if (settings.ContainsKey("backupdir"))
            {
                backupdir = (string)settings["backupdir"];
            }

            {
                string configfp = GetMyConfigFilePath();
                if (System.IO.File.Exists(configfp))
                {
                    using (System.IO.StreamReader srconfig = new System.IO.StreamReader(configfp))
                    {
                        System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(UserConfig));
                        config = (UserConfig)xs.Deserialize(srconfig);
                    }
                }
                if (null == config)
                {
                    config = new UserConfig();
                }
#if SAVE_DEBUG_BY_PROXY
                if (string.IsNullOrEmpty(config.DebugByProxyXml))
#endif
                {
                    if (settings.ContainsKey("DefaultDebugType")
                        && "proxy" == (string)settings["DefaultDebugType"])
                    {
                        config.DebugByProxyTag = "true";
                    }
                    else
                    {
                        config.DebugByProxyTag = "false";
                    }
                }
            }

            this.ActualFile = ActualFile;
            this.PrettyFile = PrettyFile;
            this.IsReadOnly = settings.ContainsKey("readonly");
            if (this.DebugSwitch)
            {
                this.IsReadOnly = true;
            }

            Application.AddMessageFilter(new MFilter(this));

            if (null == ActualFile)
            {
                if (IsReadOnly)
                {
                    throw new Exception("Read-only file '" + PrettyFile + "' does not exist");
                }

                string ctemp = TemplateJobs.SourceCode;
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
                    ctemp = ctemp.Replace("~~~NAME~~~", jobname);
                }
                Doc.Text = ctemp;
                Doc.Modified = true; // !

                SetStatus("New Jobs");
            }
            else
            {
                Doc.Text = System.IO.File.ReadAllText(ActualFile);
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
            Doc.AutoCompleteAccepted += new EventHandler<ScintillaNet.AutoCompleteAcceptedEventArgs>(Doc_AutoCompleteAccepted);

            if (DebugSwitch)
            {
                Text = "exec " + PrettyFile;
            }
            else
            {
                Text = PrettyFile + " - Jobs Editor";
            }

            lntmr = new Timer();
            lntmr.Interval = 100;
            lntmr.Tick += new EventHandler(lntmr_Tick);
            lntmr.Start();
            fixln();

            if (IsReadOnly)
            {
                Doc.Styles.Default.BackColor = Color.FromArgb(0xEE, 0xEE, 0xEE);
            }

            EnableToolbar(config.ToolbarEnabledEditor);
            EnableAutoComplete(config.AutoComplete);
            EnableSyntaxColor(config.SyntaxColor);
            EnableDebugProxy(config.DebugByProxyEnabled);
            EnableMapInputScrollControl(config.MapInputScrollControl);

            if (DebugSwitch)
            {
                debugShellExecToolStripMenuItem.Checked = true;
            }

            Doc.NativeInterface.MarkerDefine(MARKER_BREAKPOINT, (int)ScintillaNet.MarkerSymbol.Circle);
            Doc.NativeInterface.MarkerSetFore(MARKER_BREAKPOINT, ToSciColor(Color.Maroon));
            Doc.NativeInterface.MarkerSetBack(MARKER_BREAKPOINT, ToSciColor(Color.Brown));

            Doc.NativeInterface.MarkerDefine(MARKER_CURDEBUG, (int)ScintillaNet.MarkerSymbol.Arrow);
            Doc.NativeInterface.MarkerSetFore(MARKER_CURDEBUG, ToSciColor(Color.DarkGray));
            Doc.NativeInterface.MarkerSetBack(MARKER_CURDEBUG, ToSciColor(Color.Yellow));

            Doc.MarginClick += new EventHandler<ScintillaNet.MarginClickEventArgs>(Doc_MarginClick);

            DocNative = new TDocNative(Doc);

        }

        void BottomTabs_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (1 == this.BottomTabs.SelectedIndex)
            {
                this.DbgOutput.ScrollToCaret();
            }
        }

        public JobsEditor(string ActualFile, string PrettyFile, Dictionary<string, object> settings)
        {
            InitJobsEditor(ActualFile, PrettyFile, settings);
        }

        public JobsEditor(string ActualFile, string PrettyFile, bool IsReadOnly)
        {
            Dictionary<string, object> settings = new Dictionary<string, object>();
            if (IsReadOnly)
            {
                settings["readonly"] = "y";
            }
            InitJobsEditor(ActualFile, PrettyFile, settings);
        }


        /// <summary>
        /// ActualFile's Name only, not full path.
        /// </summary>
        protected internal string ActualFileName
        {
            get
            {
                if (null == ActualFile)
                {
                    return null;
                }
                return (new System.IO.FileInfo(ActualFile)).Name;
            }
        }


        public class ScintillaNativeInterfaceException: Exception
        {
            public ScintillaNativeInterfaceException(string msg)
                : base(msg)
            {
            }
        }


        public class TDocNative
        {
            ScintillaNet.Scintilla Doc;

            internal TDocNative(ScintillaNet.Scintilla Doc)
            {
                this.Doc = Doc;
            }

            void _textbounds(string methodname, int pos)
            {
                int DocTextLength = Doc.TextLength;
                if (pos < 0 || pos > DocTextLength)
                {
                    throw new ScintillaNativeInterfaceException(methodname + ": out of bounds  (pos < 0 || pos > Doc.TextLength) pos=" + pos.ToString() + "; Doc.TextLength=" + DocTextLength.ToString());
                }
            }

            internal void SetTargetStart(int pos)
            {
                _textbounds("SetTargetStart", pos);
                Doc.NativeInterface.SetTargetStart(pos);
            }

            internal void SetTargetEnd(int pos)
            {
                _textbounds("SetTargetEnd", pos);
                Doc.NativeInterface.SetTargetEnd(pos);
            }

            internal void ReplaceTarget(string s)
            {
                Doc.NativeInterface.ReplaceTarget(-1, s + "\0");
            }

            internal void ReplaceTarget(int length, string s)
            {
                if (length < 0 || length > s.Length)
                {
                    throw new ScintillaNativeInterfaceException("ReplaceTarget: invalid text length  (length < 0 || length > s.Length) length=" + length.ToString() + "; s.Length=" + s.Length.ToString());
                }
                Doc.NativeInterface.ReplaceTarget(length, s);
            }

            internal byte GetStyleAt(int pos)
            {
                _textbounds("GetStyleAt", pos);
                return Doc.NativeInterface.GetStyleAt(pos);
            }

            internal void AutoCShow(int lenEntered, string list)
            {
                Doc.NativeInterface.AutoCShow(lenEntered, list + "\0");
            }

        }

        public TDocNative DocNative;


        static int ToSciColor(Color c)
        {
            int result = 0;
            result |= c.R;
            result |= c.G << 8;
            result |= c.B << 16;
            return result;
        }


        void Doc_MarginClick(object sender, ScintillaNet.MarginClickEventArgs e)
        {
            try
            {
                switch (e.Margin.Number)
                {
                    case MARGIN_BREAKPOINT:
                        {
                            if (dbg == null || !dbg.IsWaiting)
                            {
                                lock (UiSyncLock)
                                {
                                    bool addbreakpoint = !IsBreakpoint(e.Line.Number);
                                    if (addbreakpoint)
                                    {
                                        e.Line.AddMarker(MARKER_BREAKPOINT);
                                    }
                                    else
                                    {
                                        e.Line.DeleteMarker(MARKER_BREAKPOINT);
                                    }
                                }
                                //Console.WriteLine("    Line {0} contains breakpoint? {1}", e.Line.Number, addbreakpoint);
                                if (IsDebuggerReady)
                                {
                                    dbg.SetBreakpoints();
                                }
                            }
                        }
                        break;
                }
            }
            catch (ScintillaNativeInterfaceException snie)
            {
                AlertScintillaNativeInterfaceException(snie);
            }
            catch (System.Runtime.InteropServices.SEHException seh)
            {
                AlertNativeException(seh);
            }
            catch (Exception e32)
            {
                LogOutputToFile("Doc_MarginClick: " + e32.ToString());
                {
                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                    if (computer_name.StartsWith("MAPDDRULE") || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                    {
                        SetStatus("Doc.Margin Err: " + e32.Message);
                    }
                }
                int i332 = 22 + 22;
            }
        }


        const int MARGIN_BREAKPOINT = 1;

        const int MARKER_BREAKPOINT = 1;
        const int MARKER_CURDEBUG = 3;

        int curdebugcline = -1; // 0-based line number of current MARKER_CURDEBUG.


        bool IsBreakpoint(int linenum)
        {
            foreach (ScintillaNet.Marker mk in Doc.Lines[linenum].GetMarkers())
            {
                if (1 << MARKER_BREAKPOINT ==  mk.Number)
                {
                    return true;
                }
            }
            return false;
        }


        static string GetMyConfigFilePath()
        {
            return Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\cfg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + ".cfg";
        }


        void SaveConfig()
        {
            string configfp = GetMyConfigFilePath();
            using (System.IO.FileStream fdc = new System.IO.FileStream(configfp, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
            {
                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(UserConfig));
                xs.Serialize(fdc, config);
            }
        }


        void Doc_AutoCompleteAccepted(object sender, ScintillaNet.AutoCompleteAcceptedEventArgs e)
        {
            try
            {
                string acseltext = e.Text;
                e.Cancel = true;

                {
                    int ijunk = acseltext.IndexOf("   ");
                    if (-1 != ijunk)
                    {
                        acseltext = acseltext.Substring(0, ijunk);
                    }
                }

                acseltext = acseltext.Replace(' ', '_');
                acseltext = acseltext.Replace(",_", ", ");
                acseltext = acseltext.Replace("<", "T"); // ?
                acseltext = acseltext.Replace(">", "");
                acseltext = acseltext.Replace("`", "");
                acseltext = acseltext.Replace("[", "s"); // ?
                acseltext = acseltext.Replace("]", "");
                acseltext = acseltext.Replace("&", "Ref"); // ByRef

                int acend = Doc.Selection.Start;
                int acstart = acend - 1;
                for (; ; acstart--)
                {
                    if (acstart < 0)
                    {
                        return;
                    }
                    if (Doc.CharAt(acstart) == '.')
                    {
                        acstart++;
                        break;
                    }
                }
                DocNative.SetTargetStart(acstart);
                DocNative.SetTargetEnd(acend);
                DocNative.ReplaceTarget(acseltext);

                bool fixedsel = false;
                {
                    int xstart = acseltext.IndexOf('(');
                    if (-1 != xstart)
                    {
                        xstart++;
                        int xend = acseltext.IndexOf(')', xstart);
                        if (-1 != xstart)
                        {
                            //Doc.Selection.Range = new ScintillaNet.Range(acstart + xstart, acstart + xend, Doc);
                            Doc.Selection.Range = new ScintillaNet.Range(acstart + xstart - 1, acstart + xend + 1, Doc);
                            fixedsel = true;
                        }
                    }
                }
                if (!fixedsel)
                {
                    Doc.Selection.Range = new ScintillaNet.Range(acstart + acseltext.Length, acstart + acseltext.Length, Doc);
                    fixedsel = true;
                }
            }
            catch (ScintillaNativeInterfaceException snie)
            {
                AlertScintillaNativeInterfaceException(snie);
            }
            catch (System.Runtime.InteropServices.SEHException seh)
            {
                AlertNativeException(seh);
            }
            catch (Exception ex3)
            {
                LogOutputToFile("Doc_AutoCompleteAccepted: " + ex3.ToString());
                {
                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                    if (computer_name.StartsWith("MAPDDRULE") || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                    {
                        SetStatus("Doc.ACa Err: " + ex3.Message);
                    }
                }
                int i332 = 22 + 22;
            }
        }


        static void AlertScintillaNativeInterfaceException(ScintillaNativeInterfaceException snie)
        {
            LogOutputToFile(snie.ToString());
            Console.Error.WriteLine(snie.ToString());
            MessageBox.Show("A native-interface error has occured.\r\n\r\nPlease report the exact exception as shown in the console.", "Native-Interface Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }


        static void AlertNativeException(System.Runtime.InteropServices.SEHException seh)
        {
            LogOutputToFile(seh.ToString());
            Console.Error.WriteLine(seh.ToString());
            MessageBox.Show("A fatal native error has occured.\r\n\r\nIt is recommended that you save your work and re-open the editor.\r\n\r\nPlease report the exact exception as shown in the console.", "Fatal Native Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }


        public static void LogOutputToFile(string line)
        {
            try
            {
                lock (typeof(JobsEditor))
                {
                    System.IO.StreamWriter fstm = System.IO.File.AppendText(@"dspace-editor-errors.txt");
                    string build = "";
                    /*try
                    {
                        build = "(build:" + GetBuildInfo() + ") ";
                    }
                    catch
                    {
                    }*/
                    fstm.WriteLine("[{0}] {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.Close();
                }
            }
            catch
            {
            }
        }


        static string dlldir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

        List<Type> gtypes;


        bool SymbolsLoaded
        {
            get
            {
                return null != gtypes;
            }
        }


        Type FindAcType(string name)
        {
            if (!SymbolsLoaded)
            {
                return null;
            }

            {
                int iangle = name.IndexOf('<');
                if (-1 != iangle)
                {
                    string genname = name.Substring(0, iangle);
                    if (genname == "List")
                    {
                        switch (name)
                        {
                            case "List<byte>": return typeof(List<byte>);
                            case "List<short>": return typeof(List<short>);
                            case "List<int>": return typeof(List<int>);
                            case "List<long>": return typeof(List<long>);
                            case "List<string>": return typeof(List<string>);
                        }
                        return typeof(List<object>); // ...
                    }
                    if (genname == "Dictionary")
                    {
                        return typeof(Dictionary<object, object>); // ...
                    }
                    return null; // ...
                }
            }

            {
                int ibrack = name.IndexOf('[');
                if (-1 != ibrack)
                {
                    int nbracks = 1;
                    for (int i = ibrack + 1; i < name.Length; i++)
                    {
                        if (name[i] == '[')
                        {
                            nbracks++;
                        }
                    }

                    switch (name.ToLower())
                    {
                        case "bool[]": return typeof(bool[]);
                        case "byte[]": return typeof(byte[]);
                        case "sbyte[]": return typeof(sbyte[]);
                        case "short[]": return typeof(short[]);
                        case "ushort[]": return typeof(ushort[]);
                        case "int[]": return typeof(int[]);
                        case "uint[]": return typeof(uint[]);
                        case "long[]": return typeof(long[]);
                        case "ulong[]": return typeof(ulong[]);
                        case "float[]": return typeof(float[]);
                        case "double[]": return typeof(double[]);
                        case "string[]": return typeof(string[]);
                        case "object[]": return typeof(object[]);

                        case "bool[][]": return typeof(bool[][]);
                        case "byte[][]": return typeof(byte[][]);
                        case "sbyte[][]": return typeof(sbyte[][]);
                        case "short[][]": return typeof(short[][]);
                        case "ushort[][]": return typeof(ushort[][]);
                        case "int[][]": return typeof(int[][]);
                        case "uint[][]": return typeof(uint[][]);
                        case "long[][]": return typeof(long[][]);
                        case "ulong[][]": return typeof(ulong[][]);
                        case "float[][]": return typeof(float[][]);
                        case "double[][]": return typeof(double[][]);
                        case "string[][]": return typeof(string[][]);
                        case "object[][]": return typeof(object[][]);

                    }
                    return null;
                }
            }

            {
                int ildot = name.LastIndexOf('.');
                if (-1 != ildot)
                {
                    name = name.Substring(ildot + 1);
                }
            }

            switch (name)
            {
                case "byte": name = "Byte"; break;
                case "sbyte": name = "SByte"; break;
                case "short": name = "Int16"; break;
                case "ushort": name = "UInt16"; break;
                case "int": name = "Int32"; break;
                case "uint": name = "UInt32"; break;
                case "long": name = "Int64"; break;
                case "ulong": name = "UInt64"; break;
                case "object": name = "Object"; break;
                case "string": name = "String"; break;
                case "double": name = "Double"; break;
            }

            foreach (Type t in gtypes)
            {
                string tname = t.Name;
                {
                    int ildot = tname.LastIndexOf('.');
                    if (-1 != ildot)
                    {
                        tname = tname.Substring(ildot + 1);
                    }
                }
                if (name == tname)
                {
                    return t;
                }
            }
            return null;
        }


        class TypeSorter : IComparer<Type>
        {
            public int Compare(Type x, Type y)
            {
                return string.Compare(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
            }
        }


        class TypeStringSorter : IComparer<string>
        {
            public int Compare(string x, string y)
            {
                return string.Compare(x, y, StringComparison.OrdinalIgnoreCase);
            }
        }


        void loadsymsthreadproc(object obj)
        {          
            List<Type> mygtypes = new List<Type>(100);

            {
                mygtypes.Add(typeof(InternalCode.RandomAccessEntries));
                mygtypes.Add(typeof(InternalCode.ByteSliceList));
                mygtypes.Add(typeof(InternalCode.RandomAccessOutput));
                mygtypes.Add(typeof(InternalCode.ReduceOutput));
                mygtypes.Add(typeof(InternalCode.RemoteInputStream));
                mygtypes.Add(typeof(InternalCode.RemoteOutputStream));
            }
           
            //foreach (string dllfn in System.IO.Directory.GetFiles(dlldir, "*.dll"))
            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(dlldir)).GetFiles("*.dll"))
            {
                switch (fi.Name)
                {
                    case "BTreeRangeEstimator.dll": continue;
                    case "JobsEditor.dll": continue;
                    case "MdbgCore.dll": continue;
                    case "MdbgDis.dll": continue;
                    case "SciLexer.dll": continue;
                    case "SciLexer64.dll": continue;
                    case "SciLexer32.dll": continue;
                    case "ScintillaNet.dll": continue;
                }
                //Console.WriteLine("  {0}", fi.Name);
                string dllfn = fi.FullName;
                try
                {                  
                    mygtypes.AddRange(System.Reflection.Assembly.LoadFrom(dllfn).GetTypes());
                }
                catch
                {
                }
            }
#if DEBUG
            try
            {
                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(dlldir + @"\" + dfs.DLL_DIR_NAME))
                    .GetFiles("*.dll"))
                {

                    //Console.WriteLine("  {0}", fi.Name);
                    string dllfn = fi.FullName;
                    try
                    {
                        mygtypes.AddRange(System.Reflection.Assembly.LoadFrom(dllfn).GetTypes());
                    }
                    catch
                    {
                    }
                }
            }
            catch
            {
            }
#endif
            {
                mygtypes.Add(typeof(Byte));
                mygtypes.Add(typeof(SByte));
                mygtypes.Add(typeof(Int16));
                mygtypes.Add(typeof(UInt16));
                mygtypes.Add(typeof(Int32));
                mygtypes.Add(typeof(UInt32));
                mygtypes.Add(typeof(Int64));
                mygtypes.Add(typeof(UInt64));
                mygtypes.Add(typeof(Object));
                mygtypes.Add(typeof(String));
                mygtypes.Add(typeof(Double));
            }
           
            //mygtypes.AddRange(System.Reflection.Assembly.GetAssembly(typeof(System.Xml.XmlDocument)).GetTypes()); // System.Xml.dll
            foreach (Type t in System.Reflection.Assembly.GetAssembly(typeof(System.Xml.XmlDocument)).GetTypes())
            {
                if (t.FullName.StartsWith("System.Xml."))
                {
                    mygtypes.Add(t);
                }
            }
#if DEBUG
            mygtypes.Add(typeof(System.Text.StringBuilder));
            mygtypes.Add(typeof(System.Random));
            mygtypes.Add(typeof(System.DateTime));
#endif
#if DEBUG
            mygtypes.Sort(new TypeSorter());
#endif
            gtypes = mygtypes;
        }


        Timer lntmr;

        void fixln()
        {
            int lc = Doc.Lines.Count;
            const int dw = 8;
            const int bw = 3;
            if (lc < 10)
            {
                Doc.Margins.Margin0.Width = bw + dw * 1;
            }
            else if (lc < 100)
            {
                Doc.Margins.Margin0.Width = bw + dw * 2;
            }
            else if (lc < 1000)
            {
                Doc.Margins.Margin0.Width = bw + dw * 3;
            }
            else
            {
                Doc.Margins.Margin0.Width = bw + dw * 4;
            }
        }

        void lntmr_Tick(object sender, EventArgs e)
        {
            fixln();
        }



        enum CppStyles
        {
            DEFAULT,
            COMMENT,
            COMMENTLINE,
            COMMENTDOC,
            NUMBER,
            KEYWORD,
            STRING,
            CHARACTER,
            UUID,
            PREPROCESSOR,
            OPERATOR,
            IDENTIFIER,
            STRINGEOL,
            VERBATIM,
            REGEX,
            COMMENTLINEDOC,
            WORD2,
            COMMENTDOCKEYWORD,
            COMMENTDOCKEYWORDERROR,
            //STYLE_LINENUMBER,
            //DOCUMENT_DEFAULT,

            GLOBALCLASS = 19, // keywords[3]

            XML_TAGNAME = 80,

            DYNAMICCS_DEFAULT = 90,
            DYNAMICCS_CDATA = 91,

            _END
        }


        Timer stmr;

        protected void SetStatus(string s, int duration)
        {
            if (status.Visible)
            {
                status.Text = s;
            }
            else
            {
                toolStripStatusLabel1.Text = s;
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
                toolStripStatusLabel1.Text = "";
            }
            stmr.Stop();
            stmr = null;
        }


        internal class MFilter : IMessageFilter
        {
            JobsEditor outer;


            internal MFilter(JobsEditor outer)
            {
                this.outer = outer;
            }


            bool IMessageFilter.PreFilterMessage(ref Message m)
            {
                if (m.Msg == 0x020A) // Mouse wheel.
                {
                    if ((ModifierKeys & Keys.Control) == Keys.Control)
                    {
                        return true; // Prevent.
                    }

                    if (outer.IsACDown)
                    {
                        IntPtr hw;
                        POINT pt;
                        pt.x = (int)m.LParam & 0xFFFF;
                        pt.y = (int)(((uint)m.LParam & (uint)0xFFFF0000) >> 16);
                        if (outer.RectangleToScreen(new Rectangle(new Point(0, 0), outer.ClientSize)).Contains(new Point(pt.x, pt.y))) // Only if within the editor window.
                        {
                            hw = WindowFromPoint(pt);
                            if (hw != m.HWnd)
                            {
                                // Forward the mousewheel to that window instead.
                                SendMessage(hw, 0x020A, m.WParam, m.WParam);
                            }
                        }
                        return true; // Prevent.
                    }
                }

                return false; // Continue.
            }
        }


        bool DoSave()
        {
            if (IsReadOnly)
            {
                SetStatus("Cannot save read-only");
                return false;
            }
            
            string DocText = Doc.Text;

            if (ActualFile == null)
            {
                try
                {
                    string fn = PrettyFile;
                    if (fn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        fn = fn.Substring(6);
                    }
                    dfs dc = LoadDfsConfig();
                    string[] slaves = dc.Slaves.SlaveList.Split(';');
                    if (0 == slaves.Length || dc.Slaves.SlaveList.Length == 0)
                    {
                        throw new Exception("DFS SlaveList error (machines)");
                    }
                    //Random rnd = new Random(DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    //string newactualfilehost = slaves[rnd.Next() % slaves.Length];
                    string newactualfilehost = Surrogate.MasterHost;
                    string newactualfilename = dfs.GenerateZdFileDataNodeName(fn);
                    string myActualFile = Surrogate.NetworkPathForHost(newactualfilehost) + @"\" + newactualfilename;
                    string backupfile = null;
                    try
                    {
                        System.IO.File.WriteAllText(myActualFile, DocText);
                        //+++metabackup+++
                        // Since this doesn't even exist in dfs.xml yet,
                        // writing to the actual jobs file doesn't need to be transactional.
                        if (null != backupdir)
                        {
                            try
                            {
                                backupfile = backupdir + @"\" + newactualfilename;
                                System.IO.File.WriteAllText(backupfile, DocText);
                            }
                            catch (Exception eb)
                            {
                                LogOutputToFile(eb.ToString());
                                throw new Exception("Error writing backup: " + eb.Message, eb);
                            }
                        }
                        //---metabackup---
                        Console.Write(MySpace.DataMining.DistributedObjects.Exec.Shell(
                            "DSpace -dfsbind \"" + newactualfilehost + "\" \"" + newactualfilename + "\" \"" + fn + "\" " + DfsFileTypes.JOB
                            ));
                    }
                    catch
                    {
                        try
                        {
                            System.IO.File.Delete(myActualFile);
                        }
                        catch
                        {
                        }
                        if (null != backupfile)
                        {
                            try
                            {
                                System.IO.File.Delete(backupfile);
                            }
                            catch
                            {
                            }
                        }
                        throw;
                    }
                    ActualFile = myActualFile; // Only update this when fully committed to DFS!
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    MessageBox.Show(this, "Unable to save new file to DFS:\r\n\r\n" + e.Message, "Save-New Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return false; // Important! don't continue with any of the rest if this fails!
                }
            }
            else
            {
                try
                {
                    //System.IO.File.WriteAllText(ActualFile, DocText);
                    string oldactualfile = ActualFile + ".old";
                    string newactualfile = ActualFile + ".new";
                    //+++metabackup+++
                    string backupfile = null;
                    string oldbackupfile = null, newbackupfile = null;
                    if (null != backupdir)
                    {
                        backupfile = backupdir + @"\" + ActualFileName;
                        oldbackupfile = backupfile + ".old";
                        newbackupfile = backupfile + ".new";
                    }
                    //---metabackup--

                    // Write .new
                    System.IO.File.WriteAllText(newactualfile, DocText);
                    //+++metabackup+++
                    if (null != backupdir)
                    {
                        System.IO.File.WriteAllText(newbackupfile, DocText);
                    }
                    //---metabackup--

                    // Move current to .old
                    try
                    {
                        System.IO.File.Delete(oldactualfile);
                    }
                    catch
                    {
                    }
                    System.IO.File.Move(ActualFile, oldactualfile);
                    //+++metabackup+++
                    if (null != backupdir)
                    {
                        try
                        {
                            System.IO.File.Delete(oldbackupfile);
                        }
                        catch
                        {
                        }
                        System.IO.File.Move(backupfile, oldbackupfile);
                    }
                    //---metabackup--

                    // Move .new to current.
                    System.IO.File.Move(newactualfile, ActualFile);
                    //+++metabackup+++
                    if (null != backupdir)
                    {
                        System.IO.File.Move(newbackupfile, backupfile);
                    }
                    //---metabackup--

                    // Delete .old
                    try
                    {
                        System.IO.File.Delete(oldactualfile);
                    }
                    catch
                    {
                    }
                    //+++metabackup+++
                    try
                    {
                        System.IO.File.Delete(oldbackupfile);
                    }
                    catch
                    {
                    }
                    //---metabackup--

                }
                catch (Exception ew)
                {
                    Console.WriteLine(ew.ToString());
                    MessageBox.Show(this, "Unable to save changes to DFS file:\r\n\r\n" + ew.Message, "Save-Changes Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return false; // Don't continue!
                }




                try
                {
                    //System.IO.File.WriteAllText(ActualFile, DocText);
                    string xactualfile = ActualFile + "$";
                    System.IO.File.WriteAllText(xactualfile, DocText);
                    System.IO.File.Delete(ActualFile);
                    System.IO.File.Move(xactualfile, ActualFile);
                }
                catch (Exception ew)
                {
                    Console.WriteLine(ew.ToString());
                    MessageBox.Show(this, "Unable to save changes to DFS file:\r\n\r\n" + ew.Message, "Save-Changes Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return false; // Don't continue!
                }
                //+++metabackup+++
                if (null != backupdir)
                {
                    try
                    {
                        string backupfile = backupdir + @"\" + ActualFileName;
                        //System.IO.File.WriteAllText(backupfile, DocText);
                        string xbackupfile = backupfile + "$";
                        System.IO.File.WriteAllText(xbackupfile, DocText);
                        System.IO.File.Delete(backupfile);
                        System.IO.File.Move(xbackupfile, backupfile);
                    }
                    catch (Exception eb)
                    {
                        LogOutputToFile(eb.ToString());
                        Console.Error.WriteLine("Error writing backup: " + eb.Message);
                        // Can continue from this, there just won't be a backup file.
                    }
                }
                //---metabackup---
            }

#if DEBUG
            {
                string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                if (computer_name == "MAPDCMILLER" || computer_name == "MAPDDRULE1")
                {
                    if (PrettyFile.EndsWith(".DBCORE", StringComparison.OrdinalIgnoreCase))
                    {
                        string tfspathfile = "DBCORE.TFS.txt";
                        if (!System.IO.File.Exists(tfspathfile))
                        {
                            MessageBox.Show(this, "TFS file not found: " + tfspathfile);
                            return false;
                        }
                        else
                        {
                            string tfsbasepath = System.IO.File.ReadAllText(tfspathfile).Trim();
                            SetStatus("Saving to TFS...", 1000);
                            Application.DoEvents();
                            string cofn = PrettyFile;
                            if (cofn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                cofn = cofn.Substring(6);
                            }
                            string cofilepath = tfsbasepath + @"\" + cofn;
                            if (!System.IO.File.Exists(cofilepath))
                            {
                                MessageBox.Show("TFS file not found: " + cofilepath);
                                return false;
                            }
                            else
                            {
                                const string TF_PATH = @"C:\Program Files\Microsoft Visual Studio 9.0\Common7\IDE\TF.exe";
                                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(TF_PATH, "checkout \"" + cofilepath + "\"");
                                psi.UseShellExecute = false;
                                //psi.CreateNoWindow = true;
                                Console.WriteLine("------------------------------");
                                Console.WriteLine("[{0}] TFS:", DateTime.Now.ToString());
                                if (!System.Diagnostics.Process.Start(psi).WaitForExit(10000))
                                {
                                    MessageBox.Show(this, "Waiting on TFS took too long, aborted");
                                    return false;
                                }
                                else
                                {
                                    System.IO.File.WriteAllText(cofilepath, Doc.Text);
                                    SetStatus("Saved to TFS", 2000);
                                    Application.DoEvents();
                                }
                                Console.WriteLine("------------------------------");
                            }
                        }
                    }
                }
            }
#endif

            Doc.Modified = false; // !
            return true;
        }


        private void JobsEditor_FormClosing(object sender, FormClosingEventArgs e)
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

        private void JobsEditor_Load(object sender, EventArgs e)
        {
            //Doc.Select(0, 0);
            //Doc.Selection.SelectNone();

            Doc.Select();

            if (DebugSwitch)
            {
                DebugDoc();
            }
        }

        private void Doc_Scroll(object sender, ScrollEventArgs e)
        {
            //Doc.NativeInterface.Colourise(0, -1);
        }

        private void Doc_Resize(object sender, EventArgs e)
        {
            //Doc.NativeInterface.Colourise(0, -1);
        }

        private void CancelBtn_Click(object sender, EventArgs e)
        {
            //DialogResult = DialogResult.Abort;
            Close(); // Prompt to save as needed!
        }


        static readonly char[] _lastdeclsep = new char[] { ' ', '\t', '>', ']' };

        static void _csdecl(string decl, Dictionary<string, string> results)
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(decl.Trim().Length == decl.Length);
#endif

            int ids = decl.LastIndexOfAny(_lastdeclsep);
            if (-1 == ids)
            {
                return;
            }
            ids++; // Include separator.
            string dtype = decl.Substring(0, ids).Trim();
            string dname = decl.Substring(ids).Trim();

            {
                int ildtypedot = dtype.LastIndexOf('.');
                if (-1 != ildtypedot)
                {
                    dtype = dtype.Substring(ildtypedot + 1);
                }
            }

            int i33 = 33 + 33;

            results[dname] = dtype;
        }

        static readonly char[] _lastcsparts = new char[] { '{', '}' };

        static void _cspart(StringBuilder snip, Dictionary<string, string> results)
        {
            string code = snip.ToString();

            {
                int i = code.LastIndexOfAny(_lastcsparts);
                if (-1 != i)
                {
                    code = code.Substring(i + 1);
                }
            }

            {
                int iequ = code.IndexOf('=');
                if (-1 != iequ)
                {
                    code = code.Substring(0, iequ);
                }
            }

            {
                int isem = code.LastIndexOf(';');
                if (-1 != isem)
                {
                    code = code.Substring(0, isem);
                }
            }

            code = code.Trim();

            if (code.Length <= 1)
            {
                return;
            }

            {
                int ife = code.LastIndexOf("foreach");
                if (-1 != ife)
                {
                    string fe = code.Substring(ife + 7);
                    if (fe.Length >= 2)
                    {
                        if (fe[0] == '('
                            || (fe[0] == ' ' && fe[1] == '('))
                        {
                            fe = fe.Substring(fe.IndexOf('(') + 1);
                            int iin = fe.IndexOf(" in ");
                            if (-1 != iin)
                            {
                                string fedecl = fe.Substring(0, iin).Trim();
                                _csdecl(fedecl, results);
                                return; // Handled!
                            }
                        }
                    }
                }
            }

            {
                int ife = code.LastIndexOf("using");
                if (-1 != ife)
                {
                    string fe = code.Substring(ife + 5);
                    if (fe.Length >= 2)
                    {
                        if (fe[0] == '('
                            || (fe[0] == ' ' && fe[1] == '('))
                        {
                            fe = fe.Substring(fe.IndexOf('(') + 1);
                            // The '=' was already trimmed out above.
                            string decl = fe;
                            int ix = decl.IndexOf(')');
                            if (-1 != ix)
                            {
                                decl = decl.Substring(0, ix);
                            }
                            _csdecl(decl, results);
                            return; // Handled!
                        }
                    }
                }
            }

            if (code[code.Length - 1] == ')')
            {
                {
                    int nparens = 0;
                    for (int i = code.Length - 1; ; i--)
                    {
                        if (')' == code[i])
                        {
                            nparens++;
                        }
                        if ('(' == code[i])
                        {
                            if (0 == --nparens)
                            {
                                code = code.Substring(i);
                                break;
                            }
                        }
                        if (0 == i)
                        {
                            break;
                        }
                    }
                }

                {
                    int nangles = 0;
                    int nbracks = 0;
                    int startic = 1;
                    int ic = 1;
                    bool nonident = false;
                    for (; ic < code.Length - 1; ic++)
                    {
                        if (code[ic] == '[')
                        {
                            nbracks++;
                        }
                        else if (code[ic] == ']')
                        {
                            if (nbracks > 0)
                            {
                                nbracks--;
                            }
                        }
                        else if (code[ic] == '<')
                        {
                            nangles++;
                        }
                        else if (code[ic] == '>')
                        {
                            if (nangles > 0)
                            {
                                nangles--;
                            }
                        }
                        else if (code[ic] == ',')
                        {
                            if (0 == nbracks && 0 == nangles)
                            {
                                if (!nonident)
                                {
                                    _csdecl(code.Substring(startic, ic - startic).Trim(), results);
                                }
                                nonident = false;
                                startic = ic + 1;
                            }
                        }
                        else if (code[ic] == ';')
                        {
                            nonident = false; // ...
                        }
                        else if (code[ic] == '=')
                        {
                            if (0 == nbracks && 0 == nangles)
                            {
                                nonident = false; // Reset it if it was after '='.
                            }
                        }
                        else if (code[ic] != '_' && !char.IsLetterOrDigit(code[ic]) && !char.IsWhiteSpace(code[ic]))
                        {
                            if (0 == nbracks && 0 == nangles)
                            {
                                nonident = true;
                            }
                        }
                    }
                    if (startic != ic)
                    {
                        if (!nonident)
                        {
                            _csdecl(code.Substring(startic, ic - startic).Trim(), results);
                        }
                    }
                }

                return; // Handled!
            }

            {
                {
                    int ifr = code.LastIndexOf("for");
                    if (-1 != ifr)
                    {
                        string fr = code.Substring(ifr + 3);
                        if (fr.Length >= 2)
                        {
                            if (fr[0] == '('
                                || (fr[0] == ' ' && fr[1] == '('))
                            {
                                code = fr.Substring(fr.IndexOf('(') + 1);
                            }
                        }
                    }
                }

                int nbracks = 0;
                int nangles = 0;
                int comma = 0;
                bool foundsep = false;
                //bool isarray = false;
                for (int ic = code.Length - 1; ; ic--)
                {
                    if (code[ic] == ',')
                    {
                        comma++;
                    }
                    if (code[ic] == ']')
                    {
                        nbracks++;
                        foundsep = true;
                    }
                    else if (code[ic] == '[')
                    {
                        //isarray = true;
                        if (nbracks > 0)
                        {
                            nbracks--;
                        }
                    }
                    else if (code[ic] == '>')
                    {
                        nangles++;
                        foundsep = true;
                    }
                    else if (code[ic] == '<')
                    {
                        if (nangles > 0)
                        {
                            nangles--;
                        }
                    }
                    else if (code[ic] == '(' || code[ic] == ')'
                        || code[ic] == '{' || code[ic] == '}')
                    {
                        if (0 == nangles && 0 == nbracks)
                        {
                            break;
                        }
                    }
                    else if (char.IsWhiteSpace(code[ic]))
                    {
                        foundsep = true;
                    }
                    else if (code[ic] == ';' && !foundsep)
                    {
                    }
                    else if (code[ic] != '_' && !char.IsLetterOrDigit(code[ic]) && code[ic] != '.' && code[ic] != ',')
                    {
                        if (foundsep)
                        {
                            code = code.Substring(ic + 1).Trim();
                            if (0 != code.Length)
                            {
                                _csdecl(code, results);
                            }
                        }
                        break;
                    }

                    if (0 == ic)
                    {

                        if (comma != 0)
                        {
                            string[] arr = new string[comma + 1];
                            int sep = 0;
                            string code1 = null;
                            string code2 = null;
                            sep = code.IndexOf(' ');
                            code1 = code.Substring(0, sep);
                            code2 = code.Substring(sep + 1, code.Length - 1 - sep);
                            for (int ii = 0; ii < comma + 1; ii++)
                            {
                                sep = code2.IndexOf(',');
                                if (sep != -1 )
                                {
                                    arr[ii] = code2.Substring(0, sep);
                                    code2 = code2.Substring(sep + 1, code2.Length - 1 - sep);
                                }
                                else
                                {
                                    arr[ii] = code2;
                                }

                            }
                            string code3 = null;
                            for (int iii = 0; iii < comma + 1; iii++)
                            {
                                code3 = code1 + " " + arr[iii];
                                code3 = code3.Trim();
                                if (0 != code3.Length)
                                {
                                    _csdecl(code3, results);
                                }
                                
                            }
                            foundsep = false;

                        }

                        if (foundsep)
                        {
                            code = code.Trim();
                            if (0 != code.Length)
                            {
                                _csdecl(code, results);
                            }
                        }
                        break;
                    }
                }
            }

        }

        static void LoadCsSymbols(string cs, Dictionary<string, string> results)
        {
            StringBuilder snip = new StringBuilder(0x400);
            bool prevRparen = false;
            for (int i = 0; i < cs.Length; i++)
            {
                char ch = cs[i];
                char chNext = (i + 1 < cs.Length) ? cs[i + 1] : '\0';

                if (ch == '/' && chNext == '*')
                {
                    for (i += 3; i < cs.Length; i++)
                    {
                        if (cs[i] == '/' && cs[i - 1] == '*')
                        {
                            break;
                        }
                    }
                }
                else if (ch == '/' && chNext == '/')
                {
                    for (i += 2; i < cs.Length; i++)
                    {
                        if (cs[i] == '\r' || cs[i] == '\n')
                        {
                            break;
                        }
                    }
                }
                else if (ch == '@' && chNext == '"')
                {
                    for (i += 2; i < cs.Length; i++)
                    {
                        if (cs[i] == '"')
                        {
                            break;
                        }
                    }
                }
                else if (ch == '"')
                {
                    for (i++; i < cs.Length; i++)
                    {
                        if (cs[i] == '\\')
                        {
                            i++;
                            continue;
                        }
                        if (cs[i] == '"')
                        {
                            break;
                        }
                    }
                }
                else if (ch == '\'')
                {
                    for (i++; i < cs.Length; i++)
                    {
                        if (cs[i] == '\\')
                        {
                            i++;
                            continue;
                        }
                        if (cs[i] == '\'')
                        {
                            break;
                        }
                    }
                }
                else if (ch == ')')
                {
                    snip.Append(ch);

                    prevRparen = true;
                    continue;
                }
                else if (ch == '{')
                {
                    if (prevRparen)
                    {
                        _cspart(snip, results);
                        snip.Length = 0;
                    }

                    snip.Append(ch); // After (potential) _cspart!
                }
                else if (ch == ';')
                {
                    snip.Append(ch);

                    _cspart(snip, results);
                    snip.Length = 0;
                }
                else
                {
                    snip.Append(ch);
                }

                if (!char.IsWhiteSpace(ch))
                {
                    prevRparen = false;
                }

            }
        }


        static void _appendcdata(System.Xml.XmlNode pxn, StringBuilder sb)
        {
            foreach (System.Xml.XmlNode xn in pxn.ChildNodes)
            {
                System.Xml.XmlCDataSection xcd = xn as System.Xml.XmlCDataSection;
                if (null != xcd)
                {
                    sb.AppendLine(xcd.Value);
                }
                else
                {
                    _appendcdata(xn, sb);
                }
            }
        }

        static bool LoadDSpaceSymbols(string xml, Dictionary<string, string> results)
        {
            try
            {
                System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                xd.LoadXml(xml);

                StringBuilder sb = new StringBuilder(0x400 * 8);

                _appendcdata(xd, sb);
                LoadCsSymbols(sb.ToString(), results);
                return true;
            }
            catch
            {
                return false;
            }
        }

        static bool LoadXmlDSpaceSymbolsNear(string xml, int near, Dictionary<string, string> results)
        {
            string cs = GetXmlDSpaceCsNear(xml, near, false);
            if (cs == null)
            {
                return false;
            }
            LoadCsSymbols(cs, results);
            return true;
        }


        static string GetXmlDSpaceCsNear(string xml, int near, bool StopAtNear)
        {
            int istart = xml.LastIndexOf("<![CDATA[", near, near);
            if (-1 == istart)
            {
                return null;
            }
            istart += 9;
            if (StopAtNear)
            {
                return xml.Substring(istart, near - istart);
            }
            int iend = xml.IndexOf("]]>", near);
            if (-1 == iend)
            {
                return null;
            }
            return xml.Substring(istart, iend - istart);
        }


        static List<string> SplitExprParts(string expr)
        {
            // Note: doesn't handle string/char literals.
            List<string> result = new List<string>(4 + expr.Length / 16);
            if (expr.Length > 0)
            {
                int endi = expr.Length;
                for (int i = expr.Length - 1; ; i--)
                {
                    if (expr[i] == '.')
                    {
                        result.Add(expr.Substring(i + 1, endi - i - 1).Trim());
                        endi = i;
                    }
                    else if (expr[i] == ']')
                    {
                        int nbracks = 1;
                        if (i != 0)
                        {
                            for (i--; ; i--)
                            {
                                if (expr[i] == '[')
                                {
                                    if (--nbracks == 0)
                                    {
                                        result.Add("[0]"); // ...
                                        endi = i;
                                        break;
                                    }
                                }
                                else if (expr[i] == ']')
                                {
                                    nbracks++;
                                }
                            }
                            if (i == 0)
                            {
                                break;
                            }
                        }
                    }
                    else if (expr[i] == '[') // Unmatched.
                    {
                        i++;
                        if (i != endi)
                        {
                            result.Add(expr.Substring(i, endi - i).Trim());
                        }
                        i--;
                        break;
                    }
                    else if (expr[i] == ')')
                    {
                        int nparens = 1;
                        if (i != 0)
                        {
                            for (i--; ; i--)
                            {
                                if (expr[i] == '(')
                                {
                                    if (--nparens == 0)
                                    {
                                        result.Add(expr.Substring(i, endi - i).Trim());
                                        endi = i;
                                        break;
                                    }
                                }
                                else if (expr[i] == ')')
                                {
                                    nparens++;
                                }
                                if (i == 0)
                                {
                                    break;
                                }
                            }
                        }
                    }
                    else if (expr[i] == '(') // Unmatched.
                    {
                        i++;
                        if (i != endi)
                        {
                            result.Add(expr.Substring(i, endi - i).Trim());
                        }
                        i--;
                        break;
                    }
                    else if (char.IsSeparator(expr[i])) // ',' other stuff etc
                    {
                        i++;
                        if (i != endi)
                        {
                            result.Add(expr.Substring(i, endi - i).Trim());
                        }
                        i--;
                        break;
                    }
                    if (i == 0)
                    {
                        if (i != endi)
                        {
                            result.Add(expr.Substring(0, endi).Trim());
                        }
                        break;
                    }
                }
                result.Reverse();
            }
            return result;
        }


        Type DocGetRightmostExprType(int pos, Dictionary<string, string> syms, out bool IsStatic)
        {
            if (SymbolsLoaded)
            {
                //string ctx = Doc.GetRange(Doc.Lines.FromPosition(pos).StartPosition, pos).Text.Trim();
                string ctx;
                {   
                    int DocTextLength = Doc.TextLength;
                    if (pos < 0 || pos > DocTextLength)
                    {
                        throw new ScintillaNativeInterfaceException("DocGetRightmostExprType: out of bounds  (pos < 0 || pos > Doc.TextLength) pos=" + pos.ToString());
                    }
                    ScintillaNet.Line line = Doc.Lines.FromPosition(pos);
                    if (null == line)
                    {
                        throw new ScintillaNativeInterfaceException("DocGetRightmostExprType: the Line is null at position " + pos.ToString());
                    }
                    int linestartpos = line.StartPosition;
                    if (linestartpos < 0 || linestartpos > DocTextLength)
                    {
                        throw new ScintillaNativeInterfaceException("DocGetRightmostExprType: line.StartPosition out of bounds  (linestartpos < 0 || linestartpos > DocTextLength) pos=" + pos.ToString() + "; line.StartPosition=" + line.StartPosition.ToString());
                    }
                    if (linestartpos > pos)
                    {
                        throw new ScintillaNativeInterfaceException("DocGetRightmostExprType: unexpected line.StartPosition  (linestartpos > pos) pos=" + pos.ToString() + "; line.StartPosition=" + line.StartPosition.ToString());
                    }

                    int pos1 = 0;
                    
                     for(int i = pos-1 ; i>0; i--)
                         if (Doc.Text[i] == ';' || Doc.Text[i] == '}' || Doc.Text[i] == '{' || Doc.Text[i] == '[' || Doc.Text[i]=='('
                             || Doc.Text[i] == '=' || Doc.Text[i] == ']' || Doc.Text[i] == ')')
                         {
                           
                                pos1 = i;
                                break;
                      
                         }
                        

                 
                   
                   
                    ctx = Doc.GetRange(pos1+1, pos).Text.Trim();
                    StringBuilder ctx1 = new StringBuilder();
                    for (int i = 0; i < ctx.Length; i++)
                    {

                        char ch = ctx[i];
                        char chNext = (i + 1 < ctx.Length) ? ctx[i + 1] : '\0';

                        if (ch == '/' && chNext == '*')
                        {
                            for (i += 3; i < ctx.Length; i++)
                            {
                                if (ctx[i] == '/' && ctx[i - 1] == '*')
                                {
                                    break;
                                }
                            }
                        }
                        else if (ch == '/' && chNext == '/')
                        {
                            for (i += 2; i < ctx.Length; i++)
                            {
                                if (ctx[i] == '\r' || ctx[i] == '\n')
                                {
                                    break;
                                }
                            }
                        }
                        if (ctx[i] != ' ' && ctx[i] != '\r' &&  ctx[i] != '\n' && ctx[i] != '\t' )
                        {
                            ctx1.Append(ctx[i]);
                        }


                    }
                    ctx = ctx1.ToString();
                   
                        
                }
                // '(' scan to ')' if not, use paired one as inner type...
                List<string> parts = SplitExprParts(ctx);
                Type ltype;
                if (parts.Count == 0 || parts[0].Length == 0)
                {
                    IsStatic = false;
                    return null;
                }
         
                if (parts[0][0] == '(') // Cast.
                {
                    /*
                    int illp = parts[0].LastIndexOf('(');
                    int irp = parts[0].IndexOf(')', illp);
                    if (irp == -1)
                    {
                        IsStatic = false;
                        return null;
                    }
                    string casttype = parts[0].Substring(illp + 1, irp - illp - 1);
                    IsStatic = false; // Cast is non-static.
                    return FindAcType(casttype);
                     * */
                    IsStatic = false;
                    return null;
                }
                    
                else if (syms.ContainsKey(parts[0]) )
                {
                    string dtype = syms[parts[0]];
                    ltype = FindAcType(dtype);
                    IsStatic = false;
                }
                else
                {
                    ltype = FindAcType(parts[0]);
                    IsStatic = true;
                }
                if (ltype != null)
                {
                    for (int i = 1; i < parts.Count; i++)
                    {
                        if (parts[i].Length == 0)
                        {
#if DEBUGoff
                            throw new Exception("(parts[i].Length == 0)");
#endif
                            int i33 = 33 + 33;
                            continue;
                        }
                        if (parts[i][0] == '[')
                        {
                            //if (ltype.IsArray)
                            if (ltype.HasElementType)
                            {
                                ltype = ltype.GetElementType();
                            }
                            else if (ltype.FullName == "System.String")
                            {
                                IsStatic = false;
                                ltype = typeof(char);
                            }
                            else
                            {
                                System.Reflection.MemberInfo[] minfos = ltype.GetMember("get_Item");
                                if (0 == minfos.Length || minfos[0].MemberType == System.Reflection.MemberTypes.Field)
                                {
                                    IsStatic = false;
                                    return null;
                                }
                                System.Reflection.MethodInfo mi = (System.Reflection.MethodInfo)minfos[0];
                                ltype = mi.ReturnType;
                            }
                        }
                        else if (parts[i][0] == '(')
                        {
                        }
                        else
                        {
                            System.Reflection.BindingFlags bf = System.Reflection.BindingFlags.Public;
                            if (IsStatic)
                            {
                                bf |= System.Reflection.BindingFlags.Static;
                            }
                            else
                            {
                                bf |= System.Reflection.BindingFlags.Instance;
                                bf |= System.Reflection.BindingFlags.Static; // Needed for const stuff...
                            }
                            System.Reflection.MemberInfo[] minfos = ltype.GetMember(parts[i], bf);
                            if (0 == minfos.Length)
                            {
                                IsStatic = false;
                                return null;
                            }
                            System.Reflection.MemberInfo m = minfos[0];
                            if (m.MemberType == System.Reflection.MemberTypes.Field)
                            {
                                System.Reflection.FieldInfo fi = (System.Reflection.FieldInfo)m;
                                if (IsStatic)
                                {
                                    ltype = m.ReflectedType;
                                }
                                else
                                {
                                    // Static stuff only works for non-static things if they're literal.
                                    if (fi.IsLiteral)
                                    {
                                        ltype = m.ReflectedType;
                                    }
                                    else
                                    {
                                        IsStatic = false;
                                        return null;
                                    }
                                }
                            }
                            else if (m.MemberType == System.Reflection.MemberTypes.Method)
                            {
                                System.Reflection.MethodInfo mi = (System.Reflection.MethodInfo)m;
                                ltype = mi.ReturnType;
                            }
                            else
                            {
                                if (m.MemberType == System.Reflection.MemberTypes.Property)
                                {
                                    ltype = ((System.Reflection.PropertyInfo)m).PropertyType;
                                }
                                else
                                {
                                    ltype = m.ReflectedType;
                                }
                            }
                        }
                    }
                }
                return ltype;
            }
            IsStatic = false;
            return null; // Nope!
        }


        static string getfriendlytypename(string tname)
        {
            switch (tname)
            {
                case "Byte": return "byte";
                case "Byte[]": return "byte[]";
                case "String": return "string";
                case "Object": return "object";
                //case "List`1": return "List<byte>";
                //case "IList`1": return "IList<byte>";
                default:
                    return tname.Replace("`1", "<byte>");
                //return tname;
            }
        }


        static string getparams(System.Reflection.MemberInfo m)
        {
            if (m.MemberType == System.Reflection.MemberTypes.Method)
            {
                System.Reflection.ParameterInfo[] pis = ((System.Reflection.MethodInfo)m).GetParameters();
                StringBuilder sb = new StringBuilder(20 * pis.Length);
                sb.Append('(');
                foreach (System.Reflection.ParameterInfo pi in pis)
                {
                    if (sb.Length > 1)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(getfriendlytypename(pi.ParameterType.Name));
                    sb.Append(' ');
                    sb.Append(pi.Name);
                }
                sb.Append(')');
                return sb.ToString();
            }
            return "";
        }


        static string getretname(System.Reflection.MemberInfo m)
        {
            if (m.MemberType == System.Reflection.MemberTypes.Property)
            {
                return ((System.Reflection.PropertyInfo)m).PropertyType.Name;
            }
            if (m.MemberType == System.Reflection.MemberTypes.Method)
            {
                return ((System.Reflection.MethodInfo)m).ReturnType.Name;
            }
            return "";
        }


        public bool IsACDown
        {
            get
            {
                return Doc.NativeInterface.AutoCActive();
            }
        }

        public void ACHide()
        {
            Doc.NativeInterface.AutoCCancel();
        }


        /*
         * void GetCurrentACRange(out int start, out int length)
        {
            string txt = Doc.Text;
            int pos = Doc.SelectionStart;
            for (int i = pos - 1; pos >= 0; pos--)
            {

            }
        }
         * */


        bool istartingwith(string txt, int index, string what)
        {
            if (txt.Length - index < what.Length)
            {
                return false;
            }
            return 0 == string.Compare(txt, index, what, 0, what.Length, StringComparison.OrdinalIgnoreCase);
        }


        const string CSKEYWORDS_MODIFIED = "abstract|event|struct|as|explicit|null|switch|base|extern|this|bool|false|operator|throw|break|finally|out|true|byte|fixed|override|try|case|float|params|typeof|catch|for|private|uint|char|protected|ulong|checked|goto|public|unchecked|class|if|readonly|unsafe|const|implicit|ref|ushort|continue|in|return|using|decimal|int|sbyte|virtual|default|interface|sealed|volatile|delegate|internal|short|void|do|is|sizeof|while|double|lock|stackalloc|else|long|static|enum|namespace|from|where|select|group|into|orderby|join|let|on|equals|by|ascending|descending";


        static readonly Color XMLCOLOR = Color.FromArgb(156 - 20, 90 - 20, 60 - 20);


        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Close();
        }


        /*
        string lastfind = "";
        StringComparison lastfindcase = StringComparison.CurrentCultureIgnoreCase;

        void _XFind()
        {
            int fpos = Doc.Selection.End;
            int newpos = Doc.Text.IndexOf(lastfind, fpos, lastfindcase);
            if (-1 != newpos)
            {
                Doc.Selection.Range = new ScintillaNet.Range(newpos, newpos + lastfind.Length, Doc);
            }
            else
            {
                MessageBox.Show(finddlg, "Cannot find \"" + lastfind + "\"", "Jobs Editor", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
        }

        protected void Find(string what, StringComparison cmp)
        {
            lastfind = what;
            lastfindcase = cmp;
            _XFind();
        }


        private void findDlgFindBtn_click(object sender, EventArgs e)
        {
            Find(finddlg.FindBox.Text,
                finddlg.MatchCaseCheck.Checked ? StringComparison.CurrentCulture : StringComparison.CurrentCultureIgnoreCase
                );
        }
         * */


        //FindDlg finddlg;

        private void findToolStripMenuItem_Click(object sender, EventArgs e)
        {
            /*
            if (null == finddlg)
            {
                finddlg = new FindDlg();
                finddlg.FindNextBtn.Click += new EventHandler(findDlgFindBtn_click);
                finddlg.Location = new Point(this.Location.X + 50, this.Location.Y + 150);
                finddlg.Owner = this;
            }

            finddlg.Show();
            finddlg.Activate();
            string selline = Doc.Selection.Text.Split('\n')[0].Trim('\r');
            if (selline.Length != 0)
            {
                finddlg.FindBox.Text = selline;
            }
            finddlg.FindBox.SelectAll();
            finddlg.FindBox.Select();
             * */
            Doc.FindReplace.ShowFind();
        }

        private void findNextToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //_XFind();
            Doc.FindReplace.Window.FindNext();
        }


        private void goToToolStripMenuItem_Click(object sender, EventArgs e)
        {
            GotoDlg gd = new GotoDlg(1 + Doc.Lines.FromPosition(Doc.Selection.Start).Number);
            if (DialogResult.OK == gd.ShowDialog(this))
            {
                int ln = gd.LineNumber - 1;
                if (ln < 0)
                {
                    ln = 0;
                }
                Doc.GoTo.Line(ln);
            }
        }


        private void DocContextMenu_Opening(object sender, CancelEventArgs e)
        {

        }

        private void undoToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.UndoRedo.Undo();
        }

        private void redoToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.UndoRedo.Redo();
        }

        private void cutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.Clipboard.Cut();
        }

        private void copyToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.Clipboard.Copy();
        }

        private void pasteToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.Clipboard.Paste();
        }

        private void selectAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.Selection.SelectAll();
        }

        private void deleteToolStripMenuItem_Click(object sender, EventArgs e)
        {
            SendMessage(Doc.Handle, EM_REPLACESEL, new IntPtr(1), ""); // Allow undo.
        }

        private void SaveBtn_MouseDown(object sender, MouseEventArgs e)
        {
            if (e.Button == MouseButtons.Right)
            {
                if (ActualFile == null)
                {
                    SetStatus("<null>", 30000);
                }
                else
                {
                    SetStatus(ActualFile, 30000);
                }
            }
        }

        private void Doc_KeyDown(object sender, KeyEventArgs e)
        {
            try
            {
                if (e.KeyCode == Keys.Space)
                {
                    if ((Control.ModifierKeys & Keys.Control) == Keys.Control)
                    {
                        e.Handled = true; // ...
                    }
                }
            }
            catch (ScintillaNativeInterfaceException snie)
            {
                AlertScintillaNativeInterfaceException(snie);
            }
            catch (System.Runtime.InteropServices.SEHException seh)
            {
                AlertNativeException(seh);
            }
            catch (Exception exx)
            {
                LogOutputToFile("Doc_KeyDown: " + exx.ToString());
            }
        }

        private void Doc_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.OemPeriod && e.Modifiers == Keys.None)
            {
                
                try
                {
                    int pos = Doc.Selection.Start;
                    pos--; // Dot (potentially).
                    if (DocNative.GetStyleAt(pos) == (byte)CppStyles.OPERATOR)
                    {
                        Dictionary<string, string> syms = new Dictionary<string, string>(50);
                        if (LoadXmlDSpaceSymbolsNear(Doc.Text, pos, syms))
                        {
                            bool IsStatic;
                            Type act = DocGetRightmostExprType(pos, syms, out IsStatic);
                            if (act != null)
                            {
                                Doc.NativeInterface.AutoCSetSeparator('\u0001');
                                Doc.NativeInterface.AutoCSetTypeSeparator('\u0002');

                                {
                                    List<string> aclist = new List<string>();

                                    foreach (System.Reflection.MemberInfo m in act.GetMembers())
                                    {
                                        if (m.Name == ".ctor")
                                        {
                                            continue;
                                        }
                                        string x = m.Name;
                                        if (x.StartsWith("get_") || x.StartsWith("set_") || x.StartsWith("DSpace"))
                                        {
                                            continue;
                                        }
                                        x += getparams(m);
                                        string retname = getretname(m);
                                        if (!string.IsNullOrEmpty(retname))
                                        {
                                            x += "    [" + getfriendlytypename(retname) + "]";
                                        }
                                        aclist.Add(x);
                                    }

                                    aclist.Sort(new TypeStringSorter());
                                    StringBuilder sb = new StringBuilder(aclist.Count * 12);
                                    for (int it = 0; it < aclist.Count; it++)
                                    {
                                        if (0 != it)
                                        {
                                            sb.Append('\u0001');
                                        }
                                        sb.Append(aclist[it]);
                                    }

                                    DocNative.AutoCShow(0, sb.ToString());
                                }
                            }
                        }
                    }
                }
                catch (ScintillaNativeInterfaceException snie)
                {
                    AlertScintillaNativeInterfaceException(snie);
                }
                catch (System.Runtime.InteropServices.SEHException seh)
                {
                    AlertNativeException(seh);
                }
                catch(Exception e33)
                {
                    LogOutputToFile("Doc_KeyUp: " + e33.ToString());
                    {
                        string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                        if (computer_name.StartsWith("MAPDDRULE") || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                        {
                            SetStatus("Doc.ACd Err: " + e33.Message);
                        }
                    }
                    int i332 = 22 + 22;
                }
            }
        }


        void EnableToolbar(bool x)
        {
            showToolbarToolStripMenuItem.Checked = x;
            toolStrip1.Visible = x;
        }


        void EnableMapInputScrollControl(bool x)
        {
            mapInputScrollControlToolStripMenuItem.Checked = x;
            if (null != dbg)
            {
                lock (dbg.DebuggerSyncLock)
                {
                    if (x)
                    {
                        if (dbg.IsPaused
                            && dbg.DebugExecutionContext == MySpace.DataMining.DistributedObjects.ExecutionContextType.MAP)
                        {
                            dbg._createinputscroller_unlocked();
                            dbg._showinputscroller_unlocked();
                        }
                    }
                    else
                    {
                        dbg._destroyinputscroller_unlocked();
                    }
                }
            }
        }


        void EnableAutoComplete(bool x)
        {
            autoCompleteToolStripMenuItem.Checked = x;
            if (x)
            {
                {
                    System.Threading.Thread loadsymsthread = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(loadsymsthreadproc));
                    loadsymsthread.Name = "loadsymsthread";
                    loadsymsthread.IsBackground = true;
                    loadsymsthread.Start(Doc.Text);
                }
            }
            else
            {
                gtypes = null;
            }
        }

        void EnableSyntaxColor(bool x)
        {
            //syntaxColoringToolStripMenuItem.Checked = x;
            //Doc.NativeInterface.StyleResetDefault();
            if (x)
            {
                if (ActualFile != null && ActualFile.EndsWith(".cs", StringComparison.OrdinalIgnoreCase))
                {
                    Doc.Lexing.Lexer = ScintillaNet.Lexer.Cpp;
                    //Doc.Lexing.LexerName = "cpp";
                }
                else
                {
                    Doc.Lexing.Lexer = ScintillaNet.Lexer.Cpp;
                    Doc.Lexing.LexerName = "dspace";
                }

                {
                    Doc.Lexing.Keywords[0] = CSKEYWORDS_MODIFIED.Replace('|', ' ');

                    Doc.Lexing.Keywords[1] = "ByteSlice Entry List IList StringBuilder MapOutput RandomAccessEntries RandomAccessOutput RemoteInputStream RemoteOutputStream Int16 UInt16 Int32 UInt32 Int64 UInt64 Chris Cynthia Daniel mstring mstringarray recordset ByteSliceList ReduceOutput Blob GlobalCriticalSection DfsStream";

                    Doc.Lexing.Keywords[3] = "new ToString object Object string String foreach Split LoadXml Load ReadLine Copy Format Concat GetEnumerator Insert Substring ToCharArray ToLower ToUpper Trim TrimEnd TrimStart ToPOS ToPOSString Remove RemoveAt ToArray DSpace_Log Qizmt_Log DSpace_LogResult Qizmt_LogResult Shell OuterXml InnerXml InnerText Clone MemberwiseClone SelectNodes SelectSingleNode AppendChild CloneNode CreateAttribute CraeteCDataSection CreateComment CreateDefaultAttribute CreateDocumentFragment CreateDocumentType CreateElement CreateEntityReference CreateNavigator CreateNode CreateTextNode CreateWhitespace CreateXmlDeclaration GetElementsByTagName ImportNode InsertAfter InsertBefore Normalize PrependChild ReadNode PadLeft PadRight Replace ReadBinary WriteBinary GetDateTime GetLock";

                    Doc.Styles[ScintillaNet.StylesCommon.Default].Font = this.Font;

                    Doc.Styles.ResetDefault();

                    Color backc = Doc.Styles.Default.BackColor;

                    Doc.Styles.Default.BackColor = backc;
                    //Doc.Styles[(int)CppStyles.DEFAULT].BackColor = backc;
                    //Doc.Styles[(int)CppStyles.DYNAMICCS_DEFAULT].BackColor = backc;
                    Doc.Styles.ClearAll(); // Apply the default style to others.

                    Doc.Styles[(int)CppStyles.DEFAULT].ForeColor = Color.FromArgb(0x66, 0x66, 0x99); // Default is XML!
                    Doc.Styles[(int)CppStyles.XML_TAGNAME].Font = new Font(this.Font, FontStyle.Bold);

                    Doc.Styles[(int)CppStyles.KEYWORD].ForeColor = Color.FromArgb(0, 0, 0xC0);
                    Doc.Styles[(int)CppStyles.WORD2].ForeColor = Color.FromArgb(00, 0x77, 0xAA);
                    Doc.Styles[(int)CppStyles.GLOBALCLASS].ForeColor = Color.FromArgb(0xFF, 0, 0);
                    //Doc.Styles[(int)CppStyles.GLOBALCLASS].Font = new Font(this.Font, FontStyle.Bold);
                    Doc.Styles[(int)CppStyles.COMMENT].ForeColor = Color.DarkGreen;
                    Doc.Styles[(int)CppStyles.COMMENTLINE].ForeColor = Color.DarkGreen;
                    Doc.Styles[(int)CppStyles.STRING].ForeColor = Color.Brown;
                    Doc.Styles[(int)CppStyles.CHARACTER].ForeColor = Color.Brown;
                    Doc.Styles[(int)CppStyles.VERBATIM].ForeColor = Color.Brown;
                    Doc.Styles[(int)CppStyles.PREPROCESSOR].ForeColor = Color.FromArgb(0x00, 0x88, 0xCC);

                    Doc.Styles[(int)CppStyles.DYNAMICCS_DEFAULT].ForeColor = Color.Black;
                    Doc.Styles[(int)CppStyles.DYNAMICCS_CDATA].ForeColor = Color.DarkGreen; // Like a comment...

                    //Doc.Styles[(int)CppStyles.
                }
            }
            else
            {
                Doc.Lexing.Lexer = ScintillaNet.Lexer.Null;
            }
        }

        void EnableDebugProxy(bool x)
        {
            debugByProxyToolStripMenuItem.Checked = x;

            debugShellExecToolStripMenuItem.Enabled = !x;
        }


        public bool MapInputScrollControlEnabled
        {
            get
            {
                return config.MapInputScrollControl;
            }
        }


        private void autoCompleteToolStripMenuItem_Click(object sender, EventArgs e)
        {
            config.AutoComplete = !autoCompleteToolStripMenuItem.Checked;
            SaveConfig();
            EnableAutoComplete(config.AutoComplete);
        }

        /*
        private void syntaxColoringToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //config.SyntaxColor = !syntaxColoringToolStripMenuItem.Checked;
            SaveConfig();
            EnableSyntaxColor(config.SyntaxColor);
        }
         * */

        private void debugByProxyToolStripMenuItem_Click(object sender, EventArgs e)
        {
            config.DebugByProxyEnabled = !debugByProxyToolStripMenuItem.Checked;
#if SAVE_DEBUG_BY_PROXY
            SaveConfig();
#endif
            EnableDebugProxy(config.DebugByProxyEnabled);
        }


        public string GetDocDisplayName()
        {
            if (Doc.Modified)
            {
                return PrettyFile + "(modified)";
            }
            return PrettyFile;
        }


        public bool IsDebugging = false;
        bool wasreadonly;
        static bool cleaned = false;

        void EnterDebugMode()
        {
            curdebugcline = -1;

            if (!cleaned)
            {
                cleaned = true;
                foreach (string fn in System.IO.Directory.GetFiles(".", "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~*"))
                {
                    try
                    {
                        System.IO.File.Delete(fn);
                    }
                    catch (Exception e)
                    {
                        int i33 = 33 + 44;
                    }
                }
            }

            wasreadonly = Doc.IsReadOnly;
            Doc.IsReadOnly = true;
            //startDebuggingToolStripMenuItem.Enabled = false;
            startDebuggingToolStripMenuItem.Text = "&Resume Debugging";
            stopDebuggingToolStripMenuItem.Enabled = true;
            stepIntoToolStripMenuItem.Enabled = true;
            stepOutToolStripMenuItem.Enabled = true;
            stepOverToolStripMenuItem.Enabled = true;
            DbgOutput.Clear();
            BottomTabs.SelectedIndex = 0;
            BottomSplit.Visible = true;
            BottomTabs.Visible = true;
            IsDebugging = true; // !
            MySpace.DataMining.DistributedObjects.StaticGlobals.ExecutionMode = MySpace.DataMining.DistributedObjects.ExecutionMode.DEBUG;
            debugByProxyToolStripMenuItem.Enabled = false;
            debugShellExecToolStripMenuItem.Enabled = false;

            toolStripStatusLabel1.Text = status.Text;
            panel1.Visible = false;
            statusStrip1.Visible = true;

            foreach (ToolStripItem ts in toolStrip1.Items)
            {
                string stsTag = (ts.Tag as string);
                if ("DbgOnly_1ff9" == stsTag)
                {
                    ts.Enabled = true;
                    ts.Visible = true;
                }
                else if ("DbgOffOnly_1ff9" == stsTag)
                {
                    ts.Visible = false;
                }
            }
            DebugSkipToReduceStripButton.Enabled = false;
            EnableToolbar(config.ToolbarEnabledDebugger);

        }


        // LeavingDebugMode is just disabling debugger functions but not actually out of debug mode yet.
        // e.g. user code exception from bad syntax; or right before leaving debug mode.
        void LeavingDebugMode()
        {
            skipToReduceToolStripMenuItem.Enabled = false;
            startDebuggingToolStripMenuItem.Enabled = true;
            stepIntoToolStripMenuItem.Enabled = false;
            stepOutToolStripMenuItem.Enabled = false;
            stepOverToolStripMenuItem.Enabled = false;
            stepIntoToolStripMenuItem.Enabled = false;
            stepOutToolStripMenuItem.Enabled = false;
            stepOverToolStripMenuItem.Enabled = false;
            startDebuggingToolStripMenuItem.Enabled = false; // ...
            debugByProxyToolStripMenuItem.Enabled = true;
            if (!DebuggerProxyEnabled)
            {
                debugShellExecToolStripMenuItem.Enabled = true;
            }

            {
                // Disable all debug toolbar buttons except Stop.
                foreach (ToolStripItem ts in toolStrip1.Items)
                {
                    string stsTag = (ts.Tag as string);
                    if ("DbgOnly_1ff9" == stsTag)
                    {
                        ts.Enabled = false;
                    }
                }
                DebugStripButton.Enabled = false;
                DebugStopStripButton.Enabled = true;
            }

        }

        void LeaveDebugMode()
        {
            if (IsDebugging)
            {
                foreach (ToolStripItem ts in toolStrip1.Items)
                {
                    string stsTag = (ts.Tag as string);
                    if ("DbgOnly_1ff9" == stsTag)
                    {
                        ts.Visible = false;
                    }
                    else if ("DbgOffOnly_1ff9" == stsTag)
                    {
                        ts.Enabled = true;
                        ts.Visible = true;
                    }
                }
                EnableToolbar(config.ToolbarEnabledEditor);

                BottomTabs.Visible = false;
                LeavingDebugMode();
                DebugStripButton.Enabled = true;
                BottomSplit.Visible = false;
                startDebuggingToolStripMenuItem.Enabled = true;
                startDebuggingToolStripMenuItem.Text = "&Start Debugging";
                stopDebuggingToolStripMenuItem.Enabled = false;
                Doc.IsReadOnly = wasreadonly;
                dbg.Stop();
                dbg = null;
                IsDebugging = false; // !
                MySpace.DataMining.DistributedObjects.StaticGlobals.ExecutionMode = MySpace.DataMining.DistributedObjects.ExecutionMode.RELEASE;

                status.Text = toolStripStatusLabel1.Text;
                statusStrip1.Visible = false;
                panel1.Visible = true;

                System.GC.Collect();
                System.GC.WaitForPendingFinalizers();

                RemoveCurDebugMarker();

                if (DebugSwitch)
                {
                    Close();
                }
            }
        }


        bool DebuggerProxyEnabled
        {
            get
            {
                return debugByProxyToolStripMenuItem.Checked;
            }
        }


        void RemoveCurDebugMarker()
        {
            if (-1 != curdebugcline)
            {
                Doc.Lines[curdebugcline].DeleteMarker(MARKER_CURDEBUG);
                curdebugcline = -1;
            }
        }


        class DebugThread
        {

            internal object DebuggerSyncLock
            {
                get { return this; }
            }

            object _safeCallUiSyncLock;
            internal object SafeCallUiSyncLock
            {
                get { return Outer; }
            }


            /*internal DebugThread(JobsEditor Outer)
            {
                this.Outer = Outer;
                this._safeCallSyncLock = Outer;
            }*/

            internal DebugThread(JobsEditor Outer, object safeCallUiSyncLock)
            {
                this.Outer = Outer;
                this._safeCallUiSyncLock = safeCallUiSyncLock;
            }


            internal void Start()
            {
                thread = new System.Threading.Thread(new System.Threading.ThreadStart(threadproc));
                thread.Name = "DebugThread";
                thread.IsBackground = true;
                thread.Start();
            }


            internal bool IsReady
            {
                get { return _ready; }
            }


            void threadproc()
            {
                try
                {
                    for (int dj = 0; dj < cfg.Jobs.Length; dj++)
                    {
                        if (0 != dj)
                        {
                            System.Threading.Thread.Sleep(200);
                        }
                        if (!DebugJob(dj))
                        {
                            break;
                        }
                    }
                    _TSafe_LeaveDebugMode();
                }
                catch (Exception e)
                {

#if DEBUG
                    if (!System.Diagnostics.Debugger.IsAttached)
                    {
                        System.Diagnostics.Debugger.Launch();
                    }
#endif

                    _TSafeLockedUiCall(new Action(delegate() { Outer._CannotDebug_unlocked(e); }));
                    //_TSafe_LeaveDebugMode(); // Need to let them read the error..
                }
            }


            internal void ParseAll()
            {
                //try
                {
                    for (int dj = 0; dj < cfg.Jobs.Length; dj++)
                    {
                        if (0 != dj)
                        {
                            //System.Threading.Thread.Sleep(200);
                        }
                        if (!ParseJob(dj))
                        {
                            break;
                        }
                    }
                    _TSafe_SetStatus("Parsed", 1000);
                    //_TSafe_LeaveDebugMode();
                }
                /*catch (Exception e)
                {

#if DEBUGdd
                    if (!System.Diagnostics.Debugger.IsAttached)
                    {
                        System.Diagnostics.Debugger.Launch();
                    }
#endif

                    _TSafeLockedUiCall(new Action(delegate() { Outer._ParseError_unlocked(e); }));
                    //_TSafe_LeaveDebugMode(); // Need to let them read the error..
                }*/
            }


            System.Threading.Thread thread; // Main thread for debugging and writing debugger commands.
            System.IO.StreamWriter din;
            System.IO.StreamReader dout;
            //System.IO.StreamReader derr;
            JobsEditor Outer;
            public List<int> cdatalines;
            int curcdata = 0;
            public SourceCode cfg;
            public int dbgjobindex = -1; // Not the same as jobnum!
            bool _ready = false;
            public string doctext;

            public string dbgsourcepath;
            public string dbgexepath;

            public string dbgskiptoreduce = null;

            public bool IsPaused; // Only valid if debugging.
            public bool _stepping = false;
            public MySpace.DataMining.DistributedObjects.ExecutionContextType DebugExecutionContext;

            public List<dfs.DfsFile.FileNode> dbgmapinputdfsnodes;

            public string dbgmapinputscroll = null;
            public long dbgmapinputscrollsize;
#if DEBUG
            public const int dbgmapinputscrollsize_upperbound = 0x400 * 0x400 * 88;
#else
            public const int dbgmapinputscrollsize_upperbound = int.MaxValue - 1;
#endif
            Form inputscrollform = null;
            InputScrollControl inputscrollcontrol = null;
            int inputscrollcontrol_prevvalue;
            long inputscrollcontrol_partindex;
            long inputscrollcontrol_partposition;

            // Must be done from UI thread!
            internal void _createinputscroller_unlocked()
            {
                if (null == inputscrollform)
                {
                    inputscrollcontrol_prevvalue = 0;
                    inputscrollform = new Form();
                    inputscrollform.FormClosing += new FormClosingEventHandler(_inputscrollform_FormClosing);
                    inputscrollform.Text = "Map: " + cfg.Jobs[dbgjobindex].NarrativeName;
                    inputscrollform.ShowInTaskbar = false;
                    inputscrollform.MinimizeBox = false;
                    inputscrollform.MaximizeBox = false;
                    inputscrollform.FormBorderStyle = FormBorderStyle.SizableToolWindow;
                    inputscrollform.StartPosition = FormStartPosition.Manual;
                    inputscrollcontrol = new InputScrollControl();
                    {
                        dbgmapinputscrollsize = 0;
                        foreach (dfs.DfsFile.FileNode fn in dbgmapinputdfsnodes)
                        {
                            dbgmapinputscrollsize += fn.Length;
                        }
                    }
                    if (dbgmapinputscrollsize > dbgmapinputscrollsize_upperbound)
                    {
                        inputscrollcontrol.InputScroll.Maximum = dbgmapinputscrollsize_upperbound;
                    }
                    else
                    {
                        inputscrollcontrol.InputScroll.Maximum = (int)dbgmapinputscrollsize;
                    }
                    /*if (dbgmapinputscrollsize > 0x400 * 0x400 * 64)
                    {
                        inputscrollcontrol.InputScroll.LargeChange = 0x400 * 0x400 * 64;
                    }
                    if (inputscrollcontrol.InputScroll.Maximum > 1000)
                    {
                        inputscrollcontrol.InputScroll.SmallChange = inputscrollcontrol.InputScroll.Maximum / 100;
                    }
                    else if (inputscrollcontrol.InputScroll.Maximum > 10)
                    {
                        inputscrollcontrol.InputScroll.SmallChange = inputscrollcontrol.InputScroll.Maximum / 10;
                    }*/
                    inputscrollcontrol.Enabled = false;
                    inputscrollcontrol.Parent = inputscrollform;
                    inputscrollform.ClientSize = inputscrollcontrol.Size;
                    inputscrollcontrol.Dock = DockStyle.Fill;
                    inputscrollform.Location =
                        new Point(Outer.Right - inputscrollform.Width - 32,
                            Outer.Top + (Outer.Height / 2 - inputscrollform.Height / 2));
                    inputscrollform.Owner = Outer;

                    for(int idmisr = 20; ; idmisr--)
                    {
                        try
                        {
                            // <partindex><newline><partposition><newline>
                            System.IO.File.WriteAllText(dbgmapinputscroll, "-1\r\n-1\r\n");
                            break;
                        }
                        catch
                        {
                            if (idmisr < 0)
                            {
                                throw;
                            }
                            System.Threading.Thread.Sleep(200);
                            continue;
                        }
                    }

                    inputscrollcontrol.InputScroll.Scroll += new ScrollEventHandler(_InputScroll_Scroll);
                    inputscrollcontrol.NudgeBack.Click += new EventHandler(NudgeBack_Click);
                    inputscrollcontrol.NudgeForward.Click += new EventHandler(NudgeForward_Click);

                }

            }

            internal int _getscrolltopartposinputscrollcontrol(long partindex, long partposition)
            {
                long sizetopart = 0;
                for (int ifn = 0; ifn < partindex; ifn++)
                {
                    dfs.DfsFile.FileNode fn = dbgmapinputdfsnodes[ifn];
                    sizetopart += fn.Length;
                }

                long newpos = sizetopart + partposition;
                if (dbgmapinputscrollsize > dbgmapinputscrollsize_upperbound)
                {
                    newpos = (long)((double)dbgmapinputscrollsize_upperbound * (double)newpos / (double)dbgmapinputscrollsize);
                }
                if (newpos < 0 || newpos > dbgmapinputscrollsize)
                {
                    throw new Exception("DEBUG:  invalid calculated scroll position: "
                        + newpos + " (" + dbgmapinputscrollsize + " input size)");
                }
                return (int)newpos;
            }

            // Must be done from UI thread!
            internal void _scrolltopartposinputscrollcontrol(long partindex, long partposition)
            {
                int newvalue = _getscrolltopartposinputscrollcontrol(partindex, partposition);
                inputscrollcontrol_prevvalue = newvalue;
                inputscrollcontrol.InputScroll.Value = newvalue;
                inputscrollcontrol_partindex = partindex;
                inputscrollcontrol_partposition = partposition;
            }

            // Must be done from UI thread!
            internal void _updatetextinputscrollcontrol(long partindex, long partposition)
            {
                try
                {
                    if (partindex < 0 || partindex >= dbgmapinputdfsnodes.Count)
                    {
                        throw new IndexOutOfRangeException("_updatetextinputscrollcontrol: partindex");
                    }
                    // Since this is reading the chunk directly, need to skip the header.
                    dfs.DfsFile.FileNode fn = dbgmapinputdfsnodes[(int)partindex];
                    using (DfsFileNodeStream stm = new DfsFileNodeStream(fn, true,
                        System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x400 * 4))
                    {
                        byte[] buf = new byte[32];
                        int headersize;
                        {
                            StreamReadExact(stm, buf, 4);
                            int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf);
                            headersize = hlen;
                            if (hlen > 4)
                            {
                                int hremain = hlen - 4;
                                if (hremain > buf.Length)
                                {
                                    buf = new byte[hremain];
                                }
                                StreamReadExact(stm, buf, hremain);
                            }
                        }
                        if (0 != partposition)
                        {
                            stm.Position = (long)headersize + partposition;
                        }
                        string newtext;
                        if (-1 == MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength)
                        {
                            System.IO.StreamReader sr = new System.IO.StreamReader(stm);
                            StringBuilder sbnewtext = new StringBuilder();
                            string s = sr.ReadLine();
                            if (null == s)
                            {
                                throw new Exception("Unable to read line; end of input file");
                            }
                            for (int i = 0; i < s.Length; i++)
                            {
                                char ch = s[i];
                                if (ch < ' ' || char.IsControl(ch))
                                {
                                    sbnewtext.Append("<" + (int)ch + ">");
                                }
                                else
                                {
                                    sbnewtext.Append(ch);
                                }
                            }
                            newtext = sbnewtext.ToString();
                        }
                        else
                        {
                            StringBuilder sbnewtext = new StringBuilder();
                            for (int i = 0;
                                i < MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                                i++)
                            {
                                int ich = stm.ReadByte();
                                if (-1 == ich)
                                {
                                    throw new Exception("_updatetextinputscrollcontrol: Hit EOF too early while reading record");
                                }
                                if (ich >= 127 || ich < ' ')
                                {
                                    sbnewtext.Append("<" + ich + ">");
                                }
                                else
                                {
                                    sbnewtext.Append((char)ich);
                                }
                            }
                            newtext = sbnewtext.ToString();
                        }
                        inputscrollcontrol.CurrentInputBox.Text = newtext;
                    }
                }
                catch
                {
#if DEBUG
                    System.Diagnostics.Debugger.Launch();
#endif
                    inputscrollcontrol.CurrentInputBox.Text = "?";
                }
            }

            long _getpartlength(dfs.DfsFile.FileNode fn)
            {
                using (DfsFileNodeStream stm = new DfsFileNodeStream(fn, true,
                       System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x400 * 4))
                {
                    byte[] buf = new byte[32];
                    int headersize;
                    {
                        StreamReadExact(stm, buf, 4);
                        int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf);
                        headersize = hlen;
                        if (hlen > 4)
                        {
                            int hremain = hlen - 4;
                            if (hremain > buf.Length)
                            {
                                buf = new byte[hremain];
                            }
                            StreamReadExact(stm, buf, hremain);
                        }
                    }
                    return stm.Length - headersize;
                }
            }

            // If findnextline==true, it will return -1 instead of a newline that's at the end of the file.
            long _findnewline(dfs.DfsFile.FileNode fn, long position, bool backward, bool findnextline)
            {
                using (DfsFileNodeStream stm = new DfsFileNodeStream(fn, true,
                    System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x400 * 4))
                {
                    byte[] buf = new byte[32];
                    int headersize;
                    {
                        StreamReadExact(stm, buf, 4);
                        int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf);
                        headersize = hlen;
                        if (hlen > 4)
                        {
                            int hremain = hlen - 4;
                            if (hremain > buf.Length)
                            {
                                buf = new byte[hremain];
                            }
                            StreamReadExact(stm, buf, hremain);
                        }
                    }
                    if (position < 0)
                    {
                        position = stm.Length - headersize - (-(position + 1));
                    }
                    if (0 != position)
                    {
                        stm.Position = (long)headersize + position;
                    }
                    if (backward)
                    {
                        for (; ; )
                        {
                            int iby = stm.ReadByte();
                            if (-1 == iby)
                            {
                                return -1;
                            }
                            if ('\n' == iby)
                            {
                                //if (0 == numskip--)
                                {
                                    return position;
                                }
                            }
                            if (0 == position)
                            {
                                return -1;
                            }
                            stm.Seek(-2, System.IO.SeekOrigin.Current);
                            position--;
                        }
                    }
                    else // forward:
                    {
                        for (; ; )
                        {
                            int iby = stm.ReadByte();
                            if (-1 == iby)
                            {
                                return -1;
                            }
                            if ('\n' == iby)
                            {
                                //if (0 == numskip--)
                                {
                                    if (findnextline)
                                    {
                                        if (-1 == stm.ReadByte())
                                        {
                                            return -1;
                                        }
                                    }
                                    return position;
                                }
                            }
                            position++;
                        }
                    }
                }
            }

            void _nudged()
            {
                for (int idmisr = 20; ; idmisr--)
                {
                    try
                    {
                        // <partindex><newline><partposition><newline>
                        System.IO.File.WriteAllText(dbgmapinputscroll,
                            inputscrollcontrol_partindex + "\r\n" + inputscrollcontrol_partposition + "\r\n");
                        break;
                    }
                    catch
                    {
                        if (idmisr < 0)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(200);
                        continue;
                    }
                }
                _updatetextinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
                _scrolltopartposinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
            }

            // Note: in the context of the UI thread!
            void NudgeBack_Click(object sender, EventArgs e)
            {
                if (-1 == MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength)
                {
                    if (inputscrollcontrol_partposition <= 1)
                    {
                        if (inputscrollcontrol_partindex > 0)
                        {
                            inputscrollcontrol_partindex--;
                            long pos = _findnewline(dbgmapinputdfsnodes[(int)inputscrollcontrol_partindex],
                                -3, true, false); // -1 is end of file; -2 is the \n, and -3 is part of the previous line.
                            if (-1 == pos)
                            {
                                pos = 0;
                            }
                            else
                            {
                                pos++;
                            }
                            inputscrollcontrol_partposition = pos;
                            _nudged();
                        }
                        else
                        {
                            // Don't update.
                        }
                    }
                    else
                    {
                        // Start 2 back: one back is on the newline, 2 back is in the previous line.
                        long pos = _findnewline(dbgmapinputdfsnodes[(int)inputscrollcontrol_partindex],
                            inputscrollcontrol_partposition - 2, true, false);
                        if (-1 == pos)
                        {
                            pos = 0;
                        }
                        else
                        {
                            pos++;
                        }
                        inputscrollcontrol_partposition = pos;
                        _nudged();
                    }
                }
                else
                {
                    if (inputscrollcontrol_partposition - MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength < 0)
                    {
                        if (0 == inputscrollcontrol_partindex)
                        {
                            // Don't update.
                        }
                        else
                        {
                            inputscrollcontrol_partindex--;
                            long partlength = _getpartlength(dbgmapinputdfsnodes[(int)inputscrollcontrol_partindex]);
                            inputscrollcontrol_partposition = partlength - MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                            if (inputscrollcontrol_partposition < 0)
                            {
                                inputscrollcontrol_partposition = 0;
                            }
                            _nudged();
                        }
                    }
                    else
                    {
                        inputscrollcontrol_partposition -= MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                        _nudged();
                    }
                }
            }

            // Note: in the context of the UI thread!
            void NudgeForward_Click(object sender, EventArgs e)
            {
                if (-1 == MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength)
                {
                    long pos = _findnewline(dbgmapinputdfsnodes[(int)inputscrollcontrol_partindex],
                        inputscrollcontrol_partposition, false, true);
                    if (-1 == pos)
                    {
                        inputscrollcontrol_partindex++;
                        if (inputscrollcontrol_partindex >= dbgmapinputdfsnodes.Count)
                        {
                            inputscrollcontrol_partindex = dbgmapinputdfsnodes.Count - 1;
                            // Don't update.
                        }
                        else
                        {
                            inputscrollcontrol_partposition = 0;
                            _nudged();
                        }
                    }
                    else
                    {
                        inputscrollcontrol_partposition = pos + 1;
                        _nudged();
                    }
                }
                else
                {
                    long partlength = _getpartlength(dbgmapinputdfsnodes[(int)inputscrollcontrol_partindex]);
                    if (inputscrollcontrol_partposition + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength >= partlength)
                    {
                        inputscrollcontrol_partindex++;
                        if (inputscrollcontrol_partindex >= dbgmapinputdfsnodes.Count)
                        {
                            inputscrollcontrol_partindex = dbgmapinputdfsnodes.Count - 1;
                            // Don't update.
                        }
                        else
                        {
                            inputscrollcontrol_partposition = 0;
                            _nudged();
                        }
                    }
                    else
                    {
                        inputscrollcontrol_partposition += MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                        _nudged();
                    }
                }
            }

            // Note: in the context of the UI thread!
            void _InputScroll_Scroll(object sender, ScrollEventArgs eventargs)
            {
                if (eventargs.NewValue == inputscrollcontrol_prevvalue)
                {
                    return;
                }

                long newpos = eventargs.NewValue;
                if (dbgmapinputscrollsize > dbgmapinputscrollsize_upperbound)
                {
                    newpos = (long)((double)dbgmapinputscrollsize * (double)newpos / (double)dbgmapinputscrollsize_upperbound);
                }

                long partindex = 0;
                long partposition = 0;
                if (dbgmapinputdfsnodes.Count > 0)
                {
                    long posremain = newpos;
                    foreach (dfs.DfsFile.FileNode fn in dbgmapinputdfsnodes)
                    {
                        if (posremain < fn.Length)
                        {
                            partposition = posremain;
                            break;
                        }
                        posremain -= fn.Length;
                        partindex++;
                    }
                    if (partindex == dbgmapinputdfsnodes.Count)
                    {
                        // At the very end of the last one, otherwise would be the beginning of another part.
                        partindex--;
                        partposition = dbgmapinputdfsnodes[dbgmapinputdfsnodes.Count - 1].Length;
                    }
                }

                // Snap to whole chunks.
                if (partposition > 0)
                {
                    if (eventargs.NewValue > inputscrollcontrol_prevvalue)
                    {
                        if (partindex + 1 == dbgmapinputdfsnodes.Count)
                        {
                            // Don't snap-up if there's no more parts after this.
                            eventargs.NewValue = inputscrollcontrol_prevvalue;
                            return;
                        }
                        else
                        {
                            partindex++;
                            partposition = 0;
                        }
                    }
                    else
                    {
                        partposition = 0;
                    }
                    {
                        int eanewvalue = _getscrolltopartposinputscrollcontrol(partindex, partposition);
                        try
                        {
                            eventargs.NewValue = eanewvalue;
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("DEBUG:  error while snapping input scroll to part boundary (invalid value: " + eanewvalue + ")", e);
                        }
                    }
                }

                for (int idmisr = 20; ; idmisr--)
                {
                    try
                    {
                        // <partindex><newline><partposition><newline>
                        System.IO.File.WriteAllText(dbgmapinputscroll,
                            partindex.ToString() + "\r\n"
                            + partposition.ToString() + "\r\n");
                        break;
                    }
                    catch
                    {
                        if (idmisr < 0)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(200);
                        continue;
                    }
                }

                inputscrollcontrol_prevvalue = eventargs.NewValue;

                _updatetextinputscrollcontrol(partindex, partposition);
                inputscrollcontrol_partindex = partindex;
                inputscrollcontrol_partposition = partposition;

            }

            void _inputscrollform_FormClosing(object sender, FormClosingEventArgs e)
            {
                e.Cancel = true;
                (sender as Form).Hide();
            }

            // Must be done from UI thread!
            internal void _showinputscroller_unlocked()
            {
#if DEBUG
                if (null == inputscrollform)
                {
                    throw new Exception("DEBUG:  _showinputscroller_unlocked: (null == inputscrollform)");
                }
#endif
                if (null != inputscrollform)
                {
                    if (!inputscrollform.Visible)
                    {
                        //_updatetextinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
                        inputscrollform.Show();
                    }
                }
            }

            // Must be done from UI thread!
            internal void _destroyinputscroller_unlocked()
            {
                if (null != inputscrollform)
                {
                    inputscrollform.Close();
                    inputscrollform.Dispose();
                    inputscrollform = null;
                    inputscrollcontrol = null;
                }

                try
                {
                    System.IO.File.Delete(dbgmapinputscroll);
                }
                catch
                {
                }

            }

            // Called when breakpoint hit and after a step finishes.
            void _Paused()
            {
                //Console.WriteLine(" *** PAUSED IN {0}", DebugExecutionContext);

                if (DebugExecutionContext == MySpace.DataMining.DistributedObjects.ExecutionContextType.MAP)
                {
                    if (Outer.MapInputScrollControlEnabled)
                    {
                        _TSafeLockedUiCall(
                            new Action(
                            delegate
                            {
                                _createinputscroller_unlocked();
                                inputscrollcontrol.Enabled = true;
                                _showinputscroller_unlocked();
                                _updatetextinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
                                Outer.Activate();
                            }));
                    }
                }
                else
                {
                    if (null != inputscrollform)
                    {
                        _TSafeLockedUiCall(
                            new Action(
                            delegate
                            {
                                _destroyinputscroller_unlocked();
                            }));
                    }
                }
            }

            // Called when resuming execution, not when stepping.
            // Note: not called when debugging has stopped; e.g. user cancels debug session.
            void _Unpaused()
            {
                //Console.WriteLine(" *** UN-PAUSED IN {0}", DebugExecutionContext);

                if (null != inputscrollform)
                {
                    _TSafeLockedUiCall(
                        new Action(
                        delegate
                        {
                            //_destroyinputscroller_unlocked();
                            inputscrollcontrol.Enabled = false;
                            inputscrollcontrol.CurrentInputBox.Clear();
                        }));
                }
            }

            void _DebugSessionStopped()
            {
                if (null != inputscrollform)
                {
                    _TSafeLockedUiCall(
                        new Action(
                        delegate
                        {
                            _destroyinputscroller_unlocked();
                        }));
                }
            }

            void _DebugOneJobFinished()
            {
                if (null != inputscrollform)
                {
                    _TSafeLockedUiCall(
                        new Action(
                        delegate
                        {
                            _destroyinputscroller_unlocked();
                        }));
                }
            }


            void _TSafeLockedUiCall(Action act)
            {
#if DEBUG
                if (dbgjobindex == 1)
                {
                    int i33 = 33 + 33;
                }
#endif
                //lock (SafeCallUiSyncLock)
                {
                    //Outer.Invoke(act);
                    Outer.DbgOutput.Invoke(
                        new Action(delegate()
                        {
                            lock (SafeCallUiSyncLock)
                            {
                                act();
                            }
                        }));
                }
            }


            public void Resume()
            {
                lock (DebuggerSyncLock)
                {
                    if (!IsPaused)
                    {
                        return;
                    }
                    _stepping = false;
                    _rawcmd_unlocked("cont");
                    IsPaused = false;
                    _Unpaused();
                }
            }


            internal void _stopone()
            {
                if (Outer.DebuggerProxyEnabled)
                {
                    lock (DebuggerSyncLock)
                    {
                        bool wasready = _ready;
                        _ready = false;

                        if (wasready)
                        {
                            try
                            {
                                _rawcmd_unlocked("quit");
                            }
                            catch
                            {
                            }
                        }

                    }
                }
                else
                {
                    _ready = false;
                }

                _TSafeLockedUiCall(
                    new Action(delegate()
                    {
                        if (-1 != Outer.curdebugcline)
                        {
                            Outer.Doc.Lines[Outer.curdebugcline].DeleteMarker(MARKER_CURDEBUG);
                            Outer.curdebugcline = -1;
                        }
                    }));
            }


            public void Stop()
            {
                _stopone();
                _DebugSessionStopped();
            }


            void _TSafe_LeaveDebugMode()
            {
                _TSafeLockedUiCall(new Action(Outer.LeaveDebugMode));
            }


            void SetBreakpoints(bool reset)
            {

                List<int> lms = new List<int>();
                _TSafeLockedUiCall(new Action(delegate()
                {
                    //ScintillaNet.Marker mk = Outer.Doc.Margins[MARGIN_BREAKPOINT];
                    int lm = cdatalines[curcdata];
                    for (; ; )
                    {
                        lm = Outer.Doc.NativeInterface.MarkerNext(lm, 1 << MARKER_BREAKPOINT);
                        if (-1 == lm)
                        {
                            break;
                        }
                        lms.Add(lm);
                        lm++;
                    }
                }));

                {
                    lock (DebuggerSyncLock)
                    {
                        if (reset)
                        {
                            _rawcmd_unlocked("delete"); // Delete all breakpoints
                        }
                        System.IO.FileInfo fi = null;
                        for (int i = 0; i < lms.Count; i++)
                        {
                            int lm = lms[i];
                            if (null == fi)
                            {
                                fi = new System.IO.FileInfo(jobfilename);
                            }
                            _rawcmd_unlocked("b " + fi.Name + ":" + (lm).ToString());
                        }
                    }

                }

            }

            public void SetBreakpoints()
            {
                SetBreakpoints(true);
            }


            bool _RunDebugger()
            {
                string debuggerpath = "mrdebug.exe";
                try
                {
                    string adbn = Environment.GetEnvironmentVariable("DSDEBUGGER");
                    if (null != adbn)
                    {
                        debuggerpath = adbn;
                    }
                }
                catch
                {
                }
                string debuggerbasename = debuggerpath;
                if (debuggerbasename.EndsWith(".exe", true, null))
                {
                    debuggerbasename = debuggerbasename.Substring(0, debuggerbasename.Length - 4);
                }
                {
                    int ilslash = debuggerbasename.LastIndexOf('\\');
                    if (-1 != ilslash)
                    {
                        debuggerbasename = debuggerbasename.Substring(ilslash + 1);
                    }
                }

                string dbgsargs = "\"" + dbgexepath + "\"";
                if (!string.IsNullOrEmpty(dbgskiptoreduce))
                {
                    dbgsargs += " \"" + dbgskiptoreduce + "\"";
                }
#if DEBUGout
                Console.WriteLine("STARTING DEBUGGER \"{0}\" {1}", debuggerpath, dbgsargs);
#endif
#if DEBUG
                if (0 == string.Compare(debuggerbasename, "mrdebug", true))
                {
                    dbgsargs = "-xdebug " + dbgsargs;
                }
#endif
                if (Outer.DebuggerProxyEnabled)
                {
                    _TSafe_SetStatus("Debug by Proxy", 200);

                    Surrogate.RedirectStreamIO io = new Surrogate.RedirectStreamIO();
                    din = io.StandardInput;
                    dout = io.StandardOutput;
                    io.StandardError = System.IO.StreamReader.Null;
                    io.ConsoleError = System.IO.StreamWriter.Null;
                    //derr = io.StandardError;
                    System.Threading.Thread remotedebugthread = new System.Threading.Thread(new System.Threading.ThreadStart(
                        delegate()
                        {
                            string fullargs;
                            if (-1 != debuggerpath.IndexOf(' '))
                            {
                                fullargs = "\"" + debuggerpath + "\" " + dbgsargs;
                            }
                            else
                            {
                                fullargs = debuggerpath + " " + dbgsargs;
                            }
                            Surrogate.RunStreamStdIO("localhost", fullargs, false, false, io);
                        }));
                    remotedebugthread.Name = "DebugProxy";
                    remotedebugthread.IsBackground = true;
                    remotedebugthread.Start();
                }
                else
                {
                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(debuggerpath, dbgsargs);
                    psi.CreateNoWindow = true;
                    psi.RedirectStandardInput = true;
                    psi.RedirectStandardOutput = true;
                    psi.UseShellExecute = false;
                    try
                    {
                        psi.WorkingDirectory = Environment.CurrentDirectory;
                        if (Outer.settings.ContainsKey("user"))
                        {
                            psi.UserName = (string)Outer.settings["user"];
                            if (Outer.settings.ContainsKey("domain"))
                            {
                                psi.Domain = (string)Outer.settings["domain"];
                            }
                            psi.Password = (System.Security.SecureString)Outer.settings["auth"];
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                    System.Diagnostics.Process dbgproc = System.Diagnostics.Process.Start(psi);
                    if (null == dbgproc)
                    {
                        throw new Exception("Unable to start debugger");
                    }
                    din = dbgproc.StandardInput;
                    dout = dbgproc.StandardOutput;
                    //derr = dbgproc.StandardError;
                }
#if DEBUG_SLOW_DEBUGGER
                System.Threading.Thread.Sleep(2000);
#endif
                din.WriteLine(DBG_HIDDEN); // Fix first line.
                //din.Flush();
#if DEBUGform
                Form df = null;
                TextBox fdout = null;
                TextBox fdin = null;
                _TSafeLockedUiCall(new Action(delegate()
                {
                    df = new Form();
                    df.Text = "Debug Console";
                    fdout = new TextBox();
                    fdout.Multiline = true;
                    fdout.ScrollBars = ScrollBars.Vertical;
                    fdout.Dock = DockStyle.Fill;
                    fdout.ReadOnly = true;
                    fdout.Parent = df;
                    fdin = new TextBox();
                    fdin.Dock = DockStyle.Bottom;
                    fdin.Parent = df;
                    Button send = new Button();
                    send.Parent = df;
                    df.AcceptButton = send;
                    send.Click += new EventHandler(delegate(object sender, EventArgs ea)
                    {
                        //dout.AppendText(fdin.Text + Environment.NewLine);
                        _rawcmd_unlocked(fdin.Text);
                        fdin.Text = "";
                    });
                    df.WindowState = FormWindowState.Minimized;
                    df.Show();
                }));
#endif
                _TSafeLockedUiCall(new Action(delegate()
                {
                    if (0 != Outer.DbgOutput.TextLength)
                    {
                        Outer.DbgOutput.AppendText("-\r\n");
                    }
                }));
                // Add initial breakpoints and 'continue' before ready!
                SetBreakpoints(false); // reset==false (no need to reset for first)
                List<ScintillaNet.Range> entrypoints = null;
                if (Outer.DebugStepSwitch)
                {
                    entrypoints = new List<ScintillaNet.Range>(3);
                    lock (Outer.UiSyncLock)
                    {
                        {
                            ScintillaNet.Range epr = Outer.Doc.FindReplace.Find(@"(void[     ]+Local[     ]*\([     ]*\))",
                                ScintillaNet.SearchFlags.RegExp | ScintillaNet.SearchFlags.Posix | ScintillaNet.SearchFlags.WordStart | ScintillaNet.SearchFlags.MatchCase);
                            if (null != epr)
                            {
                                entrypoints.Add(epr);
                            }
                        }
                        {
                            ScintillaNet.Range epr = Outer.Doc.FindReplace.Find(@"(void[     ]+Remote[     ]*\()",
                                ScintillaNet.SearchFlags.RegExp | ScintillaNet.SearchFlags.Posix | ScintillaNet.SearchFlags.WordStart | ScintillaNet.SearchFlags.MatchCase);
                            if (null != epr)
                            {
                                entrypoints.Add(epr);
                            }
                        }
                        {
                            ScintillaNet.Range epr = Outer.Doc.FindReplace.Find(@"(void[     ]+Map[     ]*\()",
                                ScintillaNet.SearchFlags.RegExp | ScintillaNet.SearchFlags.Posix | ScintillaNet.SearchFlags.WordStart | ScintillaNet.SearchFlags.MatchCase);
                            if (null != epr)
                            {
                                entrypoints.Add(epr);
                            }
                        }
                    }
#if DEBUG
                    if (0 == entrypoints.Count)
                    {
                        throw new Exception("Warning: no entry point found");
                    }
#endif
                }
                lock (DebuggerSyncLock)
                {
                    if (Outer.DebugStepSwitch)
                    {
                        ////////
                        {
                            System.IO.FileInfo fi = new System.IO.FileInfo(jobfilename);
                            //_rawcmd_unlocked("b " + fi.Name + ":0"); // In case no entry point found?
                            foreach (ScintillaNet.Range epr in entrypoints)
                            {
                                _rawcmd_unlocked("b " + fi.Name + ":" + (1 + epr.StartingLine.Number).ToString());
                            }
                        }
                    }

                    {
                        _rawcmd_unlocked("cont");
                        IsPaused = false;
                    }
                }
                _ready = true; // !
                bool NeedResetBreakpoints = Outer.DebugStepSwitch;
                string debuggerprompt = "(" + debuggerbasename + ")";
                for (; ; )
                {
                    if (!_ready)
                    {
                        break;
                    }
                    string dln = dout.ReadLine();
#if DEBUG_SLOW_DEBUGGER
                    System.Threading.Thread.Sleep(400);
#endif
                    if (dln == null)
                    {
                        throw new Exception("Debugger exited prematurely (dout.read is null)");
                    }
#if DEBUGout
                    Console.WriteLine("  DEBUG  IN:  {0}", dln);
#endif
                    for (; ; )
                    {
                        dln = dln.TrimStart();
                        if (!dln.StartsWith(debuggerprompt, true, null))
                        {
                            break;
                        }
                        dln = dln.Substring(debuggerprompt.Length);
                    }
#if DEBUGform
                    _TSafeLockedUiCall(new Action(delegate() { fdout.AppendText(dln + Environment.NewLine); }));
#endif
                    if (-1 == dln.IndexOf(DBG_HIDDEN))
                    {
                        _TSafeLockedUiCall(new Action(delegate()
                        {
                            Outer.DbgOutput.AppendText(dln + "\r\n");
                            Outer.DbgOutput.ScrollToCaret();
                        }));
                    }
                    if (dln.StartsWith("{DC453EE3-9EEE-48d1-B8A8-362F5A02858B}")) // DSpace_Log/Qizmt_Log
                    {
                        string s = dln.Substring(38);
                        if (s.Length > 0)
                        {
                            string logline = s;
                            //string uilogline = "DSpace_Log: " + logline;
                            string uilogline = "Qizmt_Log: " + logline;
                            _TSafeLockedUiCall(new Action(delegate()
                            {
                                if (Outer.DebugSwitch)
                                {
                                    Console.WriteLine(logline);
                                }
                                Outer.SetStatus(uilogline, 10000);
                                //Outer.DbgOutput.AppendText(uilogline + "\r\n"); // Already shown in debugger console.
                                //Outer.DbgOutput.ScrollToCaret();
                            }));
                            System.Threading.Thread.Sleep(200);
                        }
                    }
                    else if (dln.StartsWith("MDA notification: "))
                    {
                        _TSafeLockedUiCall(new Action(delegate()
                        {
                            Outer.WriteOutputRed_unlocked("Warning: an \"MDA notification\" was detected;"
                                + " please report this if problems are encountered: " + dln.Substring(18));
                        }));

                        if (dln.StartsWith("MDA notification: Name:BindingFailure"))
                        {
                            if (!IsPaused)
                            {
                                lock (DebuggerSyncLock)
                                {
                                    _rawcmd_unlocked("cont");
                                }
                            }
                        }
                    }
                    else if (dln.StartsWith("<!> CRITICAL ERROR: "))
                    {
                        _TSafeLockedUiCall(new Action(delegate()
                        {
                            Outer.ShowError_unlocked("Critical internal debugger error detected: " + dln);
                        }));
                    }
                    else if (dln.StartsWith("{B5F03A4C-F06F-49a7-A4C4-FBD9292FFB93}")) // DGlobals.Add
                    {
                        string s = dln.Substring(38);
                        if (s.Length > 0)
                        {
                            _TSafeLockedUiCall(new Action(delegate()
                            {
                                MySpace.DataMining.DistributedObjects.DGlobalsM.FromBase64StringInDebugMode(s);
                            }));
                            System.Threading.Thread.Sleep(200);
                        }
                    }
                    else if (dln.StartsWith("{B5AB3BA8-7AAB-4c6f-9387-FDCD958C9E94}")) // Now reducing.
                    {
                        DebugExecutionContext = MySpace.DataMining.DistributedObjects.ExecutionContextType.REDUCE;
                    }
                    else if (dln.StartsWith("{D46AA97E-0315-4b14-B6B0-3347A597C6EF}")) // <partindex>@<partposition>
                    {
                        if (Outer.MapInputScrollControlEnabled)
                        {
                            string s = dln.Substring(38);
                            int iat = s.IndexOf('@');
                            inputscrollcontrol_partindex = long.Parse(s.Substring(0, iat));
                            inputscrollcontrol_partposition = long.Parse(s.Substring(iat + 1));
                            //if (inputscroll_partindex >= 0)
                            {
                                if (inputscrollcontrol_partposition < 0)
                                {
                                    inputscrollcontrol_partposition = 0;
                                }
                                if (null != inputscrollform)
                                {
                                    _TSafeLockedUiCall(
                                        new Action(
                                        delegate
                                        {
                                            //_createinputscroller_unlocked();
                                            _scrolltopartposinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
                                            if (IsPaused)
                                            {
                                                _updatetextinputscrollcontrol(inputscrollcontrol_partindex, inputscrollcontrol_partposition);
                                            }
                                        }));
                                }
                            }
                        }
                    }
                    else
                    {

                        if (_inprintmultiline)
                        {
                            for (int i = 0; i < dln.Length; i++)
                            {
                                if (dln[i] == '\\')
                                {
                                    i++;
                                }
                                else if (dln[i] == '"')
                                {
                                    _inprintmultiline = !_inprintmultiline;
                                }
                            }
                            continue;
                        }

                        if (_inexception)
                        {
                            if (dln.StartsWith("_message=(0x"))
                            {
                                _lastcodeexception = dln.Substring(dln.IndexOf(')') + 1).Trim();
                                /*_TSafe_Call(new Action(delegate()
                                {
                                    Outer.WriteOutputError("Unhandled exception in code: " + _lastcodeexception);
                                }));*/
                            }
                            else if (dln == "") // End of "Function evaluation completed with an exception."
                            {
                                _inexception = false;
                                //_TSafe_DebugSync(); // NO! Infinite loop; this is probably what caused the problem!
                                _TSafeLockedUiCall(new Action(delegate()
                                {
                                    Outer._DebugFixCursor(); // ...
                                }));
                                _lastcodeexception = null;
                            }
                            else if (dln.StartsWith("Exception is called:"))
                            {
                                _inexception = false;
                                string exmsg = _lastcodeexception;
                                if (!string.IsNullOrEmpty(exmsg))
                                {
                                    _TSafeLockedUiCall(new Action(delegate()
                                    {
                                        string say = "Unhandled exception in code: " + exmsg;
                                        Outer.SetStatus(say, 10000);
                                        Outer.DebugSync(); // ?
                                        Outer.ShowError_unlocked(say);
                                    }));
                                }
                                _lastcodeexception = null;
                            }
                            continue;
                        }
                        if (dln.StartsWith("Unhandled exception generated: (0x"))
                        {
                            _inexception = true;
                            continue;
                        }
                        if (dln == "Function evaluation completed with an exception.")
                        {
                            _inexception = true;
                            string say = dln;
                            if (_tostrings.Count > 0)
                            {
                                say = "[" + _tostrings[0] + "] " + say;
                                _tostrings.RemoveAt(0); // ...
                            }
                            _TSafe_SetStatus(say, 10000);
                            System.Threading.Thread.Sleep(200);
                            continue;
                        }

                        if (!string.IsNullOrEmpty(_sync))
                        {
                            if (-1 != dln.IndexOf(_sync))
                            {
                                _sync = null; // Sync'd!
                            }
                        }
                        else
                        {
                            if (_tostrings.Count > 0 && dln.StartsWith("$result="))
                            {
                                int i = dln.IndexOf(" (0x");
                                if (-1 != i)
                                {
                                    i = dln.IndexOf(") ", i + 4);
                                    if (-1 != i)
                                    {
                                        string str = dln.Substring(i + 2).Trim();
                                        for (i = 0; i < str.Length; i++)
                                        {
                                            if (str[i] == '\\')
                                            {
                                                i++;
                                            }
                                            else if (str[i] == '"')
                                            {
                                                _inprintmultiline = !_inprintmultiline;
                                            }
                                        }
                                        _TSafeLockedUiCall(new Action(delegate()
                                        {
                                            lock (Outer.DbgVars)
                                            {
                                                int nnodes = Outer.DbgVars.Nodes.Count;
                                                string fname = _tostrings[0];
                                                _tostrings.RemoveAt(0); // ...
                                                for (int j = 0; j < nnodes; j++)
                                                {
                                                    string nstr = Outer.DbgVars.Nodes[j].Text;
                                                    if (nstr.Length > fname.Length
                                                        && nstr[fname.Length] == ' '
                                                        && nstr.StartsWith(fname))
                                                    {
                                                        string newtext = nstr + " " + str;
                                                        Outer.DbgVars.Nodes[j].Text = newtext;
                                                        break;
                                                    }
                                                }
                                            }
                                        }));
                                    }
                                }
                                continue;
                            }

                            if (expectwhere)
                            {
                                if (dln.StartsWith("0)"))
                                {
                                    expectwhere = false; // Even if not in current file.
                                    _inwhere = true;

                                    string s = dln.Substring(2);
                                    if (s.Length > 0 && s[0] == '*')
                                    {
                                        s = s.Substring(1);
                                    }
                                    s = s.Trim(); // "MyNamespace.MyClass::MyMethod +0050[native] +0010[IL] in c:\foo\dbg_cmiller~jobs_b3f46409-1c8a-4205-9998-96ec94b11c98.ds:18"

                                    int ipos = s.IndexOf(" in ");
                                    if (-1 == ipos)
                                    {
                                        ipos = s.Length;
                                    }
                                    {
                                        _TSafeLockedUiCall(new Action(delegate()
                                        {
                                            Outer.DbgCallsList.Items.Clear();
                                            Outer.DbgCallsList.Items.Add(s.Substring(0, ipos));
                                        }));

                                        _wherefoundjobfile = false;

                                        if (ipos + 4 <= s.Length)
                                        {
                                            s = s.Substring(ipos + 4); // "c:\foo\dbg_cmiller~jobs_b3f46409-1c8a-4205-9998-96ec94b11c98.ds:18"
                                        }
                                        /*if (s.Length > jobfilename.Length
                                            && s[jobfilename.Length] == ':'
                                            && s.StartsWith(jobfilename, StringComparison.OrdinalIgnoreCase)
                                            )*/
                                        int ilcolon = s.LastIndexOf(':');
                                        int cln;
                                        if (-1 != ilcolon
                                            && int.TryParse(s.Substring(ilcolon + 1), out cln))
                                        {
                                            if (s.Substring(0, ilcolon).EndsWith(".ds", true, null)) // Simple way to step around a jobs file only.
                                            {
                                                _wherefoundjobfile = true;
                                                _TSafeLockedUiCall(new Action(delegate()
                                                {
                                                    if (-1 != Outer.curdebugcline)
                                                    {
                                                        Outer.Doc.Lines[Outer.curdebugcline].DeleteMarker(MARKER_CURDEBUG);
                                                    }
                                                    Outer.curdebugcline = cln;
                                                    Outer.Doc.Lines[Outer.curdebugcline].AddMarker(MARKER_CURDEBUG);
                                                    Outer.Doc.Lines[cln].Goto();
                                                }));
                                            }
                                        }
                                        else
                                        {
                                            /*_TSafeLockedUiCall(new Action(delegate()
                                                {
                                                    if (-1 != Outer.curdebugcline)
                                                    {
                                                        Outer.Doc.Lines[Outer.curdebugcline].DeleteMarker(MARKER_CURDEBUG);
                                                    }
                                                }));*/
                                        }
                                    }
                                    continue;
                                }
                            }
                            if (_inwhere)
                            {
                                int iw = dln.IndexOf(") ");
                                if (iw > 0 && iw <= 3)
                                {
                                    string s = dln.Substring(iw + 2).Trim();
                                    int ipos = s.IndexOf(" in ");
                                    if (-1 == ipos)
                                    {
                                        ipos = s.Length;
                                    }
                                    {
                                        _TSafeLockedUiCall(new Action(delegate()
                                        {
                                            Outer.DbgCallsList.Items.Add(s.Substring(0, ipos));
                                        }));
                                    }
#if DEBUG
                                    if (!_wherefoundjobfile)
                                    {
                                        if (ipos + 4 <= s.Length)
                                        {
                                            s = s.Substring(ipos + 4); // "c:\foo\dbg_cmiller~jobs_b3f46409-1c8a-4205-9998-96ec94b11c98.ds:18"
                                        }
                                        /*if (s.Length > jobfilename.Length
                                            && s[jobfilename.Length] == ':'
                                            && s.StartsWith(jobfilename, StringComparison.OrdinalIgnoreCase)
                                            )*/
                                        int ilcolon = s.LastIndexOf(':');
                                        int cln;
                                        if (-1 != ilcolon
                                            && int.TryParse(s.Substring(ilcolon + 1), out cln))
                                        {
                                            if (s.Substring(0, ilcolon).EndsWith(".ds", true, null)) // Simple way to step around a jobs file only.
                                            {
                                                _wherefoundjobfile = true;
                                                _TSafeLockedUiCall(new Action(delegate()
                                                {
                                                    if (-1 != Outer.curdebugcline)
                                                    {
                                                        Outer.Doc.Lines[Outer.curdebugcline].DeleteMarker(MARKER_CURDEBUG);
                                                    }
                                                    Outer.curdebugcline = cln;
                                                    Outer.Doc.Lines[Outer.curdebugcline].AddMarker(MARKER_CURDEBUG);
                                                    Outer.Doc.Lines[cln].Goto();
                                                }));
                                            }
                                        }
                                    }
#endif
                                    continue;
                                }
                                else
                                {
                                    _inwhere = false;

                                    _Paused();
                                }
                            }

                            if (expectprint)
                            {
                                if (findprinteq(dln) != -1)
                                {
                                    expectprint = false;
                                    _inprint = true;
                                    _inprintmultiline = false;
                                    _TSafeLockedUiCall(new Action(delegate()
                                    {
                                        //Outer.DbgVars.BeginUpdate();
                                        Outer.DbgVars.Nodes.Clear();
                                    }));
                                }
                                else if (-1 != dln.IndexOf("185A546Ef83342FF3A0D"))
                                {
                                    expectprint = false;
                                }
                            }
                            if (_inprint)
                            {
                                int ieq = findprinteq(dln);
                                if (-1 == ieq)
                                {
                                    _inprint = false;
                                    //Console.WriteLine("-"); // ...
                                    /*_TSafe_Call(new Action(delegate()
                                    {
                                        //Outer.DbgVars.EndUpdate();
                                    }));*/
                                }
                                else
                                {
                                    string lname = dln.Substring(0, ieq).Trim();
                                    if ("A4BED95814AD446eB0C5B7B4154907A9" == lname)
                                    {
                                        // It's "dobj" so exclude it.
                                    }
                                    else
                                    {
                                        string lvalue = dln.Substring(ieq + 1);
                                        if (lvalue.Length > 0 && lvalue[0] == '(')
                                        {
                                            int irp = lvalue.IndexOf(')', 1);
                                            if (-1 != irp)
                                            {
                                                lvalue = lvalue.Substring(irp + 1);
                                            }
                                        }
                                        lvalue = lvalue.Trim();
                                        if (lvalue.Length > 0 && lvalue[0] == '<')
                                        {
                                            if (!lvalue.StartsWith("<null>"))
                                            {
                                                //lvalue += " \"" + o.ToString() + "\"";
                                                _rawcmd_unlocked("f System.Object::ToString " + lname);
                                                _tostrings.Add(lname);
                                            }
                                            if (lvalue[lvalue.Length - 1] == '>')
                                            {
                                                string lclassname = lvalue.Substring(1, lvalue.Length - 1 - 1);
                                                {
                                                    int ild = lclassname.LastIndexOf('.');
                                                    if (-1 != ild)
                                                    {
                                                        lclassname = lclassname.Substring(ild + 1);
                                                    }
                                                    lclassname = getfriendlytypename(lclassname);
                                                    lvalue = "<" + lclassname + ">";
                                                }
                                            }
                                        }
                                        else if ("array" == lvalue)
                                        {
                                            //lvalue += " \"" + o.ToString() + "\"";
                                            _rawcmd_unlocked("f System.Object::ToString " + lname);
                                            _tostrings.Add(lname);
                                        }

                                        for (int i = 0; i < lvalue.Length; i++)
                                        {
                                            if (lvalue[i] == '\\')
                                            {
                                                i++;
                                            }
                                            else if (lvalue[i] == '"')
                                            {
                                                _inprintmultiline = !_inprintmultiline;
                                            }
                                        }
                                        //Console.WriteLine("  Variable:  {0} = {1}", lname, lvalue);
                                        _TSafeLockedUiCall(new Action(delegate()
                                        {
                                            Outer.DbgVars.Nodes.Add(lname + " = " + lvalue);
                                        }));
                                    }
                                }
                                continue;
                            }
                        }

                        if (dln.StartsWith("break at #"))
                        {
                            int it = dln.IndexOf('\t');
                            if (-1 != it)
                            {
                                string s = dln.Substring(it + 1);
                                it = s.IndexOf('\t');
                                if (-1 != it)
                                {
                                    /*s = s.Substring(0, it);
                                    if (s.Length > jobfilename.Length
                                        && s[jobfilename.Length] == ':'
                                        && s.StartsWith(jobfilename, StringComparison.OrdinalIgnoreCase)
                                        )*/
                                    {
                                        IsPaused = true;
                                        if (NeedResetBreakpoints)
                                        {
                                            NeedResetBreakpoints = false;
                                            SetBreakpoints();
                                        }
                                        else
                                        {
                                            _TSafe_SetStatus("Breakpoint hit");
                                        }
                                        // int.Parse(s.Substring(jobfilename.Length));
                                        _TSafeLockedUiCall(new Action(delegate()
                                            {
                                                Outer.DebugSync();
                                            }));
                                    }
                                }
                            }
                        }
                        else if (dln == "Process exited.")
                        {
                            _TSafe_SetStatus("Job completed");
                            break;
                        }
                    }
                }
#if DEBUG_SLOW_DEBUGGER
                System.Threading.Thread.Sleep(1000);
#endif
                if (!_ready)
                {
                    return false;
                }
                _stopone();
#if DEBUGform
                df.Close();
#endif
                return true;
            }


            internal static string ExecArgsCode(IList<string> args)
            {
                StringBuilder result = new StringBuilder();
                foreach (string earg in args)
                {
                    if (0 != result.Length)
                    {
                        result.Append(',');
                    }
                    result.Append("@`");
                    result.Append(earg);
                    result.Append('`');
                }
                return result.ToString();
            }


            public static string DurationString(int secs)
            {
                int mins = secs / 60;
                int hrs = mins / 60;
                string srhrs = (mins / 60).ToString().PadLeft(2, '0');
                string srmins = (mins % 60).ToString().PadLeft(2, '0');
                string srsecs = (secs % 60).ToString().PadLeft(2, '0');
                return srhrs + ":" + srmins + ":" + srsecs;
            }


            string jobfilename;

            bool ParseJob(int jobindex)
            {
                return _ProcessJob(jobindex, false);
            }

            bool DebugJob(int jobindex)
            {
                return _ProcessJob(jobindex, true);
            }

            bool _ProcessJob(int jobindex, bool exec)
            {
#if DEBUG
                try
                {
#endif
                _ready = false;

                expectwhere = false;
                expectprint = false;
                _inprintmultiline = false;
                _inprint = false;
                _inwhere = false;
                _sync = null;
                _tostrings.Clear();
                _inexception = false;
                _lastcodeexception = null;

                dbgjobindex = jobindex;

                bool debugging = exec;
                bool localcompile = !Outer.DebuggerProxyEnabled;

                if (null == cfg || jobindex < 0 || jobindex >= cfg.Jobs.Length)
                {
                    if (debugging)
                    {
                        _TSafe_LeaveDebugMode();
                    }
                    return false;
                }

                SourceCode.Job cfgj = cfg.Jobs[jobindex];
                if (curcdata >= cdatalines.Count)
                {
                    throw new Exception("Internal CDATA error (curcdata >= cdatalines.Count)");
                }

                /* // Doesn't seem to be working as expected.
                _TSafe_Call(new Action(delegate()
                    {
                        Outer.Doc.NativeInterface.EnsureVisible(cdatalines[curcdata]);
                        Outer.Doc.NativeInterface.EnsureVisibleEnforcePolicy(cdatalines[curcdata]);
                    }));
                 * */

                string outputguid = Guid.NewGuid().ToString();
                jobfilename = Environment.CurrentDirectory + @"\dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~jobs_" + outputguid + ".ds"; // !
                System.IO.File.WriteAllText(jobfilename, doctext);

                if (debugging)
                {
                    _TSafe_SetStatus("Debugging job " + cfgj.NarrativeName + "...", 10000);
                    //_TSafe_SetStatus("Debugging job " + cfgj.NarrativeName + "..." + (Outer.DebuggerProxyEnabled ? "    [Debug by Proxy]" : ""), 10000);
                }
                else
                {
                    _TSafeLockedUiCall(new Action(
                        delegate
                        {
                            Outer.SetStatus("Parsing job " + cfgj.NarrativeName + "...", 2000);
                            Outer.Update();
                        }));
                }

                IList<string> JobOutputFiles = null;

                bool ShouldSuppressDefaultStandardOutput = false;
                DateTime starttime = DateTime.Now;
                if (0 == string.Compare("mapreduce", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                {
                    if (debugging)
                    {
                        DebugExecutionContext = MySpace.DataMining.DistributedObjects.ExecutionContextType.MAP;
                        if (Outer.DebugSwitch)
                        {
                            Console.WriteLine("[{0}]        [MapReduce: {1}]", starttime.ToString(), cfgj.NarrativeName);
                        }
                    }
                    JobOutputFiles = new string[] { cfgj.IOSettings.DFSOutput };
                    List<string> prettyfilenames = new List<string>();
                    List<int> outputrecordlengths = new List<int>();
                    List<int> inputrecordlengths = new List<int>();
                    bool partialreduce = cfgj.PartialReduce != null;
                    if (debugging)
                    {
                        if (cfgj.IOSettings.DFSOutput.Trim().Length > 0)
                        {
                            string[] dfsoutputs = cfgj.IOSettings.DFSOutput.Split(';');
                            for (int oi = 0; oi < dfsoutputs.Length; oi++)
                            {
                                string dfsoutput = dfsoutputs[oi].Trim();
                                if (dfsoutput.Length == 0)
                                {
                                    continue;
                                }
                                if (dfsoutput.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    dfsoutput = dfsoutput.Substring(6);
                                }
                                int reclen = -1;
                                int ic = dfsoutput.IndexOf('@');
                                if (-1 != ic)
                                {
                                    try
                                    {
                                        reclen = Surrogate.GetRecordSize(dfsoutput.Substring(ic + 1));
                                        dfsoutput = dfsoutput.Substring(0, ic);
                                    }
                                    catch (FormatException e)
                                    {
                                        throw new Exception(string.Format("Error: mapreduce output file record length error: {0} ({1})", dfsoutput, e.Message));
                                    }
                                    catch (OverflowException e)
                                    {
                                        throw new Exception(string.Format("Error: mapreduce output file record length error: {0} ({1})", dfsoutput, e.Message));
                                    }
                                }
                                string reason = "";
                                if (dfs.IsBadFilename(dfsoutput, out reason))
                                {
                                    throw new Exception("Invalid output file: " + reason);
                                }
                                prettyfilenames.Add(dfsoutput);
                                outputrecordlengths.Add(reclen);
                            }
                        }
                        else
                        {
                            prettyfilenames.Add("");
                            outputrecordlengths.Add(-1);
                        }
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = outputrecordlengths.Count > 0 ? outputrecordlengths[0] : -1;

                    dfs dc = LoadDfsConfig();
                    int ZMapBlockFileBufferSize = 0x400 * 400;
                    if (dc.slave.ZMapBlockFileBufferSize != "auto")
                    {
                        ZMapBlockFileBufferSize = int.Parse(dc.slave.ZMapBlockFileBufferSize);
                    }
                    if (debugging)
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = dc.Slaves.SlaveList.Split(';');
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_MaxDGlobals = dc.MaxDGlobals;
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection = cfgj.IOSettings.OutputDirection;
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection_ascending = (0 == string.Compare(cfgj.IOSettings.OutputDirection, "ascending", true));
                    }
                    else
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = new string[] { "localhost" };
                    }
                    List<string> dfsinputpaths = new List<string>();
                    List<string> inputfileswithnodes = new List<string>();
                    List<int> inputnodesoffsets = new List<int>();
                    if (debugging)
                    {
                        List<dfs.DfsFile.FileNode> nodes = new List<dfs.DfsFile.FileNode>();
                        List<string> mapfiles = new List<string>();
                        if (null != cfgj.Delta)
                        {
                            string deltafn = cfgj.Delta.Name;
                            if (deltafn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                deltafn = deltafn.Substring(6);
                            }
                            string reason = "";
                            if (dfs.IsBadFilename(deltafn, out reason))
                            {
                                throw new Exception("Invalid cache name: " + reason);
                            }
                            if (0 != cfgj.Delta.DFSInput.Length)
                            {
                                foreach (string _dp in dc.SplitInputPaths(cfgj.Delta.DFSInput))
                                {
                                    string dp = _dp;
                                    int inreclen;
                                    {
                                        int ic = dp.IndexOf('@');
                                        if (-1 != ic)
                                        {
                                            try
                                            {
                                                inreclen = Surrogate.GetRecordSize(dp.Substring(ic + 1));
                                                dp = dp.Substring(0, ic);
                                            }
                                            catch (FormatException e)
                                            {
                                                throw new Exception(string.Format("Error: cache input record length error: {0} ({1})", dp, e.Message));
                                            }
                                            catch (OverflowException e)
                                            {
                                                throw new Exception(string.Format("Error: cache input record length error: {0} ({1})", dp, e.Message));
                                            }
                                        }
                                        else
                                        {
                                            inreclen = -1;
                                        }
                                    }
                                    mapfiles.Add(dp);
                                    dfs.DfsFile df;
                                    if (inreclen > 0 || inreclen == -2)
                                    {
                                        df = dc.Find(dp, DfsFileTypes.BINARY_RECT);
                                    }
                                    else
                                    {
                                        df = dc.Find(dp);
                                    }
                                    if (null == df)
                                    {
                                        throw new Exception("Mapreduce cache input file not found in DFS: " + dp);
                                        //_TSafe_LeaveDebugMode();
                                        //return false;
                                    }
                                    if (inreclen > 0 || inreclen == -2)
                                    {
                                        if (inreclen != df.RecordLength)
                                        {
                                            throw new Exception(string.Format("Error: cache input file does not have expected record length of {0}: {1}@{2}", inreclen, dp, df.RecordLength));
                                        }
                                    }
                                    if (dp.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                    {
                                        dp = dp.Substring(6);
                                    }
                                    if (df.Nodes.Count > 0)
                                    {
                                        inputfileswithnodes.Add(dp);
                                        inputnodesoffsets.Add(nodes.Count);
                                        inputrecordlengths.Add(inreclen);
                                        nodes.AddRange(df.Nodes);
                                    }
                                }

                            }
                        }
                        
                        {
                            foreach (string _dp in dc.SplitInputPaths(cfgj.IOSettings.DFSInput))
                            {
                                string dp = _dp.Trim();
                                int inreclen;
                                if (0 != dp.Length) // Allow empty entry where input isn't wanted.
                                {
                                    if (dp.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                    {
                                        dp = dp.Substring(6);
                                    }
                                    {
                                        int ic = dp.IndexOf('@');
                                        if (-1 != ic)
                                        {
                                            try
                                            {
                                                inreclen = Surrogate.GetRecordSize(dp.Substring(ic + 1));
                                                dp = dp.Substring(0, ic);
                                            }
                                            catch (FormatException e)
                                            {
                                                throw new Exception(string.Format("Error: map input record length error: {0} ({1})", dp, e.Message));
                                            }
                                            catch (OverflowException e)
                                            {
                                                throw new Exception(string.Format("Error: map input record length error: {0} ({1})", dp, e.Message));
                                            }
                                        }
                                        else
                                        {
                                            inreclen = -1;
                                        }
                                    }
                                    mapfiles.Add(dp);

                                    dfs.DfsFile df;
                                    if (inreclen > 0 || inreclen == -2)
                                    {
                                        df = dc.Find(dp, DfsFileTypes.BINARY_RECT);
                                    }
                                    else
                                    {
                                        df = dc.Find(dp);
                                    }
                                    if (null == df)
                                    {
                                        throw new Exception("Mapreduce input file not found in DFS: " + dp);
                                        //_TSafe_LeaveDebugMode();
                                        //return false;
                                    }
                                    if (inreclen > 0 || inreclen == -2)
                                    {
                                        if (inreclen != df.RecordLength)
                                        {
                                            throw new Exception(string.Format("Error: map input file does not have expected record length of {0}: {1}@{2}", inreclen, dp, df.RecordLength));
                                        }
                                    }
                                    if (df.Nodes.Count > 0)
                                    {
                                        inputfileswithnodes.Add(dp);
                                        inputnodesoffsets.Add(nodes.Count);
                                        inputrecordlengths.Add(inreclen);
                                        nodes.AddRange(df.Nodes);
                                    }
                                }
                            }

                        }

                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = inputrecordlengths.Count > 0 ? inputrecordlengths[0] : -1;

                        dfs.MapNodesToNetworkPaths(nodes, dfsinputpaths);

                        dbgmapinputdfsnodes = nodes;

                    }

                    string outputfilebase = outputguid + ".mapreduce.remote";
                    
                    dbgmapinputscroll = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~inputscroll_" + outputfilebase + ".mis";

                    string reduceinitcode, reducecode, reducefinalcode;

                    if (4 == Outer.cdataspermapreduce)
                    {
                        reduceinitcode = @"
#line " + cdatalines[curcdata + 1].ToString() + " \"" + jobfilename + "\"\r\n"
        + ((cfgj.MapReduce.ReduceInitialize != null) ? cfgj.MapReduce.ReduceInitialize : "");

                        reducecode = @"
#line " + cdatalines[curcdata + 2].ToString() + " \"" + jobfilename + "\"\r\n"
        + cfgj.MapReduce.Reduce;

                        reducefinalcode = @"
#line " + cdatalines[curcdata + 3].ToString() + " \"" + jobfilename + "\"\r\n"
        + ((cfgj.MapReduce.ReduceFinalize != null) ? cfgj.MapReduce.ReduceFinalize : "");
                    }
                    else
                    {
                        reduceinitcode = @"
#line default
public void ReduceInitialize() { }
";

                        reducecode = @"
#line " + cdatalines[curcdata + 1].ToString() + " \"" + jobfilename + "\"\r\n"
        + cfgj.MapReduce.Reduce;

                        reducefinalcode = @"
#line default
public void ReduceFinalize() { }
";
                    }

                    StringBuilder csvoutputrecordlengths = new StringBuilder();
                    {
                        for (int li = 0; li < outputrecordlengths.Count; li++)
                        {
                            if (0 != li)
                            {
                                csvoutputrecordlengths.Append(",");
                            }
                            csvoutputrecordlengths.Append(outputrecordlengths[li]);
                        }
                    }

                    string mrrcode =
MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode
+ (@"
    public const int DSpace_BlockID = 0;
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_BlocksTotalCount = 1;
    public const int DSpace_ProcessCount = DSpace_BlocksTotalCount;
    public const int Qizmt_ProcessCount = DSpace_ProcessCount;

    public const string DSpace_SlaveHost = `localhost`;
    public const string DSpace_MachineHost = DSpace_SlaveHost;
    public const string Qizmt_MachineHost = DSpace_MachineHost;

    public const string DSpace_SlaveIP = `127.0.0.1`;
    public const string DSpace_MachineIP = DSpace_SlaveIP;
    public const string Qizmt_MachineIP = DSpace_MachineIP;

    public static readonly string[] DSpace_ExecArgs = new string[] { " + ExecArgsCode(Outer.ExecArgs) + @" };
    public static readonly string[] Qizmt_ExecArgs = DSpace_ExecArgs;

    public const int DSpace_KeyLength = " + cfgj.IOSettings.KeyLength.ToString() + @";
    public const int Qizmt_KeyLength = DSpace_KeyLength;
    
    public const string DSpace_LogName = @`" + outputfilebase + @".log`;
    public const string Qizmt_LogName = DSpace_LogName;

    public static int DSpace_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }
    public static int Qizmt_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }

    public static int DSpace_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }
    public static int Qizmt_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }


    public static void PadKeyBuffer(List<byte> keybuf)
    {
        if(keybuf.Count > DSpace_KeyLength)
        {
            throw new Exception(`Key too long`);
        }
        PadBytes(keybuf, DSpace_KeyLength, 0);
    }
    public static ByteSlice PrepareAsciiKey(string s)
    {
        List<byte> keybuf = new List<byte>(DSpace_KeyLength);
        Entry.AsciiToBytesAppend(s, keybuf);
        PadKeyBuffer(keybuf);
        return ByteSlice.Prepare(keybuf);
    }
    public static ByteSlice UnpadKeyBuffer(IList<byte> keybuf)
    {
        int len = 0;
        for(; len < keybuf.Count && keybuf[len] != 0; len++)
        {
        }
        return ByteSlice.Prepare(keybuf, 0, len);
    }
    public static ByteSlice UnpadKey(ByteSlice keybuf)
    {
        int len = 0;
        for(; len < keybuf.Length && keybuf[len] != 0; len++)
        {
        }
        return ByteSlice.Prepare(keybuf, 0, len);
    }

    public static void Qizmt_Log(string line) { DSpace_Log(line); }
    public static void DSpace_Log(string line)
    {
        foreach(string s in line.Split('\n'))
        {
            string s2 = s;
            if(s.Length != 0 && s[s.Length - 1] == '\r')
            {
                s2 = s.Substring(0, s.Length - 1);
            }
            Console.WriteLine(`{0}{1}`, `{DC453EE3-9EEE-48d1-B8A8-362F5A02858B}`, s2);
        }
    }

    public static void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
    public static void DSpace_LogResult(string name, bool passed)
    {
        if(passed)
        {
            DSpace_Log(`[PASSED] - ` + name);
        }
        else
        {
            DSpace_Log(`[FAILED] - ` + name);
        }
    }

").Replace('`', '"')
+ @"
internal struct MRRKV
{
    public ByteSlice key;
    public ByteSlice value;
}

static List<byte> CopyBytes(IList<byte> buf, int offset, int length)
{
    List<byte> result = new List<byte>(length);
    for(int i = 0; i < length; i++)
    {
        result.Add(buf[offset + i]);
    }
    return result;
}

static List<byte> FlipAllBits(IList<byte> buf, int offset, int length)
{
    List<byte> result = new List<byte>(length);
    for(int i = 0; i < length; i++)
    {
        result.Add((byte)(~buf[offset + i]));
    }
    return result;
}

class MRRMapper
{
    public static string dbgskiptoreduce = null;

    public class MRRMapOutput: MySpace.DataMining.DistributedObjects.MapOutput
    {
        public List<MRRKV> mrrentries = new List<MRRKV>();
        public List<MRRKV> proutput = new List<MRRKV>();
        int cursize = 0;
        bool partialreduce = " + (partialreduce ? "true" : "false") + @";
        MRRReducer preducer = null;
        byte[] flipbuf = null;
        bool flip = (string.Compare(StaticGlobals.DSpace_OutputDirection, ""descending"", true) == 0);     
        public void PartialReduce()
        {
            if(mrrentries.Count == 0)
            {
                return;
            }
            if(preducer == null)
            {
                preducer = new MRRReducer();
            }
            mrrentries.Sort(new System.Comparison<MRRKV>(mrr_kcmp)); // !               
            MRRReducer.RandomAccessEntries rae = new MRRReducer.RandomAccessEntries();
            rae.mrrentries = mrrentries;
            MRRReducer.RandomAccessOutput raout = new MRRReducer.RandomAccessOutput(-1);
            raout.proutput = proutput; 
            ExecutionContextType oldcontexttype = StaticGlobals.ExecutionContext;
            StaticGlobals.ExecutionContext = ExecutionContextType.PARTIALREDUCE;
            int i = 0;                           
            for(;;)
            {
                int inarow = MRRCountSameKeys(rae.mrrentries, i);
                if(inarow < 1)
                {
                    break;
                }
                rae.mrrentriesstart = i;
                rae.mrrlength = inarow;
                rae.curPos = -1;
                i += inarow; // ! 
                ByteSlice key = rae[0].Key;               
                if(flip)
                {
                    key = ByteSlice.Prepare(FlipAllBits(rae[0].Key.ToBytes(), 0, rae[0].Key.Length));
                }
                preducer.Reduce(key, rae, raout);                
            }
            StaticGlobals.ExecutionContext = oldcontexttype;
            mrrentries.Clear();
            cursize = 0;
        }

        public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
        {
            int addlen = keylength + 4 + valuelength;
            if(partialreduce && (cursize + addlen > " + ZMapBlockFileBufferSize.ToString() + @"))
            {
                PartialReduce();
            }
            MRRKV kv;
            if(string.Compare(StaticGlobals.DSpace_OutputDirection, ""descending"", true) == 0)
            {
                kv.key = ByteSlice.Prepare(FlipAllBits(keybuf, keyoffset, keylength));
            }
            else
            {
                kv.key = ByteSlice.Prepare(CopyBytes(keybuf, keyoffset, keylength));
            }            
            kv.value = ByteSlice.Prepare(CopyBytes(valuebuf, valueoffset, valuelength));
            mrrentries.Add(kv); 
            cursize += addlen;
        }
    }
    ".Replace('`', '"') + MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode + @"
#line " + cdatalines[curcdata + 0].ToString() + " \"" + jobfilename + "\"\r\n"
    + cfgj.MapReduce.Map + (@"
#line default
}

internal class MRRReducerBase
{
    public class RandomAccessEntries: IEnumerator<ByteSlice>,IEnumerable<ByteSlice>
    {
        public List<MRRKV> mrrentries;
        public int mrrentriesstart;
        public int mrrlength;
        RandomAccessEntriesItems items;
        public int curPos=-1;
        
        public int Length
        {
            get
            {
                return mrrlength;
            }
        }

        public void Dispose()
        {                
        }

        public RandomAccessEntriesItems Items
        {
            get
            {
                items.parent = this;
                return items;
            }
        }

        public ByteSlice Current
        {
            get
            {
                 return this[curPos].Value;
            }           
        }

        object System.Collections.IEnumerator.Current
        {
            get
            {
                 return this[curPos].Value;
            }  
        }

        public void Reset()
        {
            curPos = -1;
        }

        public bool MoveNext()
        {
            return (++curPos < mrrlength);
        }

        public IEnumerator<ByteSlice> GetEnumerator()
        {
            return this;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this;
        }

        public MRReduceEntry this[int index]
        {
            get
            {
                if (index < 0 || index > mrrlength)
                {
                    throw new ArgumentOutOfRangeException();
                }
                MRReduceEntry result;
                result._key = mrrentries[mrrentriesstart + index].key;
                result._value = mrrentries[mrrentriesstart + index].value;
                return result;
            }
        }
    
        
        public struct MRReduceEntry
        {
            public ByteSlice _key, _value;
            
            public ByteSlice Key
            {
                get { return _key; }
            }
            public ByteSlice Value
            {
                get { return _value; }
            }
        }

         public struct RandomAccessEntriesItems
        {
            internal RandomAccessEntries parent;

            public ByteSlice this[int index]
            {
                get
                {
                   return parent[index].Value;
                }
            }
        }

    }   

    public class ExplicitCacheAttribute: Attribute
    {
    }
    
    public class KeyRepeatedEnabledAttribute: Attribute
    {
    }

    public class RandomAccessOutput
    {
        public System.IO.Stream _outstm;
        internal RandomAccessOutput[] parentlist = null;
        public List<MRRKV> proutput = null;

        int OutputRecordLength = -1;

        public RandomAccessOutput(int outputrecordlength)
        {
            this.OutputRecordLength = outputrecordlength;
        }
        
	    public void Cache(ByteSlice key, ByteSlice value) { /* drop! */ }

        public void Cache(string key, string value){}

        public void Cache(recordset key, recordset value){}

        public void Cache(mstring key, mstring value){}

        public void Cache(mstring key, recordset value){}

        public RandomAccessOutput GetOutputByIndex(int index)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:GetOutputByIndex(int index) not supported in partial reduce`);
            }
            if(parentlist == null)
            {
                throw new Exception(`Error: RandomAccessOutput.GetOutputByIndex(int index) parent list is null.`);
            }
            if(index < 0 || index >= parentlist.Length)
            {
                throw new Exception(`Error: RandomAccessOutput.GetOutputByIndex(int index) index is out of range.`);
            }
            return parentlist[index];
        }

        public void Add(ByteSlice key, ByteSlice value)
        {
            if(StaticGlobals.ExecutionContext != ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Add(ByteSlice key, ByteSlice value) not supported in reduce`); 
            }
            MRRKV kv;
            kv.key = ByteSlice.Prepare(CopyBytes(key.ToBytes(), 0, key.Length));
            kv.value = ByteSlice.Prepare(CopyBytes(value.ToBytes(), 0, value.Length));
            proutput.Add(kv);
        }

	    public void Add(ByteSlice entry, bool addnewline)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:Add(ByteSlice entry, bool addnewline) not supported in partial reduce`);
            }
            int cnt = entry.Length;
            if(OutputRecordLength == -2)
            {
                Entry.RecordLengthToBytes(cnt, _outstm);
            }
            for(int i = 0; i < cnt; i++)
            {
                _outstm.WriteByte(entry[i]);
            }
            if(addnewline)
            {
                string nl = Environment.NewLine;
                for(int i = 0; i < nl.Length; i++)
                {
                    _outstm.WriteByte((byte)nl[i]);
                }
            }
        }

        void AddCheck(ByteSlice entry, bool addnewline)
        {
            if(OutputRecordLength > 0 || OutputRecordLength == -2)
            {
                if(addnewline)
                {
                    throw new Exception(`Cannot write-line in record rectangular binary mode`);
                }
            }
            else
            {
                if(!addnewline)
                {
                    throw new Exception(`Need write-line`);
                }
            }
            Add(entry, addnewline);
        }
        
        void AddCheck(ByteSlice entry)
        {
            AddCheck(entry, true);
        }

        public void Add(ByteSlice entry)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:Add(ByteSlice entry) not supported in partial reduce`);
            }
            bool addnewline = (OutputRecordLength < 1 && OutputRecordLength != -2);
            Add(entry, addnewline);
        }

        public void WriteLine(ByteSlice entry)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:WriteLine(ByteSlice entry) not supported in partial reduce`);
            }
            AddCheck(entry);
        }
        public void Add(string str)
        {  
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:Add(string str) not supported in partial reduce`);
            } 
            mstring ms;
            ms = mstring.Prepare(str);
            Add(ByteSlice.Prepare(ms));
         }

        public void Add(mstring ms)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:Add(mstring ms) not supported in partial reduce`);
            }
            Add(ms.ToByteSlice());
        }

        public void AddBinary(Blob b)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:AddBinary(Blob b) not supported in partial reduce`);
            }
            string s = b.ToDfsLine();
            ByteSlice bs = ByteSlice.Prepare(s);
            Add(bs);
        }

        public void Add(recordset rs)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:Add(recordset rs) not supported in partial reduce`);
            }
            int rsLength = rs.Length;
            if(OutputRecordLength != -2)
            {
                if(rsLength > OutputRecordLength)
                {
                    throw new Exception(`recordset is larger than record length; got length of ` + rsLength.ToString() + ` when expecting length of ` + OutputRecordLength.ToString());
                }
                else if(rsLength < OutputRecordLength)
                {
                    for(; rsLength < OutputRecordLength; rsLength++)
                    {
                        rs.PutByte(0);
                    }
                    //#if DEBUG
                    if(rs.Length != rsLength)
                    {
                        throw new Exception(`DEBUG: (rs.Length != rsLength)`);
                    }
                    //#endif
                }
                //#if DEBUG
                if(rs.Length != OutputRecordLength)
                {
                    throw new Exception(`DEBUG: (rs.Length != OutputRecordLength)`);
                }
                //#endif
            }            
            AddCheck(rs.ToRow(), false); // WriteRecord(rs.ToRow());
        }

        public void WriteLine(mstring ms)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:WriteLine(mstring ms) not supported in partial reduce`);
            }
            AddCheck(ms.ToByteSlice());
        }

        public void WriteRecord(ByteSlice entry)
        {
            if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
            {
                throw new Exception(`Error:WriteRecord(ByteSlice entry) not supported in partial reduce`);
            }
            if(OutputRecordLength != -2)
            {
                if(OutputRecordLength < 1)
                {
                    throw new Exception(`Cannot write records; not in fixed-length record rectangular binary mode`);
                }
                if(entry.Length != OutputRecordLength) // && OutputRecordLength > 0)
                {
                    throw new Exception(`Record length mismatch; got length of ` + entry.Length.ToString() + ` when expecting length of ` + OutputRecordLength.ToString());
                }
            }            
            AddCheck(entry, false);
        }

    }

    public virtual void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
    {
    }
}

class MRRReducer: MRRReducerBase
{

    internal bool _keyrep = false;
    internal List<byte> _keyrepbuf = null;

    public bool DSpace_KeyRepeated
    {
        get
        {
            if(null == _keyrepbuf)
            {
                throw new InvalidOperationException(`KeyRepeatedEnabled attribute required`);
            }
            return _keyrep;
        }
    }

    public bool Qizmt_KeyRepeated
    {
        get
        {
            return DSpace_KeyRepeated;
        }
    }

    ").Replace('`', '"') + MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode
      + reduceinitcode
      + reducecode
      + reducefinalcode
      + (@"
#line default
}

static int mrr_kcmp(MRRKV x, MRRKV y)
{
    int keylen = x.key.Length;
    for (int i = 0; i != keylen; i++)
    {
        int diff = (int)x.key[i] - (int)y.key[i];
        if (0 != diff)
        {
            return diff;
        }
    }
    return 0;
}

static int MRRCountSameKeys(List<MRRKV> mrrentries, int offset)
{
    if(offset >= mrrentries.Count)
    {
        return 0;
    }
    int count = 1;
    for(; offset + count < mrrentries.Count; count++)
    {
        if(0 != mrr_kcmp(mrrentries[offset + count - 1], mrrentries[offset + count]))
        {
            break;
        }
    }
    return count;
}

const string dbgmapinputscroll = `" + dbgmapinputscroll + @"`;
const bool mapinputscrolling_enabledatstartup = " + (Outer.MapInputScrollControlEnabled ? "true" : "false") + @";
bool partialreduce = " + (partialreduce ? "true" : "false") + @";
public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
{
    //Console.WriteLine(`{FE4877BA-99A2-498b-835C-25860FE90094}{0}`, dfsinput.NumberOfParts);
    
    MRRMapper.MRRMapOutput mapout = new MRRMapper.MRRMapOutput();
    MRRMapper mapper = new  MRRMapper();
    List<byte> curinput = new List<byte>(1000);
    long curinputpartpos = 0; // Position in current part where curinput started.
    long curinputpartindex = 0; // Part index for curinputpartpos.
    int curinputrecordlength = -1;
    List<byte> nextinput = new List<byte>(1000);
    long nextinputpartpos = 0; // Same ideas as curinputpartpos but for nextinput.
    long nextinputpartindex = 0; // Part index for nextinputpartpos.
    int nextinputrecordlength = -1;
    string curinputfn = null;
    string nextinputfn = null;
    bool hascur = false;
    bool hasnext = false;
    StaticGlobals.MapIteration = 0;
    StaticGlobals.ReduceIteration = 0;
    StaticGlobals.DSpace_KeyLength = DSpace_KeyLength;
    StaticGlobals.ExecutionContext = ExecutionContextType.MAP; // !
    StaticGlobals.DSpace_Last = false;
    for(;;)
    {
        if(System.IO.File.Exists(MRRMapper.dbgskiptoreduce))
        {
            try
            {
                System.IO.File.Delete(MRRMapper.dbgskiptoreduce);
            }
            catch
            {
            }
            break;
        }

        bool neednextinput = (StaticGlobals.MapIteration == 0);

        bool dbgmapinputscroll_exists = System.IO.File.Exists(dbgmapinputscroll);
        bool mapinputscrolling = mapinputscrolling_enabledatstartup || dbgmapinputscroll_exists;
        if(dbgmapinputscroll_exists)
        {
            mapinputscrolling = true;
            string[] lines;
            for(int idmisr = 20; ; idmisr--)
            {
                try
                {
                    // <partindex><newline><partposition><newline>
                    lines = System.IO.File.ReadAllLines(dbgmapinputscroll);
                    break;
                }
                catch
                {
                    if (idmisr < 0)
                    {
                        throw;
                    }
                    System.Threading.Thread.Sleep(200);
                    continue;
                }
            }
            for(int idmisr = 20; ; idmisr--)
            {
                try
                {
                    // <partindex><newline><partposition><newline>
                    System.IO.File.WriteAllText(dbgmapinputscroll, `-1\r\n-1\r\n`);
                    break;
                }
                catch
                {
                    if (idmisr < 0)
                    {
                        throw;
                    }
                    System.Threading.Thread.Sleep(200);
                    continue;
                }
            }
            if(lines.Length > 0)
            {
                long partindex;
                if(long.TryParse(lines[0], out partindex)
                    && partindex >= 0)
                {

                    long partposition = 0;
                    if(lines.Length > 1)
                    {
                        if(long.TryParse(lines[1], out partposition)
                            && partposition >= 0)
                        {
                        }
                        else
                        {
                            partposition = 0;
                        }
                    }

                    dfsinput.SeekToPart(partindex, partposition);
                    neednextinput = true;

                }
            }
        }
        
        curinput.Clear();
        if(neednextinput)
        {
            curinputpartindex = dfsinput.CurrentPart;
            curinputpartpos = dfsinput.CurrentPartPosition;
            curinputrecordlength = StaticGlobals.DSpace_InputRecordLength;
            curinputfn = StaticGlobals.DSpace_InputFileName;
            if(StaticGlobals.DSpace_InputRecordLength > 0 || StaticGlobals.DSpace_InputRecordLength == -2)
            {
                hascur = dfsinput.ReadRecordAppend(curinput);                
            }
            else
            {
                hascur = dfsinput.ReadLineAppend(curinput);
            } 
        }
        else
        {
            hascur = hasnext;
            curinputrecordlength = nextinputrecordlength;
            curinputfn = nextinputfn;
            if(hasnext)
            {
                foreach(byte nb in nextinput)
                {
                    curinput.Add(nb);
                }
            }
            curinputpartindex = nextinputpartindex;
            curinputpartpos = nextinputpartpos;
            curinputpartindex = nextinputpartindex;
        }
        if(!hascur)
        {
            break;
        }

        if(mapinputscrolling)
        {
            Console.WriteLine(`{{D46AA97E-0315-4b14-B6B0-3347A597C6EF}}{0}@{1}`, curinputpartindex, curinputpartpos);
            //Console.WriteLine(`{{4FF139F0-C5AA-40b3-8694-E2A305E508AD}}{0}`, Convert.ToBase64String(curinput.ToArray()));
        }

        {
            nextinput.Clear();
            nextinputpartindex = dfsinput.CurrentPart;
            nextinputpartpos = dfsinput.CurrentPartPosition;
            nextinputrecordlength = StaticGlobals.DSpace_InputRecordLength;
            nextinputfn = StaticGlobals.DSpace_InputFileName;
            if(StaticGlobals.DSpace_InputRecordLength > 0 || StaticGlobals.DSpace_InputRecordLength == -2)
            {
                hasnext = dfsinput.ReadRecordAppend(nextinput);
            }
            else
            {
                hasnext = dfsinput.ReadLineAppend(nextinput);
            }
        }

        if(!hasnext)
        {
            StaticGlobals.DSpace_Last = true;
        }
        
        int realinputrecordlength = StaticGlobals.DSpace_InputRecordLength;
        StaticGlobals.DSpace_InputRecordLength = curinputrecordlength;

        string realinputfilename = StaticGlobals.DSpace_InputFileName;
        StaticGlobals.DSpace_InputFileName = curinputfn;

        Stack.ResetStack();
        recordset.ResetBuffers();       
        mapper.Map(ByteSlice.Prepare(curinput), mapout);
        ++StaticGlobals.MapIteration;

        StaticGlobals.DSpace_InputRecordLength = realinputrecordlength;
        StaticGlobals.DSpace_InputFileName = realinputfilename;       
    }
    if (partialreduce)
    {
        mapout.PartialReduce();
    }    
    if(mapout.proutput != null)
    {
        mapout.mrrentries.AddRange(mapout.proutput);
    }       
    mapout.mrrentries.Sort(new System.Comparison<MRRKV>(mrr_kcmp)); // !
    MRRReducer reducer = new MRRReducer();
    {
        System.Reflection.MethodInfo reducemi = reducer.GetType().GetMethod(`Reduce`);
        object[] reduceattribs = reducemi.GetCustomAttributes(false);
        //bool expcache = false;
        for(int i = 0; i < reduceattribs.Length; i++)
        {
            /*if(null != reduceattribs[i] as MRRReducerBase.ExplicitCacheAttribute)
            {
                //expcache = true;
            }*/
            if(null != reduceattribs[i] as MRRReducerBase.KeyRepeatedEnabledAttribute)
            {
                reducer._keyrepbuf = new List<byte>();
            }
        }
    }
    MRRReducer.RandomAccessEntries rae = new MRRReducer.RandomAccessEntries();
    rae.mrrentries = mapout.mrrentries;
    int nreduceouts = " + prettyfilenames.Count.ToString() + @";
    MRRReducer.RandomAccessOutput[] reduceouts = new MRRReducer.RandomAccessOutput[nreduceouts];
    int[] outputrecordlengths = new int[] {" + csvoutputrecordlengths.ToString() + @"};
    for(int oi = 0; oi < nreduceouts; oi++)
    {
        reduceouts[oi] = new MRRReducer.RandomAccessOutput(outputrecordlengths[oi]);
        reduceouts[oi]._outstm = dfsoutput.GetOutputByIndex(oi);
        reduceouts[oi].parentlist = reduceouts;
    }
    MRRReducer.RandomAccessOutput reduceout = reduceouts[0];   
    StaticGlobals.ExecutionContext = ExecutionContextType.REDUCE; // !
    Console.WriteLine(`{B5AB3BA8-7AAB-4c6f-9387-FDCD958C9E94}`); // DebugExecutionContext = REDUCE;
    reducer.ReduceInitialize(); // !
    StaticGlobals.DSpace_Last = false;
    {
        int i = 0;
        bool flip = (string.Compare(StaticGlobals.DSpace_OutputDirection, ""descending"", true) == 0);
        byte[] flipbuf = null;
        for(;;)
        {
            int inarow = MRRCountSameKeys(rae.mrrentries, i);
            if(inarow < 1)
            {
                break;
            }
            rae.mrrentriesstart = i;
            rae.mrrlength = inarow;
            rae.curPos = -1;
            i += inarow; // !
            if(i >= rae.mrrentries.Count)
            {
                StaticGlobals.DSpace_Last = true;
            }
            Stack.ResetStack();
            recordset.ResetBuffers();


            List<byte> _keyrepbuf = reducer._keyrepbuf;
            ByteSlice key = rae[0].Key;
            if(null != _keyrepbuf)
            {
                bool kr = false;
                if(key.Length == _keyrepbuf.Count)
                {
                    bool krsofar = true;
                    for(int kri = 0; kri < key.Length; kri++)
                    {
                        if(key[kri] != _keyrepbuf[kri])
                        {
                            krsofar = false;
                            break;
                        }
                    }
                    kr = krsofar;
                }
                reducer._keyrep = kr;
                _keyrepbuf.Clear();
                key.AppendTo(_keyrepbuf);
            }
            if(flip)
            {
                if(flipbuf == null)
                {
                    flipbuf = new byte[rae[0].Key.Length];
                }
                key = rae[0].Key.FlipAllBits(flipbuf);
            }
            reducer.Reduce(key, rae, reduceout);
            ++StaticGlobals.ReduceIteration;
        }
    }
    reducer.ReduceFinalize(); // !
}

static void Main(string[] args)
{
    MySpace.DataMining.DistributedObjects.StaticGlobals.ExecutionMode = MySpace.DataMining.DistributedObjects.ExecutionMode.DEBUG;
    if(args.Length > 0)
    {
        MRRMapper.dbgskiptoreduce = args[0];
    }
    MySpace.DataMining.DistributedObjects.IRemote A4BED95814AD446eB0C5B7B4154907A9 = new {E43B0DD7-EE4B-4665-873B-A385F98957C3}(); A4BED95814AD446eB0C5B7B4154907A9.OnRemote();
}
").Replace('`', '"');
                    string[] outputfilenames = new string[prettyfilenames.Count];
                    for (int oi = 0; oi < prettyfilenames.Count; oi++)
                    {
                        outputfilenames[oi] = "zd." + Guid.NewGuid().ToString() + ".mapreduce.remote" + ".zd";
                    }
                    DebugRemote dobj = new DebugRemote(cfgj.NarrativeName + "_mapreduce_remote");
                    if (cfgj.OpenCVExtension != null)
                    {
                        dobj.AddOpenCVExtension(32);
                    }
                    if (cfgj.MemCache != null)
                    {
                        dobj.AddMemCacheExtension();
                    }
                    /*if (cfgj.Unsafe != null)
                    {
                        dobj.AddUnsafe();
                    }*/
                    dobj.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                    dobj.InputRecordLengths = inputrecordlengths;
                    
                    dobj.LocalCompile = localcompile;
                    dobj.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                    dobj.OutputRecordLengths = outputrecordlengths;
                    
                    dobj.DfsSampleDistance = 0;
                    if (debugging)
                    {
                        dobj.CompressFileOutput = dc.slave.CompressDfsChunks;
                    }
                    dobj.OutputStartingPoint = 0;
                    if (cfgj.AssemblyReferencesCount > 0)
                    {
                        cfgj.AddAssemblyReferences(dobj.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(dc.Slaves.GetFirstSlave()));
                    }
                    dobj.AddBlock(@"127.0.0.1|" + outputfilebase + @".log|slaveid=0");
                    dobj.RemoteExec(
                        dfsinputpaths,
                        new string[] { "." },
                        outputfilenames,
                        0,
                        mrrcode,
                        cfgj.Usings,
                        inputfileswithnodes,
                        inputnodesoffsets);

                    string fullsource = "using ByteSliceList = RemoteExec.UserRExec.MRRReducerBase.RandomAccessEntries;\n"
                            + "using ReduceOutput = RemoteExec.UserRExec.MRRReducerBase.RandomAccessOutput;\n";

                    fullsource += dobj.RemoteSource.Replace("{E43B0DD7-EE4B-4665-873B-A385F98957C3}", dobj.RemoteClassName);
                    dbgsourcepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~fullsource_" + outputfilebase + ".cs";
                    System.IO.File.WriteAllText(dbgsourcepath, fullsource);
                    dbgexepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~output_" + outputfilebase + ".exe";
                    try
                    {
                        // CompileSource uses a different file name, so the debugger won't step into it.
                        if (!localcompile)
                        {
                            dobj.Open();

                            dobj.CompileSourceRemote(fullsource, true, dbgexepath);

                            dobj.Close();
                        }
                        else
                        {
                            dobj.CompileSource(fullsource, true, dbgexepath);
                        }
                    }
                    catch (BadImageFormatException)
                    {
                    }
                    try
                    {
                        System.IO.File.Copy("MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe.config", dbgexepath + ".config", true);
                    }
                    catch
                    {
                    }
                    // dobj.RemoteClassName is of type IRemote

                    if (debugging)
                    {
                        dbgskiptoreduce = outputfilebase + ".str";
                        _TSafeLockedUiCall(new Action(delegate()
                            {
                                Outer.skipToReduceToolStripMenuItem.Enabled = true;
                                Outer.DebugSkipToReduceStripButton.Enabled = true;
                            }));
                        try
                        {
                            if (!_RunDebugger())
                            {
                                return false;
                            }

                            for (int oi = 0; oi < outputfilenames.Length; oi++)
                            {
                                string zdfn = "/"; // "/" is "none"
                                if (System.IO.File.Exists(outputfilenames[oi]))
                                {
                                    zdfn = outputfilenames[oi];
                                }
                                if (prettyfilenames[oi].Length > 0)
                                {
                                    string filetype = DfsFileTypes.NORMAL;
                                    if (outputrecordlengths[oi] > 0)
                                    {
                                        filetype = DfsFileTypes.BINARY_RECT + "@" + outputrecordlengths[oi].ToString();
                                    }
                                    else if (outputrecordlengths[oi] == -2)
                                    {
                                        filetype = DfsFileTypes.BINARY_RECT + "@?";
                                    }
                                    Console.Write(MySpace.DataMining.DistributedObjects.Exec.Shell(
                                    "DSpace -dfsbind \"" + System.Net.Dns.GetHostName() + "\" \"" + zdfn + "\" \"" + prettyfilenames[oi] + "\" " + filetype
                                    ));
                                }
                            }
                        }
                        finally
                        {
                            _TSafeLockedUiCall(new Action(delegate()
                            {
                                Outer.skipToReduceToolStripMenuItem.Enabled = false;
                                Outer.DebugSkipToReduceStripButton.Enabled = false;
                            }));
                            string str = dbgskiptoreduce;
                            dbgskiptoreduce = null;
                            try
                            {
                                System.IO.File.Delete(str);
                            }
                            catch
                            {
                            }
                            string str2 = dbgmapinputscroll;
                            dbgmapinputscroll = null;
                            try
                            {
                                System.IO.File.Delete(str2);
                            }
                            catch
                            {
                            }
                        }

                    }

                    if (0 == Outer.cdataspermapreduce)
                    {
                        throw new Exception("Fatal error (0 == Outer.cdataspermapreduce)");
                    }
                    curcdata += Outer.cdataspermapreduce;
                }
                else if (0 == string.Compare("remote", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                {
                    if (debugging)
                    {
                        DebugExecutionContext = MySpace.DataMining.DistributedObjects.ExecutionContextType.REMOTE;
                        if (Outer.DebugSwitch)
                        {
                            Console.WriteLine("[{0}]        [Remote: {1}]", starttime.ToString(), cfgj.NarrativeName);
                        }
                    }
                    dfs dc = LoadDfsConfig();
                    if (debugging)
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = dc.Slaves.SlaveList.Split(';');
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_MaxDGlobals = dc.MaxDGlobals;
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection = cfgj.IOSettings.OutputDirection;
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection_ascending = (0 == string.Compare(cfgj.IOSettings.OutputDirection, "ascending", true));
                        if (cfgj.IOSettings.DFS_IO_Multis != null)
                        {
                            cfgj.ExpandDFSIOMultis(dc.Slaves.SlaveList.Split(',', ';').Length, MySpace.DataMining.DistributedObjects.MemoryUtils.NumberOfProcessors);
                        }
                        if (cfgj.IOSettings.DFS_IOs.Length > 1)
                        {
                            if (DialogResult.OK != MessageBox.Show("The Remote section specifies mutliple DFS_IO sections; only the first will be debugged", "Debug", MessageBoxButtons.OKCancel, MessageBoxIcon.Information))
                            {
                                _TSafe_LeaveDebugMode(); // ...
                                return false;
                            }
                        }
                    }
                    else
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = new string[] { "localhost" };
                    }
                    string DFSWriter = "";
                    string DFSReader = "";
                    List<string> prettyfilenames = new List<string>();
                    List<int> outputrecordlengths = new List<int>();
                    if (debugging)
                    {
                        if (cfgj.IOSettings.DFS_IOs.Length != 0)
                        {
                            JobOutputFiles = new string[] { cfgj.IOSettings.DFS_IOs[0].DFSWriter };
                            DFSWriter = cfgj.IOSettings.DFS_IOs[0].DFSWriter.Trim();
                            if (!DFSWriter.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                DFSWriter = "dfs://" + DFSWriter;
                            }
                            DFSReader = cfgj.IOSettings.DFS_IOs[0].DFSReader;
                        }
                        if (DFSWriter.Length > 0)
                        {
                            string[] dfswriters = DFSWriter.Split(';');
                            for (int oi = 0; oi < dfswriters.Length; oi++)
                            {
                                string dfswriter = dfswriters[oi].Trim();
                                if (dfswriter.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    dfswriter = dfswriter.Substring(6);
                                }
                                if (dfswriter.Length == 0)
                                {
                                    continue;
                                }
                                int reclen = -1;
                                int ic = dfswriter.IndexOf('@');
                                if (-1 != ic)
                                {
                                    try
                                    {
                                        reclen = Surrogate.GetRecordSize(dfswriter.Substring(ic + 1));
                                        dfswriter = dfswriter.Substring(0, ic);
                                    }
                                    catch (FormatException e)
                                    {
                                        throw new Exception(string.Format("Error: remote output record length error: {0} ({1})", dfswriter, e.Message));
                                    }
                                    catch (OverflowException e)
                                    {
                                        throw new Exception(string.Format("Error: remote output record length error: {0} ({1})", dfswriter, e.Message));
                                    }
                                }

                                string reason = "";
                                if (dfs.IsBadFilename(dfswriter, out reason))
                                {
                                    throw new Exception("Invalid remote output file: " + reason);
                                }
                                
                                prettyfilenames.Add(dfswriter);
                                outputrecordlengths.Add(reclen);
                            }
                            
                        }
                    }
                    if (prettyfilenames.Count == 0)
                    {
                        prettyfilenames.Add("");
                        outputrecordlengths.Add(-1);
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = outputrecordlengths.Count > 0 ? outputrecordlengths[0] : -1;
                    List<string> dfsinputpaths = new List<string>();
                    List<string> inputfileswithnodes = new List<string>();
                    List<int> inputnodesoffsets = new List<int>();
                    List<int> inputrecordlengths = new List<int>();
                    if (debugging)
                    {
                        List<dfs.DfsFile.FileNode> nodes = new List<dfs.DfsFile.FileNode>();                        
                        IList<string> mapfiles = dc.SplitInputPaths(DFSReader);
                        for (int i = 0; i < mapfiles.Count; i++)
                        {
                            string dp = mapfiles[i].Trim();
                            int inreclen = -1;
                            if (0 != dp.Length) // Allow empty entry where input isn't wanted.
                            {
                                if (dp.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    dp = dp.Substring(6);
                                }
                                {
                                    int ic = dp.IndexOf('@');
                                    if (-1 != ic)
                                    {
                                        try
                                        {
                                            inreclen = Surrogate.GetRecordSize(dp.Substring(ic + 1));
                                            dp = dp.Substring(0, ic);
                                        }
                                        catch (FormatException e)
                                        {
                                            throw new Exception(string.Format("Error: remote input record length error: {0} ({1})", dp, e.Message));
                                        }
                                        catch (OverflowException e)
                                        {
                                            throw new Exception(string.Format("Error: remote input record length error: {0} ({1})", dp, e.Message));
                                        }
                                    }
                                }
                                dfs.DfsFile df;
                                if (inreclen > 0 || inreclen == -2)
                                {
                                    df = dc.Find(dp, DfsFileTypes.BINARY_RECT);
                                    if (null != df && inreclen != df.RecordLength)
                                    {
                                        throw new Exception(string.Format("Error: remote input file does not have expected record length of {0}: {1}@{2}", inreclen, dp, df.RecordLength));
                                    }
                                }
                                else
                                {
                                    df = dc.Find(dp);
                                }
                                if (null == df)
                                {
                                    throw new Exception("Remote input file not found in DFS: " + dp);
                                    //_TSafe_LeaveDebugMode();
                                    //return false;
                                }
                                if (df.Nodes.Count > 0)
                                {
                                    inputfileswithnodes.Add(dp);
                                    inputnodesoffsets.Add(nodes.Count);
                                    inputrecordlengths.Add(inreclen);
                                    nodes.AddRange(df.Nodes);
                                }
                            }
                        }

                        dfs.MapNodesToNetworkPaths(nodes, dfsinputpaths);
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = inputrecordlengths.Count > 0 ? inputrecordlengths[0] : -1;                  
                    string[] outputfilenames = new string[prettyfilenames.Count];
                    {
                        for (int oi = 0; oi < prettyfilenames.Count; oi++)
                        {
                            outputfilenames[oi] = "zd." + Guid.NewGuid().ToString() + ".remote" + ".zd";
                        }
                    }
                    DebugRemote dobj = new DebugRemote(cfgj.NarrativeName + "_remote");
                    if (cfgj.OpenCVExtension != null)
                    {
                        dobj.AddOpenCVExtension(32);
                    }
                    if (cfgj.MemCache != null)
                    {
                        dobj.AddMemCacheExtension();
                    }
                    /*if (cfgj.Unsafe != null)
                    {
                        dobj.AddUnsafe();
                    }*/
                    dobj.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                    dobj.InputRecordLengths = inputrecordlengths;
                    
                    dobj.LocalCompile = localcompile;
                    dobj.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                    dobj.OutputRecordLengths = outputrecordlengths;
                    
                    dobj.DfsSampleDistance = 0;
                    if (debugging)
                    {
                        dobj.CompressFileOutput = dc.slave.CompressDfsChunks;
                    }
                    dobj.OutputStartingPoint = 0;
                    string outputfilebase = outputguid + ".remote";
                    if (cfgj.AssemblyReferencesCount > 0)
                    {
                        cfgj.AddAssemblyReferences(dobj.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(dc.Slaves.GetFirstSlave()));
                    }
                    dobj.AddBlock(@"127.0.0.1|" + outputfilebase + @".log|slaveid=0");
                    string meta = "";
                    if (null != cfgj.IOSettings.DFS_IOs
                        && cfgj.IOSettings.DFS_IOs.Length > 0
                        && null != cfgj.IOSettings.DFS_IOs[0].Meta)
                    {
                        meta = cfgj.IOSettings.DFS_IOs[0].Meta;
                    }
                    string codectx = (@"
    public const int DSpace_BlockID = 0;
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_ProcessCount = DSpace_BlocksTotalCount;
    public const int DSpace_BlocksTotalCount = 1;
    public const int Qizmt_ProcessCount = DSpace_ProcessCount;

    public const string DSpace_SlaveHost = `localhost`;
    public const string DSpace_MachineHost = DSpace_SlaveHost;
    public const string Qizmt_MachineHost = DSpace_MachineHost;

    public const string DSpace_SlaveIP = `127.0.0.1`;
    public const string DSpace_MachineIP = DSpace_SlaveIP;
    public const string Qizmt_MachineIP = DSpace_MachineIP;

    public static readonly string[] DSpace_ExecArgs = new string[] { " + ExecArgsCode(Outer.ExecArgs) + @" };
    public static readonly string[] Qizmt_ExecArgs = DSpace_ExecArgs;
    
    public const string DSpace_OutputFilePath = `" + DFSWriter + @"`; // Includes `dfs://` if in DFS.
    public const string Qizmt_OutputFilePath = DSpace_OutputFilePath;

    public static int DSpace_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }
    public static int Qizmt_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }

    public static int DSpace_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }
    public static int Qizmt_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }

    public const string Qizmt_Meta = @`" + meta + @"`;

    const bool _ShouldDebugShellExec = " + (Outer.ShouldDebugShellExec ? "true" : "false") + @";


    public static int _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145 = 0;

    static string Shell(string line, bool suppresserrors)
    {
        if(_ShouldDebugShellExec)
        {
            bool step = false;
            if(42 == _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145)
            {
                _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145 = 0;
                step = true;
            }
            return MySpace.DataMining.DistributedObjects.Exec.DDShell(line, suppresserrors, step);
        }
        else
        {
            return MySpace.DataMining.DistributedObjects.Exec.Shell(line, suppresserrors);
        }
    }

    static string Shell(string line)
    {
        return Shell(line, false);
    }


    public static void Qizmt_Log(string line) { DSpace_Log(line); }
    public static void DSpace_Log(string line)
    {
        foreach(string s in line.Split('\n'))
        {
            string s2 = s;
            if(s.Length != 0 && s[s.Length - 1] == '\r')
            {
                s2 = s.Substring(0, s.Length - 1);
            }
            Console.WriteLine(`{0}{1}`, `{DC453EE3-9EEE-48d1-B8A8-362F5A02858B}`, s2);
        }
    }

    public static void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
    public static void DSpace_LogResult(string name, bool passed)
    {
        if(passed)
        {
            DSpace_Log(`[PASSED] - ` + name);
        }
        else
        {
            DSpace_Log(`[FAILED] - ` + name);
        }
    }

").Replace('`', '"') + MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode;
                    dobj.RemoteExec(
                        dfsinputpaths,
                        new string[] { "." },
                        outputfilenames,
                        0,
                        codectx + "\r\n#line " + cdatalines[curcdata].ToString() + " \"" + jobfilename + "\"\r\n" + cfgj.Remote + "\r\n#line default\r\nstatic void Main() { MySpace.DataMining.DistributedObjects.StaticGlobals.ExecutionMode = MySpace.DataMining.DistributedObjects.ExecutionMode.DEBUG; MySpace.DataMining.DistributedObjects.IRemote A4BED95814AD446eB0C5B7B4154907A9 = new {E43B0DD7-EE4B-4665-873B-A385F98957C3}(); A4BED95814AD446eB0C5B7B4154907A9.OnRemote(); }",
                        cfgj.Usings,
                        inputfileswithnodes,
                        inputnodesoffsets);
                    string fullsource = dobj.RemoteSource.Replace("{E43B0DD7-EE4B-4665-873B-A385F98957C3}", dobj.RemoteClassName);
                    dbgsourcepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~fullsource_" + outputfilebase + ".cs";
                    System.IO.File.WriteAllText(dbgsourcepath, fullsource);
                    dbgexepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~output_" + outputfilebase + ".exe";
                    try
                    {
                        // CompileSource uses a different file name, so the debugger won't step into it.
                        if (!localcompile)
                        {
                            dobj.Open();

                            dobj.CompileSourceRemote(fullsource, true, dbgexepath);

                            dobj.Close();
                        }
                        else
                        {
                            dobj.CompileSource(fullsource, true, dbgexepath);
                        }
                    }
                    catch (BadImageFormatException)
                    {
                    }
                    try
                    {
                        System.IO.File.Copy("MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe.config", dbgexepath + ".config", true);
                    }
                    catch
                    {
                    }
                    // dobj.RemoteClassName is of type IRemote

                    if (debugging)
                    {
                        if (!_RunDebugger())
                        {
                            return false;
                        }

                        for (int oi = 0; oi < outputfilenames.Length; oi++)
                        {
                            string zdfn = "/"; // "/" is "none"
                            if (System.IO.File.Exists(outputfilenames[oi]))
                            {
                                zdfn = outputfilenames[oi];
                            }

                            if (prettyfilenames[oi].Length > 0)
                            {
                                string filetype = DfsFileTypes.NORMAL;
                                if (outputrecordlengths[oi] > 0)
                                {
                                   filetype = DfsFileTypes.BINARY_RECT + "@" + outputrecordlengths[oi].ToString();
                                }
                                else if (outputrecordlengths[oi] == -2)
                                {
                                    filetype = DfsFileTypes.BINARY_RECT + "@?";
                                }
                                Console.Write(MySpace.DataMining.DistributedObjects.Exec.Shell(
                               "DSpace -dfsbind \"" + System.Net.Dns.GetHostName() + "\" \"" + zdfn + "\" \"" + prettyfilenames[oi] + "\" " + filetype
                               ));
                            }
                        }

                    }

                    curcdata++;
                }
                else if (0 == string.Compare("local", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                {
                    ShouldSuppressDefaultStandardOutput = cfgj.SuppressDefaultOutput != null;
                    if (debugging)
                    {
                        DebugExecutionContext = MySpace.DataMining.DistributedObjects.ExecutionContextType.LOCAL;
                        if (Outer.DebugSwitch)
                        {
                            if (!ShouldSuppressDefaultStandardOutput)
                            {
                                Console.WriteLine("[{0}]        [Local: {1}]", starttime.ToString(), cfgj.NarrativeName);
                            }
                        }
                    }
                    dfs dc = LoadDfsConfig();
                    if (debugging)
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = dc.Slaves.SlaveList.Split(';');
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_MaxDGlobals = dc.MaxDGlobals;
                    }
                    else
                    {
                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = new string[] { "localhost" };
                    }
                    string outputfilename = outputguid + ".local";
                    DebugRemote dobj = new DebugRemote(cfgj.NarrativeName + "_local");
                    if (cfgj.OpenCVExtension != null)
                    {
                        dobj.AddOpenCVExtension(32);
                    }
                    if (cfgj.MemCache != null)
                    {
                        dobj.AddMemCacheExtension();
                    }
                    /*if (cfgj.Unsafe != null)
                    {
                        dobj.AddUnsafe();
                    }*/
                    dobj.LocalCompile = localcompile;
                    if (cfgj.AssemblyReferencesCount > 0)
                    {
                        cfgj.AddAssemblyReferences(dobj.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(dc.Slaves.GetFirstSlave()));
                    }
                    dobj.AddBlock(@"127.0.0.1|" + outputfilename + @".log|slaveid=0");
                    string codectx = (@"
    public const int DSpace_BlockID = 0;
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_BlocksTotalCount = 1;
    public const int DSpace_ProcessCount = DSpace_BlocksTotalCount;
    public const int Qizmt_ProcessCount = DSpace_ProcessCount;

    public const string DSpace_SlaveHost = `localhost`;
    public const string DSpace_MachineHost = DSpace_SlaveHost;
    public const string Qizmt_MachineHost = DSpace_MachineHost;

    public const string DSpace_SlaveIP = `127.0.0.1`;
    public const string DSpace_MachineIP = DSpace_SlaveIP;
    public const string Qizmt_MachineIP = DSpace_MachineIP;

    public static readonly string[] DSpace_ExecArgs = new string[] { " + ExecArgsCode(Outer.ExecArgs) + @" };
    public static readonly string[] Qizmt_ExecArgs = DSpace_ExecArgs;

    public const string DSpace_ExecDir = @`" + System.Environment.CurrentDirectory + @"`;
    public const string Qizmt_ExecDir = DSpace_ExecDir;

    const bool _ShouldDebugShellExec = " + (Outer.ShouldDebugShellExec ? "true" : "false") + @";


    public static int _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145 = 0;

    static string Shell(string line, bool suppresserrors)
    {
        if(_ShouldDebugShellExec)
        {
            bool step = false;
            if(42 == _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145)
            {
                _StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145 = 0;
                step = true;
            }
            return MySpace.DataMining.DistributedObjects.Exec.DDShell(line, suppresserrors, step);
        }
        else
        {
            return MySpace.DataMining.DistributedObjects.Exec.Shell(line, suppresserrors);
        }
    }

    static string Shell(string line)
    {
        return Shell(line, false);
    }


    public static void Qizmt_Log(string line) { DSpace_Log(line); }
    public static void DSpace_Log(string line)
    {
        foreach(string s in line.Split('\n'))
        {
            string s2 = s;
            if(s.Length != 0 && s[s.Length - 1] == '\r')
            {
                s2 = s.Substring(0, s.Length - 1);
            }
            Console.WriteLine(`{0}{1}`, `{DC453EE3-9EEE-48d1-B8A8-362F5A02858B}`, s2);
        }
    }

    public static void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
    public static void DSpace_LogResult(string name, bool passed)
    {
        if(passed)
        {
            DSpace_Log(`[PASSED] - ` + name);
        }
        else
        {
            DSpace_Log(`[FAILED] - ` + name);
        }
    }

").Replace('`', '"') + MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode;
                    dobj.LocalExec(codectx + "\r\n#line " + cdatalines[curcdata].ToString() + " \"" + jobfilename + "\"\r\n" + cfgj.Local + "\r\n#line default\r\nstatic void Main() { MySpace.DataMining.DistributedObjects.StaticGlobals.ExecutionMode = MySpace.DataMining.DistributedObjects.ExecutionMode.DEBUG; MySpace.DataMining.DistributedObjects.IRemote A4BED95814AD446eB0C5B7B4154907A9 = new {E43B0DD7-EE4B-4665-873B-A385F98957C3}" + dobj.RemoteClassName + "(); A4BED95814AD446eB0C5B7B4154907A9.OnRemote(); }", cfgj.Usings);
                    string fullsource = dobj.RemoteSource.Replace("{E43B0DD7-EE4B-4665-873B-A385F98957C3}", dobj.RemoteClassName);
                    dbgsourcepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~fullsource_" + outputfilename + ".cs";
                    System.IO.File.WriteAllText(dbgsourcepath, fullsource);
                    dbgexepath = "dbg_" + Surrogate.SafeTextPath(JobsEdit.RealUserName) + "~output_" + outputfilename + ".exe";
                    try
                    {
                        // CompileSource uses a different file name, so the debugger won't step into it.
                        if (!localcompile)
                        {
                            dobj.Open();

                            dobj.CompileSourceRemote(fullsource, true, dbgexepath);

                            dobj.Close();
                        }
                        else
                        {
                            dobj.CompileSource(fullsource, true, dbgexepath);
                        }
                    }
                    catch (BadImageFormatException)
                    {
                    }
                    try
                    {
                        System.IO.File.Copy("MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe.config", dbgexepath + ".config", true);
                    }
                    catch
                    {
                    }
                    // dobj.RemoteClassName is of type IRemote

                    if (debugging)
                    {
                        if (!_RunDebugger())
                        {
                            return false;
                        }
                    }

                    MySpace.DataMining.DistributedObjects.DGlobalsM.ResetCache();
                    curcdata++;
                }
                if (debugging)
                {
                    if (Outer.DebugSwitch)
                    {
                        if (!ShouldSuppressDefaultStandardOutput)
                        {
                            Console.WriteLine("[{0}]        Done", starttime.ToString());
                            if (JobOutputFiles != null)
                            {
                                foreach (string outfile in JobOutputFiles)
                                {
                                    Console.WriteLine("Output:   {0}", outfile);
                                }
                            }
                            Console.WriteLine("Duration: {0}", DurationString((int)(DateTime.Now - starttime).TotalSeconds));
                        }
                    }
                    _DebugOneJobFinished();
                }

                return true;

#if DEBUG
            }
            catch(Exception e)
            {
                int i33=33+33;
                throw;
            }
#endif
            }

            internal bool expectwhere = false;
            internal bool expectprint = false;
            bool _inprintmultiline = false; // String spanning multiple lines...
            bool _inprint = false;
            bool _inwhere = false;
            bool _wherefoundjobfile = false;
            string _sync = null;
            List<string> _tostrings = new List<string>(15);
            bool _inexception = false;
            string _lastcodeexception = null;

            internal const string DBG_HIDDEN = "516B8AC36BF2A6F44D";


            // Checks if the debugger is busy waiting on responses (or isbusy).
            // Doesn't care if paused.
            public bool IsWaiting
            {
                get
                {
                    return
                        false
                        || expectwhere
                        || expectprint
                        || _inprintmultiline
                        || _inprint
                        || _inwhere
                        || _inexception
                        ;
                }
            }


            internal bool DoSyncCommand()
            {
                lock (DebuggerSyncLock)
                {
                    if (!string.IsNullOrEmpty(_sync))
                    {
                        return false;
                    }
                    _sync = DBG_HIDDEN + "sync~" + Guid.NewGuid().ToString().Substring(0, 8);
                    _rawcmd_unlocked(_sync);
                    return true;
                }
            }


            internal void _rawcmd_unlocked(string cmd)
            {
#if DEBUGout
                Console.WriteLine("  DEBUG OUT:  {0}", cmd);
#endif
                din.WriteLine(cmd);
                din.Flush();
            }


            int findprinteq(string s)
            {
                for (int i = 0; i < s.Length; i++)
                {
                    if (char.IsWhiteSpace(s[i]))
                    {
                        return -1;
                    }
                    if ('=' == s[i])
                    {
                        return i;
                    }
                }
                return -1;
            }


            void _TSafe_DebugSync()
            {
                Outer.DebugSync(); // It's already safe.
            }

            void _TSafe_SetStatus(string s, int ms)
            {
                _TSafeLockedUiCall(new Action(delegate()
                {
                    Outer.SetStatus(s, ms);
                }));
            }

            void _TSafe_SetStatus(string s)
            {
                _TSafe_SetStatus(s, 2000);
            }


        }

        DebugThread dbg;

        object UiSyncLock = null; // "UiSyncLock{524F30F0-1852-4d6d-ABD4-424C0FB89619}";


        protected static dfs LoadDfsConfig()
        {
            using (dfs.LockDfsMutex())
            {
                return dfs.ReadDfsConfig_unlocked(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
            }
        }


        void _DebugFixCursor()
        {
            if (!dbg.expectwhere)
            {
                if (IsDebuggerReady)
                {
                    lock (dbg.DebuggerSyncLock)
                    {
                        dbg.expectwhere = true;
                        dbg._rawcmd_unlocked("where");
                        dbg._rawcmd_unlocked(DebugThread.DBG_HIDDEN); // Non-where to set _inwhere=false
                    }
                }
            }
        }

        void _DebugFixLocals()
        {
            if (!dbg.expectprint)
            {
                if (IsDebuggerReady)
                {
                    lock (dbg.DebuggerSyncLock)
                    {
                        dbg.expectprint = true;
                        dbg._rawcmd_unlocked("print");
                        dbg._rawcmd_unlocked(DebugThread.DBG_HIDDEN + "185A546Ef83342FF3A0D");
                    }
                }
            }
        }


        void DebugSync()
        {
            if (IsDebuggerReady)
            {
                //if (dbg.DoSyncCommand())
                {
                    _DebugFixLocals();
                    _DebugFixCursor();
                }
            }
        }


        void DebugStop()
        {
            if (IsDebugging && dbg != null) // Not necessarily "ready" due to debug error/LeavingDebugMode.
            {
                dbg.Stop();
            }
            LeaveDebugMode();
        }


        void StepInto()
        {
            if (IsDebuggerReady)
            {
                if (dbg.IsWaiting)
                {
                    return;
                }
                bool StepInShellFunc = false;
                if (ShouldDebugShellExec)
                {
                    lock (UiSyncLock)
                    {
                        if (curdebugcline >= 0 && curdebugcline < Doc.Lines.Count)
                        {
                            string linetext = Doc.Lines[curdebugcline].Text;
                            if (-1 != linetext.IndexOf("Shell"))
                            {
                                if (System.Text.RegularExpressions.Regex.IsMatch(linetext, @"\bShell\b\("))
                                {
                                    StepInShellFunc = true;
                                }
                            }
                        }
                    }
                }
                {
                    if (StepInShellFunc)
                    {
                        lock (dbg.DebuggerSyncLock)
                        {
                            dbg._rawcmd_unlocked("set RemoteExec.UserRExec._StepIntoShellFunc_49DF97FC066447a5A325CF24C9B10145 42");
                            StepOver();
                        }
                    }
                    else
                    {
                        lock (dbg.DebuggerSyncLock)
                        {
                            dbg._stepping = true;
                            dbg._rawcmd_unlocked("si");
                        }
                        DebugSync();
                    }
                }
            }
        }


        void StepOver()
        {
            if (IsDebuggerReady)
            {
                if (dbg.IsWaiting)
                {
                    return;
                }
                lock (dbg.DebuggerSyncLock)
                {
                    dbg._stepping = true;
                    dbg._rawcmd_unlocked("so");
                }
                DebugSync();
            }
        }


        void StepOut()
        {
            if (IsDebuggerReady)
            {
                lock (dbg.DebuggerSyncLock)
                {
                    if (dbg.IsWaiting)
                    {
                        return;
                    }
                    dbg._stepping = true;
                    dbg._rawcmd_unlocked("out");
                }
                DebugSync();
            }
        }


        // Note: SkipToReduce should only be called if in Map!
        void SkipToReduce()
        {
            if (IsDebuggerReady)
            {
                if (!string.IsNullOrEmpty(dbg.dbgskiptoreduce))
                {
                    skipToReduceToolStripMenuItem.Enabled = false;
                    DebugSkipToReduceStripButton.Enabled = false;
                    SetStatus("Skipping to reduce phase after current iteration of map.", 10000);

                    lock (dbg.DebuggerSyncLock)
                    {
                        System.IO.File.WriteAllText(dbg.dbgskiptoreduce, "Skip to Reduce");
                    }

                    //DebugResume(); // Didn't want it.

                }
            }
        }


        void DebugResume()
        {
            if (IsDebugging)
            {
                if (dbg.IsPaused)
                {
                    dbg.Resume();
                }
                RemoveCurDebugMarker();
            }
        }


        int cdataspermapreduce = 0;

        void DebugDoc()
        {
            _ProcessDoc(true);
        }

        void ParseDoc()
        {
            _ProcessDoc(false);
        }

        void _ProcessDoc(bool exec)
        {
            bool debugging = exec;

            if (IsDebugging)
            {
                if (dbg.IsWaiting)
                {
                    return;
                }
                DebugResume();
                return;
            }

            if (debugging)
            {
                if (Doc.Modified)
                {
                    switch (MessageBox.Show(this, PrettyFile + " has been modified,\r\nwould you like to save it before debugging?", "Save before Debug?", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question))
                    {
                        case DialogResult.Yes:
                            DoSave();
                            break;
                        case DialogResult.No:
                            break;
                        default:
                            return; // Cancel!
                    }
                }
            }

            string ds = Doc.Text;
            try
            {
                if (debugging)
                {
                    EnterDebugMode();
                }

                SourceCode cfg = SourceCode.LoadXml(SourceCodeXPathSets, ds);

                int nummapreducejobs = 0;
                int numotherjobs = 0;

                List<int> cdatalines; // (immediately after the opening cdata)
                {
                    int jobnum = 0;
                    foreach (SourceCode.Job cfgj in cfg.Jobs)
                    {
                        if (null == cfgj.IOSettings)
                        {
                            throw new JobNumException(jobnum, "IOSettings required in job");
                        }

                        jobnum++;
                        if (0 == string.Compare("mapreduce", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                        {
                            nummapreducejobs++;
                        }
                        else if (0 == string.Compare("remote", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                        {
                            numotherjobs++;
                        }
                        /*else if (0 == string.Compare("remotemulti", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                        {
                            numotherjobs++;
                        }*/
                        else if (0 == string.Compare("local", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                        {
                            numotherjobs++;
                        }
                        else
                        {
                            throw new JobNumException(jobnum, "Job type '" + cfgj.IOSettings.JobType + "' not supported by debugger");
                        }
                    }
                    cdatalines = new List<int>();
                    {
                        int lastpos = 0;
                        string xml = Doc.Text;
                        for (; ; )
                        {
                            //int curpos = xml.IndexOf("<![CDATA[", lastpos);
                            int curpos = FindCData(xml, lastpos);
                            if (-1 == curpos)
                            {
                                break;
                            }
                            curpos += 9;
                            if (Doc.CharAt(curpos - 1) != '[')
                            {
                                Console.Error.WriteLine("Code misalignment");
                            }
                            cdatalines.Add(Doc.Lines.FromPosition(curpos).Number);
                            lastpos = curpos;
                        }
                        if (cdatalines.Count == numotherjobs + nummapreducejobs * 2)
                        {
                            cdataspermapreduce = 2;
                        }
                        else if (cdatalines.Count == numotherjobs + nummapreducejobs * 4)
                        {
                            cdataspermapreduce = 4;
                        }
                        else
                        {
                            throw new Exception("Invalid number of CDATA sections");
                        }
                    }
                }

                dbg = new DebugThread(this, UiSyncLock);
                dbg.cdatalines = cdatalines;
                dbg.doctext = Doc.Text;
                dbg.cfg = cfg;
                if (debugging)
                {
                    dbg.Start();
                }
                else
                {
                    dbg.ParseAll();
                }
            }
            catch (JobNumException e)
            {
                /*if (e.JobNum > 0)
                {
                    // Select the Job tag...
                    Doc.NativeInterface.SetTargetStart(0);
                    Doc.NativeInterface.SetTargetEnd(Doc.TextLength);
                    for (int ij = 0; ij < e.JobNum; ij++)
                    {
                        for (; ; )
                        {
                            Doc.NativeInterface.SetSearchFlags();
                            int pos = Doc.NativeInterface.SearchInTarget(0, "<\w*Job\b");
                            if(style is xml ...
                        }
                    }
                }
                 * */
                lock (UiSyncLock)
                {
                    if (debugging)
                    {
                        _CannotDebug_unlocked(e);
                    }
                    else
                    {
                        _ParseError_unlocked(e);
                    }
                }
                //LeaveDebugMode(); // Need to let them read the error..
            }
            catch (Exception e)
            {
                lock (UiSyncLock)
                {
                    if (debugging)
                    {
                        _CannotDebug_unlocked(e);
                    }
                    else
                    {
                        _ParseError_unlocked(e);
                    }
                }
                //LeaveDebugMode(); // Need to let them read the error..
            }
        }


        static bool offsetstartswith(string str, int stroffset, string swith)
        {
            int rlen = str.Length - stroffset;
            if (rlen >= swith.Length)
            {
                if (0 == string.Compare(str, stroffset, swith, 0, swith.Length))
                {
                    return true;
                }
            }
            return false;
        }


        static int FindCData(string xml, int start)
        {
            for (int i = start; i < xml.Length; i++)
            {
                if (offsetstartswith(xml, i, "<![CDATA["))
                {
                    return i;
                }
                if (offsetstartswith(xml, i, "<!--"))
                {
                    i = xml.IndexOf("-->", i + 2);
                    if (-1 == i)
                    {
                        break;
                    }
                    i += 3;
                }
            }
            return -1;
        }


        internal void _CannotDebug_unlocked(Exception e)
        {
            if ((Control.ModifierKeys & (Keys.Control | Keys.Shift)) == (Keys.Control | Keys.Shift))
            {
                Console.WriteLine("Cannot debug: " + e.ToString());
            }
            try
            {
                LogOutputToFile("Cannot debug: " + e.ToString());
            }
            catch
            {
            }

            LeavingDebugMode();

            string errdispmsg = "Cannot debug:\r\n\r\n" + e.Message;
            if (e.InnerException != null)
            {
                errdispmsg += "\r\n\r\n" + e.InnerException.Message;
            }
            //MessageBox.Show(errdispmsg, "Debug", MessageBoxButtons.OK, MessageBoxIcon.Error);
            ShowError_unlocked(errdispmsg);
        }


        internal void _ParseError_unlocked(Exception e)
        {
            if ((Control.ModifierKeys & (Keys.Control | Keys.Shift)) == (Keys.Control | Keys.Shift))
            {
                Console.WriteLine("Parse error: " + e.ToString());
            }

            //LeavingDebugMode();

            string errdispmsg = "Parse error:\r\n\r\n" + e.Message;
            if (e.InnerException != null)
            {
                errdispmsg += "\r\n\r\n" + e.InnerException.Message;
            }
            try
            {
                if (-1 != errdispmsg.IndexOf("CompileSource code compile error: ")
                    && -1 != errdispmsg.IndexOf("dbg_")
                    )
                {
                    int ic = errdispmsg.IndexOf(".ds(");
                    if (-1 != ic)
                    {
                        {
                            int icc = errdispmsg.IndexOf(',', ic);
                            if (-1 != icc)
                            {
                                string sln = errdispmsg.Substring(ic + 4, icc - (ic + 4));
                                int ln;
                                if (int.TryParse(sln, out ln))
                                {
                                    if (ln >= 0 && ln < Doc.Lines.Count)
                                    {
                                        Doc.Lines[ln].Select();
                                    }
                                }
                            }
                        }
                        {
                            int ierr = errdispmsg.IndexOf("error", ic, StringComparison.OrdinalIgnoreCase);
                            if (-1 != ierr)
                            {
                                string serr = errdispmsg.Substring(ierr);
                                int ieol = serr.IndexOfAny(new char[] { '\r', '\n' });
                                if (-1 != ieol)
                                {
                                    serr = serr.Substring(0, ieol);
                                }
                                SetStatus(serr, 1000 * 10);
                            }
                        }
                    }
                }
            }
            catch
            {
#if DEBUG
                if (!System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Launch();
                }
#endif
            }
            {
                //WriteOutputRed_unlocked(errdispmsg); // This output isn't even visible.
                //BottomTabs.SelectedIndex = 1;
                MessageBox.Show(this, errdispmsg, "Parse Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }


        public void WriteOutputRed_unlocked(string line)
        {
#if DBGOUTPUT_RTB
            DbgOutput.SelectionColor = Color.Red;
#endif
            DbgOutput.AppendText(line + "\r\n");
#if DBGOUTPUT_RTB
            DbgOutput.SelectionColor = Color.Black;
#endif
            DbgOutput.ScrollToCaret();
        }


        public void ShowError_unlocked(string line)
        {
            WriteOutputRed_unlocked(line);
            BottomTabs.SelectedIndex = 1;
            MessageBox.Show(this, line, "Debug Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }


        public class JobNumException : Exception
        {
            public int JobNum;

            public JobNumException(int jobnum, string msg)
                : base("[Job#" + jobnum.ToString() + "] " + msg)
            {
                JobNum = jobnum;
            }
        }


        private void startDebuggingToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DebugDoc();
        }

        private void stopDebuggingToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DebugStop();
        }


        [DllImport("user32.dll", CharSet = CharSet.Auto)]
        static extern uint SendMessage(IntPtr hwnd, int msg, IntPtr wparam, string lparam);

        [DllImport("user32.dll")]
        static extern uint SendMessage(IntPtr hwnd, int msg, IntPtr wparam, IntPtr lparam);

        const int EM_REPLACESEL = 194;


        [DllImport("user32.dll")]
        static extern bool ShowWindow(IntPtr hWnd, int nShowCmd);

        const int SW_SHOWNOACTIVATE = 4;


        [DllImport("user32.dll")]
        static extern IntPtr WindowFromPoint(POINT Point);

        [StructLayout(LayoutKind.Sequential)]
        struct POINT
        {
            public int x, y;
        }


        public bool IsDebuggerReady
        {
            get
            {
                return IsDebugging
                    && dbg != null // !
                    && dbg.IsReady;
            }
        }


        private void stepOverToolStripMenuItem_Click(object sender, EventArgs e)
        {
            StepOver();
        }

        private void stepIntoToolStripMenuItem_Click(object sender, EventArgs e)
        {
            StepInto();
        }

        private void stepOutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            StepOut();
        }

        private void skipToReduceToolStripMenuItem_Click(object sender, EventArgs e)
        {
            SkipToReduce();
        }

        private void DbgInput_KeyDown(object sender, KeyEventArgs e)
        {
            if (IsDebuggerReady && dbg.IsPaused)
            {
                if (e.KeyCode == Keys.Return)
                {
                    string cmd = DbgInput.Text;
                    lock (UiSyncLock)
                    {
                        DbgOutput.AppendText(cmd + "\r\n");
                        DbgInput.Clear();
                    }
                    lock (dbg.DebuggerSyncLock)
                    {
                        dbg._rawcmd_unlocked(cmd);
                    }
                    e.Handled = true;
                }
            }
        }

        private void clearToolStripMenuItem_Click(object sender, EventArgs e)
        {
            lock (UiSyncLock)
            {
                DbgOutput.Clear();
            }
        }

        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DoSave();
        }

        private void toggleBreakpointToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (dbg == null || !dbg.IsWaiting)
            {
                lock (UiSyncLock)
                {
                    ScintillaNet.Line curline = Doc.Lines.Current;
                    if (curline == null || curline.Number < 0)
                    {
                        return;
                    }
                    bool addbreakpoint = !IsBreakpoint(curline.Number);
                    if (addbreakpoint)
                    {
                        curline.AddMarker(MARKER_BREAKPOINT);
                    }
                    else
                    {
                        curline.DeleteMarker(MARKER_BREAKPOINT);
                    }
                }
                if (IsDebuggerReady)
                {
                    dbg.SetBreakpoints();
                }
            }
        }

        private void deleteAllBreakpointsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (dbg == null || !dbg.IsWaiting)
            {
                lock (UiSyncLock)
                {
                    for (int ln = 0; ln < Doc.Lines.Count; ln++)
                    {
                        if (IsBreakpoint(ln))
                        {
                            Doc.Lines[ln].DeleteMarker(MARKER_BREAKPOINT);
                        }
                    }
                }
                if (IsDebuggerReady)
                {
                    dbg.SetBreakpoints();
                }
            }
        }

        private void editToolStripMenuItem_Click(object sender, EventArgs e)
        {

        }

        private void replaceToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Doc.FindReplace.ShowReplace();
        }

        private void SaveStripButton_Click(object sender, EventArgs e)
        {
            if (!DoSave())
            {
                return;
            }
        }

        private void FindStripButton_Click(object sender, EventArgs e)
        {
            Doc.FindReplace.ShowFind();
        }

        private void DebugStripButton_Click(object sender, EventArgs e)
        {
            DebugDoc();
        }

        private void DebugStopStripButton_Click(object sender, EventArgs e)
        {
            DebugStop();
        }

        private void DebugStepIntoStripButton_Click(object sender, EventArgs e)
        {
            StepInto();
        }

        private void DebugStepOverStripButton_Click(object sender, EventArgs e)
        {
            StepOver();
        }

        private void DebugStepOutStripButton_Click(object sender, EventArgs e)
        {
            StepOut();
        }

        private void DebugSkipToReduceStripButton_Click(object sender, EventArgs e)
        {
            SkipToReduce();
        }

        private void showToolbarToolStripMenuItem_Click(object sender, EventArgs e)
        {
            bool on;
            if (!IsDebugging) // Not debugging, just editor...
            {
                on = !config.ToolbarEnabledEditor;
                config.ToolbarEnabledEditor = on;
            }
            else //if (IsDebugging)
            {
                on = !config.ToolbarEnabledDebugger;
                config.ToolbarEnabledDebugger = on;
            }
            SaveConfig();
            EnableToolbar(on);
        }

        private void debugShellExecToolStripMenuItem_Click(object sender, EventArgs e)
        {
            debugShellExecToolStripMenuItem.Checked = !debugShellExecToolStripMenuItem.Checked;
        }

        private void mapInputScrollControlToolStripMenuItem_Click(object sender, EventArgs e)
        {
            bool on = !config.MapInputScrollControl;
            config.MapInputScrollControl = on;
            SaveConfig();
            EnableMapInputScrollControl(on);
        }


        public bool ShouldDebugShellExec
        {
            get
            {
                return !DebuggerProxyEnabled
                    && debugShellExecToolStripMenuItem.Checked;
            }
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
            catch(Exception aex)
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

        private void parseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            ParseDoc();
        }

        private void ParseStripButton_Click(object sender, EventArgs e)
        {
            ParseDoc();
        }


        static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            try
            {
                while (sofar < len)
                {
                    int xread = stm.Read(buf, sofar, len - sofar);
                    if (xread <= 0)
                    {
                        break;
                    }
                    sofar += xread;
                }
                return sofar;
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("StreamRead* Requested " + len.ToString() + " bytes; " + e.Message, e);
            }
        }

        static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            if (len != StreamReadLoop(stm, buf, len))
            {
                throw new System.IO.IOException("Unable to read from stream");
            }
        }


    }


    public class UserConfig
    {
        public bool AutoComplete = true;
        public bool SyntaxColor = true;

        [System.Xml.Serialization.XmlIgnore]
        public bool DebugByProxyEnabled
        {
            get
            {
                return "true" == DebugByProxyTag;
            }
            set
            {
                DebugByProxyTag = value ? "true" : "false";
            }
        }

        [System.Xml.Serialization.XmlElement("DebugByProxy")]
#if SAVE_DEBUG_BY_PROXY
#else
        [System.Xml.Serialization.XmlIgnore]
#endif
        public string DebugByProxyTag = "";

        public bool ToolbarEnabledEditor = true;
        public bool ToolbarEnabledDebugger = true;
        public bool MapInputScrollControl = false;

    }


    // Stubbed for Type info only.
    internal class InternalCode
    {
        public class RandomAccessOutput
        {
            public void Cache(MySpace.DataMining.DistributedObjects.ByteSlice key, MySpace.DataMining.DistributedObjects.ByteSlice value)
            {
                throw new NotImplementedException();
            }

            public void Cache(string key, string value)
            {
               
                throw new NotImplementedException();
            }

            public void Cache(MySpace.DataMining.DistributedObjects.recordset key, MySpace.DataMining.DistributedObjects.recordset value)
            {
                throw new NotImplementedException();
            }

            public void Cache(MySpace.DataMining.DistributedObjects.mstring key, MySpace.DataMining.DistributedObjects.mstring value)
            {
                throw new NotImplementedException();
            }

            public void Cache(MySpace.DataMining.DistributedObjects.mstring key, MySpace.DataMining.DistributedObjects.recordset value)
            {
               
                throw new NotImplementedException();
            }

            public void Add(MySpace.DataMining.DistributedObjects.ByteSlice entry)
            {
                throw new NotImplementedException();
            }
            public void Add(string str)
            {
               
                throw new NotImplementedException();
            }
            public void Add(MySpace.DataMining.DistributedObjects.recordset rs)
            {
                throw new NotImplementedException();
            }

            public void AddBinary(MySpace.DataMining.DistributedObjects.Blob blob)
            {
                throw new NotImplementedException();
            }

            public void Add(MySpace.DataMining.DistributedObjects.mstring entry)
            {
                throw new NotImplementedException();
            }

            public void WriteLine(MySpace.DataMining.DistributedObjects.mstring entry)
            {
                throw new NotImplementedException();
            }

            public RandomAccessOutput GetOutputByIndex(int index)
            {
                throw new NotImplementedException();
            }
        }

        public class ReduceOutput : InternalCode.RandomAccessOutput
        {
        }        

        public class RandomAccessEntries
        {
            public int Length
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public MySpace.DataMining.DistributedObjects.ReduceEntry this[int index]
            {
                get
                {
                    throw new NotImplementedException();
                }
            }   

            public RandomAccessEntriesItems Items
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public MySpace.DataMining.DistributedObjects.ByteSlice Current
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public bool MoveNext()
            {
                throw new NotImplementedException();
            }

            public struct RandomAccessEntriesItems
            {
                public MySpace.DataMining.DistributedObjects.ByteSlice this[int index]
                {
                    get
                    {
                        throw new NotImplementedException();
                    }
                }
            }
        }

        public class ByteSliceList : InternalCode.RandomAccessEntries
        {
        }

        public abstract class RemoteInputStream : System.IO.Stream
        {
            public IList<string> GetSequenceInputs()
            {
                throw new NotImplementedException();
            }

            public void CreateSequenceFile(string name)
            {
                throw new NotImplementedException();
            }

            public bool EndOfStream
            {
                get
                {
                    throw new NotImplementedException();
                }
            }

            public bool ReadLineAppend(List<byte> line)
            {
                throw new NotImplementedException();
            }

            public bool ReadLineAppend(StringBuilder line)
            {
                throw new NotImplementedException();
            }

            public bool ReadLineAppend(System.IO.Stream line)
            {
                throw new NotImplementedException();
            }

            public bool ReadBinary(ref MySpace.DataMining.DistributedObjects.Blob blob)
            {
                throw new NotImplementedException();
            }
        }

        public abstract class RemoteOutputStream : System.IO.Stream
        {
            public void WriteLine(IList<byte> line)
            {
                throw new NotImplementedException();
            }

            public void WriteLine(StringBuilder line)
            {
                throw new NotImplementedException();
            }

            public void WriteLine(string line)
            {
                throw new NotImplementedException();
            }

            public void WriteRecord(IList<byte> record)
            {
                throw new NotImplementedException();
            }

            public void WriteBinary(MySpace.DataMining.DistributedObjects.Blob blob)
            {
                throw new NotImplementedException();
            }

            public RemoteOutputStream GetOutputByIndex(int index)
            {
                throw new NotImplementedException();
            }

        }

    }


    internal class TemplateJobs
    {
        internal static readonly string SourceCode = null;

        static TemplateJobs()
        {
            string email = "";
            string custodian = "";

            try
            {
                DirectorySearcher ds = new DirectorySearcher();
                ds.Filter = string.Format("(&(samAccountName={0})(objectCategory=person)(objectClass=user))", JobsEdit.RealUserName);
                SearchResult sr = ds.FindOne();

                if (sr != null)
                {
                    foreach (string var in sr.Properties.PropertyNames)
                    {
                        if (var == "mail")
                            email = sr.Properties[var][0].ToString();

                        if (var == "displayname")
                            custodian = sr.Properties[var][0].ToString();
                    }
                }
            }
            catch (Exception)
            {
            }           

            SourceCode = (@"<SourceCode>
  <Jobs>
    <Job Name=`~~~NAME~~~_Preprocessing` Custodian=`" + custodian + @"` Email=`" + email + @"`>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del ~~~NAME~~~_Input.txt`);
                Shell(@`Qizmt del ~~~NAME~~~_Output.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`~~~NAME~~~_CreateSampleData` Custodian=`" + custodian + @"` Email=`" + email + @"` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://~~~NAME~~~_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`1498,The Last Supper,100.45,374000000`);
                dfsoutput.WriteLine(`1503,Mona Lisa,4.75,600000000`);
                dfsoutput.WriteLine(`1501,Study for a portrait of Isabella d'Este,1.5,100000000`);
                dfsoutput.WriteLine(`1501,Study of horse,1.5,100000000`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`~~~NAME~~~` Custodian=`" + custodian + @"` Email=`" + email + @"`>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,double</KeyLength>
        <DFSInput>dfs://~~~NAME~~~_Input.txt</DFSInput>
        <DFSOutput>dfs://~~~NAME~~~_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                mstring sLine = mstring.Prepare(line);
                int year = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                double size = sLine.NextItemToDouble(',');
                long pixel = sLine.NextItemToLong(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(year);
                rKey.PutDouble(size);
                
                recordset rValue = recordset.Prepare();
                rValue.PutString(title);
                rValue.PutLong(pixel);
                
                output.Add(rKey, rValue);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int year = rKey.GetInt();
                double size = rKey.GetDouble();
                
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    mstring title = rValue.GetString();
                    long pixel = rValue.GetLong();
                    
                    mstring sLine = mstring.Prepare(year);
                    sLine = sLine.AppendM(',')
                        .AppendM(title)
                        .AppendM(',')
                        .AppendM(size)
                        .AppendM(',')
                        .AppendM(pixel);
                    
                    output.Add(sLine);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`~~~NAME~~~_DisplayInputData` Custodian=`" + custodian + @"` Email=`" + email + @"` Description=`Display input data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://~~~NAME~~~_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display input.
                Qizmt_Log(`Input:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`~~~NAME~~~_DisplayOutputData` Custodian=`" + custodian + @"` Email=`" + email + @"` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://~~~NAME~~~_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"');
        }       
    }

    public class JobsEdit
    {

        public static string RealUserName = Environment.UserName;


        public static void RunJobsEditor(string ActualFile, string PrettyFile)
        {
            RunJobsEditor(ActualFile, PrettyFile, false);
        }

        public static void RunJobsEditor(string ActualFile, string PrettyFile, bool IsReadOnly)
        {
            JobsEditor editor = new JobsEditor(ActualFile, PrettyFile, IsReadOnly);
            editor.ShowDialog();
            editor.Dispose();
        }

        public static void RunJobsEditor(string ActualFile, string PrettyFile, Dictionary<string, object> settings)
        {
            JobsEditor editor = new JobsEditor(ActualFile, PrettyFile, settings);
            editor.ShowDialog();
            editor.Dispose();
        }

    }


}
