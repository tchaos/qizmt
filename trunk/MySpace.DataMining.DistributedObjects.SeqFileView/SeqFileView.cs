using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace MySpace.DataMining.DistributedObjects.SeqFileView
{
    public partial class SeqFileViewForm : Form
    {
        public SeqFileViewForm()
        {
            InitializeComponent();

            rowbuf = new byte[16];
        }


        int fontheight;
        int fontwidth;
        MySpace.DataMining.SeqFileStream.SeqFileStreamReader sstm;
        byte[] rowbuf;
        uint loc = 0; // Location; depends on mode.


        public enum Mode
        {
            GENERIC,
            LINE,
            INT32
        }

        Mode mode = Mode.GENERIC;


        public void SetMode(Mode mode)
        {
            this.mode = mode;
            LabelAscii.Left = 463;
            switch (mode)
            {
                case Mode.GENERIC:
                    loc = 0;
                    LabelLocation.Text = "Offset:";
                    LabelHex.Text = "16 Bytes in Hex:";
                    LabelAscii.Text = "ASCII:";
                    break;
                case Mode.LINE:
                    loc = 1;
                    LabelLocation.Text = "Line:";
                    LabelHex.Text = "Line in Hex:";
                    LabelAscii.Left = 745;
                    LabelAscii.Text = "ASCII:";
                    break;
                case Mode.INT32:
                    loc = 0;
                    LabelLocation.Text = "Offset:";
                    LabelHex.Text = "8 Bytes (2 Int32s) in Hex:";
                    LabelAscii.Text = "Int32 to Decimal:";
                    break;
            }
        }


        public void OpenFile(string filename)
        {
            try
            {
                ButtonDown.Enabled = false;

                sstm = new MySpace.DataMining.SeqFileStream.SeqFileStreamReader(filename, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Delete, 0x100000 * 20);

                this.Text = (new System.IO.FileInfo(filename)).Name + " - SeqFileView";

                LoadFileTop();

                ButtonDown.Enabled = true;
            }
            catch (Exception e)
            {
                MessageBox.Show("Problem loading file:\r\n" + e.ToString());
            }
        }


        public void AskOpenFile(string defaultfile)
        {
            SelOpenDialog sod = new SelOpenDialog();
            //if (null != defaultfile)
            {
                sod.TextBoxFile.Text = defaultfile;
            }
            if (DialogResult.OK == sod.ShowDialog())
            {
                if (sod.RadioLineMode.Checked)
                {
                    SetMode(Mode.LINE);
                }
                else if (sod.RadioZblockMode.Checked)
                {
                    SetMode(Mode.INT32);
                }
                else
                {
                    SetMode(Mode.GENERIC);
                }
                OpenFile(sod.TextBoxFile.Text);
            }
            sod.Dispose();
            ButtonDown.Select();
        }

        public void AskOpenFile()
        {
            AskOpenFile("");
        }


        private void ButtonOpen_Click(object sender, EventArgs e)
        {
            AskOpenFile();
        }


        private void ButtonDown_Click(object sender, EventArgs e)
        {
            LoadNextPage();
        }


        void LoadFileTop()
        {
            Display.Clear();
            Display.HideSelection = true;

            Graphics g = this.CreateGraphics();
            SizeF sf = g.MeasureString("W@jlpRop", Display.Font);
            fontheight = (int)Math.Ceiling(sf.Height);
            fontwidth = (int)Math.Ceiling(sf.Width / 8);
            g.Dispose();

            LoadNextPage();
        }


        int FillRowBuf(int offset, int length)
        {
            int index = offset;
            int stop = offset + length;
            while (index < stop)
            {
                int x = sstm.Read(rowbuf, index, length - index);
                if (x <= 0)
                {
                    break;
                }
                index += x;
            }
            return index - offset;
        }


        Color getasciicolor()
        {
            Color result = SystemColors.ControlText;
            result = Color.FromArgb(result.R, result.G, ((int)result.B + 0xBB) / 2);
            return result;
        }

        Color getloccolor()
        {
            return SystemColors.GrayText;
        }

        Color gethexcolor()
        {
            return SystemColors.ControlText;
        }


        void InsertHexAndAscii(int len)
        {
            Display.SelectionColor = gethexcolor();
            for (int bi = 0; bi != len; bi++)
            {
                Display.AppendText(rowbuf[bi].ToString("X2"));
                Display.AppendText(" ");
                if (8 - 1 == (bi % 8) && bi + 1 != len) // Every 8, but not if at end.
                {
                    Display.SelectionColor = SystemColors.GrayText;
                    Display.AppendText("- ");
                    Display.SelectionColor = gethexcolor();
                }
            }

            Display.AppendText("    ");
            Color asciicolor = getasciicolor();
            Display.SelectionColor = asciicolor;
            for (int bi = 0; bi != len; bi++)
            {
                if (rowbuf[bi] < (byte)' ' || rowbuf[bi] >= 0x80)
                {
                    Display.SelectionColor = SystemColors.GrayText;
                    Display.AppendText(".");
                    Display.SelectionColor = asciicolor;
                }
                else
                {
                    Display.AppendText(((char)rowbuf[bi]).ToString());
                }
            }
        }


        void InsertLineHexAndAscii(int len, int hexwidth)
        {
            Display.SelectionColor = gethexcolor();
            int nprinted = 0;
            for (int bi = 0; bi != len; bi++)
            {
                Display.AppendText(rowbuf[bi].ToString("X2"));
                nprinted += 2;
                Display.AppendText(" ");
                nprinted++;
            }

            //Display.AppendText("    ");
            if (nprinted < hexwidth) // Should always pass.
            {
                Display.AppendText(BunchOSpaces.Substring(0, hexwidth - nprinted));
            }

            Color asciicolor = getasciicolor();
            Display.SelectionColor = asciicolor;
            for (int bi = 0; bi != len; bi++)
            {
                if (rowbuf[bi] < (byte)' ' || rowbuf[bi] >= 0x80)
                {
                    Display.SelectionColor = SystemColors.GrayText;
                    Display.AppendText(".");
                    Display.SelectionColor = asciicolor;
                }
                else
                {
                    Display.AppendText(((char)rowbuf[bi]).ToString());
                }
            }
        }


        void LoadNextGenericPage()
        {
            int nrows = Display.Height / fontheight;

            int tlen = Display.TextLength;
            Display.SelectionStart = tlen;

            for (int i = 0; i != nrows; i++)
            {
                int len = FillRowBuf(0, 16);

                if (0 != tlen)
                {
                    Display.AppendText("\r\n");
                }
                tlen += len;

                Display.SelectionColor = getloccolor();
                Display.AppendText(loc.ToString("X4"));

                Display.AppendText("     ");

                InsertHexAndAscii(len);

                loc += (uint)len;

                if (len < rowbuf.Length)
                {
                    ButtonDown.Enabled = false;
                    //break;
                }
            }
        }


        void LoadNextLinePage()
        {
            //int maxbytesperline = 1100 / fontwidth; // About 1000 horizontal pixels.
            const int maxdispcharsperline = 90;
            const int maxbytesperline = maxdispcharsperline / 2 - (maxdispcharsperline / 2) / 3; // 30 (bytes in file)
            int nrows = Display.Height / fontheight;

            if (rowbuf.Length < maxbytesperline)
            {
                rowbuf = new byte[maxbytesperline];
            }

            int tlen = Display.TextLength;
            Display.SelectionStart = tlen;

            bool done = false;
            for (int i = 0; i != nrows; i++)
            {
                int offset = 0;
                for(;;)
                {
                    if (offset >= maxbytesperline)
                    {
                        // Line too long; consume up to end of line or EOF.
                        for (; ; )
                        {
                            int y = sstm.ReadByte();
                            if (-1 == y || (byte)'\n' == y)
                            {
                                break;
                            }
                        }
                        break;
                    }

                    int x = sstm.ReadByte();
                    if (-1 == x)
                    {
                        ButtonDown.Enabled = false;
                        done = true;
                        break;
                    }
                    rowbuf[offset++] = (byte)x;
                    if ((byte)'\n' == x)
                    {
                        break;
                    }
                }
                if (0 == offset)
                {
                    break;
                }

                if (0 != tlen)
                {
                    Display.AppendText("\r\n");
                }
                tlen += offset; // ...

                Display.SelectionColor = getloccolor();
                Display.AppendText(loc.ToString("d4"));

                Display.AppendText("     ");

                InsertLineHexAndAscii(offset, maxdispcharsperline + 4); // + padding.

                //if (!ButtonDown.Enabled)
                if (done)
                {
                    break;
                }

                loc++;
            }
        }


        public static int BytesToInt(byte[] x, int offset)
        {
            int result = 0;
            result |= x[offset + 0];
            result |= (int)x[offset + 1] << 8;
            result |= (int)x[offset + 2] << 16;
            result |= (int)x[offset + 3] << 24;
            return result;
        }

        public static int BytesToInt(byte[] x)
        {
            return BytesToInt(x, 0);
        }


        void LoadNextInt32Page()
        {
            int nrows = Display.Height / fontheight;

            int tlen = Display.TextLength;
            Display.SelectionStart = tlen;

            for (int i = 0; i != nrows; i++)
            {
                int len = FillRowBuf(0, 8);
                if (8 != len)
                {
                    break;
                }

                if (0 != tlen)
                {
                    Display.AppendText("\r\n");
                }
                tlen += len;

                Display.SelectionColor = getloccolor();
                Display.AppendText(loc.ToString("X4"));

                Display.AppendText("     ");

                Display.SelectionColor = gethexcolor();
                for (int bi = 0; bi != len; bi++)
                {
                    Display.AppendText(rowbuf[bi].ToString("X2"));
                    Display.AppendText(" ");
                    if (4 - 1 == bi)
                    {
                        Display.SelectionColor = SystemColors.GrayText;
                        Display.AppendText("- ");
                        Display.SelectionColor = gethexcolor();
                    }
                }

                Display.AppendText("                            ");
                Color inttext = SystemColors.ControlText;
                inttext = Color.FromArgb(((int)inttext.R + 0xBB) / 2, inttext.G, inttext.B);
                Display.SelectionColor = inttext;
                Display.AppendText(BytesToInt(rowbuf, 0).ToString());
                Display.SelectionColor = SystemColors.GrayText;
                Display.AppendText(",");
                Display.SelectionColor = inttext;
                Display.AppendText(BytesToInt(rowbuf, 4).ToString());

                loc += (uint)len;
            }
        }


        public void LoadNextPage()
        {
            switch (mode)
            {
                case Mode.LINE:
                    LoadNextLinePage();
                    break;
                case Mode.INT32:
                    LoadNextInt32Page();
                    break;
                default:
                    LoadNextGenericPage();
                    break;
            }
            Display.SelectionStart = Display.GetCharIndexFromPosition(new Point(Display.Bottom, Display.Height));
            Display.ScrollToCaret();
        }


        static string BunchOSpaces = "                                                                                                                                ";


        CharactersForm cf = null;


        public void ShowCharactersForm()
        {
            if (null == cf)
            {
                cf = new CharactersForm();
            }
            cf.Show();
            cf.BringToFront();
        }

        public void ToggleCharactersForm()
        {
            if (null == cf || !cf.Visible)
            {
                ShowCharactersForm();
            }
            else
            {
                cf.Hide();
            }
        }


        private void LabelAnsiShow_MouseHover(object sender, EventArgs e)
        {
            //ShowCharactersForm();
        }

        private void LabelAnsiShow_MouseDown(object sender, MouseEventArgs e)
        {
            ToggleCharactersForm();
        }

    }

}

