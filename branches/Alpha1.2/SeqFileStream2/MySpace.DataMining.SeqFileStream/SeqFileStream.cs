/**************************************************************************************
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
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MySpace.DataMining.SeqFileStream
{
    public class SeqFileStreamReader : FileStream
    {
        // Adds newline character after each file if missing.
        public bool LineMode
        {
            get
            {
                return lmode;
            }

            set
            {
                lmode = value;
            }
        }


        public bool IsSequenceFiles
        {
            get
            {
                return isseqfiles;
            }
        }


        public SeqFileStreamReader(string fn, FileMode mode)
            : base(fn, mode)
        {
            curfilename = fn;
            _init();
        }

        public SeqFileStreamReader(string fn, FileMode mode, FileAccess access)
            : base(fn, mode, access)
        {
            curfilename = fn;
            _init();
        }

        public SeqFileStreamReader(string fn, FileMode mode, FileAccess access, FileShare share, int bufferSize)
            : base(fn, mode, access, share, bufferSize)
        {
            curfilename = fn;
            bufsz = bufferSize;
            _init();
        }


        public bool GZipZdFiles = false;


        public override int ReadByte()
        {
            if (isseqfiles)
            {
                int x = -1;
                while (!seqend)
                {
                    x = seqread.ReadByte();
                    if (-1 != x)
                    {
                        prevbyte = (byte)x;
                        break;
                    }
                    _movenextseq();
                    if (lmode)
                    {
                        if ('\n' != prevbyte)
                        {
                            prevbyte = (byte)'\n';
                            return '\n';
                        }
                    }
                }
                return x;
            }
            else
            {
                if (mlbufpos < mlbuflen)
                {
                    return mlbuf[mlbufpos++];
                }
                return base.ReadByte();
            }
        }


        public override int Read(
            byte[] array,
            int offset,
            int count
            )
        {
            int written = 0;
            if (isseqfiles)
            {
                while (written < count)
                {
                    if (seqend)
                    {
                        /*if (0 == written)
                        {
                            throw new EndOfStreamException("End of file; end of all files in sequences has been reached");
                        }*/
                        break;
                    }
                    written += seqread.Read(array, offset + written, count - written);
                    if (0 != written)
                    {
                        prevbyte = array[offset + written - 1];
                    }
                    if (0 == written)
                    {
                        _movenextseq();
                        if (lmode)
                        {
                            if ('\n' != prevbyte)
                            {
                                prevbyte = (byte)'\n';
                                array[offset + written] = (byte)'\n';
                                written++;
                            }
                        }
                        continue;
                    }
                    break;
                }
            }
            else
            {
                if (mlbufpos < mlbuflen)
                {
                    for (; mlbufpos < mlbuflen && written < count; mlbufpos++, written++)
                    {
                        array[offset + written] = mlbuf[mlbufpos];
                    }
                }
                if (written < count)
                {
                    written += base.Read(array, offset + written, count - written);
                }
            }
            return written;
        }


        public override void Close()
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
                curfilename = null;
                curseqfile = null;
            }

            base.Close();
        }


        public override IAsyncResult BeginRead(
            byte[] array,
            int offset,
            int numBytes,
            AsyncCallback userCallback,
            Object stateObject
            )
        {
            throw new Exception("BeginRead not supported on SeqFileStreamReader");
        }


        public override long Seek(
            long offset,
            SeekOrigin origin
            )
        {
            throw new Exception("Seek not supported on SeqFileStreamReader");
        }


        void nowrite()
        {
            throw new Exception("Writing not supported on SeqFileStreamReader");
        }

        public override IAsyncResult BeginWrite(
            byte[] array,
            int offset,
            int numBytes,
            AsyncCallback userCallback,
            Object stateObject
            )
        {
            nowrite();
            return null; // ...
        }

        public override void WriteByte(
            byte value
            )
        {
            nowrite();
        }

        public override void Write(
            byte[] array,
            int offset,
            int count
            )
        {
            nowrite();
        }


        public override bool CanRead
        {
            get
            {
                if (isseqfiles)
                {
                    if (seqend)
                    {
                        return false;
                    }
                    return true;
                }
                else
                {
                    if (mlbufpos < mlbuflen)
                    {
                        return true;
                    }
                    return base.CanRead;
                }
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }


        public override long Length
        {
            get
            {
                throw new Exception("Unable to get length");
            }
        }


        bool thisnextstreamreturned = false;

        public Stream GetNextStream(out string filename)
        {
            if (isseqfiles)
            {
                filename = curfilename;
                Stream result = curseqfile;
                _skipseq();
                return result;
            }
            else
            {
                if (!thisnextstreamreturned)
                {
                    filename = curfilename;
                    thisnextstreamreturned = true;
                    return this;
                }
                filename = null;
                return null;
            }
        }

        public Stream GetNextStream()
        {
            string filename;
            return GetNextStream(out filename);
        }


        void _init()
        {
            mlbuf = new byte[MAGIC_LINE.Length + 1]; // +1 for eol.
            for (; ; )
            {
                int x = base.ReadByte();
                if (-1 == x)
                {
                    break;
                }
                if (mlbuflen == MAGIC_LINE.Length)
                {
                    if ('\r' == (char)x || '\n' == (char)x)
                    {
                        _seqinit();
                        break;
                    }
                }
                mlbuf[mlbuflen++] = (byte)x;
                if ((char)x != MAGIC_LINE[mlbuflen - 1])
                {
                    break;
                }
            }
        }


        void _seqinit()
        {
            mlbuflen = 0;
            mlbuf = null;
            seqfilenames = new List<string>();
            using (StreamReader sf = new StreamReader(this))
            {
                for (; ; )
                {
                    string s = sf.ReadLine();
                    if (null == s)
                    {
                        break;
                    }
                    if (0 != s.Length)
                    {
                        seqfilenames.Add(s);
                    }
                }
            }
            isseqfiles = true;

            if (0 != seqfilenames.Count)
            {
                curfilename = seqfilenames[0];
                curseqfile = GetStreamFromSeqFileName(seqfilenames[0]);
            }
        }


        void _skipseq()
        {
            if (null != curseqfile)
            {
                curseqfile = null;
                seqfilenames[whichseqreader] = null;

                if (whichseqreader + 1 <= seqfilenames.Count)
                {
                    whichseqreader++;
                    if (whichseqreader < seqfilenames.Count)
                    {
                        curfilename = seqfilenames[whichseqreader];
                        curseqfile = GetStreamFromSeqFileName(seqfilenames[whichseqreader]);
                    }
                    //return true;
                }
                //return false;
            }
        }

        void _movenextseq()
        {
            if (null != curseqfile)
            {
                curseqfile.Close();
                _skipseq();
            }
        }


        // Big endian.
        static Int32 _BytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        static Int32 _BytesToInt(IList<byte> x)
        {
            return _BytesToInt(x, 0);
        }


        protected System.IO.Stream GetStreamFromSeqFileName(string fn)
        {
            Stream stm;
            if (bufsz > 0)
            {
                stm = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read, bufsz);
            }
            else
            {
                stm = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read);
            }

            if (fn.Length > 3
                && 0 == string.Compare(".gz", fn.Substring(fn.Length - 3)))
            {
                stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
            }
            else if (fn.Length > 3
                && 0 == string.Compare(".zd", fn.Substring(fn.Length - 3)))
            {
                if (GZipZdFiles)
                {
                    stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
                }
                byte[] buf = new byte[32];
                buf[0] = (byte)stm.ReadByte();
                buf[1] = (byte)stm.ReadByte();
                buf[2] = (byte)stm.ReadByte();
                int x = stm.ReadByte();
                if (-1 != x)
                {
                    buf[3] = (byte)x;
                    int headerlen = _BytesToInt(buf);
                    for (int i = 0; i < headerlen; i++)
                    {
                        stm.ReadByte();
                    }
                }
            }

            return stm;
        }


        const string MAGIC_LINE = "*sequence*";
        byte[] mlbuf;
        int mlbufpos = 0;
        int mlbuflen = 0;
        bool isseqfiles = false;
        Stream curseqfile;
        List<string> seqfilenames;
        string curfilename;
        int whichseqreader = 0;
        int bufsz = -1;
        bool lmode = true;
        byte prevbyte = 0;

        Stream seqread
        {
            get
            {
                return curseqfile;
            }
        }

        bool seqend
        {
            get
            {
                return whichseqreader >= seqfilenames.Count;
            }
        }
    }

}
