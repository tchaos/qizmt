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
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{
    public class Remote : ArrayComboList
    {

        public Remote(string objectname)
            : base(objectname, 1)
        {
        }


        public new void AddBlock(string sUserBlockInfo)
        {
            base.AddBlock("1", sUserBlockInfo);
        }


        public override char GetDistObjectTypeChar()
        {
            return 'R';
        }


        void _RemoteExec(byte[] dlldata, string classname, string pluginsource)
        {

            foreach (SlaveInfo _slave in dslaves)
            {
                BufSlaveInfo slave = (BufSlaveInfo)_slave;
                slave.nstm.WriteByte((byte)'R'); // 'R' for remoteexec
                if (null == classname)
                {
                    XContent.SendXContent(slave.nstm, "");
                }
                else
                {
                    XContent.SendXContent(slave.nstm, classname);
                }
                XContent.SendXContent(slave.nstm, pluginsource);
                XContent.SendXContent(slave.nstm, dlldata);
            }

            // "Join"...
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'.'); // Ping.
                if ((int)',' != slave.nstm.ReadByte())
                {
                    throw new Exception("RemoteExec sync error (pong)");
                }
            }
        }

        public virtual void RemoteExecFullSource(string code, string classname)
        {
            byte[] dlldata = CompilePluginSource(code);
            _RemoteExec(dlldata, classname, code);
        }


        public int RemoteExec(IList<string> inputdfsnodes, IList<string> outputdfsdirs, string outputfilename, long outputbasefilesize, string code, string[] usings)
        {
            return RemoteExec(inputdfsnodes, outputdfsdirs, outputfilename, outputbasefilesize, code, usings, null);
        }

        // Returns number of files written.
        // if outputbasefilesize>0 writes to multiple files (-1 for no max)
        // outputfilename contains %n to be replaced with current file number, starting at 0.
        // If multiple slaves, returns total files of all slaves.
        // inputdfsnodes must be accessible by the remote machine. Can be star-delimited failover names (starnames).
        // outputbasepath can contain %n for multiple outputs.
        // outputdfsdirs is a list of network directories of where to put the output files; each chunk is written to: outputdfsdirs[i % outputdfsdirs.Count]
        public int RemoteExec(IList<string> inputdfsnodes, IList<string> outputdfsdirs, string outputfilename, long outputbasefilesize, string code, string[] usings, IList<long> appendsizes)
        {
            if (outputbasefilesize <= 0)
            {
                outputbasefilesize = long.MaxValue;
            }

            StringBuilder infs = new StringBuilder();
            {
                for (int i = 0; i < inputdfsnodes.Count; i++)
                {
                    if (i != 0)
                    {
                        infs.Append(", ");
                    }
                    infs.Append("@`");
                    infs.Append(inputdfsnodes[i]);
                    infs.Append("`");
                }
            }

            StringBuilder outds = new StringBuilder();
            if(null != outputdfsdirs)
            {
                for (int i = 0; i < outputdfsdirs.Count; i++)
                {
                    if (i != 0)
                    {
                        outds.Append(", ");
                    }
                    outds.Append("@`");
                    outds.Append(outputdfsdirs[i]);
                    outds.Append("`");
                }
            }

            string susings = "";
            if (usings != null)
            {
                foreach(string nm in usings)
                {
                    susings += "using " + nm + ";" + Environment.NewLine;
                }                
            }

            RemoteExecFullSource(
(@"using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;

" + susings + @"

namespace RemoteExec
{
    public class RemoteInputStream : System.IO.Stream, MySpace.DataMining.CollaborativeFilteringObjects3.ISequenceInput
    {
        string[] infs = new string [] { " + infs.ToString() + @" };
        int onf = -1;

        const int _CookTimeout = " + CookTimeout.ToString() + @";
        const int _CookRetries = " + CookRetries.ToString() + @";

        const int InputRecordLength = " + InputRecordLength.ToString()  + @";
        
        public RemoteInputStream(UserRExec r)
        {
            this.r = r;

            StaticGlobals.DSpace_InputRecordLength = " + InputRecordLength.ToString() + @";
        }


        public IList<string> GetSequenceInputs()
        {
            return infs;
        }


        public void CreateSequenceFile(string name)
        {
            using(System.IO.StreamWriter sf = System.IO.File.CreateText(name))
            {
                sf.WriteLine(`*sequence*`);
                for(int i = 0; i < infs.Length; i++)
                {
                    int istar = infs[i].IndexOf('*');
                    if(-1 != istar)
                    {
                        sf.WriteLine(infs[i].Substring(0, istar));
                    }
                    else
                    {
                        sf.WriteLine(infs[i]);
                    }
                }
            }
        }


        public bool EndOfStream
        {
            get
            {
                return seqend;
            }
        }


        public bool ReadRecordAppend(List<byte> record)
        {
            if(InputRecordLength < 1)
            {
                throw new Exception(`Cannot read records; not in fixed-length record rectangular binary mode`);
            }
            
            int remain = InputRecordLength;
            while(remain > 0)
            {
                int ib = this.ReadByte();
                if(-1 == ib)
                {
                    return false;
                }
                record.Add((byte)ib);
                remain--;
            }
            return true;
        }

        public bool ReadRecordAppend(System.IO.Stream record)
        {
            if(InputRecordLength < 1)
            {
                throw new Exception(`Cannot read records; not in fixed-length record rectangular binary mode`);
            }

            int remain = InputRecordLength;
            while(remain > 0)
            {
                int ib = this.ReadByte();
                if(-1 == ib)
                {
                    return false;
                }
                record.WriteByte((byte)ib);
                remain--;
            }
            return true;
        }


        public bool ReadLineAppend(List<byte> line)
        {
            if(InputRecordLength > 0)
            {
                throw new Exception(`Cannot read-line in fixed-length record rectangular binary mode`);
            }

            bool readany = false;
            for(;;)
            {
                int ib = this.ReadByte();
                if(-1 == ib)
                {
                    return readany;
                }
                readany = true;
                if((int)'\n' == ib)
                {
                    break;
                }
                if((int)'\r' != ib)
                {
                    line.Add((byte)ib);
                }
            }
            return true;
        }

        public bool ReadBinary(ref Blob blob)
        {
            StringBuilder sb = new StringBuilder();
            bool readany = ReadLineAppend(sb);
            if(!readany)
            {
                return false;
            }
            blob = Blob.FromDfsLine(sb.ToString()); 
            return true;
        }

        public bool ReadLineAppend(StringBuilder line)
        {
            if(InputRecordLength > 0)
            {
                throw new Exception(`Cannot read-line in fixed-length record rectangular binary mode`);
            }

            List<byte> buf = new List<byte>();

            bool readany = false;
            for(;;)
            {
                int ib = this.ReadByte();
                if(-1 == ib)
                {
                    break;
                }
                readany = true;
                if((int)'\n' == ib)
                {
                    break;
                }
                if((int)'\r' != ib)
                {
                    buf.Add((byte)ib);                    
                }
            }
            if(buf.Count > 0)
            {
                line.Append(System.Text.Encoding.UTF8.GetString(buf.ToArray()));
            }             
            return readany;
        }

        public bool ReadLineAppend(System.IO.Stream line)
        {
            if(InputRecordLength > 0)
            {
                throw new Exception(`Cannot read-line in fixed-length record rectangular binary mode`);
            }

            bool readany = false;
            for(;;)
            {
                int ib = this.ReadByte();
                if(-1 == ib)
                {
                    return readany;
                }
                readany = true;
                if((int)'\n' == ib)
                {
                    break;
                }
                if((int)'\r' != ib)
                {
                    line.WriteByte((byte)ib);
                }
            }
            return true;
        }


        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] array, int offset, int count)
        {
            int rd = 0;
            try
            {
                if(-1 == whichseqreader)
                {
                    _seqinit();
                }
                while (rd < count)
                {
                    if (seqend)
                    {
                        /*if (0 == rd)
                        {
                            throw new EndOfStreamException(`End of file; end of all files in sequences has been reached`);
                        }*/
                        break;
                    }
                    rd += curseqfile.Read(array, offset + rd, count - rd);
                    if (0 != rd)
                    {
                        prevbyte = array[offset + rd - 1];
                    }
                    if (rd < count)
                    {
                        _movenextseq();
                        /*if (lmode)
                        {
                            if ('\n' != prevbyte)
                            {
                                prevbyte = (byte)'\n';
                                array[offset + rd] = (byte)'\n';
                                rd++;
                            }
                        }*/
                        continue;
                    }
                    break;
                }
            }
            catch
            {
                // Suppress error message. To-do: fix.
            }
            return rd;
        }

        byte[] _rb = null;
        
        public override int ReadByte()
        {
            if(null == _rb)
            {
                _rb = new byte[1];
            }
            if(1 != this.Read(_rb, 0, 1))
            {
                return -1;
            }
            return _rb[0];
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        UserRExec r;
        System.IO.Stream curseqfile = null;
        string curfilename;
        int whichseqreader = -1;
        byte prevbyte = 0;
        int bufsz = " + FILE_BUFFER_SIZE.ToString() + @"; // Buffer size.
        static byte compressenumout = " + CompressFileOutput.ToString() + @";


        void _seqinit()
        {
            if (0 != infs.Length)
            {
                whichseqreader = 0;
                curfilename = infs[0];
                curseqfile = GetStreamFromSeqFileName(curfilename, true);
            }
        }

        void _skipseq()
        {
            if (null != curseqfile)
            {
                curseqfile = null;

                if (whichseqreader < infs.Length)
                {
                    whichseqreader++;
                    if(whichseqreader < infs.Length)
                    {
                        curfilename = infs[whichseqreader];
                        curseqfile = GetStreamFromSeqFileName(infs[whichseqreader], true);
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

        protected System.IO.Stream GetStreamFromSeqFileName(string fn, bool skipdfschunkheader)
        {
            System.IO.Stream stm;
            bool anydata = false;
            if (bufsz > 0)
            {
                //stm = new System.IO.FileStream(fn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, bufsz);
                stm = new MySpace.DataMining.AELight.DfsFileNodeStream(fn, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, bufsz);
            }
            else
            {
                stm = new MySpace.DataMining.AELight.DfsFileNodeStream(fn, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, " + FILE_BUFFER_SIZE.ToString() + @");
            }
            anydata = 0 != stm.Length;
            if(1 == compressenumout)
            {
                stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
            }
            if(skipdfschunkheader && anydata)
            {
                byte[] buf = new byte[32];
                _StreamReadExact(stm, buf, 4);
                int hlen = Entry.BytesToInt(buf);
                if(hlen > 4)
                {
                    int hremain = hlen - 4;
                    if(hremain > buf.Length)
                    {
                        buf = new byte[hremain];
                    }
                    _StreamReadExact(stm, buf, hremain);
                }
            }
            return stm;
        }

        static void _StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            while (sofar < len)
            {
                int xread = stm.Read(buf, sofar, len - sofar);
                if (xread <= 0)
                {
                    throw new System.IO.IOException(`Unable to read from stream`);
                }
                sofar += xread;
            }
        }

        bool seqend
        {
            get
            {
                return whichseqreader >= infs.Length;
            }
        }

    }

    public class RemoteOutputStream : System.IO.Stream
    {
        public RemoteOutputStream(UserRExec r)
        {
            this.r = r;

            StaticGlobals.DSpace_OutputRecordLength = " + OutputRecordLength.ToString() + @";
        }

        UserRExec r;

        const int _CookTimeout = " + CookTimeout.ToString() + @";
        const int _CookRetries = " + CookRetries.ToString() + @";

        const int OutputRecordLength = " + OutputRecordLength.ToString() + @";
        const bool _WriteSamples = OutputRecordLength < 1;


        public void WriteRecord(IList<byte> record)
        {
            if(OutputRecordLength < 1)
            {
                throw new Exception(`Cannot write records; not in fixed-length record rectangular binary mode`);
            }

            int recordCount = record.Count;
            if(recordCount != OutputRecordLength) // && OutputRecordLength > 0)
            {
                throw new Exception(`Record length mismatch; got length of ` + recordCount.ToString() + ` when expecting length of ` + OutputRecordLength.ToString());
            }
            for(int i = 0; i < recordCount; i++)
            {
                this.WriteByte(record[i]);
            }
        }


        public void WriteLine(IList<byte> line)
        {
            if(OutputRecordLength > 0)
            {
                throw new Exception(`Cannot write-line in fixed-length record rectangular binary mode`);
            }

            int lc = line.Count;
            for(int i = 0; i < lc; i++)
            {
                this.WriteByte(line[i]);
            }
            string nl = Environment.NewLine;
            for(int i = 0; i < nl.Length; i++)
            {
                this.WriteByte((byte)nl[i]);
            }
        }

        public void WriteLine(StringBuilder line)
        {
            if(OutputRecordLength > 0)
            {
                throw new Exception(`Cannot write-line in fixed-length record rectangular binary mode`);
            }

            WriteLine(line.ToString());
        }

        public void WriteLine(string line)
        {
            if(OutputRecordLength > 0)
            {
                throw new Exception(`Cannot write-line in fixed-length record rectangular binary mode`);
            }

            byte[] buf = System.Text.Encoding.UTF8.GetBytes(line);
            foreach(byte b in buf)
            {
                this.WriteByte(b);
            }
          
            string nl = Environment.NewLine;
            for(int i = 0; i < nl.Length; i++)
            {
                this.WriteByte((byte)nl[i]);
            }
        }

        public void WriteBinary(Blob blob)
        {
            WriteLine(blob.ToDfsLine());
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            if(null != _outstm)
            {
                _outstm.Flush();
                if(_WriteSamples)
                {
                    _outsamps.Flush();
                }
            }
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        void _checkstream()
        {
            if(-1 == _filenum || _cursize >= _basefilesize)
            {
                if(_samechunk == 0 || -1 == _filenum)
                {
                    _nextfile();
                }
            }
        }
        
        public void Begin()
        {
            _samechunk++;
        }
        
        public void End()
        {
            if(0 == _samechunk)
            {
                throw new Exception(`RemoteOutputStream: mismatched Begin() and End()`);
            }
            _samechunk--;
            if(0 == _samechunk)
            {
                _checkstream();
            }
        }
        

        byte[] chunkline = new byte[0x400 * 0x40];
        int chunklinelen = 0;

        void _WriteToChunk(byte[] buffer, int offset, int count)
        {
            _outstm.Write(buffer, offset, count);
            
            _cursize += count;
            _totalsize += count;

            for(int i = 0; i < count; i++)
            {
                byte b = buffer[offset + i];
                if(chunklinelen < chunkline.Length)
                {
                    chunkline[chunklinelen++] = b;
                }
                if('\n' == b)
                {
                    if(_WriteSamples)
                    {
                        if(0 != chunklinelen)
                        {
                            if(_cursize >= _nextsamplepos)
                            {
                                _outsamps.Write(chunkline, 0, chunklinelen);
                                _nextsamplepos += DfsSampleDistance;
                            }
                            chunklinelen = 0;
                        }
                    }
                }
            }
        }

        // Begin() forces writes to be in the same chunk.
        public override void Write(byte[] buffer, int offset, int count)
        {
            another_write:
            //if(-1 == _filenum || _donextfile)
            if(_donextfile)
            {
                _nextfile();
            }
            
            if(0 == _samechunk)
            {
                int xc = 0;
                for(; xc < count; xc++)
                {
                    if((byte)'\n' == buffer[offset + xc])
                    {
                        if(xc + 1 + _cursize >= _basefilesize)
                        {
                            xc++; // Include the \n.
                            _WriteToChunk(buffer, offset, xc);
                            _donextfile = true;
                            if(xc >= count)
                            {
                                return;
                            }
                            offset += xc;
                            count -= xc;
                            goto another_write;
                        }
                    }
                }
            }

            _WriteToChunk(buffer, offset, count);
        }

        byte[] _wb = null;

        public override void WriteByte(byte x)
        {
            if(null == _wb)
            {
                _wb = new byte[1];
            }
            _wb[0] = x;
            Write(_wb, 0, 1);
        }


        internal void _done()
        {
            if(null != _outstm)
            {
                _outstm.Close();
                //_outstm = null;
                _filesizes.Add(HEADERSIZE + _cursize);
                if(_WriteSamples)
                {
                    _outsamps.Close();
                }
            }
        }

        internal int _getoutputfilecount(IList<long> appendsizes)
        {
            if(null != appendsizes)
            {
                for(int i = 0; i < _filenum + 1; i++)
                {
                    string fn = _basefilename.Replace(`%n`, i.ToString());
                    long x = _filesizes[i];
                    if(x >= HEADERSIZE)
                    {
                        x -= HEADERSIZE;
                    }
                    appendsizes.Add(x);
                }
            }
            return _filenum + 1;
        }
        
        void _nextfile()
        {
            _filenum++;
            string fn = _basefilename.Replace(`%n`, _filenum.ToString());
            if(null != _outstm)
            {
                _filesizes.Add(HEADERSIZE + _cursize);
                _outstm.Close();
                if(_WriteSamples)
                {
                    _outsamps.Close();
                }
            }
            if(0 == outds.Length)
            {
                throw new Exception(`RemoteOutputStream error: outputdfsdirs is empty`);
            }
            string fp = outds[(OutputStartingPoint + _filenum) % outds.Length] + @`\` + fn;
            _outstm = new System.IO.FileStream(fp, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, " + FILE_BUFFER_SIZE.ToString() + @");
            if(_WriteSamples)
            {
                _outsamps = new System.IO.FileStream(fp + `.zsa`, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, " + FILE_BUFFER_SIZE.ToString() + @");
                _nextsamplepos = 0;
            }
            
            if(1 == compressenumout)
            {
                _outstm = new System.IO.Compression.GZipStream(_outstm, System.IO.Compression.CompressionMode.Compress);
            }

            // Write header...
            Entry.ToBytes(HEADERSIZE, _smallbuf, 0);
            _bytebuf.Clear();
            Entry.ToBytesAppend64(_totalsize, _bytebuf);
            _smallbuf[4] = _bytebuf[0];
            _smallbuf[5] = _bytebuf[1];
            _smallbuf[6] = _bytebuf[2];
            _smallbuf[7] = _bytebuf[3];
            _smallbuf[8] = _bytebuf[4];
            _smallbuf[9] = _bytebuf[5];
            _smallbuf[10] = _bytebuf[6];
            _smallbuf[11] = _bytebuf[7];
            _outstm.Write(_smallbuf, 0, HEADERSIZE);

            _donextfile = false;
            _cursize = 0;
        }


        const string _basefilename = @`" + outputfilename + @"`;
        const long _basefilesize = " + outputbasefilesize.ToString() + @";
        const int OutputStartingPoint = " + OutputStartingPoint.ToString() + @";
        static byte compressenumout = " + CompressFileOutput.ToString() + @";
        
        int _filenum = -1;
        System.IO.Stream _outstm = null;
        System.IO.Stream _outsamps = null;
        long _cursize = 0;
        long _totalsize = 0;
        byte[] _smallbuf = new byte[4 + 8];
        List<byte> _bytebuf = new List<byte>();
        int _samechunk = 0;
        bool _donextfile = true; // Start!
        List<long> _filesizes = new List<long>();

        string[] outds = new string [] { " + outds.ToString() + @" };

        const int HEADERSIZE = 4 + 8;

        long _nextsamplepos = -1;

        const long DfsSampleDistance = " + DfsSampleDistance.ToString() + @";

    }

    public class UserRExec : MySpace.DataMining.DistributedObjects.IRemote
    {
        RemoteInputStream rinput;
        RemoteOutputStream routput;

        public int GetOutputFileCount(int n, IList<long> appendsizes)
        {
            return routput._getoutputfilecount(appendsizes);
        }

        public void OnRemote()
        {
            rinput = new RemoteInputStream(this);
            routput = new RemoteOutputStream(this);

            StaticGlobals.DSpace_SlaveIP = DSpace_SlaveIP;
            StaticGlobals.DSpace_SlaveHost = DSpace_SlaveHost;
            StaticGlobals.DSpace_BlocksTotalCount = DSpace_BlocksTotalCount;
            StaticGlobals.DSpace_BlockID = DSpace_BlockID;
            StaticGlobals.ExecutionContext = ExecutionContextType.REMOTE;
            StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts) + @"};
            StaticGlobals.DSpace_OutputDirection = `" + StaticGlobals.DSpace_OutputDirection + @"`;
            StaticGlobals.DSpace_OutputDirection_ascending = " + (StaticGlobals.DSpace_OutputDirection_ascending ? "true" : "false") + @";
            StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";
").Replace('`', '"') + 
            DGlobalsM.ToCode() + @"
            Remote(rinput, routput);
            routput._done();
        }

" + code + (@"
    }
}"), "UserRExec");

            return GetNumberOfRemoteOutputFilesCreated(appendsizes);
        }


        public override void BeforeLoadFullSource(string code, string classname)
        {
            throw new NotSupportedException("Remote.BeforeLoadFullSource");
        }

        public override void DoMapFullSource(IList<string> inputdfsnodes, string code, string classname)
        {
            throw new NotSupportedException("Remote.DoMapFullSource");
        }

        public override ArrayComboListEnumerator[] GetEnumeratorsWithFullSource(string code, string classname)
        {
            throw new NotSupportedException("Remote.GetEnumeratorsWithFullSource");
        }

        public override string GetMapSource(string code, string[] usings, string classname)
        {
            throw new NotSupportedException("Remote.GetMapSource");
        }


        public int OutputStartingPoint
        {
            get
            {
                return _startp;
            }

            set
            {
                _startp = value;
            }
        }

        int _startp = 0;


        // Note: appendsizes can be null, but the return will still be valid.
        // Sizes exclude header data.
        protected virtual int GetNumberOfRemoteOutputFilesCreated(IList<long> appendsizes)
        {
            int result = 0;
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'r');
                Entry.ToBytes(0, buf, 0);
                XContent.SendXContent(slave.nstm, buf, 4);
                int len;
                buf = XContent.ReceiveXBytes(slave.nstm, out len, buf);
                if (len >= 4)
                {
                    result += Entry.BytesToInt(buf);

                    if (null != appendsizes)
                    {
                        for (int offset = 4; offset + 8 <= len; offset += 8)
                        {
                            appendsizes.Add(Entry.BytesToLong(buf, offset));
                        }
                    }
                }
            }
            return result;
        }


        public void LocalExec(string code, string[] usings, string methodname)
        {
            string suings = "";
            if (usings != null)
            {
                foreach (string nm in usings)
                {
                    suings = suings + "using " + nm + ";" + Environment.NewLine;
                }
            }
            RemoteExecFullSource(
@"using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;

" + suings + @"

namespace RemoteExec
{
    public class UserRExec : MySpace.DataMining.DistributedObjects.IRemote
    {
        public int GetOutputFileCount(int n, IList<long> appendsizes)
        {
            return 0;
        }

        public void OnRemote()
        {
            StaticGlobals.DSpace_SlaveIP = DSpace_SlaveIP;
            StaticGlobals.DSpace_SlaveHost = DSpace_SlaveHost;
            StaticGlobals.DSpace_BlocksTotalCount = DSpace_BlocksTotalCount;
            StaticGlobals.DSpace_BlockID = DSpace_BlockID;
            StaticGlobals.ExecutionContext = ExecutionContextType.LOCAL;     
            StaticGlobals.DSpace_Hosts = new string[]{" + ExpandListCode(StaticGlobals.DSpace_Hosts).Replace('`', '"') + @"};    
            StaticGlobals.DSpace_MaxDGlobals = " + StaticGlobals.DSpace_MaxDGlobals.ToString() + @";
            " + DGlobalsM.ToCode() + @"   
            " + methodname + @"();
        }

" + code + @"
    }
}", "UserRExec");

            GetDGlobals();
        }

        protected virtual void GetDGlobals()
        {
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'O');
                int len = 0;
                buf = XContent.ReceiveXBytes(slave.nstm, out len, buf);
                DGlobalsM.FromBytes(buf, len, true);
            }
        }

        public void LocalExec(string code, string[] usings)
        {
            LocalExec(code, usings, "Local");
        }

    }

}

