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

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {

        public static void MemCacheCommand(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("Expected memcache sub-command");
                SetFailure();
                return;
            }

            string act = args[0].ToLower();
            switch (act)
            {
                case "create":
                    {
                        string mcname = null;
                        string mcschema = null;
                        int mcsegsize = -1;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                    case "schema":
                                        mcschema = value;
                                        break;
                                    case "segment":
                                    case "segsize":
                                    case "segmentsize":
                                        mcsegsize = ParseCapacity(value);
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }
                        if (string.IsNullOrEmpty(mcschema))
                        {
                            Console.Error.WriteLine("Expected schema=<schema>");
                            SetFailure();
                            return;
                        }
                        if (-1 != mcsegsize && mcsegsize < 1024)
                        {
                            Console.Error.WriteLine("Error: segment={0} is too small", mcsegsize);
                            SetFailure();
                            return;
                        }
                        if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            mcname = mcname.Substring(6);
                        }
                        {
                            string reason;
                            if (dfs.IsBadFilename(mcname, out reason))
                            {
                                Console.Error.WriteLine("MemCache cannot be named '{0}': {1}", mcname, reason);
                                SetFailure();
                                return;
                            }
                        }
                        dfs.DfsFile.ConfigMemCache cmc = new dfs.DfsFile.ConfigMemCache();
                        cmc.MetaFileName = "mcm." + Surrogate.SafeTextPath(mcname) + ".mcm";
                        cmc.Schema = mcschema;
                        List<int> offsets = new List<int>();
                        cmc.RowLength = Surrogate.GetRecordInfo(mcschema, out cmc.KeyOffset, out cmc.KeyLength, offsets);
                        /*if (0 == cmc.KeyOffset
                            && cmc.RowLength == cmc.KeyLength
                            && -1 == mcschema.IndexOf('['))
                        {
                            Console.WriteLine("Note: no key was specified, the key is the entire row");
                        }*/
                        if (-1 == mcsegsize)
                        {
                            const int defsegsize = 0x400 * 0x400 * 64;
                            cmc.SegmentSize = defsegsize - (defsegsize % cmc.RowLength);
                        }
                        else
                        {
                            if (0 != (mcsegsize % cmc.RowLength))
                            {
                                Console.Error.WriteLine("Segment size must be a multiple of the row length");
                                Console.Error.WriteLine("Nearest segment size is {0} bytes",
                                    mcsegsize - (mcsegsize % cmc.RowLength));
                                SetFailure();
                                return;
                            }
                            cmc.SegmentSize = mcsegsize;
                        }
                        {
                            StringBuilder sbFieldOffsets = new StringBuilder();
                            foreach (int offset in offsets)
                            {
                                if (sbFieldOffsets.Length != 0)
                                {
                                    sbFieldOffsets.Append(',');
                                }
                                sbFieldOffsets.Append(offset);
                            }
                            cmc.FieldOffsets = sbFieldOffsets.ToString();
                        }
                        dfs.DfsFile df = new dfs.DfsFile();
                        df.Nodes = new List<dfs.DfsFile.FileNode>(0);
                        df.MemCache = cmc;
                        df.Name = mcname;
                        df.XFileType = DfsFileTypes.BINARY_RECT + "@" + cmc.RowLength;
                        df.Size = 0;
                        dfs dc = LoadDfsConfig();
                        {
                            dfs.DfsFile df2 = dc.FindAny(df.Name);
                            if (null != df2)
                            {
                                Console.Error.WriteLine("Error: a file named '{0}' already exists", df2.Name);
                                SetFailure();
                                return;
                            }
                        }
                        {
                            string startmeta = GetMemCacheMetaFileHeader(df);
                            string[] slaves = dc.Slaves.SlaveList.Split(';');
                            int totalworkercount = dc.Blocks.TotalCount; // Subprocess_TotalPrime
                            StringBuilder[] permachine = new StringBuilder[slaves.Length];
                            //byte[] HEADER = new byte[4];
                            //MySpace.DataMining.DistributedObjects.Entry.ToBytes(4, HEADER, 0);
                            for (int i = 0; i < permachine.Length; i++)
                            {
                                permachine[i] = new StringBuilder(256);
                            }
                            {
                                int si = -1;
                                for (int workerid = 0; workerid < totalworkercount; workerid++)
                                {
                                    if (++si >= slaves.Length)
                                    {
                                        si = 0;
                                    }
                                    StringBuilder sb = permachine[si];
                                    sb.AppendFormat("##{1}:{0}", Environment.NewLine, workerid);
                                    // There's no segments, but write a dummy one for bookkeeping.
                                    foreach (char snc in "MemCache_" + mcname + "_empty")
                                    {
                                        sb.Append(snc);
                                    }
                                    {
                                        sb.Append(' ');
                                        /*
                                        StringBuilder newchunkpath = new StringBuilder(100);
                                        newchunkpath.Append(Surrogate.NetworkPathForHost(slaves[si]));
                                        newchunkpath.Append('\\');
                                         * */
                                        // Make up a data node chunk name.
                                        foreach (char ch in MakeMemCacheChunkName(mcname, workerid))
                                        {
                                            //newchunkpath.Append(ch);
                                            sb.Append(ch);
                                        }
                                        // Write the empty chunk.
                                        //System.IO.File.WriteAllBytes(newchunkpath.ToString(), HEADER);
                                    }
                                    //if (IsLastSegment) // true
                                    {
                                        sb.Append(' ');
                                        string shexlen = string.Format("{0:x8}", 0); // Zero-length!
                                        for (int i = 0; i < shexlen.Length; i++)
                                        {
                                            sb.Append(shexlen[i]);
                                        }
                                    }
                                    sb.AppendLine();
                                }
                            }
                            for (int si = 0; si < slaves.Length; si++)
                            {
                                string slave = slaves[si];
                                string fp = Surrogate.NetworkPathForHost(slave) + @"\" + cmc.MetaFileName;
                                using (System.IO.StreamWriter sw = new System.IO.StreamWriter(fp))
                                {
                                    sw.Write(startmeta);
                                    sw.Write(permachine[si].ToString());
                                }

                            }
                        }
                        using (LockDfsMutex())
                        {
                            dc = LoadDfsConfig(); // Load again in update lock.
                            {
                                dfs.DfsFile df2 = dc.FindAny(df.Name);
                                if (null != df2)
                                {
                                    Console.Error.WriteLine("Error: a file named '{0}' already exists", df2.Name);
                                    SetFailure();
                                    return;
                                }
                            }
                            dc.Files.Add(df);
                            UpdateDfsXml(dc);
                        }
                        try
                        {
                            // Need to commit it so that the empty chunks are in the metadata for bookkeeping.
                            // This has to be done after actually adding it to dfsxml.
                            MemCacheFlush(mcname);
                        }
                        catch(Exception e)
                        {
                            try
                            {
                                MemCacheDelete(mcname, false);
                            }
                            catch
                            {
                            }
                            Console.Error.WriteLine("Error: unable to commit newly created MemCache '{0}'; because:{1}{2}",
                                mcname, Environment.NewLine, e.ToString());
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Successfully created MemCache '{0}'", mcname);
                    }
                    break;

                case "delete":
                case "del":
                case "rm":
                    {
                        string mcname = null;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }
                        MemCacheDelete(mcname, true);
                    }
                    break;

                case "flush":
                case "commit":
                    {
                        string mcname = null;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }
                        try
                        {
                            MemCacheFlush(mcname);
                            Console.WriteLine("Done");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("    Commit was unsuccessful because: {0}", e.Message);
                            Console.WriteLine();
                            Console.Error.WriteLine(e.ToString());
                            SetFailure();
                            return;
                        }
                    }
                    break;

                case "release":
                case "rollback":
                    {
                        string mcname = null;
                        bool force = false;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                    case "-f":
                                        force = true;
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }
                        try
                        {
                            MemCacheRelease(mcname, force);
                            Console.WriteLine("Done");
                        }
                        catch (Exception e)
                        {
                            string exception = e.ToString();
                            if (-1 != exception.IndexOf("MemCacheWarning"))
                            {
                                Console.WriteLine("Warning: " + exception);
                            }
                            else
                            {
                                Console.Error.WriteLine(exception);
                                string ioe = "InvalidOperationException:";
                                if (!force && -1 != exception.IndexOf(ioe))
                                {
                                    try
                                    {
                                        string emsg = exception.Substring(exception.IndexOf(ioe) + ioe.Length)
                                            .Split('\r', '\n')[0].Trim();
                                        System.Threading.Thread.Sleep(100);
                                        Console.WriteLine();
                                        Console.WriteLine("{0}{2}{1}",
                                            false ? "\u00014" : "", false ? "\u00010" : "",
                                            emsg);
                                        System.Threading.Thread.Sleep(100);
                                    }
                                    catch
                                    {
                                    }
                                    Console.Error.WriteLine("Use rollback -f followed by killall to force rollback");
                                }
                                SetFailure();
                                return;
                            }
                        }
                    }
                    break;

                case "load":
                    {
                        string mcname = null;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }
                        MemCacheLoad(mcname);
                        Console.WriteLine("Done");
                    }
                    break;

                case "info":
                case "information":
                    {
                        string mcname = null;
                        EachArgument(args, 1,
                            new Action<string, string>(
                            delegate(string key, string value)
                            {
                                key = key.ToLower();
                                switch (key)
                                {
                                    case "name":
                                        mcname = value;
                                        break;
                                }
                            }));
                        if (string.IsNullOrEmpty(mcname))
                        {
                            Console.Error.WriteLine("Expected name=<MemCacheName>");
                            SetFailure();
                            return;
                        }

                        if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            mcname = mcname.Substring(6);
                        }
                        dfs dc = LoadDfsConfig();
                        dfs.DfsFile df = dc.FindAny(mcname);
                        if (null == df || df.MemCache == null)
                        {
                            Console.Error.WriteLine("Error: '{0}' is not a MemCache", (null == df ? mcname : df.Name));
                            SetFailure();
                            return;
                        }

                        Console.WriteLine("  MemCache:      {0}", df.Name);
                        Console.WriteLine("  Segment size:  {0} ({1})",
                            GetFriendlyByteSize(df.MemCache.SegmentSize),
                            df.MemCache.SegmentSize);
                        Console.WriteLine("  Schema:        {0}", df.MemCache.Schema);
                        Console.WriteLine("  Row Length:    {0}", df.MemCache.RowLength);
                        Console.WriteLine("  Key Offset:    {0}", df.MemCache.KeyOffset);
                        Console.WriteLine("  Key Length:    {0}", df.MemCache.KeyLength);

                    }
                    break;

                default:
                    Console.Error.WriteLine("No such sub-command for memcache: {0}", act);
                    SetFailure();
                    return;
            }

        }


        static IEnumerable<char> MakeMemCacheChunkName(string mcname, string sslaveid)
        {
            // Make up a data node chunk name.
            foreach (char ch in "zd.mc~")
            {
                yield return ch;
            }
            foreach (char ch in sslaveid)
            {
                yield return ch;
            }
            yield return '~';
            for (int i = 0, j = 0; i < 10 && j < mcname.Length; j++)
            {
                if (mcname[j] < 128 && char.IsLetterOrDigit(mcname[j]))
                {
                    i++;
                    yield return mcname[j];
                }
            }
            yield return '.';
            foreach (char ch in Guid.NewGuid().ToString())
            {
                yield return ch;
            }
            foreach (char ch in ".zd")
            {
                yield return ch;
            }
        }

        static IEnumerable<char> MakeMemCacheChunkName(string mcname, int slaveid)
        {
            return MakeMemCacheChunkName(mcname, slaveid.ToString());
        }


        static void _MemCacheRelease(dfs.DfsFile df, bool force)
        {
            dfs dc = LoadDfsConfig();
            string exception = "";
            string[] slaves = dc.Slaves.SlaveList.Split(';');
            {
                //foreach (string slave in slaves)
                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                    new Action<string>(
                    delegate(string slave)
                    {
                        System.Net.Sockets.NetworkStream nstm = Surrogate.ConnectService(slave);
                        nstm.WriteByte((byte)'C');
                        nstm.WriteByte((byte)'r');
                        XContent.SendXContent(nstm, df.Name);
                        XContent.SendXContent(nstm, new byte[1] { (byte)(force ? 1 : 0) });
                        int ich = nstm.ReadByte();
                        if ('+' != ich)
                        {
                            string errmsg = null;
                            if ('-' == ich)
                            {
                                try
                                {
                                    errmsg = XContent.ReceiveXString(nstm, null);
                                }
                                catch
                                {
                                }
                            }
                            lock (slaves)
                            {
                                string newexception;
                                if (null != errmsg)
                                {
                                    newexception = ("Error received from DO service during MemCache rollback from " + slave + ": " + errmsg);
                                }
                                else
                                {
                                    newexception = ("Did not receive a success signal from DO service during MemCache rollback from " + slave);
                                }
                                if (string.IsNullOrEmpty(exception)
                                    || -1 != exception.IndexOf("MemCacheWarning"))
                                {
                                    exception = newexception;
                                }
                            }
                        }

                    }), slaves, slaves.Length);
            }
            if (!string.IsNullOrEmpty(exception))
            {
                throw new Exception(exception);
            }
        }

        static void _MemCacheRelease(dfs.DfsFile df)
        {
            _MemCacheRelease(df, false);
        }


        static void _MemCacheLoad(dfs.DfsFile df)
        {
            dfs dc = LoadDfsConfig();
            string[] slaves = dc.Slaves.SlaveList.Split(';');
            {
                //foreach (string slave in slaves)
                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                    new Action<string>(
                    delegate(string slave)
                    {
                        System.Net.Sockets.NetworkStream nstm = Surrogate.ConnectService(slave);
                        nstm.WriteByte((byte)'C');
                        nstm.WriteByte((byte)'l');
                        XContent.SendXContent(nstm, df.Name);
                        int ich = nstm.ReadByte();
                        if ('+' != ich)
                        {
                            string errmsg = null;
                            if ('-' == ich)
                            {
                                try
                                {
                                    errmsg = XContent.ReceiveXString(nstm, null);
                                }
                                catch
                                {
                                }
                            }
                            if (null != errmsg)
                            {
                                throw new Exception("Error received from DO service during MemCache load from " + slave + ": " + errmsg);
                            }
                            throw new Exception("Did not receive a success signal from DO service during MemCache load from " + slave);
                        }

                    }), slaves, slaves.Length);
            }
        }


        public static void MemCacheRelease(string mcname, bool force)
        {
            if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                mcname = mcname.Substring(6);
            }
            dfs dc = LoadDfsConfig();
            dfs.DfsFile df = dc.FindAny(mcname);
            if (null == df || df.MemCache == null)
            {
                Console.Error.WriteLine("Error: '{0}' is not a MemCache", (null == df ? mcname : df.Name));
                SetFailure();
                return;
            }

            _MemCacheRelease(df, force);

        }

        public static void MemCacheRelease(string mcname)
        {
            MemCacheRelease(mcname, false);
        }


        public static void MemCacheLoad(string mcname)
        {
            if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                mcname = mcname.Substring(6);
            }
            dfs dc = LoadDfsConfig();
            dfs.DfsFile df = dc.FindAny(mcname);
            if (null == df || df.MemCache == null)
            {
                Console.Error.WriteLine("Error: '{0}' is not a MemCache", (null == df ? mcname : df.Name));
                SetFailure();
                return;
            }

            _MemCacheLoad(df);

        }


        public static void MemCacheFlush(string mcname)
        {
            if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                mcname = mcname.Substring(6);
            }
            dfs dc = LoadDfsConfig();
            dfs.DfsFile df = dc.FindAny(mcname);
            if (null == df || df.MemCache == null)
            {
                Console.Error.WriteLine("Error: '{0}' is not a MemCache", (null == df ? mcname : df.Name));
                SetFailure();
                return;
            }

            string tempdfsname = mcname + Guid.NewGuid() + dfs.TEMP_FILE_MARKER;
            string[] slaves = dc.Slaves.SlaveList.Split(';');
            long dfsfilesize = 0;
            {
                string tempfp = "bp-mcflush" + Guid.NewGuid() + ".tmp";
                try
                {
                    using (System.IO.StreamWriter sw = new System.IO.StreamWriter(tempfp))
                    {
                        //foreach (string slave in slaves)
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                System.Net.Sockets.NetworkStream nstm = Surrogate.ConnectService(slave);
                                nstm.WriteByte((byte)'C');
                                nstm.WriteByte((byte)'f');
                                XContent.SendXContent(nstm, df.Name);
                                int ich = nstm.ReadByte();
                                if ('+' != ich)
                                {
                                    string errmsg = null;
                                    if ('-' == ich)
                                    {
                                        try
                                        {
                                            errmsg = XContent.ReceiveXString(nstm, null);
                                        }
                                        catch
                                        {
                                        }
                                    }
                                    if (null != errmsg)
                                    {
                                        throw new Exception("Error received from DO service during MemCache commit: " + errmsg);
                                    }
                                    throw new Exception("Did not receive a success signal from DO service during MemCache commit");
                                }
                                // flushinfos: chunk name, chunk size (without header)
                                string[] flushinfos = XContent.ReceiveXString(nstm, null).Split(
                                    new char[] { '\r', '\n' },
                                    StringSplitOptions.RemoveEmptyEntries);
                                foreach (string flushinfo in flushinfos)
                                {
                                    lock (slaves)
                                    {
                                        sw.WriteLine("{0} {1}", slave, flushinfo);
                                        dfsfilesize += int.Parse(flushinfo.Split(' ')[1]);
                                    }
                                }
                            }), slaves, slaves.Length);
                    }
                    DfsBulkPut(new string[] { tempfp, tempdfsname, "rbin@" + df.RecordLength });
                }
                finally
                {
                    try
                    {
                        System.IO.File.Delete(tempfp);
                    }
                    catch
                    {
                    }
                }
            }

            using (LockDfsMutex())
            {
                dc = LoadDfsConfig();
                dfs.DfsFile df2 = dc.FindAny(tempdfsname);
                if (null == df2)
                {
                    throw new Exception("DEBUG:  Temp DFS file not found: " + tempdfsname);
                }
                for (int i = 0; i < dc.Files.Count; i++)
                {
                    if (0 == string.Compare(dc.Files[i].Name, mcname, true))
                    {
                        dc.Files.RemoveAt(i);
                        break;
                    }
                }
                df2.MemCache = df.MemCache;
                df2.Size = dfsfilesize;
                df2.Name = df.Name;
                UpdateDfsXml(dc);

            }

            {
                // Just kill the old chunks, not the MemCache stuff.
                List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>(1);
                delfiles.Add(df);
                _KillDataFileChunks_unlocked_mt(delfiles, false);
            }

        }


        public static string GetMemCacheMetaFileHeader(dfs.DfsFile df)
        {
            StringBuilder sbstartmeta = new StringBuilder();
            sbstartmeta.AppendFormat("name={1}{0}", Environment.NewLine, df.Name);
            sbstartmeta.AppendFormat("blocksize={1}{0}", Environment.NewLine, df.MemCache.SegmentSize);
            sbstartmeta.AppendFormat("rowlen={1}{0}", Environment.NewLine, df.MemCache.RowLength);
            sbstartmeta.AppendFormat("keyoffset={1}{0}", Environment.NewLine, df.MemCache.KeyOffset);
            sbstartmeta.AppendFormat("keylen={1}{0}", Environment.NewLine, df.MemCache.KeyLength);
            sbstartmeta.AppendFormat("coloffsets={1}{0}", Environment.NewLine, df.MemCache.FieldOffsets);
            sbstartmeta.AppendLine("-----------------------------------------");
            return sbstartmeta.ToString();
        }


        public static void MemCacheDelete(string mcname, bool verbose)
        {
            if (mcname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                mcname = mcname.Substring(6);
            }
            dfs.DfsFile df = null;
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                int dfindex;
                for (dfindex = 0; dfindex < dc.Files.Count; dfindex++)
                {
                    if (0 == string.Compare(dc.Files[dfindex].Name, mcname, true))
                    {
                        df = dc.Files[dfindex];
                        break;
                    }
                }
                if (null == df)
                {
                    //Console.Error.WriteLine("DFS file '{0}' does not exist", mcname);
                    //SetFailure();
                    return;
                }
                if (df.MemCache == null)
                {
                    Console.Error.WriteLine("DFS file '{0}' is not a MemCache file", df.Name);
                    SetFailure();
                    return;

                }
                dc.Files.RemoveAt(dfindex);
                UpdateDfsXml(dc);
            }

            {
                List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>(1);
                delfiles.Add(df);
                _KillMemCache_mt(delfiles, verbose);
            }
        }


        internal static void _KillMemCache_mt(List<dfs.DfsFile> delfiles, bool verbose)
        {
            dfs dc = LoadDfsConfig();
            string[] slaves = dc.Slaves.SlaveList.Split(';');
            foreach (dfs.DfsFile df in delfiles)
            {
                try
                {
                    // Unpin shared memory segments on all machines.
                    _MemCacheRelease(df, true); // force=true
                }
                catch (System.Threading.ThreadAbortException)
                {
                }
                catch (Exception e)
                {
                    if (verbose)
                    {
                        string msg = e.Message;
                        if (-1 == msg.IndexOf("MemCacheWarning"))
                        {
                            Console.WriteLine("Warning: {0}", msg);
                        }
                    }
                }
                foreach (string slave in slaves)
                {
                    try
                    {
                        string mdfp = Surrogate.NetworkPathForHost(slave)
                            + @"\" + df.MemCache.MetaFileName;
                        System.IO.File.Delete(mdfp);
                    }
                    catch
                    {
                    }
                }
            }

            _KillDataFileChunks_unlocked_mt(delfiles, false);

            if (verbose)
            {
                foreach (dfs.DfsFile df in delfiles)
                {
                    Console.WriteLine("Successfully deleted MemCache '{0}' ({1} parts)", df.Name, df.Nodes.Count);
                }
            }
        }


        public struct PinSM : IDisposable
        {
            System.Net.Sockets.NetworkStream pinstm;
            string pinhost;

            public PinSM(string host)
            {
                pinstm = null;
                pinhost = host;
            }

            void _DoConnPinSM()
            {
                if (null == pinhost)
                {
                    pinhost = "localhost";
                }
                if (null != pinstm)
                {
                    try
                    {
                        pinstm.Close();
                    }
                    catch
                    {
                    }
                    pinstm = null;
                }
                try
                {
                    System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                        System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                    sock.Connect(pinhost, 55906);
                    pinstm = new System.Net.Sockets.NetworkStream(sock, true); // ownsSocket=true
                }
                catch (Exception e)
                {
                    throw new Exception("Unable to connect to MemCachePin service [ensure MemCachePin Windows services are installed and running]", e);
                }
            }

            bool _StartCmdPinSM(char tagchar)
            {
                bool oldconn = true;
                if (null == pinstm)
                {
                    oldconn = false;
                    _DoConnPinSM();
                }
                try
                {
                    pinstm.WriteByte((byte)tagchar);
                }
                catch
                {
                    if (!oldconn)
                    {
                        throw;
                    }
                    _DoConnPinSM();
                    pinstm.WriteByte((byte)tagchar);
                }
                return oldconn;
            }

            void _SimpleCmdPinSM(char tagchar, string arg)
            {
                _StartCmdPinSM(tagchar);
                XContent.SendXContent(pinstm, arg);
                int ich = pinstm.ReadByte();
                if ('+' != ich)
                {
                    string errmsg = null;
                    try
                    {
                        if ('-' == ich)
                        {
                            errmsg = XContent.ReceiveXString(pinstm, null);
                        }
                    }
                    catch
                    {
                    }
                    if (null != errmsg)
                    {
                        throw new Exception("MemCachePin service error: " + errmsg + "  {" + tagchar + "}");
                    }
                    throw new Exception("MemCachePin service did not return a success signal  {" + tagchar + "}");
                }
            }

            public void Pin(string smname)
            {
                _SimpleCmdPinSM('p', smname);
            }

            public void Unpin(string smname)
            {
                _SimpleCmdPinSM('u', smname);
            }


            #region IDisposable Members

            public void Dispose()
            {
                if (null != pinstm)
                {
                    pinstm.Close();
                    pinstm = null;
                }
            }

            #endregion
        }


    }

}