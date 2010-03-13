using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Protocol
{
    public class DfsProtocol
    {

        static System.Threading.Thread lthd;
        static System.Net.Sockets.Socket lsock;
        static string CurrentDir;
        static string DfsDir;

        public static void Start()
        {

            CurrentDir = Environment.CurrentDirectory;
            DfsDir = CurrentDir + @"\..";

#if DEBUG
            //System.Diagnostics.Debugger.Launch();
            FastXml._Test_();
            DirectDfs._Test_();
#endif

            lthd = new System.Threading.Thread(new System.Threading.ThreadStart(ListenThreadProc));
            lthd.Name = "DfsProtocol_ListenThreadProc";
            lthd.IsBackground = true;
            lthd.Start();

        }


        static void ListenThreadProc()
        {

            bool keepgoing = true;
            while (keepgoing)
            {
                try
                {
                    if (lsock != null)
                    {
                        lsock.Close();
                    }

                    lsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                        System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                    System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55905);
                    for (int i = 0; ; i++)
                    {
                        try
                        {
                            lsock.Bind(ipep);
                            break;
                        }
                        catch
                        {
                            if (i >= 5)
                            {
                                throw;
                            }
                            System.Threading.Thread.Sleep(1000 * 4);
                            continue;
                        }
                    }

                    lsock.Listen(30);

                    for (; ; )
                    {
                        System.Net.Sockets.Socket dllclientSock = lsock.Accept();
                        DfsProtocolClientHandler ch = new DfsProtocolClientHandler();
                        System.Threading.Thread cthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ch.ClientThreadProc));
                        cthd.IsBackground = true;
                        cthd.Start(dllclientSock);
                    }
                }
                catch (System.Threading.ThreadAbortException e)
                {
                    keepgoing = false;
                }
                catch (Exception e)
                {
                    XLog.errorlog("DfsProtocol.ListenThreadProc exception: " + e.ToString());
                }
            }
        }


        public class DfsProtocolClientHandler
        {

            System.Net.Sockets.NetworkStream netstm;
            byte[] buf = new byte[1024 * 1024 * 1];
            int buflen;

            internal void ClientThreadProc(object _sock)
            {
                System.Net.Sockets.Socket clientsock = (System.Net.Sockets.Socket)_sock;
                netstm = new System.Net.Sockets.NetworkStream(clientsock);

                try
                {
                    for (bool stop = false; !stop; )
                    {
                        int ib;
                        try
                        {
                            ib = netstm.ReadByte();
                        }
                        catch(System.IO.IOException e)
                        {
                            if (e.InnerException is System.Net.Sockets.SocketException)
                            {
                                break;
                            }
                            throw;
                        }
                        if (ib < 0)
                        {
                            stop = true;
                            break;
                        }

                        switch (ib)
                        {
                            case 's': // Get DFS file size.
                                try
                                {
                                    string dfsfile = XContent.ReceiveXString(netstm, buf);
                                    string ssize = DirectDfs.GetFileSizeString(dfsfile);
                                    netstm.WriteByte((byte)'+');
                                    XContent.SendXContent(netstm, ssize);
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                }
                                break;

                            case 'n': // Get DFS file part count.
                                try
                                {
                                    string dfsfile = XContent.ReceiveXString(netstm, buf);
                                    string spartcount = DirectDfs.GetFilePartCountString(dfsfile);
                                    netstm.WriteByte((byte)'+');
                                    XContent.SendXContent(netstm, spartcount);
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                }
                                break;

                            case 'g': // Get DFS file contents.
                                try
                                {
                                    string dfsfile = XContent.ReceiveXString(netstm, buf);
                                    byte[] content = DirectDfs.GetFileContent(dfsfile);
                                    netstm.WriteByte((byte)'+');
                                    XContent.SendXContent(netstm, content);
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                }
                                break;

                            case 'p': // Set DFS file contents; replaces if exists.
                                try
                                {
                                    string dfsfile = XContent.ReceiveXString(netstm, buf);
                                    string dfsfiletype = XContent.ReceiveXString(netstm, buf);
                                    buf = XContent.ReceiveXBytes(netstm, out buflen, buf);
                                    DirectDfs.SetFileContent(dfsfile, dfsfiletype, buf, buflen);
                                    netstm.WriteByte((byte)'+');
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                }
                                break;

                            case 'd': // Delete DFS file.
                                try
                                {
                                    string dfsfile = XContent.ReceiveXString(netstm, buf);
                                    DirectDfs.DeleteFile(dfsfile);
                                    netstm.WriteByte((byte)'+');
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                }
                                break;

                            case 'c': // Close.
                                stop = true;
                                netstm.WriteByte((byte)'+');
                                break;

                            case 199: // Handshake.
                                netstm.WriteByte(199 / 3);
                                break;

                            default:
                                throw new Exception("Unknown action: " + ((int)ib).ToString() + " char: " + (char)ib);
                        }
                    }
                }
                catch (Exception e)
                {
                    XLog.errorlog("DfsProtocolClientHandler.ClientThreadProc exception: " + e.ToString());
                }
                finally
                {
                    try
                    {
                        netstm.Close();
                        netstm = null;
                        clientsock.Close();
                        clientsock = null;
                    }
                    catch
                    {

                    }
                }
            }
            


        }


        public class DirectDfs
        {
            public static string GetFileSizeString(string dfsfile)
            {
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                ValidateDfsProtocolDfsFileName(dfsfile);
                FastXml fx;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                }
                int filestart, fileend;
                if (!DfsFindFile(fx, dfsfile, out filestart, out fileend))
                {
                    throw new Exception("File not found in DFS: " + dfsfile);
                }
                int sizestart, sizeend;
                if (!fx.FindTag("Size", filestart, fileend, out sizestart, out sizeend))
                {
                    throw new Exception("Expected <Size> for DFS file '" + dfsfile + "'");
                }
                string ssize = fx.GetInnerText(sizestart, sizeend);
                return ssize;
            }

            public static long GetFileSize(string dfsfile)
            {
                string ssize = GetFileSizeString(dfsfile);
                long size;
                if (!long.TryParse(ssize, out size) || size < 0)
                {
                    throw new FormatException("<Size> for DFS file '" + dfsfile + "' has invalid value: " + ssize);
                }
                return size;
            }


            public static string GetFilePartCountString(string dfsfile)
            {
                int partcount = GetFilePartCount(dfsfile);
                string spartcount = partcount.ToString();
                return spartcount;
            }

            public static int GetFilePartCount(string dfsfile)
            {
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                ValidateDfsProtocolDfsFileName(dfsfile);
                FastXml fx;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                }
                int filestart, fileend;
                if (!DfsFindFile(fx, dfsfile, out filestart, out fileend))
                {
                    throw new Exception("File not found in DFS: " + dfsfile);
                }
                int partcount = 0;
                int pcstart, pcend = filestart;
                while (fx.FindTag("FileNode", pcend, fileend, out pcstart, out pcend))
                {
                    partcount++;
                }
                return partcount;
            }


            public static byte[] GetFileContent(string dfsfile)
            {
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                ValidateDfsProtocolDfsFileName(dfsfile);
                FastXml fx;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                }
                int filestart, fileend;
                if (!DfsFindFile(fx, dfsfile, out filestart, out fileend))
                {
                    throw new Exception("The specified file '" + dfsfile + "' does not exist in DFS");
                }
                byte[] content = ReadDfsFileContent(fx, filestart, fileend, dfsfile);
                return content;
            }


            public static void SetFileContent(string dfsfile, string dfsfiletype, byte[] content, int contentlength)
            {
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                ValidateDfsProtocolDfsFileName(dfsfile);

                FastXml fx;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                }

                string[] slaves = fx.GetTagInnerText("SlaveList").Split(';');
                int replfactor;
                if (!int.TryParse(fx.GetTagInnerText("Replication"), out replfactor))
                {
                    replfactor = 1;
                }
                string dfsfilexml = WriteDfsFileContent(dfsfile, dfsfiletype, content, contentlength, slaves, replfactor);
                List<string> oldfilenodepaths = null;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                    int fstart, fend;
                    if (DfsFindFile(fx, dfsfile, out fstart, out fend))
                    {
                        oldfilenodepaths = GetFileNodePaths(fx, fstart, fend);
                        fx.Replace(fstart, fend, dfsfilexml);
                    }
                    else
                    {
                        if (fx.FindTag("Files", out fstart, out fend))
                        {
                            fx.AppendChild(fstart, fend, dfsfilexml);
                        }
                        else
                        {
                            if (fx.FindTag("dfs", out fstart, out fend))
                            {
                                fx.AppendChild(fstart, fend, "<Files>" + dfsfilexml + "</Files>");
                            }
                            else
                            {
                                throw new Exception("Corrupt DFS.XML: dfs root tag not found");
                            }
                        }
                    }
                    {
                        string metabackupdir = null;
                        metabackupdir = fx.GetTagInnerText("MetaBackup");
                        if (null != metabackupdir && 0 == metabackupdir.Length)
                        {
                            metabackupdir = null;
                        }
                        WriteDfsXml_unlocked(fx.ToString(), DfsDir + @"\dfs.xml", metabackupdir);
                    }
                }
                if (null != oldfilenodepaths)
                {
                    MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                        new Action<string>(
                        delegate(string chunkpath)
                        {
                           try
                           {
                               System.IO.File.Delete(chunkpath);
                               System.IO.File.Delete(chunkpath + ".zsa");
                           }
                           catch
                           {
                           }
                        }), oldfilenodepaths);
                }
            }

            public static void SetFileContent(string dfsfile, string dfsfiletype, byte[] content)
            {
                SetFileContent(dfsfile, dfsfiletype, content, content.Length);
            }


            public static void DeleteFile(string dfsfile)
            {
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                ValidateDfsProtocolDfsFileName(dfsfile);
                FastXml fx;
                List<string> oldfilenodepaths;
                using (LockDfsMutex())
                {
                    fx = new FastXml(ReadDfs_unlocked(DfsDir + @"\dfs.xml"));
                    {
                        int filestart, fileend;
                        if (!DfsFindFile(fx, dfsfile, out filestart, out fileend))
                        {
                            throw new Exception("File not found in DFS: " + dfsfile);
                        }
                        oldfilenodepaths = GetFileNodePaths(fx, filestart, fileend);
                        fx.Replace(filestart, fileend, "");
                        {
                            string metabackupdir = null;
                            metabackupdir = fx.GetTagInnerText("MetaBackup");
                            if (null != metabackupdir && 0 == metabackupdir.Length)
                            {
                                metabackupdir = null;
                            }
                            WriteDfsXml_unlocked(fx.ToString(), DfsDir + @"\dfs.xml", metabackupdir);
                        }
                    }

                }
                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                    new Action<string>(
                    delegate(string chunkpath)
                    {
                        try
                        {
                            System.IO.File.Delete(chunkpath);
                            System.IO.File.Delete(chunkpath + ".zsa");
                        }
                        catch
                        {
                        }
                    }), oldfilenodepaths);
            }


            internal static void _Test_()
            {
#if DEBUG
                {
                    string dfsfile = "RDBMS_DirectDfs{" + Guid.NewGuid().ToString() + "}";
                    {
                        byte[] content = Encoding.UTF8.GetBytes("hello world" + Environment.NewLine);
                        SetFileContent(dfsfile, "zd", content);
                        if (content.Length != GetFileSize(dfsfile))
                        {
                            throw new Exception("DirectDfs test failed: unexpected file size");
                        }
                        {
                            byte[] xc = GetFileContent(dfsfile);
                            if (xc.Length != content.Length)
                            {
                                throw new Exception("DirectDfs test failed: contents don't match (Length)");
                            }
                            for (int i = 0; i < content.Length; i++)
                            {
                                if (content[i] != xc[i])
                                {
                                    throw new Exception("DirectDfs test failed: contents don't match (byte at offset " + i + ")");
                                }
                            }
                        }
                    }
                    // Now replace it:
                    {
                        byte[] content = Encoding.UTF8.GetBytes("Replacing what was there before!" + Environment.NewLine
                            + "and another line..." + Environment.NewLine);
                        SetFileContent(dfsfile, "rbin@1", content);
                        if (content.Length != GetFileSize(dfsfile))
                        {
                            throw new Exception("DirectDfs test failed: unexpected file size");
                        }
                        {
                            byte[] xc = GetFileContent(dfsfile);
                            if (xc.Length != content.Length)
                            {
                                throw new Exception("DirectDfs test failed: contents don't match (Length)");
                            }
                            for (int i = 0; i < content.Length; i++)
                            {
                                if (content[i] != xc[i])
                                {
                                    throw new Exception("DirectDfs test failed: contents don't match (byte at offset " + i + ")");
                                }
                            }
                        }
                    }
                    DeleteFile(dfsfile);
                }

#endif
            }


            static byte[] headerbuf = null; // Single threaded.
            static Random rnd = null; // Single threaded.


            static List<string> GetFileNodePaths(FastXml fx, int filestart, int fileend)
            {
                List<string> result = new List<string>();
                int start = filestart, end = fileend;
                for (; ; )
                {
                    int xs, xe;
                    if (!fx.FindTag("FileNode", start, end, out xs, out xe))
                    {
                        break;
                    }
                    {
                        string nname = fx.GetTagInnerText("Name", xs, xe);
                        string[] nhosts = fx.GetTagInnerText("Host", xs, xe).Split(';');
                        for (int i = 0; i < nhosts.Length; i++)
                        {
                            result.Add(ToNetworkPath(DfsDir, nhosts[i]) + @"\" + nname);
                        }
                    }
                    start = xe;
                }
                return result;
            }

            static List<string> GetFileNodePaths(FastXml fx, string dfsfile)
            {
                int filestart, fileend;
                if (!DfsFindFile(fx, dfsfile, out filestart, out fileend))
                {
                    throw new Exception("File not found in DFS: " + dfsfile);
                }
                return GetFileNodePaths(fx, filestart, fileend);
            }


            static byte[] ReadDfsFileContent(FastXml fx, int filestart, int fileend, string dfsfile)
            {
                long size;
                {
                    int sizestart, sizeend;
                    if (!fx.FindTag("Size", filestart, fileend, out sizestart, out sizeend))
                    {
                        throw new Exception("LoadDfsFileContents: Expected <Size> for DFS file '" + dfsfile + "'");
                    }
                    string ssize = fx.GetInnerText(sizestart, sizeend);
                    if (!long.TryParse(ssize, out size) || size < 0)
                    {
                        throw new FormatException("LoadDfsFileContents: <Size> for DFS file '" + dfsfile + "' has invalid value: " + ssize);
                    }
                    if (size > 1342177280)
                    {
                        throw new Exception("LoadDfsFileContents: file size (" + size + ") for DFS file '" + dfsfile + "' too large to load into memory");
                    }
                }
                byte[] content = new byte[size];
                int contentpos = 0;
                {
                    int cstart = filestart;
                    int cend = fileend;
                    for (; ; )
                    {
                        int nodestart, nodeend;
                        if (!fx.FindTag("FileNode", cstart, cend, out nodestart, out nodeend))
                        {
                            break;
                        }
                        string chunkname = fx.GetTagInnerText("Name", nodestart, nodeend);
                        string[] chunkhosts = fx.GetTagInnerText("Host", nodestart, nodeend).Split(';');
                        int chunkpos = 0; // Excluding header.
                        for (int ichunkhost = 0; ; )
                        {
                            try
                            {
                                string chunkpath = ToNetworkPath(DfsDir, chunkhosts[ichunkhost]) + @"\" + chunkname;
                                using (System.IO.FileStream fs = new System.IO.FileStream(chunkpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                {
                                    {
                                        if (null == headerbuf)
                                        {
                                            headerbuf = new byte[32];
                                        }
                                        // Skip the dfs-chunk file-header...
                                        int headerlength = 0;
                                        {
                                            if (4 != fs.Read(headerbuf, 0, 4))
                                            {
                                                throw new System.IO.IOException("Unable to read DFS chunk header: " + chunkpath);
                                            }
                                            {
                                                headerlength = MyBytesToInt(headerbuf, 0);
                                                if (headerlength > 4)
                                                {
                                                    int remain = headerlength - 4;
                                                    if (remain > headerbuf.Length)
                                                    {
                                                        headerbuf = new byte[remain + 100];
                                                    }
                                                    MyStreamReadExact(fs, headerbuf, remain);
                                                }
                                            }
                                        }
                                    }

                                    if (0 != chunkpos)
                                    {
                                        fs.Seek(chunkpos, System.IO.SeekOrigin.Current);
                                    }

                                    for (; ; )
                                    {

                                        int read = fs.Read(content, contentpos, content.Length - contentpos);
                                        if (read < 1)
                                        {
                                            break;
                                        }
                                        chunkpos += read;
                                        contentpos += read;
                                        if (contentpos > content.Length)
                                        {
                                            throw new Exception("LoadDfsFileContent: read too much data from DFS file '" + dfsfile + "'; file size reported is inaccurate");
                                        }
                                    }

                                }
                                break;
                            }
                            catch (System.IO.IOException)
                            {
                                ichunkhost++;
                                if (ichunkhost >= chunkhosts.Length)
                                {
                                    throw;
                                }
                                continue;
                            }
                        }
                        cstart = nodeend;
                    }
                }
                if (contentpos < content.Length)
                {
#if DEBUG
                    throw new Exception("DEBUG:  LoadDfsFileContent: (contentpos{" + contentpos
                        + "} < content.Length{" + content.Length + "}) for DFS file '" + dfsfile + "'");
#endif
                    byte[] newcontent = new byte[contentpos];
                    Buffer.BlockCopy(content, 0, newcontent, 0, contentpos);
                    content = newcontent;
                }
                return content;
            }


            // Returns XML for the FileNode.
            static string WriteDfsFileContent(string dfsfile, string dfsfiletype, byte[] content, int contentlength, string[] replhosts, int replfactor)
            {
                int reclen;
                bool writesamples;
                if (0 == string.Compare("zd", dfsfiletype, StringComparison.OrdinalIgnoreCase))
                {
                    reclen = -1;
                    writesamples = true;
                }
                else if (dfsfiletype.StartsWith("rbin@", StringComparison.OrdinalIgnoreCase))
                {
                    reclen = int.Parse(dfsfiletype.Substring(5));
                    writesamples = false;
                }
                else
                {
                    throw new NotSupportedException("Cannot create DFS file '" + dfsfile + "' of type '" + dfsfiletype + "': type not supported");
                }
                if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfsfile = dfsfile.Substring(6);
                }
                {
                    string badfnreason;
                    if (IsBadFilename(dfsfile, out badfnreason))
                    {
                        throw new Exception("Cannot set content for DFS file '" + dfsfile + "': " + badfnreason);
                    }
                }
                if (0 == contentlength)
                {
                    return string.Format("<DfsFile><Name>{0}</Name><Size>{1}</Size><Type>{2}</Type><Nodes/></DfsFile>",
                        dfsfile, contentlength, dfsfiletype);
                }
                if (-1 != reclen && contentlength < reclen)
                {
                    throw new Exception("Invalid size for DFS file '" + dfsfiletype + "': not a complete record");
                }
                string zdname = string.Format("zd.{0}.{1}.{2}.zd", 0, "directdfs", Guid.NewGuid().ToString());
                if (null == rnd)
                {
                    rnd = new Random(unchecked(System.Threading.Thread.CurrentThread.ManagedThreadId +
                        DateTime.Now.Millisecond + zdname.GetHashCode()));
                }
                List<string> onhosts = new List<string>(replfactor);
                int previhost = rnd.Next(0, replhosts.Length);
                for (int wtries = 0; onhosts.Count < replfactor; )
                {
                    try
                    {
                        if (++previhost >= replhosts.Length)
                        {
                            previhost = 0;
                        }
                        string zdhost = replhosts[previhost];
                        string zdpath = ToNetworkPath(DfsDir, zdhost) + @"\" + zdname;
                        using (System.IO.FileStream fs = new System.IO.FileStream(zdpath, System.IO.FileMode.CreateNew))
                        {
                            if (null == headerbuf)
                            {
                                headerbuf = new byte[32];
                            }
                            headerbuf[0] = 0;
                            headerbuf[1] = 0;
                            headerbuf[2] = 0;
                            headerbuf[3] = 4;
                            fs.Write(headerbuf, 0, 4);
                            fs.Write(content, 0, contentlength);
                        }
                        if (writesamples)
                        {
                            string zsapath = zdpath + ".zsa";
                            int samplelen;
                            if (-1 == reclen)
                            {
                                for (int ij = 0; ; ij++)
                                {
                                    if (ij == contentlength)
                                    {
                                        samplelen = contentlength;
                                        break;
                                    }
                                    if (content[ij] == '\n')
                                    {
                                        samplelen = ij + 1;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                samplelen = reclen;
                            }
                            using (System.IO.FileStream fs = new System.IO.FileStream(zsapath, System.IO.FileMode.CreateNew))
                            {
                                fs.Write(content, 0, samplelen);
                            }
                        }
                        onhosts.Add(zdhost);
                    }
                    catch (System.IO.IOException)
                    {
                        if (++wtries > 10 + replhosts.Length)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(1000);
                        continue;
                    }
                }
                return string.Format("<DfsFile><Name>{0}</Name><Size>{1}</Size><Type>{2}</Type><Nodes>"
                    + "<FileNode><Name>{3}</Name><Host>{4}</Host><Position>0</Position><Length>{5}</Length></FileNode></Nodes></DfsFile>",
                    dfsfile, contentlength, dfsfiletype,
                    zdname, string.Join(";", onhosts.ToArray()), contentlength);
            }


            static Int32 MyBytesToInt(IList<byte> x, int offset)
            {
                int result = 0;
                result |= (int)x[offset + 0] << 24;
                result |= (int)x[offset + 1] << 16;
                result |= (int)x[offset + 2] << 8;
                result |= x[offset + 3];
                return result;
            }

            static int MyStreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
            {
                int sofar = 0;
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

            static void MyStreamReadExact(System.IO.Stream stm, byte[] buf, int len)
            {
                if (len != MyStreamReadLoop(stm, buf, len))
                {
                    throw new System.IO.IOException("Unable to read from stream");
                }
            }


            public static string ToNetworkPath(string frompath, string host)
            {
                if (frompath.Length < 3
                       || ':' != frompath[1]
                       || '\\' != frompath[2]
                       || !char.IsLetter(frompath[0])
                       )
                {
                    if (frompath.StartsWith(@"\\"))
                    {
                        int ix = frompath.IndexOf('\\', 2);
                        if (-1 != ix)
                        {
                            return @"\\" + host + frompath.Substring(ix);
                        }
                    }
                    throw new Exception("ToNetworkPath invalid local path: " + frompath);
                }
                return @"\\" + host + @"\" + frompath.Substring(0, 1) + @"$" + frompath.Substring(2);
            }

            static string MySafeTextPath(string s)
            {
                StringBuilder sb = new StringBuilder();
                foreach (char ch in s)
                {
                    if (sb.Length >= 150)
                    {
                        sb.Append('`');
                        sb.Append(s.GetHashCode());
                        break;
                    }
                    if ('.' == ch)
                    {
                        if (0 == ch)
                        {
                            sb.Append("%2E");
                            continue;
                        }
                    }
                    if (!char.IsLetterOrDigit(ch)
                        && '_' != ch
                        && '-' != ch
                        && '.' != ch)
                    {
                        sb.Append('%');
                        if (ch > 0xFF)
                        {
                            sb.Append('u');
                            sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                        }
                        else
                        {
                            sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                        }
                    }
                    else
                    {
                        sb.Append(ch);
                    }
                }
                if (0 == sb.Length)
                {
                    return "_";
                }
                return sb.ToString();
            }


            static void ValidateDfsProtocolDfsFileName(string dfsfile)
            {
                {
                    string reason;
                    if (IsBadFilename(dfsfile, out reason))
                    {
                        throw new Exception("DFS file '" + dfsfile + "' error: " + reason);
                    }
                }
                if (!dfsfile.StartsWith("RDBMS_", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("Unable to operate on DFS file named " + dfsfile);
                }
            }

            static bool IsBadFilename(string filename, out string reason)
            {
                if (filename.Length == 0)
                {
                    reason = "Empty name";
                    return true;
                }
                for (int i = 0; i < filename.Length; i++)
                {
                    if (!char.IsLetterOrDigit(filename[i])
                        && '_' != filename[i]
                        && '-' != filename[i]
                        && '.' != filename[i]
                        && '~' != filename[i]
                        && '(' != filename[i]
                        && ')' != filename[i]
                        && '{' != filename[i]
                        && '}' != filename[i]
                        )
                    {
                        reason = "Illegal characters in name";
                        return true;
                    }
                }
                reason = "Good";
                return false;
            }


            class AEDFSM : IDisposable
            {
                internal System.Threading.Mutex mutex;

                public void Dispose()
                {
                    if (null != mutex)
                    {
                        mutex.ReleaseMutex();
                        mutex.Close();
                        mutex = null;
                    }
                }
            }

            static IDisposable LockDfsMutex()
            {
                AEDFSM x = new AEDFSM();
                System.Threading.Mutex mutex = null;
                mutex = new System.Threading.Mutex(false, "AEDFSM");
                try
                {
                    mutex.WaitOne(); // Lock also taken by Qizmt kill.
                }
                catch (System.Threading.AbandonedMutexException)
                {
                }
                x.mutex = mutex;
                return x;
            }


            static string ReadDfs_unlocked(string dfsxmlpath)
            {
                {
                    System.IO.Stream stm = null;
                    for (; ; )
                    {
                        try
                        {
                            if (null != stm)
                            {
                                stm.Close();
                                stm = null;
                            }

                            {
                                const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                                const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                                int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                                for (; ; )
                                {
                                    try
                                    {
                                        stm = new System.IO.FileStream(dfsxmlpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                                        break;
                                    }
                                    catch (System.IO.FileNotFoundException fnf)
                                    {
                                        if (--iters < 0)
                                        {
                                            throw;
                                        }
                                        System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                        continue;
                                    }
                                }
                            }

#if DEBUG
                            //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        }
                        catch (System.IO.FileNotFoundException e)
                        {
                            //throw;
                            throw new System.IO.FileNotFoundException("DFS does not exist; see dfs format", e);
                        }
                        catch (System.IO.IOException e)
                        {
                            continue;
                        }
                        break;
                    }

                    using (System.IO.StreamReader sr = new System.IO.StreamReader(stm))
                    {
                        return sr.ReadToEnd();
                    }

                }
            }

            static void _UpdateDfsXml_retryexception(string action, Exception e)
            {
                {
                    try
                    {
                        System.IO.File.AppendAllText("DFS.XML.EXCEPTION",
                            "[" + DateTime.Now.ToString() + "] {" + action + "} "
                            + e.ToString()
                            + Environment.NewLine
                            + "-----------------------"
                            + Environment.NewLine);
                        System.Threading.Thread.Sleep(1000 * 2);
                    }
                    catch (Exception eludxe)
                    {
                        throw new Exception("Exception '" + eludxe.Message + "' while logging update-dfs.xml {" + action + "} exception: " + e.Message, e);
                    }
                }
            }

            static void WriteDfsXml_unlocked(string dfsxmlc, string dfsxmlpath, string backupdir)
            {
                {
                    string oldpath = dfsxmlpath + ".old";
                    string newpath = dfsxmlpath + ".new";

                    //+++metabackup+++
                    string backupfile = null, oldbackupfile = null, newbackupfile = null;
                    if (null != backupdir)
                    {
                        backupfile = backupdir + @"\dfs-" + MySafeTextPath(System.Net.Dns.GetHostName()) + @".xml";
                        oldbackupfile = backupfile + ".old";
                        newbackupfile = backupfile + ".new";
                    }
                    //---metabackup---

                    // Write .new
                    for (; ; )
                    {
                        try
                        {
                            using (System.IO.FileStream fdc = new System.IO.FileStream(newpath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
                            {
                                using (System.IO.StreamWriter sw = new System.IO.StreamWriter(fdc))
                                {
                                    sw.Write(dfsxmlc);
                                }
                            }
                            //+++metabackup+++
                            if (null != backupdir)
                            {
                                System.IO.File.Copy(newpath, newbackupfile, true);
                            }
                            //---metabackup---
                            break;
                        }
                        catch (System.IO.IOException e)
                        {
                            _UpdateDfsXml_retryexception("write-new", e);
                            continue;
                        }
                    }

                    // Move current to .old
                    for (; ; )
                    {
                        try
                        {
                            try
                            {
                                System.IO.File.Delete(oldpath);
                            }
                            catch
                            {
                            }
                            // Check if dfs.xml exists first, because this might be the first save on format.
                            if (System.IO.File.Exists(dfsxmlpath)) // Not using dfs.DfsConfigExists()!
                            {
                                System.IO.File.Move(dfsxmlpath, oldpath);
                            }
                            break;
                        }
                        catch (System.IO.IOException e)
                        {
                            _UpdateDfsXml_retryexception("move-old", e);
                            continue;
                        }
                    }
                    //+++metabackup+++
                    if (null != backupdir)
                    {
                        for (; ; )
                        {
                            try
                            {
                                try
                                {
                                    System.IO.File.Delete(oldbackupfile);
                                }
                                catch
                                {
                                }
                                // See if backup dfs.xml exists first, because metabackup might not have been on.
                                if (System.IO.File.Exists(backupfile))
                                {
                                    System.IO.File.Move(backupfile, oldbackupfile);
                                }
                                break;
                            }
                            catch (System.IO.IOException e)
                            {
                                _UpdateDfsXml_retryexception("metabackup-move-old", e);
                                continue;
                            }
                        }
                    }
                    //---metabackup---

                    // Move .new to current.
                    for (; ; )
                    {
                        try
                        {
                            System.IO.File.Move(newpath, dfsxmlpath);
                            break;
                        }
                        catch (System.IO.IOException e)
                        {
                            _UpdateDfsXml_retryexception("move-new", e);
                            continue;
                        }
                    }
                    //+++metabackup+++
                    if (null != backupdir)
                    {
                        for (; ; )
                        {
                            try
                            {
                                System.IO.File.Move(newbackupfile, backupfile);
                                break;
                            }
                            catch (System.IO.IOException e)
                            {
                                _UpdateDfsXml_retryexception("metabackup-move-new", e);
                                continue;
                            }
                        }
                    }
                    //---metabackup---

                    // Delete .old
                    try
                    {
                        System.IO.File.Delete(oldpath);
                    }
                    catch
                    {
                    }
                    //+++metabackup+++
                    if (null != backupdir)
                    {
#if DEBUG
                        //System.Diagnostics.Debugger.Launch();
#endif
                        try
                        {
                            if (System.IO.File.Exists(oldbackupfile))
                            {
                                long lastprevid;
                                System.IO.FileInfo bfi = new System.IO.FileInfo(backupfile);
                                {
                                    System.IO.FileInfo[] prevfiles = bfi.Directory.GetFiles(bfi.Name + ".*.prev");
                                    Func<string, long> OrdFromXmlPrev = delegate(string fp)
                                    {
                                        int d = fp.LastIndexOf('.', fp.Length - 5 - 1) + 1;
                                        string snum = fp.Substring(d, (fp.Length - 5) - d);
                                        return long.Parse(snum);
                                    };
                                    Array.Sort<System.IO.FileInfo>(prevfiles, new Comparison<System.IO.FileInfo>(
                                        delegate(System.IO.FileInfo fi1, System.IO.FileInfo fi2)
                                        {
                                            long ord1 = OrdFromXmlPrev(fi1.ToString());
                                            long ord2 = OrdFromXmlPrev(fi2.ToString());
                                            if (ord1 < ord2)
                                            {
                                                return -1;
                                            }
                                            if (ord1 > ord2)
                                            {
                                                return +1;
                                            }
                                            return 0;
                                        }));
                                    const int DFSXMLPREVMAXCOUNT = 10;
                                    for (int i = 0; i < prevfiles.Length - (DFSXMLPREVMAXCOUNT - 1); i++)
                                    {
                                        try
                                        {
                                            System.IO.File.Delete(prevfiles[i].FullName);
                                        }
                                        catch
                                        {
                                        }
                                    }
                                    if (prevfiles.Length > 0)
                                    {
                                        lastprevid = OrdFromXmlPrev(prevfiles[prevfiles.Length - 1].ToString());
                                    }
                                    else
                                    {
                                        lastprevid = 0;
                                    }
                                }

                                long newprevid;
                                if (lastprevid == long.MaxValue)
                                {
                                    newprevid = 1;
                                }
                                else
                                {
                                    newprevid = lastprevid + 1;
                                }
                                System.IO.File.Move(oldbackupfile, bfi.FullName + "." + newprevid + ".prev");

                            }
                        }
                        catch
                        {
                        }
                        try
                        {
                            System.IO.File.Delete(oldbackupfile);
                        }
                        catch
                        {
                        }
                    }
                    //---metabackup---

                }
            }


            static bool DfsFindFile(FastXml fx, string dfsfile, out int tagstart, out int tagend)
            {
                int filestart, fileend;
                for (int pos = 0; fx.FindTag("DfsFile", pos, out filestart, out fileend); pos = fileend)
                {
                    int namestart, nameend;
                    if (fx.FindTag("Name", filestart, fileend, out namestart, out nameend))
                    {
                        string onname = fx.GetInnerText(namestart, nameend);
                        if (0 == string.Compare(onname, dfsfile, StringComparison.OrdinalIgnoreCase))
                        {
                            tagstart = filestart;
                            tagend = fileend;
                            return true;
                        }
                    }
                }
                tagstart = 0;
                tagend = 0;
                return false;
            }
        }


        public struct FastXml
        {
            string xml;

            public FastXml(string xml)
            {
                this.xml = xml;
            }

            // Warning: looking for tag Foo in "<Foo><Foo></Foo></Foo>" will return "<Foo><Foo></Foo>".
            // Warning: doesn't consider CDATA or comments.
            public bool FindTag(string name, int startindex, int endindex, out int tagstart, out int tagend)
            {
                int end = endindex;
                // Find opening tag.
                int i = startindex;
                for (; ; i++)
                {
                    if (i >= end)
                    {
                        tagstart = end;
                        tagend = tagstart;
                        return false;
                    }
                    if (xml[i] == '<')
                    {
                        if (IsStringAt(xml, name, i + 1))
                        {
                            int j = i + 1 + name.Length;
                            if (j < end &&
                                (char.IsWhiteSpace(xml[j]) || '>' == xml[j] || '/' == xml[j]))
                            {
                                tagstart = i;
                                i = j;
                                break;
                            }
                        }
                    }
                }
                // Find closing tag.
                bool foundgt = false; // Found opening tag's > yet? if not, look for />
                bool prevslash = false; // Only useful with foundgt.
                bool prevlt = false;
                bool prevltslash = false;
                for (; ; i++)
                {
                    if (i >= end)
                    {
                        break;
                    }
                    if (!foundgt)
                    {
                        if (xml[i] == '>')
                        {
                            if (prevslash)
                            {
                                tagend = i + 1;
                                return true;
                            }
                            foundgt = true;
                        }
                        if (xml[i] == '/')
                        {
                            prevslash = true;
                        }
                        else if(!char.IsWhiteSpace(xml[i]))
                        {
                            prevslash = false;
                        }
                    }
                    if (!char.IsWhiteSpace(xml[i]))
                    {
                        if (prevltslash)
                        {
                            prevltslash = false;
                            if (IsStringAt(xml, name, i))
                            {
                                int j = i + name.Length;
                                if (j < end &&
                                    (char.IsWhiteSpace(xml[j]) || '>' == xml[j]))
                                {
                                    i = j;
                                    for (; i < end; i++)
                                    {
                                        if (xml[i] == '>')
                                        {
                                            tagend = i + 1;
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                        if (prevlt)
                        {
                            prevltslash = false;
                            if (xml[i] == '/')
                            {
                                prevltslash = true;
                            }
                        }
                        prevlt = false;
                        if (xml[i] == '<')
                        {
                            prevlt = true;
                        }
                    }
                }
                tagend = end;
                return false;
            }

            public bool FindTag(string name, int startindex, out int tagstart, out int tagend)
            {
                return FindTag(name, startindex, xml.Length, out tagstart, out tagend);
            }

            public bool FindTag(string name, out int tagstart, out int tagend)
            {
                return FindTag(name, 0, xml.Length, out tagstart, out tagend);
            }


            // Finds last closing tag between start and end and inserts childxml before it.
            public void AppendChild(int start, int end, string childxml)
            {
                bool prevslash = false;
                for (int i = end - 1; i >= start; i--)
                {
                    if (xml[i] == '/')
                    {
                        prevslash = true;
                    }
                    else if (xml[i] == '<' && prevslash)
                    {
                        Replace(i, i, childxml);
                        return;
                    }
                    else if (!char.IsWhiteSpace(xml[i]))
                    {
                        prevslash = false;
                    }
                }
                throw new Exception("Unable to append child XML: " + childxml);
            }


            static bool IsStringAt(string LookIn, string LookFor, int LookAtIndex)
            {
                for (int i = 0; ; i++)
                {
                    if (i >= LookFor.Length)
                    {
                        return true;
                    }
                    if (LookAtIndex + i >= LookIn.Length)
                    {
                        break;
                    }
                    if (LookIn[LookAtIndex + i] != LookFor[i])
                    {
                        break;
                    }
                }
                return false;
            }


            public string GetInnerText(int start, int end)
            {
                StringBuilder sb = new StringBuilder();
                bool intag = false;
                bool needspace = false;
                bool anyamps = false;
                for (int i = start; i < end; i++)
                {
                    if (intag)
                    {
                        if (xml[i] == '>')
                        {
                            intag = false;
                            needspace = 0 != sb.Length;
                        }
                    }
                    else
                    {
                        if (xml[i] == '<')
                        {
                            intag = true;
                        }
                        else
                        {
                            if (char.IsWhiteSpace(xml[i]))
                            {
                                needspace = 0 != sb.Length;
                            }
                            else
                            {
                                if (needspace)
                                {
                                    sb.Append(' ');
                                    needspace = false;
                                }
                                sb.Append(xml[i]);
                                if (xml[i] == '&')
                                {
                                    anyamps = true;
                                }
                            }
                        }
                    }
                }
                if (anyamps)
                {
                    System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                    xd.LoadXml("<x></x>");
                    xd["x"].InnerXml = sb.ToString();
                    return xd["x"].InnerText;
                }
                return sb.ToString();
            }

            public string GetTagInnerText(string FindTagName, int start, int end)
            {
                int findstart, findend;
                if (!FindTag(FindTagName, start, end, out findstart, out findend))
                {
                    return "";
                }
                return GetInnerText(findstart, findend);
            }

            public string GetTagInnerText(string FindTagName)
            {
                return GetTagInnerText(FindTagName, 0, xml.Length);
            }


            public void Replace(int start, int end, string ReplaceWith)
            {
                xml = xml.Substring(0, start) + ReplaceWith + xml.Substring(end);
            }

            public void Insert(int index, string InsertWhat)
            {
                xml = xml.Substring(0, index) + InsertWhat + xml.Substring(index);
            }

            public int Length
            {
                get
                {
                    return xml.Length;
                }
            }

            public override string ToString()
            {
                return xml;
            }


            internal static void _Test_()
            {
#if DEBUG
                {
                    FastXml fx = new FastXml("foo <foo> foo </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 4 != start || 20 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("foo <foo > foo </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 4 != start || 21 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("foo <foo foo=`hi`> foo </foo> foo".Replace('`', '"'));
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 4 != start || 29 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("foo <foo/> foo </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 4 != start || 10 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foofoo> <foo> foo </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 9 != start || 25 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foofoo> <foo> foo </foofoo> </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 9 != start || 35 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foofoo foo=`foo`> <foo/> foo </foofoo> </foo> foo".Replace('`', '"'));
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 19 != start || 25 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foofoo/> <foo/> foo </foofoo> </foo> foo");
                    int start, end;
                    if (!fx.FindTag("foo", 0, out start, out end) || 10 != start || 16 != end)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }

                {
                    FastXml fx = new FastXml("<foo>bar</foo>");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }

                {
                    FastXml fx = new FastXml(" <foo> bar </foo> ");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml(" <foo> <hello> bar </hello> baz </foo> ");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar baz")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foo> bar<x/><hello>baz</hello>");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar baz")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foo> bar<x/><hello/>baz</foo>");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar baz")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml("<foo> bar<x>qux</x><hello/>baz</foo>");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "bar qux baz")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml(" a <foo/> b ");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
                {
                    FastXml fx = new FastXml(" <foo> a <x/> &lt; b </foo> ");
                    int start, end;
                    fx.FindTag("foo", 0, out start, out end);
                    string s = fx.GetInnerText(start, end);
                    if (s != "a < b")
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw new Exception("TEST FAILED");
                    }
                }
#endif
            }


        }


    }

}
