using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Protocol
{
    partial class QueryAnalyzer_Protocol
    {
        public class RIndex
        {
            public string IndexName;
            public int RowSize;
            public int KeyOffset;
            public int KeySize;
            //public string ChunkInfoString;
            public string[] ChunkInfo;
            public MySpace.DataMining.Binary.LongByteArray KeyInfo;
        }

        public class RIndexServClientHandler
        {

            public static void LoadRIndexMI()
            {
                foreach (string zukfile in System.IO.Directory.GetFiles(".", "zuk.Index.*.zuk"))
                {
                    LoadOneRIndexMI(zukfile);
                }
            }

            static void LoadOneRIndexMI(string zukfile)
            {
                using (System.IO.FileStream fs = new System.IO.FileStream(zukfile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                {
                    long zuksize = fs.Length;

                }
            }


            System.Net.Sockets.NetworkStream netstm;
            byte[] buf = new byte[1024 * 1024 * 8];
            int buflen;


            internal void ClientThreadProc(object _sock)
            {
                System.Net.Sockets.Socket clientsock = (System.Net.Sockets.Socket)_sock;
                netstm = new System.Net.Sockets.NetworkStream(clientsock);

                try
                {
                    for (bool stop = false; !stop; )
                    {
                        int ib = netstm.ReadByte();
                        if (ib < 0)
                        {
                            stop = true;
                            break;
                        }

                        switch (ib)
                        {
                            case (byte)'p': // Get partial.
                                {
                                    try
                                    {
                                        string IndexName = XContent.ReceiveXString(netstm, buf);
                                        string ChunksDir = XContent.ReceiveXString(netstm, buf);
                                        string ChunkInfoString = XContent.ReceiveXString(netstm, buf);
                                        string[] ChunkInfo = ChunkInfoString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries); // "<chunkname> <size>"
                                        XContent.ReceiveXBytes(netstm, out buflen, buf);
                                        if (buflen < 4)
                                        {
                                            throw new Exception("Expected int RowSize, didn't get enough bytes");
                                        }
                                        int RowSize = ClientHandler.BytesToInt(buf, 0);
                                        const int KeyOffset = 0;
                                        const int KeySize = 1 + 8;

                                        if (buf.Length < RowSize)
                                        {
                                            buf = new byte[RowSize];
                                        }

                                        List<byte> firstkeysbuf = new List<byte>(ChunkInfo.Length * KeySize);
                                        byte[] lastkey = null;
                                        string zukfile = "zuk.Index." + IndexName + ".zuk";
                                        using (System.IO.FileStream fUniqueKeys = new System.IO.FileStream(zukfile, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None))
                                        {
                                            {
                                                byte[] IndexNameBytes = Encoding.UTF8.GetBytes(IndexName);
                                                ClientHandler.MyToBytes(IndexNameBytes.Length, buf, 0);
                                                fUniqueKeys.Write(buf, 0, 4);
                                                fUniqueKeys.Write(IndexNameBytes, 0, IndexNameBytes.Length);
                                            }
                                            {
                                                ClientHandler.MyToBytes(RowSize, buf, 0);
                                                fUniqueKeys.Write(buf, 0, 4);
                                            }
                                            {
                                                ClientHandler.MyToBytes(KeyOffset, buf, 0);
                                                fUniqueKeys.Write(buf, 0, 4);
                                            }
                                            {
                                                ClientHandler.MyToBytes(KeySize, buf, 0);
                                                fUniqueKeys.Write(buf, 0, 4);
                                            }
                                            {
                                                byte[] ChunkInfoStringBytes = Encoding.UTF8.GetBytes(ChunkInfoString);
                                                ClientHandler.MyToBytes(ChunkInfoStringBytes.Length, buf, 0);
                                                fUniqueKeys.Write(buf, 0, 4);
                                                fUniqueKeys.Write(ChunkInfoStringBytes, 0, ChunkInfoStringBytes.Length);
                                            }
                                            for (int ic = 0; ic < ChunkInfo.Length; ic++)
                                            {
                                                string info = ChunkInfo[ic];
                                                int isp = info.IndexOf(' ');
                                                string chunkname = info.Substring(0, isp);
                                                string ssize = info.Substring(isp + 1);
                                                int size = int.Parse(ssize);
                                                using (System.IO.FileStream fs = new System.IO.FileStream(ChunksDir + @"\" + chunkname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                                {
                                                    // Skip the dfs-chunk file-header...
                                                    int headerlength = 0;
                                                    {
                                                        if (4 != fs.Read(buf, 0, 4))
                                                        {
                                                            continue;
                                                        }
                                                        {
                                                            headerlength = ClientHandler.BytesToInt(buf, 0);
                                                            if (headerlength > 4)
                                                            {
                                                                int hremain = headerlength - 4;
                                                                if (hremain > buf.Length)
                                                                {
                                                                    buf = new byte[hremain];
                                                                }
                                                                ClientHandler.StreamReadExact(fs, buf, hremain);
                                                            }
                                                        }
                                                    }
                                                    {
                                                        bool first = true;
                                                        // Seek to the first key...
                                                        fs.Seek(KeyOffset, System.IO.SeekOrigin.Current);
                                                        for (int offset = 0; offset + RowSize <= size; offset += RowSize)
                                                        {
                                                            ClientHandler.StreamReadExact(fs, buf, KeySize);
                                                            bool same;
                                                            if (null == lastkey)
                                                            {
                                                                same = false;
                                                                lastkey = new byte[KeySize];
                                                            }
                                                            else
                                                            {
                                                                same = true;
                                                                for (int ik = 0; ik < KeySize; ik++)
                                                                {
                                                                    if (lastkey[ik] != buf[ik])
                                                                    {
                                                                        same = false;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            if (!same)
                                                            {
                                                                if (0 == buf[0]) // if(false==IsNull) For now: only including non-nulls.
                                                                {
                                                                    // For now: sending keys of KeySize-1 to exclude Nullable IsNull.
                                                                    if (first)
                                                                    {
                                                                        first = false;
                                                                        for (int ik = 1; ik < KeySize; ik++) // For now: start at 1.
                                                                        {
                                                                            firstkeysbuf.Add(buf[ik]);
                                                                        }
                                                                    }

                                                                    {
                                                                        fUniqueKeys.Write(buf, 1, KeySize - 1); // For now: start at 1.
                                                                    }
                                                                    {
                                                                        ClientHandler.MyToBytes(headerlength + offset, buf, 0);
                                                                        fUniqueKeys.Write(buf, 0, 4);
                                                                    }
                                                                    {
                                                                        ClientHandler.MyToBytes(ic, buf, 0); // Offset in chunk files list.
                                                                        fUniqueKeys.Write(buf, 0, 4);
                                                                    }
                                                                    {
                                                                        ClientHandler.MyToBytes(headerlength + offset, buf, 0); // Offset to KV in chunk file.
                                                                        fUniqueKeys.Write(buf, 0, 4);
                                                                    }

                                                                }
                                                                for (int ik = 0; ik < KeySize; ik++)
                                                                {
                                                                    lastkey[ik] = buf[ik];
                                                                }
                                                            }
                                                            // Seek up to the next key...
                                                            fs.Seek(RowSize - KeySize + KeyOffset, System.IO.SeekOrigin.Current);
                                                        }
                                                    }

                                                }
                                            }

                                            // Footer.
                                            ClientHandler.MyToBytes(1745264789, buf, 0);
                                            fUniqueKeys.Write(buf, 0, 4);
                                        }

                                        LoadOneRIndexMI(zukfile);

                                        XContent.SendXContent(netstm, firstkeysbuf.ToArray());
                                        netstm.WriteByte((byte)'+');
                                    }
                                    catch(Exception ep)
                                    {
                                        netstm.WriteByte((byte)'-');
                                        XContent.SendXContent(netstm, ep.ToString());
                                        throw;
                                    }
                                }
                                break;

                            case (byte)'i': // Getting all Master Indexes.
                                try
                                {
                                    System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(".");
                                    System.IO.FileInfo[] indfiles = dir.GetFiles("ind.Index.*.ind");
                                    ClientHandler.MyToBytes(indfiles.Length, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);
                                    for (int fi = 0; fi < indfiles.Length; fi++)
                                    {
                                        System.IO.FileInfo indfile = indfiles[fi];
                                        string[] parts = indfile.Name.Split('.');
                                        string indexName = parts[2];
                                        XContent.SendXContent(netstm, indexName);
                                        if (indfile.Length > 0)
                                        {
                                            using (System.IO.FileStream fs = new System.IO.FileStream(indfile.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                            {
                                                int filelen = (int)indfile.Length;
                                                if (buf.Length < filelen)
                                                {
                                                    buf = new byte[filelen];
                                                }
                                                fs.Read(buf, 0, filelen);
                                                XContent.SendXContent(netstm, buf, filelen);
                                                fs.Close();
                                            }
                                        }
                                        else
                                        {
                                            XContent.SendXContent(netstm, buf, 0);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                    throw;
                                }                              
                                break;

                            case (byte)'s': //Search master index.
                                {
                                    string indexname = XContent.ReceiveXString(netstm, buf);
                                    byte[] keybuf = new byte[9];
                                    XContent.ReceiveXBytes(netstm, out buflen, keybuf);
                                    string chunkname = XContent.ReceiveXString(netstm, buf);
                                    bool ispin = false;

                                    //Query succeeded.
                                    netstm.WriteByte((byte)'+');
                                   
                                    int columncount = 3;
                                    ClientHandler.MyToBytes(columncount, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);
                                   
                                    for (int i = 0; i < columncount; i++)
                                    {
                                        string cname = "Column" + i.ToString();
                                        Type ctype = typeof(System.Int64);
                                        int csize = 8;
                                        int frontbytes = 1;
                                        int backbytes = 0;

                                        XContent.SendXContent(netstm, cname);
                                        XContent.SendXContent(netstm, ctype.FullName);
                                        ClientHandler.MyToBytes(frontbytes, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                        ClientHandler.MyToBytes(csize, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                        ClientHandler.MyToBytes(backbytes, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                    }

                                    //return rows. 
                                    List<byte> allrowsbuf = new List<byte>(1024 * 1024 * 1);
                                    if (chunkname.Length > 0)
                                    {                 
                                        List<byte[]> chunkbuf = LoadFileChunk(chunkname, indexname, ispin, true);
                                        if (chunkbuf.Count == 0)
                                        {
                                            throw new Exception("Chunk referenced by a master index is not supposed to be empty: " + chunkname);
                                        }

                                        int left = 0;
                                        int right = chunkbuf.Count - 1;
                                        int result = BSearch(chunkbuf, keybuf, ref left, ref right, 9);
                                        if (result > -1) 
                                        {
                                            int startingrowpos = result;

                                            //begin a row       
                                            for (int ci = 0; ci < 27; ci++)
                                            {
                                                allrowsbuf.Add(chunkbuf[result][ci]);
                                            }

                                            bool lookforward = true;                                            
                                            int rowpos = startingrowpos;
                                            for (; ; )
                                            {
                                                if (++rowpos > chunkbuf.Count - 1)
                                                {
                                                    break;
                                                }
                                                if (CompareBytes(chunkbuf[rowpos], keybuf, 9) == 0)
                                                {
                                                    for (int ci = 0; ci < 27; ci++)
                                                    {
                                                        allrowsbuf.Add(chunkbuf[rowpos][ci]);
                                                    }
                                                }
                                                else
                                                {
                                                    lookforward = false;
                                                    break;
                                                }
                                            }

                                            bool lookbackward = true;
                                            rowpos = startingrowpos;
                                            for (; ; )
                                            {
                                                if (--rowpos < 0)
                                                {
                                                    break;
                                                }
                                                if (CompareBytes(chunkbuf[rowpos], keybuf, 9) == 0)
                                                {
                                                    for (int ci = 0; ci < 27; ci++)
                                                    {
                                                        allrowsbuf.Add(chunkbuf[rowpos][ci]);
                                                    }
                                                }
                                                else
                                                {
                                                    lookbackward = false;
                                                    break;
                                                }
                                            }

                                            int startingchunkpos = -1;
                                            List<KeyValuePair<string, byte[]>> mi = null;
                                            if (lookforward || lookbackward)
                                            {
                                                mi = LoadMasterIndex(indexname, ref ispin);                                                
                                                for (int i = 0; i < mi.Count; i++)
                                                {
                                                    if (string.Compare(chunkname, mi[i].Key, true) == 0)
                                                    {
                                                        startingchunkpos = i;
                                                        break;
                                                    }
                                                }
                                                if (startingchunkpos == -1)
                                                {
                                                    throw new Exception("Chunk file name " + chunkname + " is not found in master index: " + indexname);
                                                }
                                            }

                                            int chunkpos = startingchunkpos;
                                            while (lookforward)
                                            {         
                                                if (++chunkpos > mi.Count - 1)
                                                {
                                                    break;
                                                }
                                                else if (CompareBytes(mi[chunkpos].Value, keybuf, 9) != 0)
                                                {
                                                    break;
                                                }
                                                else
                                                {
                                                    chunkbuf = LoadFileChunk(mi[chunkpos].Key, indexname, ispin, false);
                                                    rowpos = -1;
                                                }
                                                  
                                                for (; ; )
                                                {
                                                    if (++rowpos > chunkbuf.Count - 1)
                                                    {
                                                        break;
                                                    }
                                                    if (CompareBytes(chunkbuf[rowpos], keybuf, 9) == 0)
                                                    {
                                                        for (int ci = 0; ci < 27; ci++)
                                                        {
                                                            allrowsbuf.Add(chunkbuf[rowpos][ci]);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        lookforward = false;
                                                        break;
                                                    }
                                                }                 
                                            }

                                            chunkpos = startingchunkpos;
                                            while (lookbackward)
                                            {
                                                if (--chunkpos < 0)
                                                {
                                                    break;
                                                }
                                                else
                                                {
                                                    chunkbuf = LoadFileChunk(mi[chunkpos].Key, indexname, ispin, false);
                                                    rowpos = chunkbuf.Count;
                                                }

                                                for (; ; )
                                                {
                                                    if (--rowpos < 0)
                                                    {
                                                        break;
                                                    }
                                                    if (CompareBytes(chunkbuf[rowpos], keybuf, 9) == 0)
                                                    {
                                                        for (int ci = 0; ci < 27; ci++)
                                                        {
                                                            allrowsbuf.Add(chunkbuf[rowpos][ci]);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        lookbackward = false;
                                                        break;
                                                    }
                                                } 
                                            }
                                        }
                                    }
                                    XContent.SendXContent(netstm, allrowsbuf.ToArray());                                 
                                    //end all rows.
                                }
                                break;

                            case (byte)'c': // Close.
                                stop = true;
                                netstm.WriteByte((byte)'+');
                                break;

                            default:
                                throw new Exception("unknown action");
                        }
                    }
                }
                catch (Exception e)
                {
                    XLog.errorlog("RIndexServClientHandler.ClientThreadProc exception: " + e.ToString());
                }
                finally
                {
                    netstm.Close();
                    netstm = null;
                    clientsock.Close();
                    clientsock = null;
                }
            }

            private int CompareBytes(byte[] x, byte[] y, int length)
            {
                for (int i = 0; i < length; i++)
                {
                    if (x[i] != y[i])
                    {
                        return x[i] - y[i];
                    }
                }
                return 0;
            }

            private int BSearch(List<byte[]> keys, byte[] key, ref int left, ref int right, int length)
            {
                int cl = CompareBytes(key, keys[left], length);
                if (cl == 0)
                {
                    return left;
                }
                int cr = CompareBytes(key, keys[right], length);
                if (cr == 0)
                {
                    return right;
                }
                if (left == right && cl > 0)
                {
                    return -1;
                }
                if (cl < 0 || cr > 0)   //out of range
                {
                    return -2;
                }
                if (right - left < 2)
                {
                    return -1;   //nothing in between 
                }
                int mid = (right - left) / 2 + left;
                int cm = CompareBytes(key, keys[mid], length);
                if (cm == 0)
                {
                    return mid;
                }
                if (cm > 0)
                {
                    left = mid;
                }
                else
                {
                    right = mid;
                }
                return BSearch(keys, key, ref left, ref right, length);
            }

            private List<KeyValuePair<string, byte[]>> LoadMasterIndex(string indexName, ref bool isPinMemory)
            {
                string indexfn = CurrentDirNetPath + @"\ind.Index." + indexName + ".ind";
                List<KeyValuePair<string, byte[]>> mi = new List<KeyValuePair<string, byte[]>>();

                using (System.IO.FileStream fs = new System.IO.FileStream(indexfn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                {
                    for (; ; )
                    {
                        byte[] keybuf = new byte[9];
                        if (fs.Read(keybuf, 0, 9) < 9)
                        {
                            break;
                        }                        
                        int chunknamebuflen = 0;
                        for (; ; )
                        {
                            int nextb = fs.ReadByte();
                            if (nextb == -1 || nextb == (byte)'\0')
                            {
                                break;
                            }
                            buf[chunknamebuflen++] = (byte)nextb;
                        }
                        string chunkname = System.Text.Encoding.UTF8.GetString(buf, 0, chunknamebuflen).ToLower();
                        mi.Add(new KeyValuePair<string, byte[]>(chunkname, keybuf));
                    }
                    fs.Close();
                }

                string pinfn = CurrentDirNetPath + @"\ind.Pin." + indexName + ".ind";
                isPinMemory = System.IO.File.Exists(pinfn);
                return mi;
            }

            private List<byte[]> LoadFileChunk(string chunkname, string indexname, bool ispin, bool determinepinornot)
            {
                string _chunkname = chunkname.ToLower();
                string _indexname = indexname.ToLower();
                lock (rindexpins)
                {
                    if (rindexpins.ContainsKey(_indexname))
                    {
                        if (rindexpins[_indexname].ContainsKey(_chunkname))
                        {
                            return rindexpins[_indexname][_chunkname];
                        }
                    }
                }

                List<byte[]> fbuf = new List<byte[]>();
                using (System.IO.FileStream fs = new System.IO.FileStream(chunkname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                {                    
                    {
                        // Skip the dfs-chunk file-header...
                        int headerlength = 0;
                        {
                            if (buf.Length < 4)
                            {
                                buf = new byte[4];
                            }
                            if (4 != fs.Read(buf, 0, 4))
                            {
                                return fbuf;
                            }
                            {
                                headerlength = ClientHandler.BytesToInt(buf, 0);
                                if (headerlength > 4)
                                {
                                    int hremain = headerlength - 4;
                                    if (hremain > buf.Length)
                                    {
                                        buf = new byte[hremain];
                                    }
                                    ClientHandler.StreamReadExact(fs, buf, hremain);
                                }
                            }
                        }
                    }

                    for (; ; )
                    {
                        byte[] linebuf = new byte[9 * 3];
                        int read = fs.Read(linebuf, 0, 9 * 3);
                        if (read < 9 * 3)
                        {
                            break;
                        }
                        fbuf.Add(linebuf);
                    }     
                    fs.Close();
                }

                if (determinepinornot)
                {
                    ispin = System.IO.File.Exists(CurrentDirNetPath + @"\ind.Pin." + indexname + ".ind");
                }
                if(ispin)
                {
                    lock (rindexpins)
                    {
                        if (!rindexpins.ContainsKey(_indexname))
                        {
                            rindexpins.Add(_indexname, new Dictionary<string, List<byte[]>>());
                        }
                        if (!rindexpins[_indexname].ContainsKey(_chunkname))
                        {
                            rindexpins[_indexname].Add(_chunkname, fbuf);
                        }
                    }
                }

                return fbuf;
            }
        }
    }
}
