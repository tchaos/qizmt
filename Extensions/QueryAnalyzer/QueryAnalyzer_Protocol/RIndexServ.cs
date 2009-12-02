using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Protocol
{
    partial class QueryAnalyzer_Protocol
    {  

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
                                    catch (Exception ep)
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
                                    string sysindexesfn = "sys.indexes";
                                    string sysindexesxml = "";
                                    if (System.IO.File.Exists(sysindexesfn))
                                    {
                                        System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                                        xi.Load(sysindexesfn);
                                        sysindexesxml = xi.OuterXml;                                       
                                    }
                                    XContent.SendXContent(netstm, sysindexesxml);

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
                                    string keydata = XContent.ReceiveXString(netstm, buf);
                                    bool ispin = false;
                                    bool ispinhash = false;
                                    bool keepvalueorder = false;
                                    
                                    string keydatatype = "long";
                                    int keylen = 9;
                                    int keyoffset = 0;
                                    int rowlen = 9 * 3;        
                            
                                    Column[] columns = null;

                                    string sysindexesfn = "sys.indexes";
                                    if (System.IO.File.Exists(sysindexesfn))
                                    {
                                        System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                                        xi.Load(sysindexesfn);
                                        System.Xml.XmlNodeList xnIndexes = xi.SelectNodes("/indexes/index");
                                        foreach (System.Xml.XmlNode xnIndex in xnIndexes)
                                        {
                                            if (string.Compare(indexname, xnIndex["name"].InnerText, true) == 0)
                                            {                                               
                                                int ordinal = Int32.Parse(xnIndex["ordinal"].InnerText);
                                                System.Xml.XmlNode xnPin = xnIndex.SelectSingleNode("pin");
                                                ispin = (xnPin.InnerText == "1");
                                                System.Xml.XmlNode xnPinHash = xnIndex.SelectSingleNode("pinHash");
                                                ispinhash = (xnPinHash != null && xnPinHash.InnerText == "1");
                                                keepvalueorder = (xnIndex["keepValueOrder"].InnerText == "1");
                                                System.Xml.XmlNode xnTable = xnIndex.SelectSingleNode("table");
                                                System.Xml.XmlNodeList xnCols = xnTable.SelectNodes("column");
                                                columns = new Column[xnCols.Count];
                                                rowlen = 0;
                                                for (int ci = 0; ci < xnCols.Count; ci++)
                                                {
                                                    System.Xml.XmlNode xnCol = xnCols[ci];
                                                    int colBytes = Int32.Parse(xnCol["bytes"].InnerText);
                                                    string colType = xnCol["type"].InnerText.ToLower();
                                                    if (colType.StartsWith("char(", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        colType = "char";
                                                    }
                                                    if (ci == ordinal)
                                                    {
                                                        keydatatype = colType;
                                                        keylen = colBytes;
                                                    }
                                                    else if(ci < ordinal)
                                                    {
                                                        keyoffset += colBytes;
                                                    }
                                                    rowlen += colBytes;

                                                    Column col;
                                                    col.Name = xnCol["name"].InnerText;
                                                    switch(colType)
                                                    {
                                                        case "long":
                                                            col.Type = typeof(System.Int64);
                                                            break;

                                                        case "int":
                                                            col.Type = typeof(System.Int32);
                                                            break;

                                                        case "double":
                                                            col.Type = typeof(System.Double);
                                                            break;

                                                        case "char":
                                                            col.Type = typeof(System.String);
                                                            break;

                                                        case "datetime":
                                                            col.Type = typeof(System.DateTime);
                                                            break;

                                                        default:
                                                            throw new Exception("Type: " + colType + " is invalid.");
                                                    }
                                                    col.Size = colBytes - 1;
                                                    col.FrontBytes = 1;
                                                    col.BackBytes = 0;
                                                    columns[ci] = col;
                                                }
                                                break;
                                            }
                                        }
                                    }                       

                                    if(columns == null)
                                    {
                                        columns = new Column[3];
                                        for(int ci = 0; ci < columns.Length; ci++)
                                        {
                                            Column col;
                                            col.Name = "Column" + ci.ToString();
                                            col.Type = typeof(System.Int64);
                                            col.Size = 8;
                                            col.FrontBytes = 1;
                                            col.BackBytes = 0;
                                            columns[ci] = col;
                                        }
                                    }

                                    //Query succeeded.
                                    netstm.WriteByte((byte)'+');

                                    ClientHandler.MyToBytes(columns.Length, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);

                                    foreach(Column col in columns)
                                    {
                                        XContent.SendXContent(netstm, col.Name);
                                        XContent.SendXContent(netstm, col.Type.FullName);
                                        ClientHandler.MyToBytes(col.FrontBytes, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                        ClientHandler.MyToBytes(col.Size, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                        ClientHandler.MyToBytes(col.BackBytes, buf, 0);
                                        XContent.SendXContent(netstm, buf, 4);
                                    }                                                                     

                                    //return rows.    
                                    byte[] recordbuf = new byte[rowlen + (0x400 * 0x400 * 20)];
                                    int recordbufpos = 0;
                                    byte[] keybuf = new byte[keylen];

                                    string[] xparts = keydata.Split('\0');
                                    for(int pi = 0; pi < xparts.Length; pi += 2)
                                    {
                                        string chunkname = xparts[pi];
                                        string skey = xparts[pi + 1];

                                        keybuf[0] = 0; // IsNull=false
                                        switch (keydatatype)
                                        {
                                            case "long":
                                                {
                                                    long key = long.Parse(skey);
                                                    UInt64 ukey = (ulong)(key + long.MaxValue + 1);
                                                    ClientHandler.Int64ToBytes((Int64)ukey, keybuf, 1);
                                                }
                                                break;

                                            case "int":
                                                {
                                                    int key = int.Parse(skey);
                                                    uint ukey = (uint)(key + int.MaxValue + 1);
                                                    ClientHandler.Int32ToBytes((int)ukey, keybuf, 1);
                                                }
                                                break;

                                            case "double":
                                                {
                                                    double key = double.Parse(skey);
                                                    ClientHandler.DoubleToBytes(key, keybuf, 1);
                                                }
                                                break;

                                            case "datetime":
                                                {
                                                    DateTime key = DateTime.Parse(skey);
                                                    ClientHandler.Int64ToBytes(key.Ticks, keybuf, 1);
                                                }
                                                break;

                                            case "char":
                                                {
                                                    string key = skey.Replace("''", "'");
                                                    byte[] strbuf = System.Text.Encoding.Unicode.GetBytes(key);
                                                    if (strbuf.Length > keylen - 1)
                                                    {
                                                        throw new Exception("String too large.");
                                                    }
                                                    for (int si = 0; si < strbuf.Length; si++)
                                                    {
                                                        keybuf[si + 1] = strbuf[si];
                                                    }
                                                    int padlen = keylen - 1 - strbuf.Length;
                                                    for (int si = strbuf.Length + 1; padlen > 0; padlen--)
                                                    {
                                                        keybuf[si++] = 0;
                                                    }
                                                }
                                                break;
                                        }
                                        
                                        if (chunkname.Length > 0)
                                        {
                                            ChunkRowsData chunkbuf = LoadFileChunk(chunkname, indexname, ispin, rowlen, ispinhash, keyoffset);
                                            if (chunkbuf.NumberOfRows == 0)
                                            {
                                                throw new Exception("Chunk referenced by a master index is not supposed to be empty: " + chunkname);
                                            }

                                            long left = 0;
                                            long right = chunkbuf.NumberOfRows - 1;
                                            if (ispinhash && chunkbuf.Hash != null)
                                            {
                                                int shortkey = QueryAnalyzer_Protocol.TwoBytesToInt(keybuf[1], keybuf[2]);
                                                Position hpos = chunkbuf.Hash[shortkey];
                                                left = hpos.Offset;
                                                right = left + hpos.Length - 1;
                                            }
                                            long result = -1;
                                            if (right >= 0)
                                            {
                                                result = BSearch(chunkbuf, keybuf, ref left, ref right, keyoffset);
                                            }
                                            if (result > -1)
                                            {       
                                                #region keepvalueorderIsfalse
                                                if (!keepvalueorder)
                                                {
                                                    long startingrowpos = result;

                                                    //begin a row      
                                                    CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                    for (int ci = 0; ci < rowlen; ci++)
                                                    {
                                                        recordbuf[recordbufpos++] = chunkbuf[result][ci];
                                                    }

                                                    bool lookforward = true;
                                                    long rowpos = startingrowpos;
                                                    for (; ; )
                                                    {
                                                        if (++rowpos > chunkbuf.NumberOfRows - 1)
                                                        {
                                                            break;
                                                        }
                                                        if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                        {
                                                            CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                            for (int ci = 0; ci < rowlen; ci++)
                                                            {
                                                                recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
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
                                                        if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                        {
                                                            CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                            for (int ci = 0; ci < rowlen; ci++)
                                                            {
                                                                recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
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
                                                        mi = LoadMasterIndex(indexname, keylen);
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
                                                        else if (CompareBytes(mi[chunkpos].Value, keybuf, keylen) != 0)
                                                        {
                                                            break;
                                                        }
                                                        else
                                                        {
                                                            chunkbuf = LoadFileChunk(mi[chunkpos].Key, indexname, ispin, rowlen, ispinhash, keyoffset);
                                                            rowpos = -1;
                                                        }

                                                        for (; ; )
                                                        {
                                                            if (++rowpos > chunkbuf.NumberOfRows - 1)
                                                            {
                                                                break;
                                                            }
                                                            if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                            {
                                                                CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                                for (int ci = 0; ci < rowlen; ci++)
                                                                {
                                                                    recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
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
                                                            chunkbuf = LoadFileChunk(mi[chunkpos].Key, indexname, ispin, rowlen, ispinhash, keyoffset);
                                                            rowpos = chunkbuf.NumberOfRows;
                                                        }

                                                        for (; ; )
                                                        {
                                                            if (--rowpos < 0)
                                                            {
                                                                break;
                                                            }
                                                            if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                            {
                                                                CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                                for (int ci = 0; ci < rowlen; ci++)
                                                                {
                                                                    recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
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
                                                #endregion
                                                #region keepvalueorder
                                                else
                                                {
                                                    long startingrowpos = result;

                                                    long firstrowindex = startingrowpos;
                                                    bool lookbackward = true;
                                                    long rowpos = startingrowpos;
                                                    for (; ; )
                                                    {
                                                        if (--rowpos < 0)
                                                        {
                                                            break;
                                                        }
                                                        if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                        {
                                                            firstrowindex = rowpos;
                                                        }
                                                        else
                                                        {
                                                            lookbackward = false;
                                                            break;
                                                        }
                                                    }

                                                    List<KeyValuePair<string, byte[]>> mi = null;
                                                    int startingchunkpos = -1;
                                                    if (lookbackward)
                                                    {
                                                        mi = LoadMasterIndex(indexname, keylen);
                                                        startingchunkpos = FindChunkIndexFromMasterIndex(chunkname, mi);
                                                        
                                                        if (startingchunkpos > 0)
                                                        {
                                                            ChunkRowsData prevchunkbuf = LoadFileChunk(mi[startingchunkpos - 1].Key, indexname, ispin, rowlen, ispinhash, keyoffset);
                                                            rowpos = prevchunkbuf.NumberOfRows;
                                                            long prevchunkfirstrowindex = -1;
                                                            for (; ; )
                                                            {
                                                                if (--rowpos < 0)
                                                                {
                                                                    break;
                                                                }
                                                                if (CompareBytes(keybuf, prevchunkbuf[rowpos], keylen, keyoffset) == 0)
                                                                {
                                                                    prevchunkfirstrowindex = rowpos;
                                                                }
                                                                else
                                                                {
                                                                    lookbackward = false;
                                                                    break;
                                                                }
                                                            }

                                                            if (prevchunkfirstrowindex > -1)
                                                            {
                                                                //Begin first row.
                                                                for (long ri = prevchunkfirstrowindex; ri < prevchunkbuf.NumberOfRows; ri++)
                                                                {
                                                                    CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                                    for (int ci = 0; ci < rowlen; ci++)
                                                                    {
                                                                        recordbuf[recordbufpos++] = prevchunkbuf[ri][ci];
                                                                    }
                                                                } 
                                                            }
                                                        }
                                                    }

                                                    for (long ri = firstrowindex; ri <= startingrowpos; ri++)
                                                    {
                                                        CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                        for (int ci = 0; ci < rowlen; ci++)
                                                        {
                                                            recordbuf[recordbufpos++] = chunkbuf[ri][ci];
                                                        }
                                                    }

                                                    bool lookforward = true;
                                                    rowpos = startingrowpos;
                                                    for (; ; )
                                                    {
                                                        if (++rowpos > chunkbuf.NumberOfRows - 1)
                                                        {
                                                            break;
                                                        }
                                                        if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                        {
                                                            CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                            for (int ci = 0; ci < rowlen; ci++)
                                                            {
                                                                recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
                                                            }
                                                        }
                                                        else
                                                        {
                                                            lookforward = false;
                                                            break;
                                                        }
                                                    }

                                                    if (lookforward)
                                                    {
                                                        if (startingchunkpos == -1)
                                                        {
                                                            mi = LoadMasterIndex(indexname, keylen);
                                                            startingchunkpos = FindChunkIndexFromMasterIndex(chunkname, mi);
                                                        }

                                                        int chunkpos = startingchunkpos;
                                                        while (lookforward)
                                                        {
                                                            if (++chunkpos > mi.Count - 1)
                                                            {
                                                                break;
                                                            }
                                                            else if (CompareBytes(mi[chunkpos].Value, keybuf, keylen) != 0)
                                                            {
                                                                break;
                                                            }
                                                            else
                                                            {
                                                                chunkbuf = LoadFileChunk(mi[chunkpos].Key, indexname, ispin, rowlen, ispinhash, keyoffset);
                                                                rowpos = -1;
                                                            }

                                                            for (; ; )
                                                            {
                                                                if (++rowpos > chunkbuf.NumberOfRows - 1)
                                                                {
                                                                    break;
                                                                }
                                                                if (CompareBytes(keybuf, chunkbuf[rowpos], keylen, keyoffset) == 0)
                                                                {
                                                                    CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                                    for (int ci = 0; ci < rowlen; ci++)
                                                                    {
                                                                        recordbuf[recordbufpos++] = chunkbuf[rowpos][ci];
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    lookforward = false;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                #endregion                                                
                                            }
                                        }
                                    }

                                    if (recordbufpos > 0)
                                    {
                                        netstm.WriteByte((byte)'+');
                                        XContent.SendXContent(netstm, recordbuf, 0, recordbufpos);
                                        recordbufpos = 0;
                                    }

                                    netstm.WriteByte((byte)'.');
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

            private void CheckBatchCapacity(ref int recordbufpos, byte[] recordbuf, int rowlength)
            {
                if (recordbufpos + rowlength > recordbuf.Length)
                {
                    netstm.WriteByte((byte)'+');
                    XContent.SendXContent(netstm, recordbuf, 0, recordbufpos);
                    recordbufpos = 0;
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

            private int CompareBytes(byte[] x, RowData y, int length, int rowkeyoffset)
            {
                for (int i = 0; i < length; i++)
                {
                    if (x[i] != y[i + rowkeyoffset])
                    {
                        return x[i] - y[i + rowkeyoffset];
                    }
                }
                return 0;
            }

            private long BSearch(ChunkRowsData keys, byte[] key, ref long left, ref long right, int rowkeyoffset)
            {
                int cl = CompareBytes(key, keys[left], key.Length, rowkeyoffset);
                if (cl == 0)
                {
                    return left;
                }
                int cr = CompareBytes(key, keys[right], key.Length, rowkeyoffset);
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
                long mid = (right - left) / 2 + left;
                int cm = CompareBytes(key, keys[mid], key.Length, rowkeyoffset);
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
                return BSearch(keys, key, ref left, ref right, rowkeyoffset);
            }

            private List<KeyValuePair<string, byte[]>> LoadMasterIndex(string indexName, int keylength)
            {
                string indexfn = CurrentDirNetPath + @"\ind.Index." + indexName + ".ind";
                List<KeyValuePair<string, byte[]>> mi = new List<KeyValuePair<string, byte[]>>();

                using (System.IO.FileStream fs = new System.IO.FileStream(indexfn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                {
                    //Skip header                    
                    fs.Seek((long)keylength, System.IO.SeekOrigin.Current);                    

                    for (; ; )
                    {
                        byte[] keybuf = new byte[keylength];
                        if (fs.Read(keybuf, 0, keylength) < keylength)
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

                return mi;
            }

            private ChunkRowsData LoadFileChunk(string chunkname, string indexname, bool ispin, int rowlength, bool ispinhash, int keyoffset)
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

                ChunkRowsData fbuf;
                using (System.IO.FileStream fs = new System.IO.FileStream(chunkname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                {
                    long datalength;
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
                                throw new Exception("Invalid chunk '" + chunkname + "' for index '" + indexname + "': unable to read chunk file header");
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
                        datalength = fs.Length - headerlength;
                    }

                    {
                        MySpace.DataMining.Binary.LongByteArray fbufBytes = new MySpace.DataMining.Binary.LongByteArray(datalength);
                        fbuf = new ChunkRowsData(fbufBytes, rowlength);
                    }

                    byte[] xbuf = new byte[0x400 * 0x400 * 4];
                    for (long xoffset = 0; xoffset < datalength; )
                    {
                        int read = fs.Read(xbuf, 0, xbuf.Length);
                        if (read <= 0)
                        {
                            throw new Exception("Unexpected end-of-file when reading chunk '"
                                + chunkname + "' of index '"
                                + indexname + "'; expected "
                                + (datalength - xoffset).ToString() + " more bytes");
                        }
                        for (int ir = 0; ir < read; ir++, xoffset++)
                        {
                            fbuf.Bytes[xoffset] = xbuf[ir];
                        }
                    }
                }

                if (ispinhash)
                {
                    fbuf.MakeHash(keyoffset);
                }
               
                if (ispin)
                {
                    lock (rindexpins)
                    {
                        if (!rindexpins.ContainsKey(_indexname))
                        {
                            rindexpins.Add(_indexname, new Dictionary<string, ChunkRowsData>());
                        }
                        if (!rindexpins[_indexname].ContainsKey(_chunkname))
                        {
                            rindexpins[_indexname].Add(_chunkname, fbuf);
                        }
                    }
                }

                return fbuf;
            }

            private int FindChunkIndexFromMasterIndex(string chunkname, List<KeyValuePair<string, byte[]>> mi)
            {
                for (int i = 0; i < mi.Count; i++)
                {
                    if (string.Compare(chunkname, mi[i].Key, true) == 0)
                    {
                        return i;
                    }
                }                
                throw new Exception("Chunk file name " + chunkname + " is not found in master index");                
            }

            internal struct Column
            {
                public string Name;
                public Type Type;
                public int Size;
                public int FrontBytes;
                public int BackBytes;
            }
        }
    }
}
