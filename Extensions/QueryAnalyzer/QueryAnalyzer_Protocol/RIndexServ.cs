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
                                                    col.sType = colType;
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
                                            col.sType = "long";
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
                                            bool isfirstkeyofchunk = false;
                                            if (chunkname[chunkname.Length - 1] == '*')
                                            {
                                                isfirstkeyofchunk = true;
                                                chunkname = chunkname.Substring(0, chunkname.Length - 1);
                                            }
               
                                            ChunkRowsData chunkbuf = LoadFileChunk(chunkname, indexname, ispin, rowlen, ispinhash, keyoffset);
                                            long result = -1;
                                            if (chunkbuf.NumberOfRows > 0)
                                            {
                                                long left = 0;
                                                long right = chunkbuf.NumberOfRows - 1;
                                                if (ispinhash && chunkbuf.Hash != null)
                                                {
                                                    int shortkey = QueryAnalyzer_Protocol.TwoBytesToInt(keybuf[1], keybuf[2]);
                                                    Position hpos = chunkbuf.Hash[shortkey];
                                                    left = hpos.Offset;
                                                    right = left + hpos.Length - 1;
                                                }                                                
                                                if (right >= 0)
                                                {
                                                    result = BSearch(chunkbuf, keybuf, ref left, ref right, keyoffset);
                                                }
                                            }
                                            {
                                                if (result > -1 || isfirstkeyofchunk)
                                                {
                                                    #region keepvalueorderIsfalse
                                                    if (!keepvalueorder)
                                                    {
                                                        long startingrowpos = result;

                                                        //begin a row
                                                        if (result > -1)
                                                        {
                                                            CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                            for (int ci = 0; ci < rowlen; ci++)
                                                            {
                                                                recordbuf[recordbufpos++] = chunkbuf[result][ci];
                                                            }
                                                        }                                                        

                                                        bool lookforward = true;
                                                        long rowpos = startingrowpos;
                                                        if (result > -1)
                                                        {
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
                                                        else if (chunkbuf.NumberOfRows > 0)   //isfirstkeyofchunk
                                                        {
                                                            lookforward = false;
                                                        }

                                                        bool lookbackward = true;
                                                        rowpos = startingrowpos;
                                                        if (result > -1)
                                                        {
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
                                                        if (result > -1)
                                                        {
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

                                                        if (result > -1)
                                                        {
                                                            for (long ri = firstrowindex; ri <= startingrowpos; ri++)
                                                            {
                                                                CheckBatchCapacity(ref recordbufpos, recordbuf, rowlen);
                                                                for (int ci = 0; ci < rowlen; ci++)
                                                                {
                                                                    recordbuf[recordbufpos++] = chunkbuf[ri][ci];
                                                                }
                                                            }
                                                        }
                                                        
                                                        bool lookforward = true;
                                                        rowpos = startingrowpos;
                                                        if (result > -1)
                                                        {
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
                                                        else if (chunkbuf.NumberOfRows > 0)
                                                        {
                                                            lookforward = false;
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

                            case (byte)'u': //insert/delete rows from table
                                {                                   
                                    string[] batchCmds = XContent.ReceiveXString(netstm, buf).Split(new string[]{"\0\0"}, StringSplitOptions.None);
                                   
                                    RIndexUpdateCoord coord = new RIndexUpdateCoord(this, batchCmds);

                                    System.Threading.Thread[] threads = new System.Threading.Thread[8];

                                    for (int ti = 0; ti < threads.Length; ti++)
                                    {
                                        RIndexUpdate ru = new RIndexUpdate(coord);
                                        System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ThreadStart(ru.ThreadProc));
                                        threads[ti] = th;
                                        th.Start();
                                    }

                                    for (int ti = 0; ti < threads.Length; ti++)
                                    {
                                        threads[ti].Join();
                                    }

                                    netstm.WriteByte((byte)'+');
                                }
                                break;

                            case (byte)'c': // Close.
                                stop = true;
                                netstm.WriteByte((byte)'+');
                                break;

                            default:
                                throw new Exception("unknown action:" + ((int)ib).ToString() + " char: " + (char)ib);
                        }
                    }
                }
                catch (Exception e)
                {
                    XLog.errorlog("RIndexServClientHandler.ClientThreadProc exception: " + e.ToString());
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

            private int CompareBytes(byte[] x, byte[] y, int length, int xOffset, int yOffset)
            {
                for (int i = 0; i < length; i++)
                {
                    if (x[i + xOffset] != y[i + yOffset])
                    {
                        return x[i + xOffset] - y[i + yOffset];
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

            private int CompareBytes(RowData x, RowData y, int length, int rowkeyoffset)
            {
                for (int i = 0; i < length; i++)
                {
                    if (x[i + rowkeyoffset] != y[i + rowkeyoffset])
                    {
                        return x[i + rowkeyoffset] - y[i + rowkeyoffset];
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
                return LoadFileChunk(chunkname, indexname, ispin, rowlength, ispinhash, keyoffset, buf, true);
            }

            private ChunkRowsData LoadFileChunk(string chunkname, string indexname, bool ispin, int rowlength, bool ispinhash, int keyoffset, byte[] buffer, bool failover)
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

                string chunkfilename;
                string chunkhosts;
                string qizmtrootdir;
                {
                    int lastdel = chunkname.LastIndexOf(@"\");
                    chunkfilename = chunkname.Substring(lastdel + 1);
                    int del = chunkname.IndexOf(@"\", 2);
                    chunkhosts = chunkname.Substring(2, del - 2);
                    qizmtrootdir = chunkname.Substring(del + 1, lastdel - del - 1);
                }

                ChunkRowsData fbuf;
                System.IO.Stream fs = null;
                if (failover)
                {
                    fs = new DfsFileNodeStream(chunkhosts, chunkfilename, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x1000, qizmtrootdir);
                }
                else
                {
                    fs = new System.IO.FileStream(qizmtrootdir.Replace('$',':') + @"\" + chunkfilename, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 0x1000);
                }
                
                try
                {   
                    long datalength;
                    {
                        // Skip the dfs-chunk file-header...
                        int headerlength = 0;
                        {
                            if (buffer.Length < 4)
                            {
                                buffer = new byte[4];
                            }
                            if (4 != fs.Read(buffer, 0, 4))
                            {
                                throw new Exception("Invalid chunk '" + chunkname + "' for index '" + indexname + "': unable to read chunk file header");
                            }
                            {
                                headerlength = ClientHandler.BytesToInt(buffer, 0);
                                if (headerlength > 4)
                                {
                                    int hremain = headerlength - 4;
                                    if (hremain > buffer.Length)
                                    {
                                        buffer = new byte[hremain];
                                    }
                                    ClientHandler.StreamReadExact(fs, buffer, hremain);
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
                finally
                {
                    fs.Close();
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

            private void ConvertStringColumnValueToBinary(string skey, int keylen, string keytype, byte[] buffer, int bufferOffset)
            {
                buffer[bufferOffset] = 0; //is null = false on first byte.
                switch (keytype)
                {
                    case "long":
                        {
                            long key = Int64.Parse(skey);
                            UInt64 ukey = (ulong)(key + long.MaxValue + 1);
                            ClientHandler.Int64ToBytes((Int64)ukey, buffer, bufferOffset + 1);
                        }
                        break;

                    case "int":
                        {
                            int key = Int32.Parse(skey);
                            uint ukey = (uint)(key + int.MaxValue + 1);
                            ClientHandler.Int32ToBytes((int)ukey, buffer, bufferOffset + 1);
                        }
                        break;

                    case "double":
                        {
                            double key = Double.Parse(skey);
                            ClientHandler.DoubleToBytes(key, buffer, bufferOffset + 1);
                        }
                        break;

                    case "datetime":
                        {
                            skey = skey.Substring(1, skey.Length - 2);
                            DateTime key = DateTime.Parse(skey);
                            ClientHandler.Int64ToBytes(key.Ticks, buffer, bufferOffset + 1);
                        }
                        break;

                    case "char":
                        {
                            skey = skey.Substring(1, skey.Length - 2);
                            string key = skey.Replace("''", "'");
                            byte[] strbuf = System.Text.Encoding.Unicode.GetBytes(key);
                            if (strbuf.Length > keylen - 1)
                            {
                                throw new Exception("String too large.");
                            }
                            for (int si = 0; si < strbuf.Length; si++)
                            {
                                buffer[bufferOffset + si + 1] = strbuf[si];
                            }
                            int padlen = keylen - 1 - strbuf.Length;
                            for (int si = bufferOffset + strbuf.Length + 1; padlen > 0; padlen--)
                            {
                                buffer[si++] = 0;
                            }
                        }
                        break;
                }
            }

            internal struct Column
            {
                public string Name;
                public Type Type;
                public int Size;
                public int FrontBytes;
                public int BackBytes;
                public string sType;
            }

            internal struct Table
            {
                public string Name;
                public Column[] Columns;
            }

            internal struct Index
            {
                public string Name;
                public int Ordinal;
                public bool PinHash;
                public bool PinMemory;
                public Table Table;
                public string KeyDataType;
                public int KeyLength;
                public int KeyOffset;
                public int RowLength;
                public string OutlierMode;
                public int OutlierMax;
            }

            private class RIndexUpdateCoord
            {
                internal RIndexServClientHandler Handler;
                internal string[] BatchCmds;
                internal int CurBatchCmdPos;
                internal Dictionary<string, Index> LoadedIndexes;

                public RIndexUpdateCoord(RIndexServClientHandler handler, string[] batchcmds)
                {
                    Handler = handler;
                    BatchCmds = batchcmds;
                    CurBatchCmdPos = -1;
                    LoadedIndexes = new Dictionary<string, Index>();
                }
            }

            private class RIndexUpdate
            {                
                RIndexUpdateCoord coord;                

                public RIndexUpdate(RIndexUpdateCoord updatecoord)
                {
                    coord = updatecoord;
                }

                public void ThreadProc()
                {
                    byte[] _buf = new byte[1024 * 1024 * 5];
                    List<byte[]> tempbuf = new List<byte[]>();

                    for (; ; )
                    {
                        string thisbatch = "";
                        lock (coord)
                        {
                            coord.CurBatchCmdPos++;
                            if (coord.CurBatchCmdPos >= coord.BatchCmds.Length)
                            {
                                break;
                            }
                            thisbatch = coord.BatchCmds[coord.CurBatchCmdPos];
                        }

                        if (thisbatch.Length == 0)
                        {
                            continue;
                        }
                       
                        string[] parts = thisbatch.Split('\0');
                        string indexName = parts[0].ToLower();
                        string chunkName = parts[1];
                        int insertCount = Int32.Parse(parts[2]);

                        Index thisIndex;
                        lock (coord)
                        {
                            if (!coord.LoadedIndexes.ContainsKey(indexName))
                            {
                                string sysindexesfn = "sys.indexes";
                                if (System.IO.File.Exists(sysindexesfn))
                                {
                                    bool ispin = false;
                                    bool ispinhash = false;
                                    string tableName = "";
                                    string keydatatype = "long";
                                    int keylen = 9;
                                    int keyoffset = 0;
                                    int rowlen = 9 * 3;

                                    Column[] columns = null;
                                    System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                                    xi.Load(sysindexesfn);
                                    System.Xml.XmlNodeList xnIndexes = xi.SelectNodes("/indexes/index");
                                    bool indexFound = false;
                                    foreach (System.Xml.XmlNode xnIndex in xnIndexes)
                                    {
                                        if (string.Compare(indexName, xnIndex["name"].InnerText, true) == 0)
                                        {
                                            int ordinal = Int32.Parse(xnIndex["ordinal"].InnerText);
                                            System.Xml.XmlNode xnPin = xnIndex.SelectSingleNode("pin");
                                            ispin = (xnPin.InnerText == "1");
                                            System.Xml.XmlNode xnPinHash = xnIndex.SelectSingleNode("pinHash");
                                            ispinhash = (xnPinHash != null && xnPinHash.InnerText == "1");
                                            System.Xml.XmlNode xnTable = xnIndex.SelectSingleNode("table");
                                            tableName = xnTable["name"].InnerText;
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
                                                else if (ci < ordinal)
                                                {
                                                    keyoffset += colBytes;
                                                }
                                                rowlen += colBytes;

                                                Column col;
                                                col.Name = xnCol["name"].InnerText;
                                                col.sType = colType;
                                                switch (colType)
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

                                            Table tab;
                                            tab.Name = tableName;
                                            tab.Columns = columns;

                                            Index ind;
                                            ind.Name = indexName;
                                            ind.Ordinal = ordinal;
                                            ind.PinHash = ispinhash;
                                            ind.PinMemory = ispin;
                                            ind.Table = tab;
                                            ind.KeyDataType = keydatatype;
                                            ind.KeyLength = keylen;
                                            ind.KeyOffset = keyoffset;
                                            ind.RowLength = rowlen;
                                            ind.OutlierMode = xnIndex.SelectSingleNode("outlier/mode").InnerText;
                                            ind.OutlierMax = Int32.Parse(xnIndex.SelectSingleNode("outlier/max").InnerText);

                                            coord.LoadedIndexes.Add(indexName, ind);
                                            indexFound = true;
                                            break;
                                        }
                                    }
                                    if (!indexFound)
                                    {
                                        throw new Exception("Index is not found.");
                                    }
                                }
                            }
                            thisIndex = coord.LoadedIndexes[indexName];
                        }

                        ChunkRowsData chunk = coord.Handler.LoadFileChunk(chunkName, indexName, thisIndex.PinMemory, thisIndex.RowLength, thisIndex.PinHash, thisIndex.KeyOffset, _buf, false);

                        long newChunkSize = chunk.Bytes.LongLength + ((long)insertCount * (long)thisIndex.RowLength);

                        ChunkRowsData newchunk;
                        {
                            MySpace.DataMining.Binary.LongByteArray fbufBytes = new MySpace.DataMining.Binary.LongByteArray(newChunkSize);
                            newchunk = new ChunkRowsData(fbufBytes, thisIndex.RowLength);
                        }

                        //merge
                        byte[] updateKeyBuf = new byte[thisIndex.KeyLength];
                        byte[] tempKeyBuf = new byte[thisIndex.KeyLength];
                        int updatepos = 3;
                        long chunkpos = 0;
                        long newchunkpos = 0;
                        int outliermax = thisIndex.OutlierMax > 0 ? thisIndex.OutlierMax : Int32.MaxValue;
                        while (updatepos < parts.Length && chunkpos < chunk.NumberOfRows)
                        {
                            string cmd = parts[updatepos];
                            {
                                string xcmd = cmd;
                                RDBMS_DBCORE.Qa.NextPart(ref xcmd);   //action
                                if (!(string.Compare("KEY", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true) == 0 &&
                                    string.Compare("=", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true) == 0))
                                {
                                    throw new Exception("KEY expected");
                                }
                                string skey = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                                if (skey == "-")
                                {
                                    string numpart = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                                    skey += numpart;
                                }
                                coord.Handler.ConvertStringColumnValueToBinary(skey, thisIndex.KeyLength, thisIndex.KeyDataType, updateKeyBuf, 0);
                            }

                            RowData chunkrow = chunk[chunkpos];
                            int compare = coord.Handler.CompareBytes(updateKeyBuf, chunkrow, thisIndex.KeyLength, thisIndex.KeyOffset);

                            if (compare == 0)
                            {
                                //apply updates to chunk.
                                tempbuf.Clear();

                                for (; ; )
                                {
                                    byte[] onerow = new byte[chunkrow.Length];
                                    for (int yi = 0; yi < onerow.Length; yi++)
                                    {
                                        onerow[yi] = chunkrow[yi];
                                    }
                                    tempbuf.Add(onerow);

                                    long prevchunkpos = chunkpos;
                                    if (++chunkpos > chunk.NumberOfRows - 1)
                                    {
                                        break;
                                    }

                                    if (coord.Handler.CompareBytes(chunk[prevchunkpos], chunk[chunkpos], thisIndex.KeyLength, thisIndex.KeyOffset) != 0)
                                    {
                                        break;
                                    }
                                    else
                                    {
                                        chunkrow = chunk[chunkpos];
                                    }
                                }

                                updatepos = ApplyUpdates(parts, updatepos, tempbuf, thisIndex, updateKeyBuf, tempKeyBuf);  
                                newchunkpos = AddRowsToNewChunk(tempbuf, thisIndex.OutlierMode, outliermax, newchunk, newchunkpos);
                            }
                            else if (compare < 0)
                            {
                                //key in update is smaller than chunk key.
                                tempbuf.Clear();
                                updatepos = ApplyUpdates(parts, updatepos, tempbuf, thisIndex, updateKeyBuf, tempKeyBuf);
                                newchunkpos = AddRowsToNewChunk(tempbuf, thisIndex.OutlierMode, outliermax, newchunk, newchunkpos);
                            }
                            else
                            {
                                //key in chunk is smaller than update.
                                int curkeycount = 0;
                                for (; ; )
                                {
                                    curkeycount++;
                                    for (int yi = 0; yi < chunkrow.Length; yi++)
                                    {
                                        newchunk.Bytes[newchunkpos++] = chunkrow[yi];
                                    }

                                    long prevchunkpos = chunkpos;
                                    if (++chunkpos > chunk.NumberOfRows - 1)
                                    {
                                        break;
                                    }

                                    if (coord.Handler.CompareBytes(chunk[prevchunkpos], chunk[chunkpos], thisIndex.KeyLength, thisIndex.KeyOffset) != 0)
                                    {
                                        break;
                                    }
                                    else
                                    {
                                        chunkrow = chunk[chunkpos];
                                    }
                                }

                                if (curkeycount > outliermax)
                                {
                                    newchunkpos = RemoveOutliersFromNewChunk(newchunk, curkeycount, outliermax, thisIndex.OutlierMode, thisIndex.RowLength, newchunkpos);
                                }
                            }
                        }

                        //Flush remaining                                                                                    
                        while (updatepos < parts.Length)
                        {
                            string cmd = parts[updatepos];
                            {
                                string xcmd = cmd;
                                RDBMS_DBCORE.Qa.NextPart(ref xcmd);   //action
                                if (!(string.Compare("KEY", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true) == 0 &&
                                    string.Compare("=", RDBMS_DBCORE.Qa.NextPart(ref xcmd), true) == 0))
                                {
                                    throw new Exception("KEY expected");
                                }
                                string skey = RDBMS_DBCORE.Qa.NextPart(ref xcmd);                                
                                if (skey == "-")
                                {
                                    string numpart = RDBMS_DBCORE.Qa.NextPart(ref xcmd);
                                    skey += numpart;
                                }
                                coord.Handler.ConvertStringColumnValueToBinary(skey, thisIndex.KeyLength, thisIndex.KeyDataType, updateKeyBuf, 0);
                            }

                            tempbuf.Clear();
                            updatepos = ApplyUpdates(parts, updatepos, tempbuf, thisIndex, updateKeyBuf, tempKeyBuf);
                            newchunkpos = AddRowsToNewChunk(tempbuf, thisIndex.OutlierMode, outliermax, newchunk, newchunkpos);
                        }

                        if (chunkpos < chunk.NumberOfRows)
                        {
                            if (chunk.NumberOfRows - chunkpos <= outliermax)
                            {
                                for (long ki = chunkpos; ki < chunk.NumberOfRows; ki++)
                                {
                                    RowData onerow = chunk[ki];
                                    for (int yi = 0; yi < onerow.Length; yi++)
                                    {
                                        newchunk.Bytes[newchunkpos++] = onerow[yi];
                                    }
                                }
                            }
                            else
                            {
                                while (chunkpos < chunk.NumberOfRows)
                                {
                                    int curkeycount = 0;
                                    for (; ; )
                                    {
                                        curkeycount++;
                                        RowData onerow = chunk[chunkpos];
                                        for (int yi = 0; yi < onerow.Length; yi++)
                                        {
                                            newchunk.Bytes[newchunkpos++] = onerow[yi];
                                        }

                                        long prevchunkpos = chunkpos;
                                        if (++chunkpos > chunk.NumberOfRows - 1)
                                        {
                                            break;
                                        }

                                        if (coord.Handler.CompareBytes(chunk[prevchunkpos], chunk[chunkpos], thisIndex.KeyLength, thisIndex.KeyOffset) != 0)
                                        {
                                            break;
                                        }
                                    }
                                    if (curkeycount > outliermax)
                                    {
                                        newchunkpos = RemoveOutliersFromNewChunk(newchunk, curkeycount, outliermax, thisIndex.OutlierMode, thisIndex.RowLength, newchunkpos);
                                    }
                                }                                
                            }                            
                        }

                        //Done
                        if (newchunk.Bytes.LongLength != newchunkpos)
                        {
                            MySpace.DataMining.Binary.LongByteArray fbufBytes = new MySpace.DataMining.Binary.LongByteArray(newchunkpos);
                            ChunkRowsData fixedNewChunk = new ChunkRowsData(fbufBytes, thisIndex.RowLength);
                            long actualNumberOfRows = fixedNewChunk.NumberOfRows;
                            long fixedNewChunkPos = 0;
                            for (long ki = 0; ki < actualNumberOfRows; ki++)
                            {
                                RowData onerow = newchunk[ki];
                                for (int yi = 0; yi < onerow.Length; yi++)
                                {
                                    fixedNewChunk.Bytes[fixedNewChunkPos++] = onerow[yi];
                                }
                            }

                            newchunk = fixedNewChunk;
                        }

                        {  
                            int del = chunkName.IndexOf(@"\", 2);
                            string chunkpath = chunkName.Substring(del + 1).Replace('$', ':');
                            string rename = chunkpath + "_updating_" + DateTime.Now.ToString("yyyyMMddHHmmss");
                            System.IO.File.Move(chunkpath, rename);
                            using (System.IO.FileStream fs = new System.IO.FileStream(chunkpath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
                            {
                                //header
                                ClientHandler.Int32ToBytes(4 + 8, _buf, 0);// Size of header.
                                ClientHandler.Int64ToBytes(newchunk.Bytes.LongLength, _buf, 4);
                                fs.Write(_buf, 0, 4 + 8);

                                for (long ki = 0; ki < newchunk.NumberOfRows; ki++)
                                {
                                    RowData onerow = newchunk[ki];
                                    for (int yi = 0; yi < onerow.Length; yi++)
                                    {
                                        fs.WriteByte(onerow[yi]);
                                    }
                                }
                                fs.Close();
                            }
                            System.IO.File.Delete(rename);                            

                            UpdateChunkPinMemory(chunkName, indexName, newchunk);
                        }
                    }                    
                }

                private long RemoveOutliersFromNewChunk(ChunkRowsData newchunk, int curkeycount, int outliermax, string outliermode, int rowlength, long newchunkpos)
                {                    
                    if (outliermode == "delete")
                    {
                        newchunkpos = newchunkpos - (curkeycount * rowlength);
                    }
                    else if (outliermode == "fifo")
                    {
                        long targetstartindex = newchunkpos - (curkeycount * rowlength);
                        long numberOfBytesToMove = outliermax * rowlength;
                        long sourcestartindex = newchunkpos - numberOfBytesToMove;
                        for (long bi = 0; bi < numberOfBytesToMove; bi++)
                        {
                            newchunk.Bytes[targetstartindex++] = newchunk.Bytes[sourcestartindex++];
                        }
                        newchunkpos = targetstartindex;
                    }
                    else if (outliermode == "random")
                    {
                        long rowsstart = newchunkpos - (curkeycount * rowlength);
                        Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                        int rowsremain = curkeycount;
                        while (rowsremain > outliermax)
                        {
                            int deleteind = rnd.Next(0, Int32.MaxValue) % rowsremain;
                            long targetstartindex = rowsstart + deleteind * rowlength;
                            long numberOfBytesToMove = (rowsremain - deleteind - 1) * rowlength;
                            long sourcestartindex = targetstartindex + rowlength;
                            for (long bi = 0; bi < numberOfBytesToMove; bi++)
                            {
                                newchunk.Bytes[targetstartindex++] = newchunk.Bytes[sourcestartindex++];
                            }
                            newchunkpos = targetstartindex;
                            rowsremain--;
                        }
                    }                    
                    return newchunkpos;
                }

                private long AddRowsToNewChunk(List<byte[]> rowsbuf, string outliermode, int outliermax, ChunkRowsData newchunk, long newchunkpos)
                {
                    int startindex = 0;
                    int endindex = rowsbuf.Count;

                    if (rowsbuf.Count > outliermax)
                    {
                        if (outliermode == "delete")
                        {
                            startindex = 0;
                            endindex = 0;
                        }
                        else if (outliermode == "fifo")
                        {
                            startindex = rowsbuf.Count - outliermax;
                            endindex = rowsbuf.Count;
                        }
                        else if (outliermode == "random")
                        {
                            Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                            int removecnt = rowsbuf.Count - outliermax;
                            for (int ri = 0; ri < removecnt; ri++)
                            {
                                int removeindex = rnd.Next(0, Int32.MaxValue) % rowsbuf.Count;
                                rowsbuf.RemoveAt(removeindex);
                            }
                            startindex = 0;
                            endindex = rowsbuf.Count;
                        }
                    }

                    for (int i = startindex; i < endindex; i++)
                    {
                        byte[] tb = rowsbuf[i];
                        foreach (byte tbb in tb)
                        {
                            newchunk.Bytes[newchunkpos++] = tbb;
                        }
                    }

                    return newchunkpos;
                }

                private void UpdateChunkPinMemory(string chunkname, string indexname, ChunkRowsData newChunkRowsData)
                {
                    string _chunkname = chunkname.ToLower();
                    string _indexname = indexname.ToLower();
                    lock (rindexpins)
                    {
                        if (rindexpins.ContainsKey(_indexname))
                        {
                            if (rindexpins[_indexname].ContainsKey(_chunkname))
                            {
                                rindexpins[_indexname][_chunkname] = newChunkRowsData;
                            }
                        }
                    }
                }

                private int ApplyUpdates(string[] cmds, int cmdStart, List<byte[]> chunkRows, Index index, byte[] updateKeyBuf, byte[] tempKeyBuf)
                {
                    int i = cmdStart;
                    for (; i < cmds.Length; i++)
                    {
                        string cmd = cmds[i];

                        bool isInsert = RDBMS_DBCORE.Qa.NextPart(ref cmd) == "0";

                        if (!(string.Compare("KEY", RDBMS_DBCORE.Qa.NextPart(ref cmd), true) == 0 &&
                            string.Compare("=", RDBMS_DBCORE.Qa.NextPart(ref cmd), true) == 0))
                        {
                            throw new Exception("KEY expected");
                        }
                        string skey = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                        if (skey == "-")
                        {
                            string numpart = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                            skey += numpart;
                        }
                        coord.Handler.ConvertStringColumnValueToBinary(skey, index.KeyLength, index.KeyDataType, tempKeyBuf, 0);

                        if (coord.Handler.CompareBytes(tempKeyBuf, updateKeyBuf, index.KeyLength) != 0)
                        {
                            //update key changes.
                            return i;
                        }

                        if (isInsert)
                        {
                            //insert at the end of chunkRows.
                            if (RDBMS_DBCORE.Qa.NextPart(ref cmd) != "(")
                            {
                                throw new Exception("( expected");
                            }
                            int rowoffset = 0;
                            byte[] newrow = new byte[index.RowLength];
                            for (int ci = 0; ci < index.Table.Columns.Length; ci++)
                            {
                                Column thisCol = index.Table.Columns[ci];
                                string sColValue = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                if (sColValue.Length == 0)
                                {
                                    throw new Exception("Expected column value.");
                                }
                                if (sColValue == "-")
                                {
                                    string numpart = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                    sColValue += numpart;
                                }
                                int colBytes = thisCol.Size + thisCol.FrontBytes + thisCol.BackBytes;
                                coord.Handler.ConvertStringColumnValueToBinary(sColValue, colBytes, thisCol.sType, newrow, rowoffset);
                                rowoffset += colBytes;
                                RDBMS_DBCORE.Qa.NextPart(ref cmd); //,
                            }

                            chunkRows.Add(newrow);
                        }
                        else
                        {
                            //scan chunkRows and delete.
                            int andColOrdinal = -1;
                            int andColOffset = 0;
                            byte[] andColBuf = null;
                            if (string.Compare("AND", RDBMS_DBCORE.Qa.NextPart(ref cmd), true) == 0)
                            {
                                andColOrdinal = Int32.Parse(RDBMS_DBCORE.Qa.NextPart(ref cmd));
                                if (RDBMS_DBCORE.Qa.NextPart(ref cmd) != "=")
                                {
                                    throw new Exception("= expected");
                                }
                                string andColValue = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                if (andColValue == "-")
                                {
                                    string numpart = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                    andColValue += numpart;
                                }

                                Column andCol = index.Table.Columns[andColOrdinal];
                                andColBuf = new byte[andCol.Size + andCol.FrontBytes + andCol.BackBytes];

                                for (int coli = 0; coli < andColOrdinal; coli++)
                                {
                                    andColOffset += index.Table.Columns[coli].Size + index.Table.Columns[coli].FrontBytes + index.Table.Columns[coli].BackBytes;
                                }

                                coord.Handler.ConvertStringColumnValueToBinary(andColValue, andColBuf.Length, andCol.sType, andColBuf, 0);
                            }

                            List<int> deleteInd = new List<int>(chunkRows.Count);
                            for (int ri = 0; ri < chunkRows.Count; ri++)
                            {
                                bool toDelete = false;
                                if (andColBuf != null)
                                {
                                    if (coord.Handler.CompareBytes(andColBuf, chunkRows[ri], andColBuf.Length, 0, andColOffset) == 0)
                                    {
                                        toDelete = true;
                                    }
                                }
                                else
                                {
                                    toDelete = true;
                                }

                                if (toDelete)
                                {
                                    deleteInd.Add(ri);
                                }
                            }

                            int deletedSoFar = 0;
                            foreach (int di in deleteInd)
                            {
                                chunkRows.RemoveAt(di - deletedSoFar);
                                deletedSoFar++;
                            }
                        }
                    }

                    return i;
                }
            }
        }
    }
}
