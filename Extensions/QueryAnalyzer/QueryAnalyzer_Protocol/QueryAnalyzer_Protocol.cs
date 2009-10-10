using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace QueryAnalyzer_Protocol
{
    partial class QueryAnalyzer_Protocol : ServiceBase
    {
        internal static string dspaceexe = "dspace.exe";

        System.Threading.Thread lthd = null;
        System.Net.Sockets.Socket lsock = null;

        System.Threading.Thread stressthd = null;
        System.Net.Sockets.Socket stresssock = null;

        System.Threading.Thread rindexservthd = null;
        System.Net.Sockets.Socket rindexservsock = null;

        System.Threading.Thread rindexcleanupthd = null;

        internal static string CurrentDirNetPath;
        internal static Dictionary<string, Dictionary<string, ChunkRowsData>> rindexpins = new Dictionary<string, Dictionary<string, ChunkRowsData>>();

        public struct ChunkRowsData
        {
            public MySpace.DataMining.Binary.LongByteArray Bytes;
            public int RowLength;

            public ChunkRowsData(MySpace.DataMining.Binary.LongByteArray Bytes, int RowLength)
            {
                this.Bytes = Bytes;
                this.RowLength = RowLength;
            }

            public long NumberOfRows
            {
                get
                {
                    return Bytes.LongLength / RowLength;
                }
            }

            public RowData this[long index]
            {
                get
                {
                    RowData rd;
                    rd.Chunk = this;
                    rd.ChunkOffset = index * RowLength;
                    return rd;
                }
            }
        }

        public struct RowData
        {
            public ChunkRowsData Chunk;
            public long ChunkOffset;

            public int Length
            {
                get
                {
                    return Chunk.RowLength;
                }
            }

            public byte this[int index]
            {
                get
                {
                    if (index < 0 || index >= Chunk.RowLength)
                    {
                        throw new IndexOutOfRangeException("RowData index out of bounds");
                    }
                    return Chunk.Bytes[ChunkOffset + index];
                }

                set
                {
                    if (index < 0 || index >= Chunk.RowLength)
                    {
                        throw new IndexOutOfRangeException("RowData index out of bounds");
                    }
                    Chunk.Bytes[ChunkOffset + index] = value;
                }
            }
        }


        public QueryAnalyzer_Protocol()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
#if DEBUG
            {
                string _test_dspaceexe = @"C:\SimpleSolutions\Applications\DataMining\API\MySpace.Datamining.DistributedObjects\bin\Debug\dspace.exe";
                if (System.IO.File.Exists(_test_dspaceexe))
                {
                    dspaceexe = _test_dspaceexe;
                }
            }
            {
                string _test_dspaceexe = @"C:\Users\" + Environment.UserName + @"\Solutions\Applications\DataMining\API\MySpace.Datamining.DistributedObjects\bin\Debug\dspace.exe";
                if (System.IO.File.Exists(_test_dspaceexe))
                {
                    dspaceexe = _test_dspaceexe;
                }
            }
            {
                string _test_dspaceexe = @"C:\dspace\dspace.exe";
                if (System.IO.File.Exists(_test_dspaceexe))
                {
                    dspaceexe = _test_dspaceexe;
                }
            }
#endif
            string service_base_dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            System.Environment.CurrentDirectory = service_base_dir;

            CurrentDirNetPath = @"\\" + System.Net.Dns.GetHostName() + @"\" + System.Environment.CurrentDirectory.Replace(':', '$');

            lthd = new System.Threading.Thread(new System.Threading.ThreadStart(ListenThreadProc));
            lthd.IsBackground = true;
            lthd.Start();

            stressthd = new System.Threading.Thread(new System.Threading.ThreadStart(StressListenThreadProc));
            stressthd.IsBackground = true;
            stressthd.Start();

            rindexservthd = new System.Threading.Thread(new System.Threading.ThreadStart(RIndexServListenThreadProc));
            rindexservthd.IsBackground = true;
            rindexservthd.Start();

            rindexcleanupthd = new System.Threading.Thread(new System.Threading.ThreadStart(RIndexCleanupPinsProc));
            rindexcleanupthd.IsBackground = true;
            rindexcleanupthd.Start();
        }

        protected override void OnStop()
        {
            if (null != lthd)
            {
                lsock.Close();
                lsock = null;

                lthd.Abort();
                lthd = null;
            }

            if (null != stressthd)
            {
                stresssock.Close();
                stresssock = null;

                stressthd.Abort();
                stressthd = null;
            }
        }

        private void ListenThreadProc()
        {
            try
            {
                lsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55902);
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
                    ClientHandler ch = new ClientHandler();
                    System.Threading.Thread cthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ch.ClientThreadProc));
                    cthd.IsBackground = true;
                    cthd.Start(dllclientSock);
                }
            }
            catch (System.Threading.ThreadAbortException e)
            {
            }
            catch (Exception e)
            {
                XLog.errorlog("ListenThreadProc exception: " + e.ToString());
            }
        }

        private void StressListenThreadProc()
        {
            try
            {
                stresssock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55903);
                for (int i = 0; ; i++)
                {
                    try
                    {
                        stresssock.Bind(ipep);
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

                stresssock.Listen(30);

                for (; ; )
                {
                    System.Net.Sockets.Socket dllclientSock = stresssock.Accept();
                    StressClientHandler ch = new StressClientHandler();
                    System.Threading.Thread cthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ch.ClientThreadProc));
                    cthd.IsBackground = true;
                    cthd.Start(dllclientSock);
                }
            }
            catch (System.Threading.ThreadAbortException e)
            {
            }
            catch (Exception e)
            {
                XLog.errorlog("StressListenThreadProc exception: " + e.ToString());
            }
        }

        private void RIndexCleanupPinsProc()
        {
            for (; ; )
            {
                List<string> bads = new List<string>(rindexpins.Count);
                foreach (string indexname in rindexpins.Keys)
                {
                    if (!System.IO.File.Exists("ind.Index." + indexname + ".ind"))
                    {
                        bads.Add(indexname);
                    }
                }
                foreach (string bad in bads)
                {
                    rindexpins.Remove(bad);
                }
                System.Threading.Thread.Sleep(1000 * 10);
            }
        }

        private void RIndexServListenThreadProc()
        {
            try
            {
                //RIndexServClientHandler.LoadRIndexMI();

                rindexservsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55904);
                for (int i = 0; ; i++)
                {
                    try
                    {
                        rindexservsock.Bind(ipep);
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

                rindexservsock.Listen(30);

                for (; ; )
                {
                    System.Net.Sockets.Socket dllclientSock = rindexservsock.Accept();
                    RIndexServClientHandler ch = new RIndexServClientHandler();
                    System.Threading.Thread cthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ch.ClientThreadProc));
                    cthd.IsBackground = true;
                    cthd.Start(dllclientSock);
                }
            }
            catch (System.Threading.ThreadAbortException e)
            {
            }
            catch (Exception e)
            {
                XLog.errorlog("RIndexServListenThreadProc exception: " + e.ToString());
            }
        }


    }

    class StressClientHandler
    {
        private bool stoptest = false;
        private System.Threading.Thread testthd = null;
        private DbConnection conn = null;
        private int error = 0;

        internal void ClientThreadProc(object _sock)
        {
            System.Net.Sockets.Socket clientsock = (System.Net.Sockets.Socket)_sock;
            System.Net.Sockets.NetworkStream netstm = new System.Net.Sockets.NetworkStream(clientsock);
            byte[] buf = new byte[1024 * 1024 * 8];

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
                        case (byte)'p': //ping
                            {
                                if (System.Threading.Interlocked.Equals(0, error))
                                {
                                    netstm.WriteByte((byte)'+');
                                }
                                else
                                {
                                    netstm.WriteByte((byte)'-');
                                }
                            }
                            break;

                        case (byte)'s': //start stress test
                            {
                                if (testthd != null)
                                {
                                    netstm.WriteByte((byte)'-');
                                    throw new Exception("Only one test thread per connection is allowed");
                                }

                                int whattest = netstm.ReadByte();
                                string cmdText = XContent.ReceiveXString(netstm, buf);
                                string connstr = XContent.ReceiveXString(netstm, buf);

                                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                                conn = fact.CreateConnection();
                                conn.ConnectionString = connstr;
                                conn.Open();
                                DbCommand cmd = conn.CreateCommand();
                                cmd.CommandText = cmdText;

                                if (whattest == (byte)'n') //non-query
                                {
                                    testthd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                                    {
                                        try
                                        {
                                            while (!stoptest)
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            System.Threading.Interlocked.Increment(ref error);
                                            XLog.errorlog("StressClientHandler.ClientThreadProc exception: " + e.ToString());
                                        }
                                    }));
                                }
                                else
                                {
                                    testthd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                                    {
                                        try
                                        {
                                            DbDataReader reader = null;
                                            while (!stoptest)
                                            {
                                                if (reader != null)
                                                {
                                                    reader.Close();
                                                }
                                                reader = cmd.ExecuteReader();
                                                while (reader.Read() && !stoptest)
                                                {
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            System.Threading.Interlocked.Increment(ref error);
                                            XLog.errorlog("StressClientHandler.ClientThreadProc exception: " + e.ToString());
                                        }
                                    }));
                                }

                                testthd.IsBackground = true;
                                testthd.Start();
                                netstm.WriteByte((byte)'+');  //Test started successfully.
                            }
                            break;

                        case (byte)'t': //stop stress test
                            {
                                stoptest = true;
                                testthd.Join();
                                netstm.WriteByte((byte)'+');
                                stop = true;
                            }
                            break;

                        default:
                            throw new Exception("unknown action");
                    }
                }
            }
            catch (Exception e)
            {
                XLog.errorlog("StressClientHandler.ClientThreadProc exception: " + e.ToString());
            }
            finally
            {
                if (conn != null)
                {
                    try
                    {
                        conn.Close();
                        conn = null;
                    }
                    catch
                    {
                    }
                }
                netstm.Close();
                netstm = null;
                clientsock.Close();
                clientsock = null;
            }
        }
    }

    class ClientHandler
    {
        public bool BulkInsertEnabled = true;

        string bulkinsertingtable = null; // Name of table currently bulk-inserting into.
        string bulkinsertingtabledfsfile; // DFS file name of table bulkinsertingtable.
        string[] bulkinserthosts = null;
        string[] bulkinsertdirs = null; // Indices match up with bulkinserthosts.
        int bulkinsertinghostindex = -1; // Which index in bulkinserthosts I'm currently bulk-inserting to.
        System.IO.StreamWriter bulkinsertlistfile = null; // File listing the parts to bulkput.
        string bulkinsertlistfilepath; // Path to bulkinsertlistfile.
        System.IO.FileStream bulkinsertchunkfile = null; // Current chunk being written (on bulkinserthosts[bulkinsertinghostindex])
        string bulkinsertchunkfilehost;
        string bulkinsertchunkfilename; // Name only, of bulkinsertchunkfile.
        long bulkinsertchunkfilesize; // Number of bytes written to bulkinsertchunkfile (excluding header)
        long BULKINSERTCHUNKSIZEMAX;
        long bulkinsertchunknumber = 0;
        int[] bulkinsertingRowTypeSizes;
        string[] bulkinsertingRowTypes;
        int bulkinsertingOutputRowLength;

        private bool BulkInsert(string cmd)
        {
            if (!BulkInsertEnabled)
            {
                return false;
            }

            if (0 == string.Compare("INSERT", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
            {
                string s;

                s = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                if (0 == string.Compare("DFSTEMP", s, true))
                {
                    s = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                }
                if (0 == string.Compare("INTO", s, true))
                {
                    string tn = RDBMS_DBCORE.Qa.NextPart(ref cmd);

                    if (0 == string.Compare("VALUES", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                        && 0 == string.Compare("(", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
                    {

                        if (batchon)
                        {
                            FlushCommand(batchfname);
                        }

                        bool newbulkinsertchunk = true;
                        if (null != bulkinsertingtable)
                        {
                            if (0 == string.Compare(bulkinsertingtable, tn, true))
                            {
                                newbulkinsertchunk = false;
                                if (bulkinsertchunkfilesize >= BULKINSERTCHUNKSIZEMAX)
                                {
                                    newbulkinsertchunk = true;
                                    _FinishCurrentBulkInsertChunk();
                                }
                            }
                            else
                            {
                                // Different table, so finish...
                                FinishBulkInsert();
#if DEBUG
                                if (null != bulkinsertingtable)
                                {
                                    throw new Exception("DEBUG:  (null != bulkinsertingtable) after FinishBulkInsert()");
                                }
#endif
                            }
                        }
                        if (null == bulkinsertingtable)
                        {
                            // Start new bulk-insert!

#if DEBUG
                            if (!newbulkinsertchunk)
                            {
                                throw new Exception("DEBUG:  (null == bulkinsertingtable) && (!newbulkinsertchunk)");
                            }
#endif

                            // Share a single cluster lock:
                            string bihpartsoutput = null;
                            string systablesfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\ST_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
                            {
                                if (null == bulkinserthosts)
                                {
                                    bihpartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls -healthy");
                                }
                                (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec("SysTablesXmlFile", "'" + systablesfilepath + "'");
                            }

                            if (null == bulkinserthosts)
                            {
                                string[] bihlines = bihpartsoutput
                                    .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                                bulkinserthosts = new string[bihlines.Length];
                                bulkinsertdirs = new string[bihlines.Length];
                                for (int ib = 0; ib < bihlines.Length; ib++)
                                {
                                    string bihline = bihlines[ib];
                                    int ispbih = bihline.IndexOf(' ');
                                    string host = bihline.Substring(0, ispbih);
                                    string netpath = bihline.Substring(ispbih + 1);
                                    bulkinserthosts[ib] = host;
                                    bulkinsertdirs[ib] = netpath;
                                }
                                {
                                    // Shuffle up the hosts (and shuffle dirs the same):
                                    Random bulkinsertrand = new Random(unchecked(
                                        System.Threading.Thread.CurrentThread.ManagedThreadId
                                            + DateTime.Now.Millisecond));
                                    for (int ib = 0; ib < bulkinserthosts.Length; ib++)
                                    {
                                        int swapib = bulkinsertrand.Next(0, bulkinserthosts.Length);
                                        string temphost = bulkinserthosts[ib];
                                        string tempdir = bulkinsertdirs[ib];
                                        bulkinserthosts[ib] = bulkinserthosts[swapib];
                                        bulkinsertdirs[ib] = bulkinsertdirs[swapib];
                                        bulkinserthosts[swapib] = temphost;
                                        bulkinsertdirs[swapib] = tempdir;
                                    }
                                }
                            }

                            string tnt = tn;
                            if (tnt.Length > 6)
                            {
                                tnt = tnt.Substring(0, 6);
                            }
                            //bulkinsertlistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\RDBMS_BulkInsert_" + tnt + "_" + Guid.NewGuid().ToString().Substring(9, 13); // Too long.
                            bulkinsertlistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\BI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                            bulkinsertlistfile = new System.IO.StreamWriter(bulkinsertlistfilepath);

                            {
                                System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                                xd.Load(systablesfilepath);
                                System.IO.File.Delete(systablesfilepath);
                                System.Xml.XmlElement xeTables = xd.SelectSingleNode("/tables") as System.Xml.XmlElement;
                                if (null == xeTables)
                                {
                                    throw new Exception("SysTables format critical failure");
                                }
                                System.Xml.XmlElement xeTable = null;
                                foreach (System.Xml.XmlNode xn in xeTables.ChildNodes)
                                {
                                    if (0 == string.Compare(tn, xn["name"].InnerText, true))
                                    {
                                        xeTable = xn as System.Xml.XmlElement;
                                        break;
                                    }
                                }
                                if (null == xeTable)
                                {
                                    throw new Exception("No such table or table cannot be inserted into: " + tn);
                                }
                                bulkinsertingtabledfsfile = xeTable["file"].InnerText;
                                {
                                    string RowInfo;
                                    string TypeInfo; // Type
                                    bulkinsertingOutputRowLength = 0;
                                    {
                                        StringBuilder sbRowInfo = new StringBuilder();
                                        StringBuilder sbTypeInfo = new StringBuilder(); // Type
                                        foreach (System.Xml.XmlNode xn in xeTable.SelectNodes("column"))
                                        {
                                            if (0 != sbRowInfo.Length)
                                            {
                                                sbRowInfo.Append(',');
                                                sbTypeInfo.Append(','); // Type
                                            }
                                            string stsize = xn["bytes"].InnerText;
                                            int tsize = int.Parse(stsize);
                                            bulkinsertingOutputRowLength += tsize;
                                            sbRowInfo.Append(stsize);
                                            sbTypeInfo.Append(xn["type"].InnerText); // Type
                                        }
                                        RowInfo = sbRowInfo.ToString();
                                        TypeInfo = sbTypeInfo.ToString(); // Type
                                    }

                                    string[] sRowTypeSizes = RowInfo.Split(',');
                                    bulkinsertingRowTypeSizes = new int[sRowTypeSizes.Length];
                                    for (int i = 0; i < bulkinsertingRowTypeSizes.Length; i++)
                                    {
                                        bulkinsertingRowTypeSizes[i] = int.Parse(sRowTypeSizes[i]);
                                    }

                                    bulkinsertingRowTypes = TypeInfo.Split(',');
                                }
                            }

                            bulkinsertingtable = tn;
                        }
                        else
                        {
                            // Resuming current bulk-insert!..
                        }
                        if (newbulkinsertchunk) // New chunk (but may be same table).
                        {
                            if (++bulkinsertinghostindex >= bulkinserthosts.Length)
                            {
                                bulkinsertinghostindex = 0;
                            }
                            bulkinsertchunkfilehost = bulkinserthosts[bulkinsertinghostindex];
                            bulkinsertchunkfilename = string.Format("zd.{0}.{1}.{2}.zd", bulkinsertchunknumber++, "qabulkput", Guid.NewGuid().ToString());
                            bulkinsertchunkfile = new System.IO.FileStream(bulkinsertdirs[bulkinsertinghostindex] + @"\" + bulkinsertchunkfilename, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None);
                            byte[] NOHEADER = new byte[4];
                            MyToBytes(4, new byte[4], 0);
                            bulkinsertchunkfile.Write(NOHEADER, 0, NOHEADER.Length); // Header.
                            bulkinsertchunkfilesize = 0;
                        }

                        string[] uservalues = new string[bulkinsertingRowTypes.Length];
                        {
                            int nuservals = 0;
                            bool expectcomma = false;
                            string vtok = "";
                            for (; ; )
                            {
                                vtok = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                if ("," == vtok)
                                {
                                    if (!expectcomma)
                                    {
                                        throw new Exception("Unexpected comma in VALUES (...)");
                                    }
                                    expectcomma = false;
                                    continue;
                                }
                                {
                                    // Expect value or ")"!
                                    if ("" == vtok)
                                    {
                                        if (nuservals == uservalues.Length)
                                        {
                                            throw new Exception("Expected ) after VALUES(...");
                                        }
                                        else
                                        {
                                            throw new Exception("Expected " + nuservals.ToString() + " values and ) after VALUES(...");
                                        }
                                    }
                                    if (")" == vtok)
                                    {
                                        if (nuservals != uservalues.Length)
                                        {
                                            throw new Exception("Expected " + nuservals.ToString() + " values in VALUES(...");
                                        }
                                        break;
                                    }
                                    if (expectcomma)
                                    {
                                        throw new Exception("Expected comma in VALUES (...)");
                                    }
                                    if (nuservals >= uservalues.Length)
                                    {
                                        throw new Exception("Too many values in VALUES(...)");
                                    }
                                    string val = vtok;
                                    if ("+" == val || "-" == val)
                                    {
                                        vtok = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                        if (0 == vtok.Length)
                                        {
                                            throw new Exception("Expected number after sign");
                                        }
                                        val = val + vtok;
                                    }
                                    uservalues[nuservals++] = val;
                                }
                                expectcomma = true;
                            }
                        }

                        List<byte> valuebuf = new List<byte>();
                        //valuebuf.Clear();
                        int icol = 0;
                        try
                        {
                            for (; icol < bulkinsertingRowTypes.Length; icol++)
                            {
                                string uval = uservalues[icol];
                                string type = bulkinsertingRowTypes[icol];
                                int tsize = bulkinsertingRowTypeSizes[icol];
                                if (0 == string.Compare("NULL", uval, true))
                                {
                                    valuebuf.Add(1); // Nullable, IsNull=true!
                                    for (int remain = (tsize - 1); remain > 0; remain--)
                                    {
                                        valuebuf.Add(0); // Padding to column size.
                                    }
                                }
                                else
                                {
                                    if (type.StartsWith("char"))
                                    {
                                        if (uval.Length <= 2 || '\'' != uval[0] || '\'' != uval[uval.Length - 1])
                                        {
                                            throw new Exception("Invalid string: " + uval);
                                        }
                                        string x = uval.Substring(1, uval.Length - 2).Replace("''", "'");
                                        byte[] bx = System.Text.Encoding.Unicode.GetBytes(x);
                                        if (bx.Length > tsize - 1)
                                        {
                                            throw new Exception("String too large for " + type);
                                        }
                                        valuebuf.Add(0); // Not null.
                                        valuebuf.AddRange(bx);
                                        for (int remain = (tsize - 1) - bx.Length; remain > 0; remain--)
                                        {
                                            valuebuf.Add(0); // Padding to char column size.
                                        }
                                    }
                                    else if ("int" == type)
                                    {
                                        int x = int.Parse(uval);
                                        RDBMS_DBCORE.Qa.IntToDbBytesAppend(x, valuebuf);
                                    }
                                    else if ("long" == type)
                                    {
                                        long x = long.Parse(uval);
                                        RDBMS_DBCORE.Qa.LongToDbBytesAppend(x, valuebuf);
                                    }
                                    else if ("double" == type)
                                    {
                                        double x = double.Parse(uval);
                                        RDBMS_DBCORE.Qa.DoubleToDbBytesAppend(x, valuebuf);
                                    }
                                    else if ("DateTime" == type)
                                    {
                                        DateTime dt = DateTime.Parse(uval.Substring(1, uval.Length - 2));
                                        RDBMS_DBCORE.Qa.DateTimeToDbBytesAppend(dt, valuebuf);
                                    }
                                    else
                                    {
                                        throw new Exception("Unhandled data type " + type);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            throw new Exception("Problem with value " + uservalues[icol] + " of type " + bulkinsertingRowTypes[icol] + ": " + e.Message, e);
                        }

                        if (valuebuf.Count != bulkinsertingOutputRowLength)
                        {
                            throw new Exception("Record length mismatch (" + valuebuf.Count + " != " + bulkinsertingOutputRowLength.ToString() + ") [#8129]");
                        }
                        else
                        {
                            int valuebufCount = valuebuf.Count;
                            for (int i = 0; i < valuebufCount; i++)
                            {
                                bulkinsertchunkfile.WriteByte(valuebuf[i]);
                            }
                            bulkinsertchunkfilesize += valuebufCount;
                        }

                        return true;
                    }
                }
            }

            // Isn't a bulk-insertable insert command,
            // so finish any pending bulk inserts
            // in preparation for the non-bulk-insert command.
            FinishBulkInsert();
            return false;
        }

        private void _FinishCurrentBulkInsertChunk()
        {
#if DEBUG
            if (null == bulkinsertchunkfile)
            {
                throw new Exception("DEBUG:  _FinishCurrentBulkInsertChunk: (null == bulkinsertchunkfile)");
            }
            if (null == bulkinsertlistfile)
            {
                throw new Exception("DEBUG:  _FinishCurrentBulkInsertChunk: (null == bulkinsertlistfile)");
            }
            if (bulkinsertchunkfilesize <= 0)
            {
                throw new Exception("DEBUG:  _FinishCurrentBulkInsertChunk: (bulkinsertchunkfilesize <= 0)");
            }
#endif
            bulkinsertchunkfile.Close();
            bulkinsertchunkfile = null;
            if (bulkinsertchunkfilesize > 0)
            {
                bulkinsertlistfile.WriteLine("{0} {1} {2}", bulkinsertchunkfilehost, bulkinsertchunkfilename, bulkinsertchunkfilesize);
            }
        }

        private bool FinishBulkInsert()
        {
            if (null != bulkinsertingtable)
            {
                if (null != bulkinsertchunkfile)
                {
                    _FinishCurrentBulkInsertChunk();
                }

                string TableName = bulkinsertingtable;

                bulkinsertchunknumber = 0;
                bulkinsertingtable = null;

                bulkinsertlistfile.Close();
                bulkinsertlistfile = null;

                string dfsfilename = "RDBMS_QaBulkPut_" + TableName + "_" + Guid.NewGuid().ToString();
                using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
                {
                    Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " bulkput \"" + bulkinsertlistfilepath + "\" \"" + dfsfilename + "\" rbin@" + bulkinsertingOutputRowLength.ToString());
                    Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " combine \"" + dfsfilename + "\" \"" + bulkinsertingtabledfsfile + "\"");
                }

                System.IO.File.Delete(bulkinsertlistfilepath);
                bulkinsertlistfilepath = null;

                return true;
            }
            return false;
        }

        private void BatchFlushCommand(string fname, string cmd, long batchsize)
        {
            try
            {
                FinishBulkInsert();

                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(fname, true))
                {
                    writer.Write(cmd);
                    writer.Write("\r\n\0\r\n");
                    writer.Close();
                }

                System.IO.FileInfo fi = new System.IO.FileInfo(fname);
                if (fi.Length >= batchsize)
                {
                    FlushCommand(fname);
                }
            }
            catch (Exception e)
            {
                throw new CommandFlushAbortException(e.ToString());
            }
        }

        private void FlushCommand(string fname)
        {
            if (!System.IO.File.Exists(fname))
            {
                return;
            }

            try
            {
                string netfpath = @"\\" + System.Net.Dns.GetHostName() + @"\" + System.Environment.CurrentDirectory.Replace(':', '$') + @"\" + fname;

                string output = (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec("BATCHEXECUTE", "'" + netfpath + "'");

                if (output.IndexOf("Error:{", StringComparison.OrdinalIgnoreCase) > -1 &&
                    output.IndexOf("Details:", StringComparison.OrdinalIgnoreCase) > -1 &&
                    output.IndexOf("at ", StringComparison.OrdinalIgnoreCase) > -1)
                {
                    throw new Exception("exec RDBMS_QueryAnalyzer.DBCORE BATCHEXECUTE error: " + output);
                }
            }
            catch (Exception e)
            {
                throw new CommandFlushAbortException(e.ToString());
            }
            finally
            {
                System.IO.File.Delete(fname);
            }
        }


        void _CreateRIndex(string IndexName, string SourceTable)
        {
            // Create index IndexName from table SourceTable.

            string systablesfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\CRI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec("SysTablesXmlFile", "'" + systablesfilepath + "'");
            }

            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
            xd.Load(systablesfilepath);
            System.IO.File.Delete(systablesfilepath);
            System.Xml.XmlElement xeTables = xd.SelectSingleNode("/tables") as System.Xml.XmlElement;
            if (null == xeTables)
            {
                throw new Exception("SysTables format critical failure");
            }
            System.Xml.XmlElement xeTable = null;
            foreach (System.Xml.XmlNode xn in xeTables.ChildNodes)
            {
                if (0 == string.Compare(SourceTable, xn["name"].InnerText, true))
                {
                    xeTable = xn as System.Xml.XmlElement;
                    break;
                }
            }
            if (null == xeTable)
            {
                throw new Exception("No such table or table cannot have RINDEX applied: " + SourceTable);
            }

            string sRowSize = xeTable["size"].InnerText;
            int RowSize = int.Parse(sRowSize);

            if (RowSize < 9 || 9 != int.Parse(xeTable.SelectNodes("column")[0]["bytes"].InnerText))
            {
                throw new Exception("First column of table must be 9 bytes (LONG, DATETIME or CHAR(4))");
            }

            string SourceTableDfsFile = xeTable["file"].InnerText;

            string outputBulkGet1;
            string bulkcrilistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\BCRI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
            string htnppartsoutput;
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                htnppartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls");
                outputBulkGet1 = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " bulkget \"" + bulkcrilistfilepath + "\" \"" + SourceTableDfsFile + "\"");
            }

            string[] bulkparts; // Lines of "<host>[;<...>] <chunkname> <size>" but <size> excludes the size of the header.
            {
                bulkparts = System.IO.File.ReadAllLines(bulkcrilistfilepath);
                if (bulkparts.Length == 1 && string.IsNullOrEmpty(bulkparts[0]))
                {
                    bulkparts = new string[0];
                }
                System.IO.File.Delete(bulkcrilistfilepath);
            }

            Dictionary<string, string> HostToNetPath;
            {
                string[] htnlines = htnppartsoutput
                    .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                HostToNetPath = new Dictionary<string, string>(htnlines.Length, new _CaseInsensitiveEqualityComparer_74823());
                for (int ip = 0; ip < htnlines.Length; ip++)
                {
                    string htnline = htnlines[ip];
                    int isphtn = htnline.IndexOf(' ');
                    string host = htnline.Substring(0, isphtn);
                    host = _IPAddressUtil.GetName(host);
                    string netpath = htnline.Substring(isphtn + 1);
                    HostToNetPath[host] = netpath;
                }
            }

            List<string>[] HostToChunkInfo = new List<string>[AllProtocolHosts.Length]; // "<chunkname> <size>"
            for (int ih = 0; ih < HostToChunkInfo.Length; ih++)
            {
                List<string> chunkinfo = new List<string>();
                string host = AllProtocolHosts[ih];
                host = _IPAddressUtil.GetName(host);
                for (int curbulkpart = 0; curbulkpart < bulkparts.Length; curbulkpart++)
                {
                    if (null != bulkparts[curbulkpart])
                    {
                        string[] bparts = bulkparts[curbulkpart].Split(' ');
                        string[] bhosts = bparts[0].Split(';');
                        string ssize = bparts[2];
                        string bchunkname = bparts[1];
                        string corighost = bhosts[0];
                        string chost = _IPAddressUtil.GetName(corighost);
                        if (!HostToNetPath.ContainsKey(host))
                        {
                            throw new Exception("DFS file chunk " + bchunkname + " on machine " + corighost + " is not accessible (not part of this cluster or is down)");
                        }
                        if (0 != string.Compare(chost, host, true))
                        {
                            continue;
                        }
                        chunkinfo.Add(bchunkname + " " + ssize);
                        bulkparts[curbulkpart] = null;
                    }
                }
                HostToChunkInfo[ih] = chunkinfo;
            }
            for (int curbulkpart = 0; curbulkpart < bulkparts.Length; curbulkpart++)
            {
                if (null != bulkparts[curbulkpart])
                {
                    throw new Exception("Unassigned DFS file chunk: " + bulkparts[curbulkpart]);
                }
            }

            System.Net.Sockets.NetworkStream[] nhosts = new System.Net.Sockets.NetworkStream[HostToChunkInfo.Length];
            try
            {
                const int KeyOffset = 0;
                const int KeySize = 1 + 8;
                byte[][] HostToFirstKeys = new byte[HostToChunkInfo.Length][];
                byte[][] nlbufs = new byte[HostToChunkInfo.Length][];

                // Get partials...
                //for (int ih = 0; ih < HostToChunkInfo.Length; ih++)
                MySpace.DataMining.Threading.ThreadTools.Parallel(
                    new Action<int>(
                    delegate(int ih)
                    {
                        int nlbuflen;
                        byte[] nlbuf = nlbufs[ih];
                        if (null == nlbuf)
                        {
                            nlbuf = new byte[1024 * 1024 * 8];
                            nlbufs[ih] = nlbuf;
                        }
                        List<string> chunkinfo = HostToChunkInfo[ih];
                        string orighost = AllProtocolHosts[ih];
                        string host = _IPAddressUtil.GetName(orighost);
                        if (!HostToNetPath.ContainsKey(host))
                        {
                            throw new Exception("Machine " + orighost + " is not accessible (not part of this cluster or is down)");
                        }
                        string netpath = HostToNetPath[host];
                        System.Net.Sockets.Socket xsock = null;
                        xsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                        xsock.Connect(host, 55904);
                        System.Net.Sockets.NetworkStream netstm = new System.Net.Sockets.NetworkStream(xsock, true); // Owned=true
                        nhosts[ih] = netstm;
                        netstm.WriteByte((byte)'p'); // Get partial.
                        XContent.SendXContent(netstm, IndexName);
                        XContent.SendXContent(netstm, netpath);
                        XContent.SendXContent(netstm, string.Join(";", chunkinfo.ToArray()));
                        {
                            MyToBytes(RowSize, nlbuf, 0);
                            XContent.SendXContent(netstm, nlbuf, 4);
                        }
                        nlbuf = XContent.ReceiveXBytes(netstm, out nlbuflen, nlbuf); // For now: keys of KeySize-1 to exclude Nullable IsNull.
                        {
                            byte[] UniqueKeys = new byte[nlbuflen];
                            Buffer.BlockCopy(nlbuf, 0, UniqueKeys, 0, nlbuflen);
                            HostToFirstKeys[ih] = UniqueKeys;
                        }
                    }), HostToChunkInfo.Length, HostToChunkInfo.Length);

                // Publish MI to all!
                //for (int ih = 0; ih < HostToChunkInfo.Length; ih++)
                MySpace.DataMining.Threading.ThreadTools.Parallel(
                    new Action<int>(
                    delegate(int ih)
                    {
                        int nlbuflen;
                        byte[] nlbuf = nlbufs[ih];
                        if (null == nlbuf)
                        {
                            nlbuf = new byte[1024 * 1024 * 8];
                            nlbufs[ih] = nlbuf;
                        }
                        System.Net.Sockets.NetworkStream netstm = nhosts[ih];
                        netstm.WriteByte((byte)'i'); // Master Index.
                        for (int i = 0; i < HostToChunkInfo.Length; i++)
                        {
                            netstm.WriteByte((byte)'+');
                            string orighost = AllProtocolHosts[i];
                            string host = _IPAddressUtil.GetName(orighost);
                            XContent.SendXContent(netstm, host);
                            XContent.SendXContent(netstm, HostToFirstKeys[ih]);
                        }
                        netstm.WriteByte((byte)'-'); // Done.
                        {
                            int nch = netstm.ReadByte();
                            if ('-' == nch)
                            {
                                throw new Exception("RIndexServ did not return a success: " + XContent.ReceiveXString(netstm, nlbuf));
                            }
                            else if ('+' != nch)
                            {
                                throw new Exception("RIndexServ did not return a success");
                            }
                        }
                    }), HostToChunkInfo.Length, HostToChunkInfo.Length);
            }
            finally
            {
                for (int ih = 0; ih < nhosts.Length; ih++)
                {
                    if (null != nhosts[ih])
                    {
                        nhosts[ih].Close();
                        nhosts[ih] = null;
                    }
                }
            }

        }

        void _CreateRIndexNonPartial(string IndexName, string SourceTable, bool pinmemory, string keycolumn)
        {
            string systablesfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\CRI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec("SysTablesXmlFile", "'" + systablesfilepath + "'");
            }

            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
            xd.Load(systablesfilepath);
            System.IO.File.Delete(systablesfilepath);
            System.Xml.XmlElement xeTables = xd.SelectSingleNode("/tables") as System.Xml.XmlElement;
            if (null == xeTables)
            {
                throw new Exception("SysTables format critical failure");
            }
            System.Xml.XmlElement xeTable = null;
            foreach (System.Xml.XmlNode xn in xeTables.ChildNodes)
            {
                if (0 == string.Compare(SourceTable, xn["name"].InnerText, true))
                {
                    xeTable = xn as System.Xml.XmlElement;
                    break;
                }
            }
            if (null == xeTable)
            {
                throw new Exception("No such table or table cannot have RINDEX applied: " + SourceTable);
            }           

            string indexfilename = GetIndexFileName(IndexName);
            if (System.IO.File.Exists(indexfilename))
            {
                throw new Exception("Index: " + IndexName + " already exists.");
            }

            System.Xml.XmlNodeList xnColumns = xeTable.SelectNodes("column");
            int keyordinal = -1;
            int keyoffset = 0;
            int keylen = 0;
            if (keycolumn.Length > 0)
            {
                for (int ci = 0; ci < xnColumns.Count; ci++)
                {
                    System.Xml.XmlNode xn = xnColumns[ci];
                    int colbytes = int.Parse(xn["bytes"].InnerText);
                    if (string.Compare(xn["name"].InnerText, keycolumn, true) == 0)
                    {
                        keyordinal = ci;
                        keylen = colbytes;
                        break;
                    }
                    keyoffset += colbytes;
                }
                if (keyordinal == -1)
                {
                    throw new Exception("Key column: " + keycolumn + " is not found in table definition.");
                }
            }
            else
            {
                string sRowSize = xeTable["size"].InnerText;
                int RowSize = int.Parse(sRowSize);
                if (RowSize != 9 * 3 || 9 != int.Parse(xnColumns[0]["bytes"].InnerText))
                {
                    throw new Exception("Table must contain exactly 3 columns of LONG");
                }
                keyordinal = 0;
                keyoffset = 0;
                keylen = 9;
            }

            string SourceTableDfsFile = xeTable["file"].InnerText;

            string outputBulkGet1;
            string bulkcrilistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\BCRI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
            string htnppartsoutput;
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                htnppartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls");
                outputBulkGet1 = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " bulkget \"" + bulkcrilistfilepath + "\" \"" + SourceTableDfsFile + "\"");
            }

            string[] bulkparts; // Lines of "<host>[;<...>] <chunkname> <size>" but <size> excludes the size of the header.
            {
                bulkparts = System.IO.File.ReadAllLines(bulkcrilistfilepath);
                if (bulkparts.Length == 1 && string.IsNullOrEmpty(bulkparts[0]))
                {
                    bulkparts = new string[0];
                }
                System.IO.File.Delete(bulkcrilistfilepath);
            }

            Dictionary<string, string> HostToNetPath;
            {
                string[] htnlines = htnppartsoutput
                    .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                HostToNetPath = new Dictionary<string, string>(htnlines.Length, new _CaseInsensitiveEqualityComparer_74823());
                for (int ip = 0; ip < htnlines.Length; ip++)
                {
                    string htnline = htnlines[ip];
                    int isphtn = htnline.IndexOf(' ');
                    string host = htnline.Substring(0, isphtn);
                    //host = _IPAddressUtil.GetName(host);
                    string netpath = htnline.Substring(isphtn + 1);
                    HostToNetPath[host] = netpath;
                }
            }

            string mifn = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\CMI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
            using (System.IO.FileStream mi = new System.IO.FileStream(mifn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
            {
                byte[] rowbuf = new byte[keylen];
                foreach (string line in bulkparts)
                {
                    string[] parts = line.Split(' ');
                    string[] hosts = parts[0].Split(';');
                    string chunkname = parts[1];
                    long chunksize = Int64.Parse(parts[2]);
                    if (chunksize == 0)
                    {
                        continue;
                    }
                    string host = null;
                    for (int hi = 0; hi < hosts.Length; hi++)
                    {
                        if (HostToNetPath.ContainsKey(hosts[hi]))
                        {
                            host = hosts[hi];
                            break;
                        }
                    }
                    if (host == null)
                    {
                        throw new Exception("Chunk " + chunkname + " is not accessible (not part of this cluster or is down)");
                    }
                    using (System.IO.FileStream fs = new System.IO.FileStream(HostToNetPath[host] + @"\" + chunkname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                    {
                        {
                            // Skip the dfs-chunk file-header...
                            int headerlength = 0;
                            {
                                if (rowbuf.Length < 4)
                                {
                                    rowbuf = new byte[4];
                                }
                                if (4 != fs.Read(rowbuf, 0, 4))
                                {
                                    continue;
                                }
                                {
                                    headerlength = BytesToInt(rowbuf, 0);
                                    if (headerlength > 4)
                                    {
                                        int hremain = headerlength - 4;
                                        if (hremain > rowbuf.Length)
                                        {
                                            rowbuf = new byte[hremain];
                                        }
                                        StreamReadExact(fs, rowbuf, hremain);
                                    }
                                }
                            }
                        }
                        if (keyoffset > 0)
                        {
                            fs.Seek((long)keyoffset, System.IO.SeekOrigin.Current);
                        }
                        StreamReadExact(fs, rowbuf, keylen);
                        mi.Write(rowbuf, 0, keylen);
                        byte[] chunknamebuf = System.Text.Encoding.UTF8.GetBytes(HostToNetPath[host] + @"\" + chunkname);
                        mi.Write(chunknamebuf, 0, chunknamebuf.Length);
                        mi.WriteByte((byte)'\0');
                        fs.Close();
                    }
                }
                mi.Close();
            }

            string targetfn = indexfilename;
            string targetpath = System.Environment.CurrentDirectory.Replace(':', '$');
            foreach (string host in HostToNetPath.Keys)
            {
                System.IO.File.Copy(mifn, @"\\" + host + @"\" + targetpath + @"\" + targetfn, true);
            }
            System.IO.File.Delete(mifn);

            if (pinmemory)
            {
                string temppfn = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\CMI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                System.IO.FileStream pfs = System.IO.File.Create(temppfn);
                pfs.Close();
                string pinfn = GetIndexPinFileName(IndexName);
                foreach (string host in HostToNetPath.Keys)
                {
                    System.IO.File.Copy(temppfn, @"\\" + host + @"\" + targetpath + @"\" + pinfn, true);
                }
                System.IO.File.Delete(temppfn);
            }

            {
                string sysindexesfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\sys.indexes";
                System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                if (System.IO.File.Exists(sysindexesfilepath))
                {
                    xi.Load(sysindexesfilepath);                    
                }
                else
                {
                    System.Xml.XmlElement xeIndexes = xi.CreateElement("indexes");
                    xi.AppendChild(xeIndexes);
                }
                System.Xml.XmlElement xeIndex = xi.CreateElement("index");                
                System.Xml.XmlElement xeIndexName = xi.CreateElement("name");
                xeIndexName.InnerText = IndexName;
                xeIndex.AppendChild(xeIndexName);
                System.Xml.XmlElement xeOrdinal = xi.CreateElement("ordinal");
                xeOrdinal.InnerText = keyordinal.ToString();
                xeIndex.AppendChild(xeOrdinal);
                System.Xml.XmlElement xePin = xi.CreateElement("pin");
                xePin.InnerText = pinmemory ? "1" : "0";
                xeIndex.AppendChild(xePin);
                System.Xml.XmlElement xeIndTable = xi.CreateElement("table");
                xeIndex.AppendChild(xeIndTable);
                System.Xml.XmlElement xeIndTableName = xi.CreateElement("name");
                xeIndTableName.InnerText = SourceTable;
                xeIndTable.AppendChild(xeIndTableName);
                foreach (System.Xml.XmlNode xn in xnColumns)
                {
                    System.Xml.XmlElement xeCol = xi.CreateElement("column");
                    xeIndTable.AppendChild(xeCol);
                    System.Xml.XmlElement xeColName = xi.CreateElement("name");
                    xeColName.InnerText = xn["name"].InnerText;
                    xeCol.AppendChild(xeColName);
                    System.Xml.XmlElement xeColType = xi.CreateElement("type");
                    xeColType.InnerText = xn["type"].InnerText;
                    xeCol.AppendChild(xeColType);
                    System.Xml.XmlElement xeColBytes = xi.CreateElement("bytes");
                    xeColBytes.InnerText = xn["bytes"].InnerText;
                    xeCol.AppendChild(xeColBytes);  
                }
                xi.DocumentElement.AppendChild(xeIndex);

                string tempsifn = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\CMI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                xi.Save(tempsifn);
                foreach (string host in HostToNetPath.Keys)
                {
                    System.IO.File.Copy(tempsifn, @"\\" + host + @"\" + targetpath + @"\sys.indexes", true);
                }
                System.IO.File.Delete(tempsifn);
            }
        }

        string GetIndexFileName(string indexName)
        {
            return "ind.Index." + indexName + ".ind";
        }

        string GetIndexPinFileName(string indexName)
        {
            return "ind.Pin." + indexName + ".ind";
        }

        bool QlCreateRIndex(string cmd)
        {
            if (0 == string.Compare("CREATE", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                && 0 == string.Compare("RINDEX", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
            {
                string IndexName = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                if (0 == string.Compare("FROM", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
                {
                    string SourceTable = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                    if (0 == SourceTable.Length)
                    {
                        throw new Exception("Expected table name for CREATE RINDEX");
                    }

                    bool pinmem = false;
                    string keycolumn = "";
                    for (; ; )
                    {
                        string nextcmd = RDBMS_DBCORE.Qa.NextPart(ref cmd).ToLower();
                        if (nextcmd.Length == 0)
                        {
                            break;
                        }
                        switch (nextcmd)
                        {
                            case "pinmemory":
                                pinmem = true;
                                break;

                            case "on":
                                {
                                    keycolumn = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                                    if (keycolumn.Length == 0)
                                    {
                                        throw new Exception("Expected key column name for CREATE RINDEX ON");
                                    }
                                }
                                break;
                        }
                    }                  
                    _CreateRIndexNonPartial(IndexName, SourceTable, pinmem, keycolumn);
                    return true;
                }
                else
                {
                    throw new Exception("Expected FROM after CREATE RINDEX " + IndexName);
                }
            }
            return false;
        }

        bool QlAlterRIndexRenameSwap(string cmd)
        {
            if (0 == string.Compare("ALTER", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                && 0 == string.Compare("RINDEX", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
            {
                string indexname1 = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                if (indexname1.Length == 0)
                {
                    throw new Exception("ALTER RINDEX expects an index name.");
                }
                if (0 == string.Compare("RENAME", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                && 0 == string.Compare("SWAP", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
                {
                    string indexname2 = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                    if (indexname2.Length == 0)
                    {
                        throw new Exception("ALTER RINDEX expects an index name.");
                    }
                    _AlterRIndexRenameSwap(indexname1, indexname2);
                    return true;
                }
            }
            return false;
        }

        void _AlterRIndexRenameSwap(string indexname1, string indexname2)
        {
            string pinfn1 = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\ind.Pin." + indexname1 + ".ind";
            string pinfn2 = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\ind.Pin." + indexname2 + ".ind";

            if (System.IO.File.Exists(pinfn1) || System.IO.File.Exists(pinfn2))
            {
                throw new Exception("ALTER RINDEX only operates on indexes that are not memory pinned.");
            }

            string htnppartsoutput;
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                htnppartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls");
            }
            Dictionary<string, string> HostToNetPath;
            {
                string[] htnlines = htnppartsoutput
                    .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                HostToNetPath = new Dictionary<string, string>(htnlines.Length, new _CaseInsensitiveEqualityComparer_74823());
                for (int ip = 0; ip < htnlines.Length; ip++)
                {
                    string htnline = htnlines[ip];
                    int isphtn = htnline.IndexOf(' ');
                    string host = htnline.Substring(0, isphtn);
                    string netpath = htnline.Substring(isphtn + 1);
                    HostToNetPath[host] = netpath;
                }
            }
            string targetpath = System.Environment.CurrentDirectory.Replace(':', '$');
            string indexfn1 = targetpath + @"\ind.Index." + indexname1 + ".ind";
            string indexfn2 = targetpath + @"\ind.Index." + indexname2 + ".ind";
            string renaming = targetpath + @"\ind.Index.Renaming." + indexname1 + ".ind";

            foreach (string host in HostToNetPath.Keys)
            {
                string indexnetpath1 = @"\\" + host + @"\" + indexfn1;
                string indexnetpath2 = @"\\" + host + @"\" + indexfn2;
                string renamingnetpath = @"\\" + host + @"\" + renaming;
                System.IO.File.Move(indexnetpath1, renamingnetpath);
                System.IO.File.Move(indexnetpath2, indexnetpath1);
                System.IO.File.Move(renamingnetpath, indexnetpath2);
            }
        }

        bool QlDropRIndex(string cmd)
        {
            if (0 == string.Compare("DROP", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
               && 0 == string.Compare("RINDEX", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
            {
                string indexname = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                if (indexname.Length == 0)
                {
                    throw new Exception("DROP RINDEX expects an index name.");
                }
                _DropRIndex(indexname);
                return true;
            }
            return false;
        }

        void _DropRIndex(string indexname)
        {
            string htnppartsoutput;
            using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
            {
                htnppartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls");
            }
            Dictionary<string, string> HostToNetPath;
            {
                string[] htnlines = htnppartsoutput
                    .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                HostToNetPath = new Dictionary<string, string>(htnlines.Length, new _CaseInsensitiveEqualityComparer_74823());
                for (int ip = 0; ip < htnlines.Length; ip++)
                {
                    string htnline = htnlines[ip];
                    int isphtn = htnline.IndexOf(' ');
                    string host = htnline.Substring(0, isphtn);
                    string netpath = htnline.Substring(isphtn + 1);
                    HostToNetPath[host] = netpath;
                }
            }
            string targetpath = System.Environment.CurrentDirectory.Replace(':', '$');
            string indexfn = targetpath + @"\ind.Index." + indexname + ".ind";
            string pinfn = targetpath + @"\ind.Pin." + indexname + ".ind";

            foreach (string host in HostToNetPath.Keys)
            {
                string indexnetpath = @"\\" + host + @"\" + indexfn;
                System.IO.File.Delete(indexnetpath);
                string pinnetpath = @"\\" + host + @"\" + pinfn;
                System.IO.File.Delete(pinnetpath);
            }

            string sysindexesfn = "sys.indexes";
            if (System.IO.File.Exists(sysindexesfn))
            {
                System.Xml.XmlDocument xi = new System.Xml.XmlDocument();
                xi.Load(sysindexesfn);
                System.Xml.XmlNodeList xnIndexes = xi.SelectNodes("/indexes/index");
                bool found = false;
                for(int i = 0; i < xnIndexes.Count; i++)
                {
                    System.Xml.XmlNode xnIndex = xnIndexes[i];
                    if (string.Compare(xnIndex["name"].InnerText, indexname, true) == 0)
                    {
                        xi.DocumentElement.RemoveChild(xnIndex);
                        found = true;
                        break;
                    }
                }
                if (found)
                {
                    string tempsifn = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\DMI_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                    xi.Save(tempsifn);
                    foreach (string host in HostToNetPath.Keys)
                    {
                        System.IO.File.Copy(tempsifn, @"\\" + host + @"\" + targetpath + @"\sys.indexes", true);
                    }
                    System.IO.File.Delete(tempsifn);
                }
            }
        }

        bool QlAlterTableRenameSwap(string cmd)
        {
            if (0 == string.Compare("ALTER", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                && 0 == string.Compare("TABLE", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
            {
                string TableName1 = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                if (0 == string.Compare("RENAME", RDBMS_DBCORE.Qa.NextPart(ref cmd), true)
                    && 0 == string.Compare("SWAP", RDBMS_DBCORE.Qa.NextPart(ref cmd), true))
                {

                    string TableName2 = RDBMS_DBCORE.Qa.NextPart(ref cmd);
                    if (0 == TableName1.Length || 0 == TableName2.Length)
                    {
                        throw new Exception("Invalid table name in ALTER TABLE " + TableName1 + " RENAME SWAP " + TableName2);
                    }
                    if (0 != RDBMS_DBCORE.Qa.NextPart(ref cmd).Length)
                    {
                        throw new Exception("Unexpected data after ALTER TABLE query");
                    }

                    FinishBulkInsert();
                    if (batchon)
                    {
                        FlushCommand(batchfname);
                    }

                    Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " swap \"dfs://RDBMS_Table_" + TableName1 + "\" \"dfs://RDBMS_Table_" + TableName2 + "\"");
                    return true;

                }
            }
            return false;
        }


        class _IPAddressUtil
        {

            public static string GetIPv4Address(string HostnameOrIP)
            {
                System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(HostnameOrIP);
                for (int i = 0; i < addresslist.Length; i++)
                {
                    if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                    {
                        return addresslist[i].ToString();
                    }
                }
                throw new Exception("IPAddressUtil.GetAddress: No IPv4 address found for " + HostnameOrIP);
            }

            public static string GetNameNoCache(string ipaddr)
            {
                try
                {
                    System.Net.IPHostEntry iphe = System.Net.Dns.GetHostEntry(ipaddr);
                    if (null == iphe || null == iphe.HostName)
                    {
                        return ipaddr;
                    }
                    return iphe.HostName;
                }
                catch (Exception e)
                {
#if CLIENT_LOG_ALL
                AELight.LogOutputToFile("CLIENT_LOG_ALL: IPAddressUtil.GetNameNoCache: unable to lookup host for IP address '" + ipaddr + "': " + e.ToString());
#endif
                    return ipaddr;
                }
            }

            static Dictionary<string, string> namecache = new Dictionary<string, string>(new _CaseInsensitiveEqualityComparer_74823());
            public static string GetName(string ipaddr)
            {
                lock (namecache)
                {
                    if (!namecache.ContainsKey(ipaddr))
                    {
                        string result = GetNameNoCache(ipaddr);
                        namecache[ipaddr] = result;
                        //namecache[result] = result;
                        return result;
                    }
                }
                return namecache[ipaddr];
            }

            public static void FlushCachedNames()
            {
                lock (namecache)
                {
                    namecache = new Dictionary<string, string>(new _CaseInsensitiveEqualityComparer_74823());
                }
            }


            // Returns the current host from the list of hosts, or null if not found.
            // Considers different ways of representing the current hostname.
            public static string FindCurrentHost(IList<string> hosts)
            {
                string myhost1 = System.Net.Dns.GetHostName();
                string myhost2 = _IPAddressUtil.GetIPv4Address(myhost1);
                string myhost3 = "localhost";
                string myhost4 = "127.0.0.1";
                string myhost5 = Environment.MachineName;
                string myhost6 = _IPAddressUtil.GetName(myhost2);
                string selfhost = null;
                for (int i = 0; i < hosts.Count; i++)
                {
                    string xhost = hosts[i];
                    if (0 == string.Compare(myhost1, xhost, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(myhost2, xhost, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(myhost3, xhost, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(myhost4, xhost, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(myhost5, xhost, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(myhost6, xhost, StringComparison.OrdinalIgnoreCase)
                        )
                    {
                        selfhost = xhost;
                        break;
                    }
                }
                return selfhost;
            }

        }


        string sessionID = "";
        string batchfname = "";
        long batchsize = 0;
        bool batchon = false;

        string[] AllProtocolHosts;


        internal void ClientThreadProc(object _sock)
        {
            System.Net.Sockets.Socket clientsock = (System.Net.Sockets.Socket)_sock;
            System.Net.Sockets.NetworkStream netstm = new System.Net.Sockets.NetworkStream(clientsock);
            byte[] buf = new byte[1024 * 1024 * 8];
            string DelayedErrorMsg = null;

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
                        case (byte)'o': //open connection
                            {
                                int bc = 0;
                                XContent.ReceiveXBytes(netstm, out bc, buf);
                                batchsize = BytesToLong(buf, 0);
                                batchon = batchsize > 0;
                                XContent.ReceiveXBytes(netstm, out bc, buf);
                                BULKINSERTCHUNKSIZEMAX = BytesToLong(buf, 0);
                                AllProtocolHosts = XContent.ReceiveXString(netstm, buf).Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                                sessionID = Guid.NewGuid().ToString();
                                batchfname = "batch_" + sessionID + ".txt";
                                netstm.WriteByte((byte)'+');
                                XContent.SendXContent(netstm, sessionID);
                            }
                            break;

                        case (byte)'q': //query
                            {
                                string escaped_cmd;
                                bool IsDfsRef = false;
                                bool IsPortion = false;
                                int portion = 0;
                                int totportion = 0;
                                {
                                    string norm_cmd = XContent.ReceiveXString(netstm, null).Trim();
                                    {
                                        string cleaned_cmd = "";
                                        IsPortion = RDBMS_DBCORE.Qa.IsPortionCmd(norm_cmd, ref portion, ref totportion, ref cleaned_cmd);
                                        if (IsPortion)
                                        {
                                            norm_cmd = cleaned_cmd;
                                        }
                                    }
                                    IsDfsRef = RDBMS_DBCORE.Qa.CanUseDfsRef(norm_cmd);
                                    escaped_cmd = EscapeCommandText(norm_cmd);
                                }

                                // Check for PIN FOR in SELECT; remove if present.
                                int PinForSecs = 0;
                                RDBMS_DBCORE.Qa.HasPinFor(escaped_cmd, ref PinForSecs, ref escaped_cmd);

                                //add switch
                                {
                                    int space = escaped_cmd.IndexOf(' ');
                                    string DfsReq = IsDfsRef ? " dfsref" : " dfstemp";
                                    if (space > -1)
                                    {
                                        escaped_cmd = escaped_cmd.Substring(0, space) + DfsReq + escaped_cmd.Substring(space);
                                    }
                                    else
                                    {
                                        escaped_cmd = escaped_cmd + DfsReq;
                                    }
                                }

                                string output = null;
                                System.Xml.XmlDocument metadata = null;
                                try
                                {
                                    FinishBulkInsert();
                                    if (batchon)
                                    {
                                        FlushCommand(batchfname);
                                    }

                                    using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
                                    {
                                        output = (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec(escaped_cmd);
                                    }

                                    if (output.IndexOf(" Exception detected from", StringComparison.OrdinalIgnoreCase) > -1 &&
                                        output.IndexOf("error:", StringComparison.OrdinalIgnoreCase) > -1 &&
                                        output.IndexOf("at ", StringComparison.OrdinalIgnoreCase) > -1)
                                    {
                                        throw new Exception("exec RDBMS_QueryAnalyzer.DBCORE error: " + output);
                                    }

                                    metadata = new System.Xml.XmlDocument();
                                    metadata.LoadXml(output);
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                    throw;
                                }

                                //Query succeeded.
                                netstm.WriteByte((byte)'+');

                                System.Xml.XmlNode node;
                                if (IsDfsRef)
                                {
                                    node = metadata.SelectSingleNode("//queryresults/reftable");
                                }
                                else
                                {
                                    node = metadata.SelectSingleNode("//queryresults/temptable");
                                }
                                string dfstable = node.InnerText;

                                System.Xml.XmlNodeList nodes = metadata.SelectNodes("//queryresults/field");

                                //column count
                                int columncount = nodes.Count;
                                MyToBytes(columncount, buf, 0);
                                XContent.SendXContent(netstm, buf, 4);

                                //column metadata
                                MetaData[] columns = new MetaData[columncount];
                                for (int i = 0; i < columncount; i++)
                                {
                                    System.Xml.XmlNode n = nodes[i];
                                    string cname = n.Attributes["name"].Value;
                                    Type ctype = Type.GetType(n.Attributes["cstype"].Value);
                                    int csize = Int32.Parse(n.Attributes["bytes"].Value);
                                    int frontbytes = Int32.Parse(n.Attributes["frontbytes"].Value);
                                    int backbytes = Int32.Parse(n.Attributes["backbytes"].Value);

                                    XContent.SendXContent(netstm, cname);
                                    XContent.SendXContent(netstm, ctype.FullName);
                                    MyToBytes(frontbytes, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);
                                    MyToBytes(csize, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);
                                    MyToBytes(backbytes, buf, 0);
                                    XContent.SendXContent(netstm, buf, 4);

                                    MetaData md = MetaData.Prepare(cname, ctype, csize, frontbytes, backbytes);
                                    columns[i] = md;
                                }

                                node = metadata.SelectSingleNode("//queryresults/recordcount");
                                int recordcount = Int32.Parse(node.InnerText);

                                node = metadata.SelectSingleNode("//queryresults/recordsize");
                                int recordsize = Int32.Parse(node.InnerText);

                                node = metadata.SelectSingleNode("//queryresults/parts");
                                //int partscount = Int32.Parse(node.InnerText);

                                //int curpart = -1;
                                bool eof = true;
                                System.IO.FileStream fs = null;
                                byte[] rowbuf = new byte[recordsize];
                                long bytesremain = 0;
                                string tmpdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + System.Environment.CurrentDirectory.Replace(':', '$');

                                Random qrand = new Random(unchecked(
                                    System.Threading.Thread.CurrentThread.ManagedThreadId
                                    + DateTime.Now.Millisecond * 3));

                                // Share the cluster critical section:
                                string outputBulkGet1;
                                string htnppartsoutput;
                                //string bulkquerylistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\RDBMS_BulkQuery_" + Guid.NewGuid().ToString().Substring(0, 13); // Too long.
                                string bulkquerylistfilepath = QueryAnalyzer_Protocol.CurrentDirNetPath + @"\BQ_" + Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace('/', '-');
                                using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
                                {
                                    htnppartsoutput = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " slaveinstalls -healthy");
                                    outputBulkGet1 = Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " bulkget \"" + bulkquerylistfilepath + "\" \"" + dfstable + "\"");
                                }

                                string[] bulkparts; // Lines of "<host>[;<...>] <chunkname> <size>" but <size> excludes the size of the header.
                                int curbulkpart = -1;
                                int bulkpartscount;
                                {
                                    bulkparts = System.IO.File.ReadAllLines(bulkquerylistfilepath);
                                    if (bulkparts.Length == 1 && string.IsNullOrEmpty(bulkparts[0]))
                                    {
                                        bulkparts = new string[0];
                                    }
                                    bulkpartscount = bulkparts.Length;
                                    System.IO.File.Delete(bulkquerylistfilepath);
                                }
                                if (IsPortion)
                                {
                                    recordcount = 0;
                                    if (bulkpartscount < totportion)
                                    {
                                        totportion = bulkpartscount;
                                    }

                                    if (portion <= totportion)
                                    {
                                        int delta = bulkpartscount / totportion;
                                        curbulkpart = delta * (portion - 1);
                                        int lastbulkpart = 0;
                                        if (portion == totportion) //if last portion, get until last part is reached
                                        {
                                            lastbulkpart = bulkpartscount;
                                        }
                                        else
                                        {
                                            lastbulkpart = curbulkpart + delta;
                                        }

                                        for (int ip = curbulkpart; ip < lastbulkpart; ip++)
                                        {
                                            int partrecordcount = (int)(long.Parse(bulkparts[ip].Split(' ')[2]) / recordsize);
                                            recordcount += partrecordcount;
                                        }
                                        curbulkpart--;
                                    }
                                }

                                string KeepFileName = null;
                                if (IsDfsRef)
                                {
                                    if (PinForSecs > 0)
                                    {
                                        // Important: get first part name BEFORE scrambling (DFSREF).
                                        {
                                            string FirstPartName;
                                            string bpline = bulkparts.Length > 0 ? bulkparts[0] : null;
                                            if (string.IsNullOrEmpty(bpline))
                                            {
                                                // The table is empty...
                                                FirstPartName = null;
                                            }
                                            else
                                            {
                                                string[] bparts = bpline.Split(' ');
                                                FirstPartName = bparts[1];
                                            }
                                            if (null != FirstPartName)
                                            {
                                                KeepFileName = FirstPartName + ".keep";
                                            }
                                        }
                                    }
                                }

                                Dictionary<string, string> HostToNetPath;
                                {
                                    string[] htnlines = htnppartsoutput
                                        .Trim().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                                    HostToNetPath = new Dictionary<string, string>(htnlines.Length, new _CaseInsensitiveEqualityComparer_74823());
                                    for (int ip = 0; ip < htnlines.Length; ip++)
                                    {
                                        string htnline = htnlines[ip];
                                        int isphtn = htnline.IndexOf(' ');
                                        string host = htnline.Substring(0, isphtn);
                                        string netpath = htnline.Substring(isphtn + 1);
                                        HostToNetPath[host] = netpath;
                                    }
                                }

                                System.Threading.Thread keeperthread = null;
                                if (null != KeepFileName)
                                {
#if DEBUG
                                    if (PinForSecs < 1)
                                    {
                                        throw new Exception("DEBUG:  (null != KeepFileName) && (PinForSecs < 1)");
                                    }
#endif
                                    int kfnRandNum = qrand.Next();
                                    keeperthread = new System.Threading.Thread(
                                        new System.Threading.ThreadStart(
                                        delegate()
                                        {
                                            string keepfilepath;
                                            {
                                                string keepfilenetdir;
                                                {
                                                    Dictionary<string, string>.ValueCollection kfhAllNetDirs = HostToNetPath.Values;
                                                    keepfilenetdir = kfhAllNetDirs.ElementAt(kfnRandNum % kfhAllNetDirs.Count);
                                                }
                                                keepfilepath = keepfilenetdir + @"\" + KeepFileName;
                                            }
                                            try
                                            {
                                                // Keeper loop!
                                                byte[] nobytes = new byte[0];
                                                for (; ; )
                                                {
                                                    try
                                                    {
                                                        System.IO.File.WriteAllBytes(keepfilepath, nobytes);
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    System.Threading.Thread.Sleep(1000 * PinForSecs);
                                                }
                                            }
                                            catch (System.Threading.ThreadInterruptedException)
                                            {
                                                // Expected interrupt.
                                            }
                                            // Note: can't delete keepfilepath because another client might need it.
                                        }));
                                    keeperthread.IsBackground = true;
                                    keeperthread.Start();
                                }
                                try
                                {
                                    byte[] onerecord = new byte[recordsize];
                                    byte[] recordbuf = new byte[recordsize + (0x400 * 0x400 * 5)];
                                    int recordbufpos = 0;
                                    for (int i = 0; i < recordcount; i++)
                                    {
                                        if (eof)
                                        {
                                            if (fs != null)
                                            {
                                                fs.Close();
                                                fs = null;
                                            }
                                            if (++curbulkpart > bulkpartscount)
                                            {
                                                throw new Exception("Part count is out of range and records are not all read.");
                                            }
                                            string[] bparts = bulkparts[curbulkpart].Split(' ');
                                            string[] bhosts = bparts[0].Split(';');
                                            string bchunkname = bparts[1];
                                            bytesremain = long.Parse(bparts[2]);
                                            string netdir = null;
                                            for (int ibh = 0; ibh < bhosts.Length; ibh++)
                                            {
                                                if (HostToNetPath.ContainsKey(bhosts[ibh]))
                                                {
                                                    netdir = HostToNetPath[bhosts[ibh]];
                                                    break;
                                                }
                                            }
                                            if (null == netdir)
                                            {
                                                throw new Exception("Unable to find healthy machine with part of result: " + bchunkname + " only resides on hosts: " + string.Join(", ", bhosts));
                                            }
                                            fs = new System.IO.FileStream(netdir + @"\" + bchunkname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                                            {
                                                // Skip the dfs-chunk file-header...
                                                int headerlength = 0;
                                                {
                                                    if (rowbuf.Length < 4)
                                                    {
                                                        rowbuf = new byte[4];
                                                    }
                                                    if (4 != fs.Read(rowbuf, 0, 4))
                                                    {
                                                        return;
                                                    }
                                                    {
                                                        headerlength = BytesToInt(rowbuf, 0);
                                                        if (headerlength > 4)
                                                        {
                                                            int hremain = headerlength - 4;
                                                            if (hremain > rowbuf.Length)
                                                            {
                                                                rowbuf = new byte[hremain];
                                                            }
                                                            StreamReadExact(fs, rowbuf, hremain);
                                                        }
                                                    }
                                                }
                                            }
                                            eof = false;
                                        }

                                        //begin a row
                                        if (recordbufpos + recordsize > recordbuf.Length)
                                        {
                                            netstm.WriteByte((byte)'+');
                                            XContent.SendXContent(netstm, recordbuf, recordbufpos);
                                            recordbufpos = 0;
                                        }
                                        {
                                            StreamReadExact(fs, onerecord, recordsize);
                                            Buffer.BlockCopy(onerecord, 0, recordbuf, recordbufpos, recordsize);
                                            recordbufpos += recordsize;
                                            bytesremain -= recordsize;
                                        }

                                        if (bytesremain <= 0)
                                        {
                                            eof = true;
                                        }
                                    }
                                    if (recordbufpos > 0)
                                    {
                                        netstm.WriteByte((byte)'+');
                                        XContent.SendXContent(netstm, recordbuf, recordbufpos);
                                        recordbufpos = 0;
                                    }
                                    if (fs != null)
                                    {
                                        fs.Close();
                                        fs = null;
                                    }
                                }
                                finally
                                {
                                    if (null != keeperthread)
                                    {
                                        keeperthread.Interrupt();
                                        if (!keeperthread.Join(1000 * 60))
                                        {
                                            try
                                            {
                                                keeperthread.Abort();
                                            }
                                            catch
                                            {
                                            }
                                            throw new Exception("Keeper thread (DFSREF) did not finish in a timely fashion {q}");
                                        }
                                        keeperthread = null;
                                    }
                                }

                                netstm.WriteByte((byte)'.');
                                //end all rows.

                                if (!IsDfsRef)
                                {
                                    if (dfstable.StartsWith("dfs://RDBMS_Table_", true, null))
                                    {
                                        throw new Exception("Attempting to delete table file owned by caller: " + dfstable);
                                    }
                                    using (GlobalCriticalSection.GetLock_internal("DsQaAdo"))
                                    {
                                        Exec.Shell(QueryAnalyzer_Protocol.dspaceexe + " del " + dfstable, false);
                                    }
                                }
                            }
                            break;

                        case (byte)'N': // Batched non-query.
                            {
                                int lenN;
                                int indexN = 0;
                                buf = XContent.ReceiveXBytes(netstm, out lenN, buf);

                                if (null != DelayedErrorMsg)
                                {
                                    string derrmsg = DelayedErrorMsg;
                                    DelayedErrorMsg = null;
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, derrmsg);
                                    break;
                                }
                                // Report a success now.
                                netstm.WriteByte((byte)'+');

                                StringBuilder sbnorm = new StringBuilder();
                                for (bool endN = false; !endN; )
                                {
                                    try
                                    {
                                        string norm_cmd;
                                        {
                                            sbnorm.Length = 0;
                                            for (; ; )
                                            {
                                                if (indexN == lenN || indexN + 1 == lenN)
                                                {
                                                    endN = true;
                                                    break;
                                                }
                                                else if (0 == buf[indexN] && 0 == buf[indexN + 1])
                                                {
                                                    indexN += 2;
                                                    break;
                                                }
                                                else
                                                {
                                                    char ch = (char)(((UInt16)buf[indexN++]) << 8);
                                                    ch |= (char)buf[indexN++];
                                                    sbnorm.Append(ch);
                                                    continue;
                                                }
                                            }
                                            if (0 == sbnorm.Length)
                                            {
                                                throw new Exception("Batched non-query error: empty query detected");
                                            }
                                            norm_cmd = sbnorm.ToString();
                                        }
                                        if (QlCreateRIndex(norm_cmd))
                                        {
                                            // RINDEX created.
                                        }
                                        else if (QlAlterRIndexRenameSwap(norm_cmd))
                                        {

                                        }
                                        else if (QlDropRIndex(norm_cmd))
                                        {

                                        }
                                        else if (QlAlterTableRenameSwap(norm_cmd))
                                        {
                                            // Name swapped.
                                        }
                                        else if (BulkInsert(norm_cmd))
                                        {
                                            // Bulk inserted.
                                        }
                                        else if (batchon)
                                        {
                                            BatchFlushCommand(batchfname, norm_cmd, batchsize);
                                        }
                                        else
                                        {
                                            string escaped_cmd = EscapeCommandText(norm_cmd);

                                            string output = (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec(escaped_cmd);

                                            if (output.IndexOf("Error:{", StringComparison.OrdinalIgnoreCase) > -1 &&
                                                output.IndexOf("Details:", StringComparison.OrdinalIgnoreCase) > -1 &&
                                                output.IndexOf("at ", StringComparison.OrdinalIgnoreCase) > -1)
                                            {
                                                throw new Exception("exec RDBMS_QueryAnalyzer.DBCORE error: " + output);
                                            }
                                        }
                                    }
                                    catch (Exception bnqe)
                                    {
                                        DelayedErrorMsg = "Batched non-query error: " + bnqe.ToString();
                                        XLog.errorlog("ClientThreadProc exception: " + DelayedErrorMsg);
                                        break;
                                    }
                                }

                            }
                            break;

                        case (byte)'n': //non-query
                            {
                                string norm_cmd = XContent.ReceiveXString(netstm, null).Trim();

                                try
                                {
                                    if (QlCreateRIndex(norm_cmd))
                                    {
                                        // RINDEX created.
                                    }
                                    else if (QlAlterRIndexRenameSwap(norm_cmd))
                                    {

                                    }
                                    else if (QlDropRIndex(norm_cmd))
                                    {

                                    }
                                    else if (QlAlterTableRenameSwap(norm_cmd))
                                    {
                                        // Name swapped.
                                    }
                                    else if (BulkInsert(norm_cmd))
                                    {
                                        // Bulk inserted.
                                    }
                                    else if (batchon)
                                    {
                                        BatchFlushCommand(batchfname, norm_cmd, batchsize);
                                    }
                                    else
                                    {
                                        string escaped_cmd = EscapeCommandText(norm_cmd);

                                        string output = (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec(escaped_cmd);

                                        if (output.IndexOf("Error:{", StringComparison.OrdinalIgnoreCase) > -1 &&
                                            output.IndexOf("Details:", StringComparison.OrdinalIgnoreCase) > -1 &&
                                            output.IndexOf("at ", StringComparison.OrdinalIgnoreCase) > -1)
                                        {
                                            throw new Exception("exec RDBMS_QueryAnalyzer.DBCORE error: " + output);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                    throw;
                                }

                                //Query succeeded.
                                netstm.WriteByte((byte)'+');

                                //# of rows affected.
                                MyToBytes(0, buf, 0);
                                XContent.SendXContent(netstm, buf, 4);
                            }
                            break;

                        case (byte)'c': //close
                            {
                                try
                                {
                                    FinishBulkInsert();
                                    if (batchon)
                                    {
                                        FlushCommand(batchfname);
                                    }
                                }
                                catch (Exception e)
                                {
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, e.ToString());
                                    throw;
                                }
                                stop = true;
                                if (null != DelayedErrorMsg)
                                {
                                    string derrmsg = DelayedErrorMsg;
                                    DelayedErrorMsg = null;
                                    netstm.WriteByte((byte)'-');
                                    XContent.SendXContent(netstm, derrmsg);
                                    break;
                                }
                                netstm.WriteByte((byte)'+');
                            }
                            break;

                        default:
                            throw new Exception("unknown action");
                    }
                }
            }
            catch (CommandFlushAbortException e)
            {
                XLog.errorlog("ClientThreadProc exception: Error while flushing: " + e.ToString());
            }
            catch (Exception e)
            {
                XLog.errorlog("ClientThreadProc exception: " + e.ToString());

                try
                {
                    FinishBulkInsert();
                    if (batchon)
                    {
                        FlushCommand(batchfname);
                    }
                }
                catch (Exception ex)
                {
                    XLog.errorlog("ClientThreadProc exception: " + ex.ToString());
                }
            }
            finally
            {
                netstm.Close();
                netstm = null;
                clientsock.Close();
                clientsock = null;
            }
        }

        class _CaseInsensitiveEqualityComparer_74823 : IEqualityComparer<string>
        {
            bool IEqualityComparer<string>.Equals(string x, string y)
            {
                return 0 == string.Compare(x, y, StringComparison.OrdinalIgnoreCase);
            }

            int IEqualityComparer<string>.GetHashCode(string obj)
            {
                unchecked
                {
                    int result = 8385;
                    int cnt = obj.Length;
                    for (int i = 0; i < cnt; i++)
                    {
                        result <<= 4;
                        char ch = obj[i];
                        if (ch >= 'A' && ch <= 'Z')
                        {
                            ch = (char)('a' + (ch - 'A'));
                        }
                        result += ch;
                    }
                    return result;
                }
            }
        }

        private string EscapeCommandText(string txt)
        {
            return txt.Replace("&", "&amp;").Replace("\"", "&quot;").Replace("\r", "&#13;").Replace("\n", "&#10;").Replace("<", "&lt;").Replace(">", "&gt;");
        }

        struct MetaData
        {
            public string Name;
            public Type Type;
            public int Size;
            public int FrontBytes;
            public int BackBytes;

            public static MetaData Prepare(string name, Type type, int size, int frontbytes, int backbytes)
            {
                MetaData md;
                md.Name = name;
                md.Type = type;
                md.Size = size;
                md.FrontBytes = frontbytes;
                md.BackBytes = backbytes;
                return md;
            }
        }

        public static void MyToBytes(Int32 x, byte[] resultbuf, int bufoffset)
        {
            Int32ToBytes(x, resultbuf, bufoffset);
        }

        public static void MyInt64ToBytes(long x, byte[] resultbuf, int bufoffset)
        {
            Int64ToBytes(x, resultbuf, bufoffset);
        }

        public static void Int64ToBytes(long x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 56);
            resultbuf[bufoffset + 1] = (byte)(x >> 48);
            resultbuf[bufoffset + 2] = (byte)(x >> 40);
            resultbuf[bufoffset + 3] = (byte)(x >> 32);
            resultbuf[bufoffset + 4] = (byte)(x >> 24);
            resultbuf[bufoffset + 5] = (byte)(x >> 16);
            resultbuf[bufoffset + 6] = (byte)(x >> 8);
            resultbuf[bufoffset + 7] = (byte)x;
        }

        public static void Int32ToBytes(Int32 x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 24);
            resultbuf[bufoffset + 1] = (byte)(x >> 16);
            resultbuf[bufoffset + 2] = (byte)(x >> 8);
            resultbuf[bufoffset + 3] = (byte)x;
        }

        public static void DoubleToBytes(double x, byte[] buffer, int offset)
        {
            if (double.IsNaN(x))
            {
                buffer[offset] = 0x61;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (double.IsNegativeInfinity(x))
            {
                buffer[offset] = 0x62;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (isNegativeZero(x))
            {
                buffer[offset] = 0x64;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (x == 0)
            {
                buffer[offset] = 0x65;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (double.IsPositiveInfinity(x))
            {
                buffer[offset] = 0x67;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            long l = BitConverter.DoubleToInt64Bits(x);

            if (x > 0)
            {
                buffer[offset] = 0x66;
                Int64ToBytes(l, buffer, offset + 1);
                return;
            }

            buffer[offset] = 0x63;
            Int64ToBytes(~l, buffer, offset + 1);
        }

        internal static bool isNegativeZero(double x)
        {
            return x == 0 && 1 / x < 0;
        }

        public static long BytesToLong(IList<byte> x, int offset)
        {
            long result = 0;
            result |= (long)x[offset + 0] << 56;
            result |= (long)x[offset + 1] << 48;
            result |= (long)x[offset + 2] << 40;
            result |= (long)x[offset + 3] << 32;
            result |= (long)x[offset + 4] << 24;
            result |= (long)x[offset + 5] << 16;
            result |= (long)x[offset + 6] << 8;
            result |= x[offset + 7];
            return result;
        }

        public static Int32 BytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        internal static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
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

        internal static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            if (len != StreamReadLoop(stm, buf, len))
            {
                throw new System.IO.IOException("Unable to read from stream");
            }
        }

        public class CommandFlushAbortException : Exception
        {
            public CommandFlushAbortException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }

            public CommandFlushAbortException(string msg)
                : base(msg)
            {
            }

            public CommandFlushAbortException(Exception innerException)
                : base("Command Flush Abort", innerException)
            {
            }
        }
    }
}
