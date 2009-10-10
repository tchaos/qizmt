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

//#define VARIABLE_NETWORK_PATHS


using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;


namespace MySpace.DataMining.DistributedObjects5
{
    public abstract class DistObjectBase: IDisposable
    {
        string objectname;


        public DistObjectBase(string objectname)
        {
            this.objectname = objectname;
        }


        public abstract int SlaveCount
        {
            get;
        }


        ~DistObjectBase()
        {
            Close();
        }


        public abstract void Close();


        public string ObjectName
        {
            get
            {
                return objectname;
            }
        }


        public static byte[] ToBytes(string x)
        {
            return Encoding.UTF8.GetBytes(x);
        }


        public static void ToBytes(int x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)(x >> 8);
            resultbuf[bufoffset + 2] = (byte)(x >> 16);
            resultbuf[bufoffset + 3] = (byte)(x >> 24);
        }

        public static byte[] ToBytes(int x)
        {
            byte[] result = new byte[4];
            ToBytes(x, result, 0);
            return result;
        }


        public static void LongToBytes(long x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)(x >> 8);
            resultbuf[bufoffset + 2] = (byte)(x >> 16);
            resultbuf[bufoffset + 3] = (byte)(x >> 24);
            resultbuf[bufoffset + 3] = (byte)(x >> 32);
            resultbuf[bufoffset + 5] = (byte)(x >> 40);
            resultbuf[bufoffset + 6] = (byte)(x >> 48);
            resultbuf[bufoffset + 7] = (byte)(x >> 56);
        }
        
        public static byte[] LongToBytes(long x)
        {
            byte[] result = new byte[8];
            LongToBytes(x, result, 0);
            return result;
        }


        public static byte[] ToBytes(int[] x)
        {
            byte[] result = new byte[x.Length * 4];
            int offset = 0;
            foreach (int n in x)
            {
                ToBytes(n, result, offset);
                offset += 4;
            }
            return result;
        }

        public static byte[] ToBytes(long[] x)
        {
            byte[] result = new byte[x.Length * 8];
            int offset = 0;
            foreach (long n in x)
            {
                LongToBytes(n, result, offset);
                offset += 8;
            }
            return result;
        }


        public static string BytesToString(byte[] x)
        {
            return Encoding.UTF8.GetString(x);
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

        public static long BytesToLong(byte[] x, int offset)
        {
            long result = 0;
            result |= x[offset + 0];
            result |= (long)x[offset + 1] << 8;
            result |= (long)x[offset + 2] << 16;
            result |= (long)x[offset + 3] << 24;
            result |= (long)x[offset + 4] << 32;
            result |= (long)x[offset + 5] << 40;
            result |= (long)x[offset + 6] << 48;
            result |= (long)x[offset + 7] << 56;
            return result;
        }

        public static long BytesToLong(byte[] x)
        {
            return BytesToLong(x, 0);
        }


        public static int[] BytesToIntArray(byte[] x, int maxbytes)
        {
            int[] result = new int[maxbytes / 4];
            int xoffset = 0;
            for (int roffset = 0; roffset != result.Length; roffset++)
            {
                result[roffset] = BytesToInt(x, xoffset);
                xoffset += 4;
            }
            return result;
        }

        public static int[] BytesToIntArray(byte[] x)
        {
            return BytesToIntArray(x, x.Length);
        }


        public static long[] BytesToLongArray(byte[] x, int maxbytes)
        {
            long[] result = new long[maxbytes / 8];
            int xoffset = 0;
            for (int roffset = 0; roffset != result.Length; roffset++)
            {
                result[roffset] = BytesToLong(x, xoffset);
                xoffset += 8;
            }
            return result;
        }

        public static long[] BytesToLongArray(byte[] x)
        {
            return BytesToLongArray(x, x.Length);
        }


        protected internal virtual int CalcHashCode(IList<byte> x, int xoffset, int xlen)
        {
            unchecked
            {
                int hc = 41622;
                for (int i = 0; i != xlen; i++)
                {
                    hc = ((hc << 5) + hc) + (int)x[xoffset + i];
                }
                return hc & 0x7FFFFFFF;
            }
        }

        protected internal int CalcHashCode(byte[] x)
        {
            return CalcHashCode(x, 0, x.Length);
        }


        // Determines which partition...

        protected internal virtual int DetermineSlave(int hashcode)
        {
            return hashcode % SlaveCount;
        }

        protected internal int DetermineSlave(IList<byte> key, int keyoffset, int keylen)
        {
            return DetermineSlave(CalcHashCode(key, keyoffset, keylen));
        }

        protected internal int DetermineSlave(byte[] key)
        {
            return DetermineSlave(key, 0, key.Length);
        }


        #region IDisposable Members

        public virtual void Dispose()
        {
            Close();
            System.GC.SuppressFinalize(this);
        }

        #endregion


        const string MYTEMPDIR = @".\temp";


        public static void CleanCompilerFiles()
        {
            try
            {
                System.IO.DirectoryInfo tdir = new System.IO.DirectoryInfo(MYTEMPDIR);
                foreach (System.IO.FileInfo outfi in tdir.GetFiles("*.out"))
                {
                    string pattern = outfi.FullName.Replace(".out", ".*");
                    foreach (System.IO.FileInfo fi in tdir.GetFiles(pattern))
                    {
                        fi.Delete();
                    }
                }
            }
            catch
            {
            }
        }

        static DistObjectBase()
        {
            if (!System.IO.Directory.Exists(MYTEMPDIR))
            {
                System.IO.Directory.CreateDirectory(MYTEMPDIR);
            }
            Environment.SetEnvironmentVariable("TEMP", MYTEMPDIR);
            Environment.SetEnvironmentVariable("TMP", MYTEMPDIR);
        }

    }


    public class SlaveInfo
    {
        public NetworkStream nstm;
        public int slaveID;
        internal string[] blockinfo;
        internal string sblockinfo; // @"H|objectname|200MB|localhost|C:\hashtable_logs0|slaveid=0"
        public int pid;


        public string Host
        {
            get
            {
                return blockinfo[3];
            }
        }


        public virtual void SlaveErrorCheck()
        {
        }
    }


    public class SlaveException : Exception
    {
        public SlaveException(string msg)
            : base("Exception from slave: " + msg)
        {
        }
    }


    public abstract class DistObject : DistObjectBase
    {
        public static int FILE_BUFFER_SIZE = MySpace.DataMining.DistributedObjects.MemoryUtils.DefaultFileBufferSize;

        public const int BUF_SIZE = 0x400 * 0x400 * 1;


        protected System.Collections.Generic.List<SlaveInfo> dslaves; // Indexed by ID.
        protected bool didopen = false;

        internal long jid = 0;
        internal string sjid = "0";


        public DistObject(string objectname) :
            base(objectname)
        {
            dslaves = new System.Collections.Generic.List<SlaveInfo>(8);
        }


        public void SetJID(long jid)
        {
            if (0 != this.jid)
            {
                throw new Exception("DEBUG:  SetJID: cannot set JID twice");
            }
            this.jid = jid;
            this.sjid = jid.ToString();
        }


        public override int SlaveCount
        {
            get
            {
                return dslaves.Count;
            }
        }


        public override void Close()
        {
            if (null != dslaves)
            {
                foreach (SlaveInfo slave in dslaves)
                {
                    try
                    {
                        lock (slave)
                        {
                            slave.nstm.WriteByte((byte)'\\');

                            slave.nstm.Close(1000);
                            slave.nstm.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                    }
                }
                dslaves = null;
            }
        }


        public abstract char GetDistObjectTypeChar();


        public virtual SlaveInfo createSlaveInfo()
        {
            return new SlaveInfo();
        }


        public virtual void AddBlock(string capacity, string sUserBlockInfo)
        {
            if (didopen)
            {
                throw new Exception("Attempted to AddBlock after Open");
            }

            // @"H|objectname|200MB|localhost|C:\hashtable_logs0|slaveid=0"
            string sblockinfo = GetDistObjectTypeChar().ToString() + "|" + ObjectName + "|" + capacity + "|" + sUserBlockInfo;
            string[] blockinfo = sblockinfo.Split('|');

            SlaveInfo slave = createSlaveInfo();
            string xslaveid = blockinfo[5];
            if ("slaveid=" != xslaveid.Substring(0, 8))
                throw new Exception("AddBlock: sub process ID failure: bad slaveID string: " + xslaveid);
            string sslaveID = xslaveid.Substring(8);
            slave.slaveID = int.Parse(sslaveID);
            if (slave.slaveID < 0)
                throw new Exception("AddBlock: invalid sub process ID: negative value: " + sslaveID);
            if (slave.slaveID != dslaves.Count)
            {
                throw new Exception("AddBlock: sub process ID not next: found " + sslaveID + " expected " + dslaves.Count.ToString());
            }
            slave.blockinfo = blockinfo;
            slave.sblockinfo = sblockinfo;

            dslaves.Add(slave);
            if (slave.slaveID != dslaves.Count - 1)
            {
                throw new Exception("AddBlock: sub process ID sanity check failed");
            }
        }


        public void GetSlavePidInfoAppend(StringBuilder sb)
        {
            if (!didopen)
            {
                throw new Exception("Cannot get Slave PID information before Open");
            }

            for (int i = 0; i < dslaves.Count; i++)
            {
                if (0 != i)
                {
                    sb.Append(',');
                }
                sb.Append(dslaves[i].Host);
                sb.Append('=');
                sb.Append(dslaves[i].pid);
            }
        }

        public string GetSlavePidInfo()
        {
            StringBuilder sb = new StringBuilder(30);
            GetSlavePidInfoAppend(sb);
            return sb.ToString();
        }


        public virtual void Open()
        {
            if (didopen)
            {
                throw new Exception("Attempted to Open after already Open");
            }
            didopen = true;

            try
            {
                Socket lsock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                IPEndPoint lipep;
                //int lptests = 0;
                for (; ; )
                {
                    // Try a few ports...
                    lipep = new IPEndPoint(IPAddress.Any, GetSlavePort());
                    try
                    {
                        lsock.Bind(lipep);
                        lsock.Listen(2);
                    }
                    catch (SocketException e)
                    {
                        //if (++lptests < 4)
                        {
                            continue;
                        }
                        //throw new Exception(e.ToString() + "  [Note: ensure port " + lipep.Port.ToString() + " is free / not blocked (ports >= " + SlavePortMin.ToString() + ")]");
                    }
                    break;
                }

                foreach (SlaveInfo slave in dslaves)
                {
                    Socket servSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    servSock.Connect(slave.blockinfo[3], 55900);

                    NetworkStream servStm = new XNetworkStream(servSock);

                    servStm.WriteByte((byte)'B'); // AddBlock.
                    XContent.SendXContent(servStm, slave.sblockinfo);
                    XContent.SendXContent(servStm, lipep.Port.ToString());
                    XContent.SendXContent(servStm, sjid);

                    Socket slaveSock = lsock.Accept();
                    slave.nstm = new XNetworkStream(slaveSock);
                    if (1 != slave.nstm.ReadByte())
                    {
                        throw new Exception("Sub process connection error: invalid handshake  [Non-sub-process connection?]");
                    }
                    {
                        int len;
                        byte[] bpid = XContent.ReceiveXBytes(slave.nstm, out len, null);
                        if (len < 4)
                        {
                            throw new Exception("Sub process connection error: invalid SlavePID handshake");
                        }
                        slave.pid = BytesToInt(bpid);
                    }

                    servStm.WriteByte((byte)'\\');
                    servStm.Close(1000);
                }

                lsock.Close();
            }
            catch (FormatException e)
            {
                throw new FormatException("Format error in Open: " + e.ToString());
            }
            catch (Exception e)
            {
                throw new Exception("Error in Open: " + e.ToString() + "  [Note: ensure the Windows service is running]");
            }
        }


        public const int SlavePortMin = 1025;
        public const int SlavePortMax = 65500;
        private static int prevslaveport = int.MaxValue;
        private static Random slaveportrand = new Random();
        private static Dictionary<int, bool> BannedSlavePorts;

        static DistObject()
        {
            BannedSlavePorts = new Dictionary<int, bool>();
            BannedSlavePorts[123] = true;
            BannedSlavePorts[135] = true;
            BannedSlavePorts[137] = true;
            BannedSlavePorts[138] = true;
            BannedSlavePorts[139] = true;
            BannedSlavePorts[161] = true;
            BannedSlavePorts[445] = true;
            BannedSlavePorts[500] = true;
            BannedSlavePorts[1025] = true;
            BannedSlavePorts[1026] = true;
            BannedSlavePorts[1027] = true;
            BannedSlavePorts[1028] = true;
            BannedSlavePorts[1139] = true;
            BannedSlavePorts[1248] = true;
            BannedSlavePorts[1900] = true;
            BannedSlavePorts[2301] = true;
            BannedSlavePorts[2381] = true;
            BannedSlavePorts[3389] = true;
            BannedSlavePorts[4500] = true;
            BannedSlavePorts[5101] = true;
            BannedSlavePorts[5355] = true;
            BannedSlavePorts[5464] = true;
            BannedSlavePorts[5492] = true;
            BannedSlavePorts[6004] = true;
            BannedSlavePorts[13922] = true;
            BannedSlavePorts[16992] = true;
            BannedSlavePorts[16993] = true;
            BannedSlavePorts[22743] = true;
            BannedSlavePorts[22757] = true;
            BannedSlavePorts[22764] = true;
            BannedSlavePorts[31038] = true;
            BannedSlavePorts[32072] = true;
            BannedSlavePorts[32073] = true;
            BannedSlavePorts[33358] = true;
            BannedSlavePorts[33359] = true;
            BannedSlavePorts[33363] = true;
            BannedSlavePorts[33391] = true;
            BannedSlavePorts[33416] = true;
            BannedSlavePorts[33517] = true;
            BannedSlavePorts[34779] = true;
            BannedSlavePorts[34908] = true;
            BannedSlavePorts[34929] = true;
            BannedSlavePorts[35337] = true;
            BannedSlavePorts[36208] = true;
            BannedSlavePorts[50551] = true;
            BannedSlavePorts[50785] = true;
            BannedSlavePorts[52059] = true;
            BannedSlavePorts[52060] = true;
            BannedSlavePorts[52061] = true;
            BannedSlavePorts[52062] = true;
            BannedSlavePorts[52063] = true;
            BannedSlavePorts[52076] = true;
            BannedSlavePorts[52537] = true;
            BannedSlavePorts[55900] = true;
            BannedSlavePorts[55901] = true;
            BannedSlavePorts[55902] = true;
            BannedSlavePorts[55903] = true;
            BannedSlavePorts[55904] = true;
            BannedSlavePorts[58326] = true;
            BannedSlavePorts[58343] = true;
            BannedSlavePorts[58789] = true;
            BannedSlavePorts[59515] = true;
            BannedSlavePorts[59516] = true;
            BannedSlavePorts[59759] = true;
            BannedSlavePorts[61516] = true;
            BannedSlavePorts[61517] = true;
            BannedSlavePorts[61521] = true;
            BannedSlavePorts[61885] = true;
            BannedSlavePorts[62136] = true;
            BannedSlavePorts[62799] = true;
            BannedSlavePorts[63428] = true;
            BannedSlavePorts[63453] = true;
            BannedSlavePorts[64468] = true;
        }

        static int GetSlavePort()
        {
            lock (typeof(DistObject))
            {
                if (prevslaveport >= SlavePortMax)
                {
                    prevslaveport = slaveportrand.Next(SlavePortMin, SlavePortMax);
                }
                prevslaveport++;
            }
            if (BannedSlavePorts.ContainsKey(prevslaveport))
            {
                return GetSlavePort();
            }
            return prevslaveport;
        }

#if VARIABLE_NETWORK_PATHS
#else
        static string _netpath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
#endif


        static string LocalPathToNetworkPath(string localpath, string host)
        {
            if (localpath.Length < 3
                   || ':' != localpath[1]
                   || '\\' != localpath[2]
                   || !char.IsLetter(localpath[0])
                   )
            {
                throw new Exception("LocalPathToNetworkPath invalid local path: " + localpath);
            }
            return @"\\" + host + @"\" + localpath.Substring(0, 1) + @"$" + localpath.Substring(2);
        }


        public static string GetNetworkPath(System.Net.Sockets.NetworkStream nstm, string host)
        {
#if VARIABLE_NETWORK_PATHS
            nstm.WriteByte((byte)'d'); // Get current directory.

            if ((int)'+' != nstm.ReadByte())
            {
                throw new Exception("GetNetworkPath failure (service didn't report success)");
            }

            return LocalPathToNetworkPath(XContent.ReceiveXString(nstm, null), host);
#else
            return LocalPathToNetworkPath(_netpath, host);
#endif
        }


        public static string GetNetworkPath(string host)
        {
#if VARIABLE_NETWORK_PATHS
            System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            servSock.Connect(host, 55900);
            System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

            string netpath = GetNetworkPath(servStm, host);

            servSock.Close();
            servStm.Close();

            return netpath;
#else
            return LocalPathToNetworkPath(_netpath, host);
#endif
        }


    }


    public abstract class DistObject2 : DistObject
    {
        public DistObject2(string objectname) : base(objectname) { }


        // Use methods from IMapReduce's Entry instead
        public static void ToBytes() { }
        public static void LongToBytes() { }
        public static void BytesToString() { }
        public static void BytesToInt() { }
        public static void BytesToLong() { }
        public static void BytesToIntArray() { }
        public static void BytesToLongArray() { }


        public static void CloseUnreferencedObjects()
        {
            System.GC.Collect();
            System.GC.WaitForPendingFinalizers();
        }


        internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "do5log");

        public static void LogLine(string line)
        {
            try
            {
                logmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                System.IO.StreamWriter fstm = System.IO.File.AppendText("do5.txt");
                string build = "";
                try
                {
                    System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                    System.Reflection.AssemblyName an = asm.GetName();
                    int bn = an.Version.Build;
                    int rv = an.Version.Revision;
                    build = "(do5." + bn.ToString() + "." + rv.ToString() + ") ";
                }
                catch
                {
                }
                fstm.WriteLine("[{0} {1}ms] {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                fstm.Close();
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }

    }


    public abstract class DistObject5 : DistObject2
    {
        public DistObject5(string objectname) : base(objectname) { }


        public void SendXmlConfig(string xml)
        {
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'<');
                XContent.SendXContent(slave.nstm, xml);
            }

            PlusValidate("SendXmlConfig");
        }


        public void Ping(string action)
        {
            foreach (SlaveInfo slave in dslaves)
            {
                slave.nstm.WriteByte((byte)'.'); // Ping.
                if ((int)',' != slave.nstm.ReadByte())
                {
                    throw new Exception(((null != action) ? (action + ": ") : "") + "Sync error (pong)");
                }
            }
        }

        public void Ping()
        {
            Ping(null);
        }


        protected void PlusValidate(string name)
        {
            foreach (SlaveInfo slave in dslaves)
            {
                if ((int)'+' != slave.nstm.ReadByte())
                {
                    throw new Exception("Sub process did not report a success for " + name);
                }
            }
        }

    }


}
