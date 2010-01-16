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

#define SLAVE_TRACE

#if DEBUG
//#define SLAVE_TRACE_PORT
#endif

using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Runtime.InteropServices;


namespace MySpace.DataMining.DistributedObjects5
{
    public interface IDistObject
    {
        void ProcessCommands(NetworkStream nstm);
    }


    // An exception originating from a user plugin.
    public class UserException : Exception
    {
        Exception ue;

        public UserException(Exception e)
            : base("UserException: " + e.Message)
        {
            this.ue = e;
        }


        public override string ToString()
        {
            return "UserException: " + ue.ToString();
        }

    }


    public abstract class DistObjectBase : IDistObject
    {
        public static int FILE_BUFFER_SIZE = MySpace.DataMining.DistributedObjects.MemoryUtils.DefaultFileBufferSize;

        public const int BUF_SIZE = 0x400 * 0x400 * 1;


        string lasterror = null;
        int maxerrors = 1;

        public void SetError(string msg)
        {
            lasterror = msg;

            if (maxerrors > 0)
            {
                maxerrors--;

                XLog.errorlog(msg);

                try
                {
                    XLog.errorlog(msg, XLog.UserLogFile);
                }
                catch
                {
                }
            }

            System.Threading.Thread.Sleep(200);
        }


        public static byte[] GetSliceCopy(byte[] buf, int len)
        {
            byte[] result = new byte[len];
            for (int i = 0; i != len; i++)
            {
                result[i] = buf[i];
            }
            return result;
        }

        public static byte[] AppendSliceCopy(byte[] buf, byte[] appendbuf, int appendlen)
        {
            byte[] result = new byte[buf.Length + appendlen];
            int i = 0;
            for (; i != buf.Length; i++)
            {
                result[i] = buf[i];
            }
            for (; i != result.Length; i++)
            {
                result[i] = appendbuf[i - buf.Length];
            }
            return result;
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


        public NetworkStream nstm;
        public byte[] buf;


        public DistObjectBase()
        {
            this.buf = new byte[BUF_SIZE];
        }


        private void noimpl()
        {
            throw new Exception("No implementation for this object");
        }


        // Note: returned array is invalidated on next operation.
        // Warning: return's Length is not valid data length, but preallocation buffer.
        public abstract byte[] GetValue(byte[] key, out int valuelength);

        // Warning: key needs to be immutable!
        public abstract void CopyAndSetValue(byte[] key, byte[] value, int valuelength);


        public virtual void AppendValue(byte[] key, byte[] avalue, int avaluelength)
        {
            noimpl();
        }


        protected virtual void ReloadConfig()
        {
            if (null != DistributedObjectsSlave.xslave)
            {
                System.Xml.XmlNode xFileBufferSizeOverride = DistributedObjectsSlave.xslave["FileBufferSizeOverride"];
                if (null != xFileBufferSizeOverride)
                {
                    try
                    {
                        int fbs = int.Parse(xFileBufferSizeOverride.InnerText);
                        if (fbs > 0)
                        {
                            FILE_BUFFER_SIZE = fbs;
                        }
                    }
                    catch
                    {
                    }
                }
            }
        }


        protected virtual void ProcessCommand(NetworkStream nstm, char tag)
        {
            throw new Exception("Invalid tag for command: unrecognized tag: " + tag.ToString());
        }


        protected char CurrentCommand = '.';

        public virtual void ProcessCommands(NetworkStream nstm)
        {
            //nstm = new XNetworkStream(sock);
            int x;
            this.nstm = nstm;
            try
            {
                for (; ; )
                {
                    x = nstm.ReadByte();
                    if (x < 0)
                    {
                        //throw new Exception("Unable to read tag for command");
                        break;
                    }
                    buf[0] = (byte)x;
                    CurrentCommand = (char)x;

                    if ('\\' == (char)buf[0])
                    {
                        if (XLog.logging)
                        {
                            XLog.log("Received close command from service; closing");
                        }
                        break;
                    }
                    else if('<' == (char)buf[0])
                    {
                        try
                        {
                            string xml = XContent.ReceiveXString(nstm, buf);
                            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                            xd.LoadXml(xml);
                            DistributedObjectsSlave.InitXmlConfig(xd);
                            ReloadConfig();
                        }
                        catch
                        {
                            nstm.WriteByte((byte)'-');
                            throw;
                        }
                        nstm.WriteByte((byte)'+');
                        continue;
                    }
                    else if ('?' == (char)buf[0])
                    {
                        if (null != lasterror)
                        {
                            string lerr = lasterror;
                            lasterror = null; // Also resets error!
                            XContent.SendXContent(nstm, lerr);
                        }
                        else
                        {
                            XContent.SendXContent(nstm, buf, 0); // No error!
                        }
                        continue;
                    }
                    /*
                    if (XLog.logging)
                    {
                        XLog.log("Slave processing command " + ((char)buf[0]).ToString());
                    }
                     * */
                    ProcessCommand(nstm, (char)buf[0]);
                }
            }
            catch (System.IO.IOException ioex)
            {
                // Drop SocketException (shutdown during tasks), rethrow others.
                if ((ioex.InnerException as SocketException) == null)
                {
                    throw;
                }
#if DEBUG
                XLog.errorlog("DistributedObjectsSlave Warning: IOException+SocketException during task shutdown: " + ioex.ToString());
#endif
            }

            nstm.Close();
            nstm.Dispose();
        }

    }


    public static class XLog
    {
        public static bool logging = false;

        internal static Mutex logmutex = new Mutex(false, "distobjlog");


        internal static string UserLogFile;


        public static void errorlog(string line, string fn)
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
                using (StreamWriter fstm = File.AppendText(fn))
                {
                    string build = "";
                    try
                    {
                        System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                        System.Reflection.AssemblyName an = asm.GetName();
                        int bn = an.Version.Build;
                        int rv = an.Version.Revision;
                        build = "(build:" + bn.ToString() + "." + rv.ToString() + ") ";
                    }
                    catch
                    {
                    }
                    fstm.WriteLine(@"[{0} {1}ms] \\{2} DistributedObjectsSlave error: {3}{4}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, System.Net.Dns.GetHostName(), build, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.WriteLine();
                }
            }
            catch
            {
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }

        public static void errorlog(string line)
        {
            errorlog(line, "slave-log.txt");
        }


        static bool _failoverfailed = false;

        public static void failoverlog(string s)
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
                if (!_failoverfailed)
                {
                    if (s.StartsWith("x "))
                    {
                        _failoverfailed = true; // Don't keep logging these failures; the first is enough.
                    }
                    using (StreamWriter fstm = File.AppendText(UserLogFile + ".fo"))
                    {
                        int inl = s.IndexOf('\n');
                        if (-1 != inl)
                        {
                            s = s.Substring(0, inl);
                        }
                        s = s.Trim();
                        fstm.WriteLine(s);
                    }
                }
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }


        public static void log(string line, string fn)
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
                using (StreamWriter fstm = File.AppendText(fn))
                {
                    fstm.WriteLine("[{0} {1}ms] {2}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.WriteLine();
                }
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }

        public static void log(string line)
        {
            log(line, "slave-log.txt");
        }

#if NOTUSED
        public static void log(IList<string> lines, string fn)
        {
            StringBuilder sb = new StringBuilder(100);
            for (int i = 0; i < lines.Count; i++)
            {
                if(0 != i)
                {
                    sb.Append(Environment.NewLine);
                }
                sb.Append(lines[i]);
            }
            log(sb.ToString(), fn);
        }

        public static void log(IList<string> lines)
        {
            log(lines, "slave-log.txt");
        }
#endif

    }


    public class AppendBuffer
    {
        public byte[] buf; // Warning: buf.Length is preallocation buffer! vlength is valid-data length.
        internal int vlength = 0;


        public int GetLength()
        {
            return vlength;
        }

        public void SetLength(int len)
        {
            if (len > vlength)
            {
                if (len > buf.Length)
                {
                    byte[] newbuf = new byte[len];
                    for (int i = 0; i != vlength; i++)
                    {
                        newbuf[i] = buf[i];
                    }
                    buf = newbuf;
                }
            }
            else
            {
                vlength = len;
            }
        }


        public void Set(byte[] value, int valuelength)
        {
            SetLength(valuelength);
            for (int i = 0; i != valuelength; i++)
            {
                buf[i] = value[i];
            }
            vlength = valuelength;
        }

        // Warning: return's Length not length of valid data, but length of preallocation buffer.
        public byte[] Get(out int valuelength)
        {
            valuelength = vlength;
            return buf;
        }


        public void Append(byte[] avalue, int avaluelength)
        {
            if (vlength + avaluelength > buf.Length)
            {
                byte[] newbuf = new byte[(vlength + avaluelength) * 2]; // Grow!
                for (int i = 0; i != vlength; i++)
                {
                    newbuf[i] = buf[i];
                }
                buf = newbuf;
            }

            for (int i = 0; i != avaluelength; i++)
            {
                buf[vlength + i] = avalue[i];
            }
            vlength += avaluelength;
        }


        public AppendBuffer(int initbuffersize)
        {
            buf = new byte[initbuffersize];
        }

        public AppendBuffer()
            : this(32)
        {
        }
    }


    class DistributedObjectsSlave
    {
        public static System.Xml.XmlNode xslave; // XML config for slave; null if no config file.


        // Assumes xd has been loaded!
        public static void InitXmlConfig(System.Xml.XmlDocument xd)
        {
            try
            {
                xslave = xd["slave"];

                if (null != xslave) // !
                {
                    System.Xml.XmlNode xlog = xslave["log"];
                    if (null != xlog)
                    {
                        System.Xml.XmlAttribute xaenabled = xlog.Attributes["enabled"];
                        if (null != xaenabled)
                        {
                            XLog.logging = 0 == string.Compare(xaenabled.Value, "true", true);
                        }
                    }
                }
            }
            catch (System.Xml.XmlException e)
            {
                //XLog.errorlog("slave config xml error: " + e.ToString());
                throw new Exception("Sub process config xml error", e);
            }
        }


#if SLAVE_TRACE_PORT
        static List<int> DOSlave_TracePorts = new List<int>();
#endif


        public static long jid;
        public static string sjid;


        // args: <ipaddr> <portnum> <typechar> <capacity> <logfile> <jid>
        static void Main(string[] args)
        {

            try
            {

#if DEBUGnoisy
                XLog.errorlog("DistributedObjectsSlave EntryPoint: " + Environment.CommandLine);
#endif

#if DEBUG
                //System.Threading.Thread.Sleep(1000 * 8);
                {
                    //string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                    //if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER")
                    {
                        if (System.IO.File.Exists("sleep.txt"))
                        {
                            System.Threading.Thread.Sleep(1000 * 8);
                            int i32 = 1 + 32;
                        }
                    }
                }
#endif

#if DEBUG
                SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);
#endif

                XLog.UserLogFile = args[4];

#if SLAVE_TRACE
                try
                {
                    Thread mainthread = System.Threading.Thread.CurrentThread;
                    System.Threading.Thread stthd = new System.Threading.Thread(
                        new System.Threading.ThreadStart(
                        delegate
                        {
                            string spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
                            try
                            {
                                string dotracefile = spid + ".trace";
                                const string tracefiledelim = "{C8683F6C-0655-42e7-ACD9-0DDED6509A7C}";
                                for (; ; )
                                {
                                    System.IO.StreamWriter traceout = null;
                                    for (System.Threading.Thread.Sleep(1000 * 60)
                                        ; !System.IO.File.Exists(dotracefile)
                                        ; System.Threading.Thread.Sleep(1000 * 60))
                                    {
                                    }
                                    {
                                        string[] tfc;
                                        try
                                        {
                                            tfc = System.IO.File.ReadAllLines(dotracefile);
                                        }
                                        catch
                                        {
                                            continue;
                                        }
                                        if (tfc.Length < 1 || "." != tfc[tfc.Length - 1])
                                        {
                                            continue;
                                        }
                                        try
                                        {
                                            System.IO.File.Delete(dotracefile);
                                        }
                                        catch
                                        {
                                            continue;
                                        }
                                        if ("." != tfc[0])
                                        {
                                            string traceoutfp = tfc[0];
                                            try
                                            {
                                                traceout = System.IO.File.CreateText(traceoutfp);
                                                traceout.Write("BEGIN:");
                                                traceout.WriteLine(tracefiledelim);
                                            }
                                            catch
                                            {
                                                continue;
                                            }
                                        }
                                    }
                                    if (null == traceout)
                                    {
                                        XLog.log("SLAVE_TRACE: " + spid + " Start");
                                    }
                                    for (; ; System.Threading.Thread.Sleep(1000 * 60))
                                    {
                                        {
                                            try
                                            {
#if SLAVE_TRACE_PORT
                                                if (null == traceout)
                                                {
                                                    try
                                                    {
                                                        StringBuilder sbtp = new StringBuilder();
                                                        sbtp.Append("SLAVE_TRACE_PORT: " + spid + ":");
                                                        for (int i = 0; i < DOSlave_TracePorts.Count; i++)
                                                        {
                                                            sbtp.Append(' ');
                                                            sbtp.Append(DOSlave_TracePorts[i]);
                                                        }
                                                        if (0 == DOSlave_TracePorts.Count)
                                                        {
                                                            sbtp.Append(" None");
                                                        }
                                                        XLog.log(sbtp.ToString());
                                                    }
                                                    catch
                                                    {
                                                    }
                                                }
#endif
                                                bool thdsuspended = false;
                                                try
                                                {
                                                    mainthread.Suspend();
                                                    thdsuspended = true;
                                                }
                                                catch (System.Threading.ThreadStateException)
                                                {
                                                }
                                                try
                                                {
                                                    System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(mainthread, false);
                                                    StringBuilder sbst = new StringBuilder();
                                                    const int maxframesprint = 15;
                                                    for (int i = 0, imax = Math.Min(maxframesprint, st.FrameCount); i < imax; i++)
                                                    {
                                                        if (0 != sbst.Length)
                                                        {
                                                            sbst.Append(", ");
                                                        }
                                                        string mn = "N/A";
                                                        try
                                                        {
                                                            System.Reflection.MethodBase mb = st.GetFrame(i).GetMethod();
                                                            mn = mb.ReflectedType.Name + "." + mb.Name;
                                                        }
                                                        catch
                                                        {
                                                        }
                                                        sbst.Append(mn);
                                                    }
                                                    if (st.FrameCount > maxframesprint)
                                                    {
                                                        sbst.Append(" ... ");
                                                        sbst.Append(st.FrameCount - maxframesprint);
                                                        sbst.Append(" more");
                                                    }
                                                    if (null == traceout)
                                                    {
                                                        XLog.log("SLAVE_TRACE: " + spid + " Trace: " + sbst.ToString());
                                                    }
                                                    else
                                                    {
                                                        traceout.WriteLine(sbst.ToString());
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    XLog.log("SLAVE_TRACE: " + spid + " Error: " + e.ToString());
                                                }
                                                finally
                                                {
                                                    if (thdsuspended)
                                                    {
                                                        mainthread.Resume();
                                                    }
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                XLog.log("SLAVE_TRACE: " + spid + " " + mainthread.Name + " Trace Error: Cannot access thread: " + e.ToString());
                                            }
                                        }

                                        if (null != traceout)
                                        {
                                            traceout.Write(tracefiledelim);
                                            traceout.WriteLine(":END");
                                            traceout.Close();
                                            break;
                                        }
                                    }

                                }
                            }
                            catch (Exception e)
                            {
                                XLog.log("SLAVE_TRACE: " + spid + " Trace Failure: " + e.Message);
                            }
                        }));
                    stthd.IsBackground = true;
                    stthd.Start();
                }
                catch (Exception est)
                {
                    XLog.log("SLAVE_TRACE: Thread start error: " + est.ToString());
                }
#endif

                sjid = args[5];
                jid = long.Parse(sjid);

                try
                {
                    Environment.SetEnvironmentVariable("DOSLAVE", "DO5");
                }
                catch
                {
                }

                System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
#if DEBUGslaveconfigload
                Random _scsr = new Random(DateTime.Now.Millisecond / 2 + System.Threading.Thread.CurrentThread.ManagedThreadId / 2);
                //for (int i = 0; i < 50; i++)
                {
                    System.Threading.Thread.Sleep(_scsr.Next(50, 200));
                    try
                    {
                        xd.Load("slaveconfig.j" + sjid + ".xml");
                        InitXmlConfig(xd);
                    }
                    catch (System.IO.FileNotFoundException e)
                    {
                        //System.Diagnostics.Debugger.Launch();
                    }
                    catch (Exception e)
                    {
                        System.Diagnostics.Debugger.Launch();
                        throw;
                    }
                }
#else
                try
                {
                    xd.Load("slaveconfig.j" + sjid + ".xml");
                    InitXmlConfig(xd);
                }
                catch (System.IO.FileNotFoundException e)
                {
                }
#endif

                if (XLog.logging)
                {
                    XLog.log("New Sub Process: '" + args[0] + "' '" + args[1] + "' '" + args[2] + "' '" + args[3] + "' '" + args[4] + "' '" + args[5] + "'");
                }

                Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                sock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                string host = args[0];
                int portnum = ushort.Parse(args[1]);
#if SLAVE_TRACE_PORT
                DOSlave_TracePorts.Add(portnum);
#endif
                char typechar = args[2][0];
                try
                {
                    Thread.Sleep(100); // Give DLL a chance to start accepting.
                    sock.Connect(host, portnum);
                }
                catch (Exception e)
                {
                    throw new Exception("Sub-process-to-client(dll) connection error: " + e.ToString() + "  [Note: ensure port " + portnum.ToString() + " is free / not blocked]");
                }
                IDistObject dop;
                if ('H' == typechar)
                {
                    int capacity = ParseCapacity(args[3]);
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating Hashtable with capacity " + args[3]);
                    }
                    dop = HashtableObjectPart.Create(capacity);
                }
                else if ('8' == typechar)
                {
                    int capacity = ParseCapacity(args[3]);
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating IntComboList with capacity " + args[3]);
                    }
                    dop = IntComboListPart.Create(capacity);
                }
                else if ('C' == typechar) // 12 hex
                {
                    int capacity = ParseCapacity(args[3]);
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating LongIntComboList with capacity " + args[3]);
                    }
                    dop = LongIntComboListPart.Create(capacity);
                }
                else if ('A' == typechar)
                {
                    //int capacity = ParseCapacity(args[3]);
                    string[] a = args[3].Split(';');
                    int count_capacity = ParseCapacity(a[0]);
                    int estimated_row_capacity = ParseCapacity(a[1]);
                    int keylen = int.Parse(a[2]);
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating ArrayComboList(keylength=" + keylen.ToString() + ") with capacity " + args[3]);
                    }
                    dop = ArrayComboListPart.Create(count_capacity, estimated_row_capacity, keylen);
                }
                else if ('R' == typechar)
                {
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating Remote");
                    }
                    dop = RemotePart.Create();
                }
                else if ('F' == typechar)
                {
                    //int capacity = ParseCapacity(args[3]);
                    string[] a = args[3].Split(';');
                    int keylen = ParseCapacity(a[2]);
                    int valuelen = ParseCapacity(a[3]);
                    if (XLog.logging)
                    {
                        XLog.log("Connected; creating FixedArrayComboList(keylength=" + keylen.ToString() + ";valuelength=" + valuelen.ToString() + ")");
                    }
                    dop = FixedArrayComboListPart.Create(keylen, valuelen);
                }
                else
                {
                    throw new Exception("Data type not supported");
                }

                NetworkStream nstm = new XNetworkStream(sock);
                {
                    // New handshake with client(dll)...
                    nstm.WriteByte(1);
                    // ... and the SlavePID:
                    int SlavePID = System.Diagnostics.Process.GetCurrentProcess().Id;
                    byte[] bpid = DistObjectBase.ToBytes(SlavePID);
                    XContent.SendXContent(nstm, bpid);
                }

                {
                    int pid = System.Diagnostics.Process.GetCurrentProcess().Id;
                    string spid = pid.ToString();
                    string pidfilename = spid + ".j" + sjid + ".slave.pid";
                    StreamWriter pidfile = new StreamWriter(pidfilename);
                    pidfile.WriteLine(pid);
                    pidfile.WriteLine(System.DateTime.Now);
                    pidfile.WriteLine("jid={0}", sjid);
                    pidfile.Flush();
                    for (; ; )
                    {
                        try
                        {
                            dop.ProcessCommands(nstm);
                            break;
                        }
                        catch (DistObjectAbortException e)
                        {
                            XLog.errorlog("ProcessCommands exception: " + e.ToString() + " ((DistObjectAbortException))");
                            break; // !
                        }
                        catch (Exception e)
                        {
                            XLog.errorlog("ProcessCommands exception: " + e.ToString() + " ((recovering from this exception))");
                        }
                    }
                    pidfile.Close();
                    try
                    {
                        System.IO.File.Delete(pidfilename);
                    }
                    catch
                    {
                    }
                }
            }
            catch (Exception e)
            {
                XLog.errorlog("Main exception: " + e.ToString());
            }
        }


        public class DistObjectAbortException : Exception
        {
            public DistObjectAbortException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }

            public DistObjectAbortException(string msg)
                : base(msg)
            {
            }

            public DistObjectAbortException(Exception innerException)
                : base("Job Abort", innerException)
            {
            }
        }


        static bool _CanSlaveExceptionFailover_top(Exception e)
        {
            return (null != e as System.IO.IOException)
                || (null != e as System.Net.Sockets.SocketException)
                || (null != e as System.Security.SecurityException)
                || (null != e as System.UnauthorizedAccessException)
                ;
        }

        public static bool CanSlaveExceptionFailover(Exception e)
        {
            return _CanSlaveExceptionFailover_top(e)
                || _CanSlaveExceptionFailover_top(e.InnerException);
        }



        public const int ServerPortMin = 60531;
        public const int ServerPortMax = ServerPortMin + 1800;
        private static int prevserverport = -1;

        // Generic server port.
        public static int GetNextServerPort()
        {
            /*
            Interlocked.CompareExchange(prevslaveport, SlavePortMin - 1, SlavePortMax);
            Interlocked.Increment(prevslaveport);
             * */
            lock (typeof(DistributedObjectsSlave)) // ...
            {
                if (prevserverport == -1)
                {
                    Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    prevserverport = rnd.Next(ServerPortMin, ServerPortMax) - 1;
                }
                if (prevserverport == ServerPortMax)
                {
                    prevserverport = ServerPortMin - 1;
                }
                prevserverport++;
            }
            return prevserverport;
        }


        public static int ParseCapacity(string capacity)
        {
            try
            {
                if (null == capacity || 0 == capacity.Length)
                {
                    throw new Exception("Invalid capacity: capacity not specified");
                }
                switch (capacity[capacity.Length - 1])
                {
                    case 'B':
                        if (1 == capacity.Length)
                        {
                            throw new Exception("Invalid capacity: " + capacity);
                        }
                        switch (capacity[capacity.Length - 2])
                        {
                            case 'K': // KB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                            case 'M': // MB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                            case 'G': // GB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                            default: // Just bytes with B suffix.
                                return int.Parse(capacity.Substring(0, capacity.Length - 1));
                        }
                    //break;

                    default: // Assume just bytes without a suffix.
                        return int.Parse(capacity);
                }
            }
            catch (FormatException e)
            {
                throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.ToString());
            }
            catch (OverflowException e)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.ToString());
            }
        }


        public static ulong GetCurrentDiskFreeBytes()
        {
            ulong fba, tnb, tnfb;
            if (!GetDiskFreeSpaceEx(null, out fba, out tnb, out tnfb))
            {
                return 0;
            }
            return fba;
        }


        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern bool GetDiskFreeSpaceEx(
            string DirectoryName,
            out ulong FreeBytesAvailable,
            out ulong TotalNumberOfBytes,
            out ulong TotalNumberOfFreeBytes);

        [DllImport("kernel32.dll")]
        static extern bool SetPriorityClass(IntPtr hProcess, uint dwPriorityClass);

        const int BELOW_NORMAL_PRIORITY_CLASS = 0x00004000;

        [DllImport("kernel32.dll")]
        static extern IntPtr GetCurrentProcess();


    }
}
