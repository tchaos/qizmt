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
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

using System.Runtime.InteropServices;

namespace MemCachePin
{
    public partial class MemCachePinService : ServiceBase
    {
        static System.Threading.Thread lthd;
        static System.Net.Sockets.Socket lsock;
        static int userhit = 0;

        struct PSM
        {
            internal string name;
            internal IntPtr hmap;
            internal IntPtr ipview;
        }
        static List<PSM> pins = new List<PSM>(100);


        static int IndexOfPSM_unlocked(string smname)
        {
            //lock (pins)
            {
                for (int i = 0; i < pins.Count; i++)
                {
                    if (0 == string.Compare(smname, pins[i].name, true))
                    {
                        return i;
                    }
                }
                return -1;
            }
        }


        public MemCachePinService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                string service_base_dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                System.Environment.CurrentDirectory = service_base_dir;

                lthd = new System.Threading.Thread(new System.Threading.ThreadStart(ListenThreadProc));
                lthd.IsBackground = true;
                lthd.Start();
            }
            catch (Exception e)
            {
                XLog.errorlog("OnStart exception: " + e.ToString());
            }
        }

        protected override void OnStop()
        {
        }


        static void ListenThreadProc()
        {
            try
            {
                lsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55906);
                for (int i = 0; ; i++)
                {
                    try
                    {
                        lsock.Bind(ipep);
                        break;
                    }
                    catch (Exception e)
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

                    System.Threading.Thread cthd = new System.Threading.Thread(
                        new System.Threading.ParameterizedThreadStart(ClientThreadProc));
                    cthd.Name = "ClientThread" + (++userhit).ToString();
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


        static void ClientThreadProc(object obj)
        {
            System.Net.Sockets.Socket dllclientSock = null;
            byte[] buf = new byte[0x400 * 4];
            try
            {
                dllclientSock = (System.Net.Sockets.Socket)obj;
                System.Net.Sockets.NetworkStream netstm = new System.Net.Sockets.NetworkStream(dllclientSock);
                for (bool run = true; run; )
                {
                    int ich = -1;
                    try
                    {
                        ich = netstm.ReadByte();
                    }
                    catch
                    {
                    }
                    if (ich == -1)
                    {
                        break;
                    }
                    switch (ich)
                    {
                        case 'p': // Pin shared memory.
                            {
                                try
                                {
                                    string smname = XContent.ReceiveXString(netstm, buf);

                                    IntPtr hf = INVALID_HANDLE_VALUE;
                                    IntPtr hmap = CreateFileMapping(hf, IntPtr.Zero, PAGE_READWRITE, 1, @"Global\" + smname);
                                    int lasterror = Marshal.GetLastWin32Error();
                                    if (IntPtr.Zero == hmap)
                                    {
                                        if (8 == lasterror)
                                        {
                                            throw new Exception("Shared memory segment named '" + smname + "' cannot be allocated; CreateFileMapping failed with ERROR_NOT_ENOUGH_MEMORY",
                                                new OutOfMemoryException());
                                        }
                                        throw new Exception("Shared memory segment named '" + smname + "' cannot be allocated; CreateFileMapping failed with GetLastWin32Error=" + lasterror);
                                    }
                                    if (ERROR_ALREADY_EXISTS != lasterror)
                                    {
                                        throw new Exception("Shared memory segment named '" + smname + "' not found");
                                    }
                                    IntPtr ipview = MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
                                    if (IntPtr.Zero == ipview)
                                    {
                                        lasterror = Marshal.GetLastWin32Error();
                                        CloseHandle(hmap);
                                        if (8 == lasterror)
                                        {
                                            throw new Exception("Shared memory segment named '" + smname + "' cannot be mapped into memory; MapViewOfFile failed with ERROR_NOT_ENOUGH_MEMORY",
                                                new OutOfMemoryException());
                                        }
                                        throw new Exception("Shared memory segment named '" + smname + "' cannot be mapped into memory; MapViewOfFile failed with GetLastWin32Error=" + lasterror);
                                    }

                                    PSM psm;
                                    psm.hmap = hmap;
                                    psm.ipview = ipview;
                                    psm.name = smname;
                                    lock (pins)
                                    {
                                        pins.Add(psm);
                                    }

                                    netstm.WriteByte((byte)'+');

                                }
                                catch(Exception e)
                                {
                                    try
                                    {
                                        netstm.WriteByte((byte)'-');
                                        XContent.SendXContent(netstm, e.ToString());
                                    }
                                    catch
                                    {
                                        throw new Exception("Unable to report exception to caller (pin shared memory)", e);
                                    }
                                }
                            }
                            break;

                        case 'u': // Unpin shared memory.
                            {
                                try
                                {
                                    string smname = XContent.ReceiveXString(netstm, buf);

                                    bool found = false;
                                    PSM psm = new PSM();
                                    lock (pins)
                                    {
                                        int ipsm = IndexOfPSM_unlocked(smname);
                                        if (-1 != ipsm)
                                        {
                                            psm = pins[ipsm];
                                            pins.RemoveAt(ipsm);
                                            found = true;
                                        }
                                    }
                                    if (found)
                                    {
                                        UnmapViewOfFile(psm.ipview);
                                        CloseHandle(psm.hmap);
                                        netstm.WriteByte((byte)'+');
                                    }
                                    else
                                    {
                                        netstm.WriteByte((byte)'-');
                                        XContent.SendXContent(netstm, "Cannot unpin; shared memory segment not pinned: " + smname);
                                    }

                                }
                                catch (Exception e)
                                {
                                    try
                                    {
                                        netstm.WriteByte((byte)'-');
                                        XContent.SendXContent(netstm, e.ToString());
                                    }
                                    catch
                                    {
                                        throw new Exception("Unable to report exception to caller (unpin shared memory)", e);
                                    }
                                }
                            }
                            break;

                        case 'c': // Close.
                            run = false;
                            break;
                    }
                }
                netstm.Close();
                dllclientSock.Close();
            }
            catch (Exception e)
            {
                XLog.errorlog("ClientThreadProc exception: " + e.ToString());

                try
                {
                    dllclientSock.Close();
                }
                catch (Exception e2)
                {
                }
            }
        }


        public static class XLog
        {
            public static bool logging = false;

            internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "MemCachePinLog");


            public static void errorlog(string line)
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
                    using (System.IO.StreamWriter fstm = System.IO.File.AppendText("MemCachePin-errors.txt"))
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
                        fstm.WriteLine("[{0} {1}ms] MemCachePin service error: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
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


            public static void log(string name, string line)
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
                    System.IO.StreamWriter fstm = System.IO.File.AppendText(name + ".txt");
                    fstm.WriteLine("{0}", line);
                    fstm.Close();
                }
                finally
                {
                    logmutex.ReleaseMutex();
                }
            }
        }


        #region WIN32API

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr CreateFile(
           String lpFileName, int dwDesiredAccess, int dwShareMode,
           IntPtr lpSecurityAttributes, int dwCreationDisposition,
           int dwFlagsAndAttributes, IntPtr hTemplateFile);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr CreateFileMapping(
           IntPtr hFile, IntPtr lpAttributes, int flProtect,
           uint dwMaximumSizeHigh, uint dwMaximumSizeLow,
           String lpName);

        internal static IntPtr CreateFileMapping(
           IntPtr hFile, IntPtr lpAttributes, int flProtect,
           long dwMaximumSize,
           String lpName)
        {
            return CreateFileMapping(hFile, lpAttributes, flProtect,
                (uint)((ulong)dwMaximumSize >> 32), (uint)dwMaximumSize,
                lpName);
        }

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool FlushViewOfFile(
           IntPtr lpBaseAddress, int dwNumBytesToFlush);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern IntPtr MapViewOfFile(
           IntPtr hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh,
           int dwFileOffsetLow, int dwNumBytesToMap);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern IntPtr OpenFileMapping(
           int dwDesiredAccess, bool bInheritHandle, String lpName);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool UnmapViewOfFile(IntPtr lpBaseAddress);

        [DllImport("kernel32", SetLastError = true)]
        internal static extern bool CloseHandle(IntPtr handle);

        internal const int ERROR_ALREADY_EXISTS = 183;

        internal static IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        internal const int PAGE_READWRITE = 0x4;

        internal const int FILE_MAP_WRITE = 0x2;
        internal const int FILE_MAP_READ = 0x4;
        internal const int FILE_MAP_ALL_ACCESS = 0xF001F;

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall, SetLastError = true)]
        internal static extern IntPtr CreateFile(
              string lpFileName,
              uint dwDesiredAccess,
              uint dwShareMode,
              IntPtr SecurityAttributes,
              uint dwCreationDisposition,
              uint dwFlagsAndAttributes,
              IntPtr hTemplateFile
              );

        #endregion


    }
}
