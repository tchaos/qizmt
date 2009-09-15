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


//#define SPINWAIT_RETURN


using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;


namespace MySpace.DataMining.CollaborativeFilteringObjects3
{
    public class SlaveMemory
    {
        protected internal class Slave
        {
            protected internal Slave(SlaveMemory parent, int slavenum)
            {
                this.parent = parent;
                this.slavenum = slavenum;
            }


            protected internal void startprocess()
            {
                string flags = "";
#if SPINWAIT_RETURN
#else
                flags += "r";
#endif
                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(parent.slaveprocessname, "~sm_bmpc " + parent.kbasename + " " + parent.blocksize.ToString() + " " + parent.threadviews.Count.ToString() + " " + parent.maxpacketsize.ToString() + " " + slavenum.ToString() + " " + System.Diagnostics.Process.GetCurrentProcess().Id.ToString() + " " + flags);
                psi.CreateNoWindow = true;
                psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                psi.UseShellExecute = false;
                slaveproc = System.Diagnostics.Process.Start(psi);
            }


            internal System.Diagnostics.Process slaveproc;
            internal SlaveMemory parent;
            internal int slavenum;
        }


        internal class ComBuf
        {
            internal ComBuf(ThreadView parent, int slavenum, int buflen)
            {
                if (buflen <= 0 || buflen >= 1073741824)
                {
                    throw new Exception("Slave memory communication buffer has invalid byte size: " + buflen.ToString());
                }

                this.parent = parent;
                this.slavenum = slavenum;
                this.buflen = buflen;
            }


            internal unsafe void start()
            {
                hmap = CreateFileMapping(INVALID_HANDLE_VALUE, IntPtr.Zero, PAGE_READWRITE, 0, buflen, parent.kname + "_block" + slavenum.ToString());
                if (IntPtr.Zero == hmap)
                {
                    throw new SlaveMemoryException("Unable to create shared memory communication buffer");
                }

                pview = MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
                if (IntPtr.Zero == pview)
                {
                    CloseHandle(hmap);
                    hmap = IntPtr.Zero;
                    throw new SlaveMemoryException("Unable to map shared memory communication buffer");
                }

                this.ewh = new System.Threading.EventWaitHandle(false, System.Threading.EventResetMode.AutoReset, parent.kname + "_event" + slavenum.ToString());
#if SPINWAIT_RETURN
#else
                this.ewhreturn = new System.Threading.EventWaitHandle(false, System.Threading.EventResetMode.AutoReset, parent.kname + "_returnevent" + slavenum.ToString());
#endif

                // Ensure whole communication buffer is valid...
                try
                {
                    byte* com = (byte*)pview;
                    com[buflen - 1] = 0xAA;
                }
                catch (Exception e)
                {
                    throw new Exception("SlaveMemory: error accessing entire shared memory communication buffer", e);
                }
            }


            void _clean()
            {
                if (IntPtr.Zero != hmap)
                {
                    unsafe
                    {
                        byte* com = (byte*)pview;
                        com[0] = 0; // Exit thread
                        xsignal();
                    }

                    UnmapViewOfFile(pview);
                    CloseHandle(hmap);
                    hmap = IntPtr.Zero;
                }
            }

            public void Dispose()
            {
                _clean();
                System.GC.SuppressFinalize(this);
            }


            ~ComBuf()
            {
                _clean();
            }


            internal void xsignal()
            {
                ewh.Set();
            }


#if SPINWAIT_RETURN
#else
            internal void waitreturnsignal()
            {
                ewhreturn.WaitOne();
            }
#endif


            ThreadView parent;
            int slavenum;
            int buflen;
            System.Threading.EventWaitHandle ewh;
#if SPINWAIT_RETURN
#else
            System.Threading.EventWaitHandle ewhreturn;
#endif
            IntPtr hmap = IntPtr.Zero;
            internal IntPtr pview;
        }


        public class ThreadView : IDisposable
        {
            internal ThreadView(SlaveMemory parent, int threadnum, int maxpacketsize)
            {
                this.parent = parent;
                this.threadnum = threadnum;
                this.maxpacketsize = maxpacketsize;
                this.kname = parent.kbasename + "_thread" + threadnum.ToString();

                _batspill = new List<Batch>();
            }


            List<Batch> _batspill;

            public struct Batch
            {
                public static Batch Create(long soffset, byte[] dest, int destoffset, int length)
                {
                    Batch result;
                    result.soffset = soffset;
                    result.dest = dest;
                    result.destoffset = destoffset;
                    result.length = length;
                    return result;
                }

                public static Batch Create(long soffset, byte[] dest)
                {
                    return Create(soffset, dest, 0, dest.Length);
                }

                public long soffset;
                public byte[] dest;
                public int destoffset;
                public int length;
            }


            public void Get(List<Batch> bat)
            {
                unsafe
                {
                    _batspill.Clear();
                    for (int curslaveindex = 0; curslaveindex < parent.slaves.Count; curslaveindex++)
                    {
                        ComBuf cb = combufs[curslaveindex];
                        byte* com = (byte*)cb.pview;
                        byte* comnext = com;
                        byte* comend = com + 1 + 8 + 4 + parent.maxpacketsize;
                        for (int ib = 0; ib < bat.Count; ib++)
                        {
                            int slaveindex = (int)(bat[ib].soffset / parent.blocksize);
                            if (slaveindex == curslaveindex)
                            {
                                long slaveoffset = bat[ib].soffset % parent.blocksize;
                                long spill = (slaveoffset + (long)bat[ib].length) - parent.blocksize;
                                int slavelength = bat[ib].length;
                                if (spill > 0)
                                {
#if DEBUG
                                    parent._batchspilled = true;
#endif
                                    if (spill > int.MaxValue)
                                    {
                                        throw new Exception("SlaveMemory: spill overflow");
                                    }
                                    _batspill.Add(bat[ib]);
                                    continue; // This one spills; skip it for now.
                                }

                                // ... batch it up...
                                if (comnext + 1 + 8 + 4 + slavelength + 1 > comend)
                                {
                                    throw new SlaveMemoryException("Communication buffer overflow: batched too much memory");
                                }
                                comnext[0] = 3; // Batch read
                                Int64ToBytes(slaveoffset, comnext, 1);
                                Int32ToBytes(slavelength, comnext, 1 + 8);
                                comnext += 1 + 8 + 4 + slavelength;
                            }
                        }
                        if (comnext != com)
                        {
                            comnext[0] = 255; // End of batch.
                            cb.xsignal();
#if SPINWAIT_RETURN
                            for (; ; ) // Spin lock for result.
                            {
                                System.Threading.Thread.SpinWait(8);
                                if (255 == com[0])
                                {
                                    break;
                                }
                            }
#else
                            cb.waitreturnsignal();
#endif
                            // Now read the results into the destinations...
                            comnext = com;
                            for (int ib = 0; ib < bat.Count; ib++)
                            {
                                int slaveindex = (int)(bat[ib].soffset / parent.blocksize);
                                if (slaveindex == curslaveindex)
                                {
                                    long slaveoffset = bat[ib].soffset % parent.blocksize;
                                    int slavelength = bat[ib].length;
                                    pmemcpy(bat[ib].dest, bat[ib].destoffset, comnext + 1 + 8 + 4, slavelength);
                                    comnext += 1 + 8 + 4 + slavelength;
                                }
                            }
                        }
                    }
                    if (0 != _batspill.Count)
                    {
                        for (int i = 0; i < _batspill.Count; i++)
                        {
                            Get(_batspill[i].soffset, _batspill[i].dest, _batspill[i].destoffset, _batspill[i].length);
                        }
                    }
                }
            }

            public void Get(long soffset, byte[] dest, int destoffset, int length)
            {
                for (; ; )
                {
                    int slaveindex = (int)(soffset / parent.blocksize);
                    long slaveoffset = soffset % parent.blocksize;
                    long spill = (slaveoffset + (long)length) - parent.blocksize;
                    int slavelength = length;
                    if (spill > 0)
                    {
                        if (spill > int.MaxValue)
                        {
                            throw new Exception("SlaveMemory: spill overflow");
                        }
                        slavelength -= (int)spill;
                    }

                    unsafe
                    {
                        ComBuf cb = combufs[slaveindex];
                        byte* com = (byte*)cb.pview;
                        Int64ToBytes(slaveoffset, com, 1);
                        Int32ToBytes(slavelength, com, 1 + 8);
                        com[0] = 1; // Read
                        cb.xsignal();
#if SPINWAIT_RETURN
                        for (; ; ) // Spin lock for result.
                        {
                            System.Threading.Thread.SpinWait(8);
                            if (255 == com[0])
                            {
                                break;
                            }
                        }
#else
                        cb.waitreturnsignal();
#endif
                        pmemcpy(dest, destoffset, com + 1 + 8 + 4, slavelength);
                    }

                    if (spill <= 0)
                    {
                        break;
                    }
                    soffset += slavelength;
                    destoffset += slavelength;
                    length -= slavelength;
                }
            }

            public void Get(long soffset, byte[] dest)
            {
                Get(soffset, dest, 0, dest.Length);
            }


            public void Set(List<Batch> bat)
            {
                unsafe
                {
                    _batspill.Clear();
                    for (int curslaveindex = 0; curslaveindex < parent.slaves.Count; curslaveindex++)
                    {
                        ComBuf cb = combufs[curslaveindex];
                        byte* com = (byte*)cb.pview;
                        byte* comnext = com;
                        byte* comend = com + 1 + 8 + 4 + parent.maxpacketsize;
                        for (int ib = 0; ib < bat.Count; ib++)
                        {
                            int slaveindex = (int)(bat[ib].soffset / parent.blocksize);
                            if (slaveindex == curslaveindex)
                            {
                                long slaveoffset = bat[ib].soffset % parent.blocksize;
                                long spill = (slaveoffset + (long)bat[ib].length) - parent.blocksize;
                                int slavelength = bat[ib].length;
                                if (spill > 0)
                                {
#if DEBUG
                                    parent._batchspilled = true;
#endif
                                    if (spill > int.MaxValue)
                                    {
                                        throw new Exception("SlaveMemory: spill overflow");
                                    }
                                    _batspill.Add(bat[ib]);
                                    continue; // This one spills; skip it for now.
                                }

                                // ... batch it up...
                                if (comnext + 1 + 8 + 4 + slavelength + 1 > comend)
                                {
                                    throw new SlaveMemoryException("Communication buffer overflow: batched too much memory");
                                }
                                comnext[0] = 4; // Batch write
                                Int64ToBytes(slaveoffset, comnext, 1);
                                Int32ToBytes(slavelength, comnext, 1 + 8);
                                pmemcpy(comnext + 1 + 8 + 4, bat[ib].dest, bat[ib].destoffset, slavelength);
                                comnext += 1 + 8 + 4 + slavelength;
                            }
                        }
                        if (comnext != com)
                        {
                            comnext[0] = 255; // End of batch.
                            cb.xsignal();
#if SPINWAIT_RETURN
                            for (; ; ) // Spin lock for result.
                            {
                                System.Threading.Thread.SpinWait(8);
                                if (255 == com[0])
                                {
                                    break;
                                }
                            }
#else
                            cb.waitreturnsignal();
#endif
                        }
                    }
                    if (0 != _batspill.Count)
                    {
                        for (int i = 0; i < _batspill.Count; i++)
                        {
                            Set(_batspill[i].soffset, _batspill[i].dest, _batspill[i].destoffset, _batspill[i].length);
                        }
                    }
                }
            }

            public void Set(long soffset, byte[] src, int srcoffset, int length)
            {
                for (; ; )
                {
                    int slaveindex = (int)(soffset / parent.blocksize);
                    long slaveoffset = soffset % parent.blocksize;
                    long spill = (slaveoffset + (long)length) - parent.blocksize;
                    int slavelength = length;
                    if (spill > 0)
                    {
                        if (spill > int.MaxValue)
                        {
                            throw new Exception("SlaveMemory: spill overflow");
                        }
                        slavelength -= (int)spill;
                    }

                    unsafe
                    {
                        ComBuf cb = combufs[slaveindex];
                        byte* com = (byte*)cb.pview;
                        Int64ToBytes(slaveoffset, com, 1);
                        Int32ToBytes(slavelength, com, 1 + 8);
                        pmemcpy(com + 1 + 8 + 4, src, srcoffset, slavelength);
                        com[0] = 2; // Write
                        cb.xsignal();
#if SPINWAIT_RETURN
                        for (; ; ) // Spin lock for completion.
                        {
                            System.Threading.Thread.SpinWait(8);
                            if (255 == com[0])
                            {
                                break;
                            }
                        }
#else
                        cb.waitreturnsignal();
#endif
                    }

                    if (spill <= 0)
                    {
                        break;
                    }
                    soffset += slavelength;
                    srcoffset += slavelength;
                    length -= slavelength;
                }
            }

            public void Set(long soffset, byte[] src)
            {
                Set(soffset, src, 0, src.Length);
            }


            protected internal void _close()
            {
                if (null != combufs)
                {
                    foreach (ComBuf cb in combufs)
                    {
                        cb.Dispose();
                    }
                    combufs = null;
                }
            }


            protected internal void createcombufs()
            {
                combufs = new List<ComBuf>(parent.slaves.Count);
                int buflen = 1 + 8 + 4 + maxpacketsize;
                for (int i = 0; i < parent.slaves.Count; i++)
                {
                    ComBuf cb = new ComBuf(this, i, buflen);
                    cb.start();
                    combufs.Add(cb);
                }
            }


            public SlaveMemory Parent
            {
                get
                {
                    return parent;
                }
            }


            void _clean()
            {
            }

            public void Dispose()
            {
                _clean();
                System.GC.SuppressFinalize(this);
            }


            ~ThreadView()
            {
                _clean();
            }


            int threadnum;
            SlaveMemory parent;
            internal string kname;
            internal List<ComBuf> combufs;
            int maxpacketsize; // Note: excludes action byte, offset and length.

        }


        public string Name
        {
            get
            {
                return userobjectname;
            }
        }


        public IList<ThreadView> ThreadViews
        {
            get
            {
                return threadviews;
            }
        }


        public SlaveMemory(string objectname, string slaveprocessname, long blocksize, int maxpacketsize, int nthreads, int nslaves)
        {
            this.slaveprocessname = slaveprocessname;
            this.blocksize = blocksize;
            this.userobjectname = objectname;
            this.kbasename = "SM" + System.Diagnostics.Process.GetCurrentProcess().Id.ToString() + "_" + objectname;
            this.maxpacketsize = maxpacketsize;

            threadviews = new List<ThreadView>(nthreads);
            for (int nt = 0; nt < nthreads; nt++)
            {
                threadviews.Add(new ThreadView(this, nt, maxpacketsize));
            }

            slaves = new List<Slave>(nslaves);
            for (int ns = 0; ns < nslaves; ns++)
            {
                slaves.Add(new Slave(this, ns));
            }
        }


        public void Open()
        {
            unsafe
            {
                for (int nt = 0; nt < threadviews.Count; nt++)
                {
                    threadviews[nt].createcombufs();
                }

                for (int ns = 0; ns < slaves.Count; ns++)
                {
                    slaves[ns].startprocess();
                }
            }
        }


        public void Close()
        {
            for (int nt = 0; nt < threadviews.Count; nt++)
            {
                threadviews[nt]._close();
            }
        }


        public int MaxPacketSize
        {
            get
            {
                return maxpacketsize;
            }
        }


        internal string slaveprocessname;
        string userobjectname;
        internal string kbasename;
        List<ThreadView> threadviews;
        internal List<Slave> slaves;
        internal long blocksize;
        int maxpacketsize;

#if DEBUG
        public bool _batchspilled = false;
#endif


        public unsafe static void Int32ToBytes(Int32 x, byte* resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)((UInt32)x >> 8);
            resultbuf[bufoffset + 2] = (byte)((UInt32)x >> 16);
            resultbuf[bufoffset + 3] = (byte)((UInt32)x >> 24);
        }

        public unsafe static void Int64ToBytes(Int64 x, byte* resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)((UInt64)x >> 8);
            resultbuf[bufoffset + 2] = (byte)((UInt64)x >> 16);
            resultbuf[bufoffset + 3] = (byte)((UInt64)x >> 24);
            resultbuf[bufoffset + 4] = (byte)((UInt64)x >> 32);
            resultbuf[bufoffset + 5] = (byte)((UInt64)x >> 40);
            resultbuf[bufoffset + 6] = (byte)((UInt64)x >> 48);
            resultbuf[bufoffset + 7] = (byte)((UInt64)x >> 56);
        }


        public unsafe static Int32 BytesToInt32(byte* x, int offset)
        {
            int result = 0;
            result |= x[offset + 0];
            result |= (Int32)((UInt32)x[offset + 1] << 8);
            result |= (Int32)((UInt32)x[offset + 2] << 16);
            result |= (Int32)((UInt32)x[offset + 3] << 24);
            return result;
        }

        public unsafe static Int64 BytesToInt64(byte* x, int offset)
        {
            Int64 result = 0;
            result |= x[offset + 0];
            result |= (Int64)((UInt64)x[offset + 1]) << 8;
            result |= (Int64)((UInt64)x[offset + 2]) << 16;
            result |= (Int64)((UInt64)x[offset + 3]) << 24;
            result |= (Int64)((UInt64)x[offset + 4]) << 32;
            result |= (Int64)((UInt64)x[offset + 5]) << 40;
            result |= (Int64)((UInt64)x[offset + 6]) << 48;
            result |= (Int64)((UInt64)x[offset + 7]) << 56;
            return result;
        }


        public unsafe static void pmemcpy(byte* dest, byte* src, int length)
        {
            uint numints = (uint)length >> 2;
            int* idest = (int*)dest;
            int* isrc = (int*)src;
            for (int i = 0; i < numints; i++)
            {
                idest[i] = isrc[i];
            }
            for (uint remainpos = numints << 2; remainpos < (uint)length; remainpos++)
            {
                dest[remainpos] = src[remainpos];
            }
        }

        public unsafe static void pmemcpy(byte* dest, byte[] src, int srcoffset, int length)
        {
            fixed (byte* psrc = src)
            {
                pmemcpy(dest, psrc + srcoffset, length);
            }
        }

        public unsafe static void pmemcpy(byte[] dest, int destoffset, byte* src, int length)
        {
            fixed (byte* pdest = dest)
            {
                pmemcpy(pdest + destoffset, src, length);
            }
        }


        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern IntPtr CreateFile(
           String lpFileName, int dwDesiredAccess, int dwShareMode,
           IntPtr lpSecurityAttributes, int dwCreationDisposition,
           int dwFlagsAndAttributes, IntPtr hTemplateFile);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern IntPtr CreateFileMapping(
           IntPtr hFile, IntPtr lpAttributes, int flProtect,
           int dwMaximumSizeHigh, int dwMaximumSizeLow,
           String lpName);

        [DllImport("kernel32", SetLastError = true)]
        public static extern bool FlushViewOfFile(
           IntPtr lpBaseAddress, int dwNumBytesToFlush);

        [DllImport("kernel32", SetLastError = true)]
        public static extern IntPtr MapViewOfFile(
           IntPtr hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh,
           int dwFileOffsetLow, int dwNumBytesToMap);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern IntPtr OpenFileMapping(
           int dwDesiredAccess, bool bInheritHandle, String lpName);

        [DllImport("kernel32", SetLastError = true)]
        public static extern bool UnmapViewOfFile(IntPtr lpBaseAddress);

        [DllImport("kernel32", SetLastError = true)]
        public static extern bool CloseHandle(IntPtr handle);

        [DllImport("kernel32")]
        public static extern uint GetLastError();

        public static IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        public const int PAGE_READWRITE = 0x4;

        public const int FILE_MAP_WRITE = 0x2;
        public const int FILE_MAP_READ = 0x4;
        public const int FILE_MAP_ALL_ACCESS = 0xF001F;

    }


    public class SlaveMemoryException : Exception
    {
        public SlaveMemoryException(string msg)
            : base(msg)
        {
        }
    }


    public class SlaveProcess
    {
        // Returns null if not, or the SlaveProcess instance if so.
        public static SlaveProcess IsSlaveProcess(string[] mainargs)
        {
            if (mainargs.Length == 0 || "~sm_bmpc" != mainargs[0])
            {
                return null;
            }
            return new SlaveProcess(mainargs);
        }


        /*
        public void Run()
        {
            Start();
            WaitAll();
            Verify();
        }
         * */


        public void Start()
        {
            for (int nt = 0; nt < nthreads; nt++)
            {
                SlaveThread st = new SlaveThread();
                st.parent = this;
                st.threadnum = nt;
                st.start();
                sthreads.Add(st);
            }
        }


        public Exception GetLastThreadException()
        {
            Exception result = slavethreadprocexception;
            slavethreadprocexception = null;
            return result;
        }


        public void Verify()
        {
            Exception e = GetLastThreadException();
            if (null != e)
            {
                throw e;
            }
        }


        public void WaitAll()
        {
            foreach (SlaveThread st in sthreads)
            {
                if (null == st)
                {
                    throw new SlaveMemoryException("Thread not running");
                }
                st.thread.Join();
            }
        }


        protected SlaveProcess(string[] mainargs)
            : this(mainargs[1], long.Parse(mainargs[2]), int.Parse(mainargs[3]), int.Parse(mainargs[4]), int.Parse(mainargs[5]), int.Parse(mainargs[6]),
                ((mainargs.Length > 7) ? mainargs[7] : ""))
        {
        }

        protected SlaveProcess(string kbasename, long blocksize, int nthreads, int packetsize, int slavenum, int mainpid, string flags)
        {
            this.kbasename = kbasename;
            this.blocksize = blocksize;
            this.nthreads = nthreads;
            this.packetsize = packetsize;
            this.slavenum = slavenum;
            //this.mainpid = mainpid;
            this.mainproc = System.Diagnostics.Process.GetProcessById(mainpid);
            this.flags = flags;

            usereturnevent = -1 != flags.IndexOf('r');

            this.block = new LongByteArray(blocksize);

            this.sthreads = new List<SlaveThread>(nthreads);
        }


        internal class SlaveThread
        {
            internal SlaveProcess parent;
            internal int threadnum;
            string kname;

            IntPtr hmap = IntPtr.Zero;
            IntPtr pview;
            System.Threading.EventWaitHandle ewh;
            System.Threading.EventWaitHandle ewhreturn; // Null if using spinlock (SPINWAIT_RETURN).
            internal System.Threading.Thread thread;


            internal void signalreturnevent()
            {
                ewhreturn.Set();
            }


            internal void start()
            {
                kname = parent.kbasename + "_thread" + threadnum.ToString();

                hmap = SlaveMemory.CreateFileMapping(SlaveMemory.INVALID_HANDLE_VALUE, IntPtr.Zero, SlaveMemory.PAGE_READWRITE, 0, 1 + 8 + 4 + parent.packetsize, kname + "_block" + parent.slavenum.ToString());
                if (IntPtr.Zero == hmap)
                {
                    throw new SlaveMemoryException("Unable to opeb shared memory communication buffer");
                }

                pview = SlaveMemory.MapViewOfFile(hmap, SlaveMemory.FILE_MAP_ALL_ACCESS, 0, 0, 0);
                if (IntPtr.Zero == pview)
                {
                    SlaveMemory.CloseHandle(hmap);
                    hmap = IntPtr.Zero;
                    throw new SlaveMemoryException("Unable to map shared memory communication buffer");
                }

                this.ewh = System.Threading.EventWaitHandle.OpenExisting(kname + "_event" + parent.slavenum.ToString());
                if (parent.usereturnevent)
                {
                    this.ewhreturn = System.Threading.EventWaitHandle.OpenExisting(kname + "_returnevent" + parent.slavenum.ToString());
                }

                this.thread = new System.Threading.Thread(new System.Threading.ThreadStart(slavethreadproc));
                this.thread.Start();
            }


            void _clean()
            {
                if (IntPtr.Zero != hmap)
                {
                    SlaveMemory.UnmapViewOfFile(pview);
                    SlaveMemory.CloseHandle(hmap);
                    hmap = IntPtr.Zero;
                }
            }

            public void Dispose()
            {
                _clean();
                System.GC.SuppressFinalize(this);
            }


            ~SlaveThread()
            {
                _clean();
            }


            internal void slavethreadproc()
            {
                try
                {
                    unsafe
                    {
                        byte* com = (byte*)pview;

                        LongByteArray block = parent.block;

                        long offset = 0;
                        int length = 0;
                        for (; ; )
                        {
                            if (!ewh.WaitOne(1000, true))
                            {
                                if (parent.mainproc.WaitForExit(1))
                                {
                                    //throw new SlaveMemoryException("Slave process exiting because main process exited");
                                    break;
                                }
                                continue;
                            }
                            byte action = com[0];
                            switch (action)
                            {
                                case 1: // Read
                                case 3: // Batch read
                                    {
                                        byte* comnext = com;
                                        for (; ; )
                                        {
                                            offset = SlaveMemory.BytesToInt64(comnext, 1);
                                            length = SlaveMemory.BytesToInt32(comnext, 1 + 8);
                                            int pos = 1 + 8 + 4;
                                            long stop = offset + length;
                                            if (stop > block.LongLength)
                                            {
                                                throw new SlaveMemoryException("Read out of bounds (offset=" + offset.ToString() + ";length=" + length.ToString() + ")");
                                            }
                                            if (length > parent.packetsize)
                                            {
                                                throw new SlaveMemoryException("Read out of bounds (length>packetsize; length=" + length.ToString() + ")");
                                            }
                                            while (offset < stop)
                                            {
                                                comnext[pos++] = block[offset++];
                                            }
                                            if (3 == action)
                                            {
                                                comnext += 1 + 8 + 4 + length;
                                                if (action == comnext[0])
                                                {
                                                    continue;
                                                }
                                            }
                                            break;
                                        }
                                        com[0] = 255; // Done!
                                        if (parent.usereturnevent)
                                        {
                                            signalreturnevent();
                                        }
                                    }
                                    break;

                                case 2: // Write
                                case 4: // Batch write
                                    {
                                        byte* comnext = com;
                                        for (; ; )
                                        {
                                            offset = SlaveMemory.BytesToInt64(comnext, 1);
                                            length = SlaveMemory.BytesToInt32(comnext, 1 + 8);
                                            int pos = 1 + 8 + 4;
                                            long stop = offset + length;
                                            if (stop > block.LongLength)
                                            {
                                                throw new SlaveMemoryException("Write out of bounds (offset=" + offset.ToString() + ";length=" + length.ToString() + ")");
                                            }
                                            if (length > parent.packetsize)
                                            {
                                                throw new SlaveMemoryException("Write out of bounds (length>packetsize; length=" + length.ToString() + ")");
                                            }
                                            while (offset < stop)
                                            {
                                                block[offset++] = comnext[pos++];
                                            }
                                            if (4 == action)
                                            {
                                                comnext += 1 + 8 + 4 + length;
                                                if (action == comnext[0])
                                                {
                                                    continue;
                                                }
                                            }
                                            break;
                                        }
                                        com[0] = 255; // Done!
                                        if (parent.usereturnevent)
                                        {
                                            signalreturnevent();
                                        }
                                    }
                                    break;

                                case 0: // Exit
                                    com[0] = 255; // Done.
                                    if (parent.usereturnevent)
                                    {
                                        signalreturnevent();
                                    }
                                    return; // !
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (null != parent.slavethreadprocexception)
                    {
                        //e.InnerException = parent.slavethreadprocexception;
                        //parent.slavethreadprocexception = e;
                    }
                    else
                    {
                        parent.slavethreadprocexception = e;
                    }
                }
            }

        }


        internal string kbasename;
        internal long blocksize;
        internal int nthreads;
        internal int packetsize;
        internal int slavenum;
        internal Exception slavethreadprocexception = null;
        List<SlaveThread> sthreads;
        //int mainpid;
        System.Diagnostics.Process mainproc;
        string flags;
        bool usereturnevent = false;

        LongByteArray block;

    }

}

