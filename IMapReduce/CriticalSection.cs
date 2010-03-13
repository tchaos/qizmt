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

using MySpace.DataMining.AELight;

namespace MySpace.DataMining.DistributedObjects
{   
    internal class CriticalSection
    {
        private System.Net.Sockets.Socket sock = null;
        private System.Net.Sockets.NetworkStream ntsm = null;
        private bool hasLock = false;
        private static CriticalSection currentSection = null;
        private byte[] locknamebytes = null;

#if DEBUG
        private static string logfile = @"C:\temp\log_" + StaticGlobals.ExecutionContext + Guid.NewGuid().ToString() + ".txt";
#endif

        public static CriticalSection Create()
        {
            return Create(null);
        }

        public static CriticalSection Create(string lockname)
        {
            if (null == lockname)
            {
                if (currentSection != null)
                {
                    return currentSection;
                }
            }
            else
            {
                if (0 == lockname.Length)
                {
                    throw new Exception("Empty lock name is invalid");
                }
            }

            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            System.Net.Sockets.NetworkStream ntsm = null;
            try
            {
                sock.Connect(Surrogate.MasterHost, 55900);
                ntsm = new XNetworkStream(sock);
                ntsm.WriteByte((byte)'J'); //hand shake with surrogate
                if (ntsm.ReadByte() == (byte)'+')
                {
                    CriticalSection sec = new CriticalSection();
                    sec.sock = sock;
                    sec.ntsm = ntsm;
                    sec.hasLock = false;
                    if (null == lockname)
                    {
                        currentSection = sec;
                    }
                    else
                    {
                        sec.locknamebytes = Encoding.UTF8.GetBytes(lockname);
                    }
                    return sec;
                }
                else
                {
                    throw new Exception("CriticalSection.Create() handshake failed.");
                }     
            }
            catch
            {
                if (ntsm != null)
                {
                    ntsm.Close();
                    ntsm = null;
                }
                sock.Close();
                sock = null;
                throw;
            }
        }

        void _open()
        {
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
               System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            System.Net.Sockets.NetworkStream ntsm = null;
            try
            {
                sock.Connect(Surrogate.MasterHost, 55900);
                ntsm = new XNetworkStream(sock);
                ntsm.WriteByte((byte)'J'); //hand shake with surrogate
                if (ntsm.ReadByte() == (byte)'+')
                {
                    this.sock = sock;
                    this.ntsm = ntsm;
                }
                else
                {
                    throw new Exception("CriticalSection.Create() handshake failed.");
                }
            }
            catch
            {
                if (ntsm != null)
                {
                    ntsm.Close();
                    ntsm = null;
                }
                sock.Close();
                sock = null;
                throw;
            }
        }

        void _reopen()
        {
            try
            {
                ntsm.Close();
                ntsm = null;
                sock.Close();
                sock = null;
            }
            catch
            {
            }
            hasLock = false;
            _open();
        }

        public void ReleaseLock()
        {
            if (ntsm == null)
            {
                throw new Exception("CriticalSection.ReleaseLock() failed:  Network stream has already been closed due to previous exceptions.");
            }
            if (!hasLock)
            {
                throw new Exception("CriticalSection.ReleaseLock() failed:  The current critical section does not own the lock.");
            }
            try
            {
                ntsm.WriteByte((byte)'+');  //release lock
                if (ntsm.ReadByte() == (int)'+')
                {
                    hasLock = false;
                }
                else
                {
                    throw new Exception("CriticalSection.ReleaseLock() handshake failed.");
                }
            }
            catch
            {
                if (object.ReferenceEquals(this, currentSection))
                {
                    currentSection = null;
                }
                ntsm.Close();
                ntsm = null;
                sock.Close();
                sock = null;
                throw;
            }
        }

        public bool EnterLock()
        {
            if (ntsm == null)
            {
                throw new Exception("CriticalSection.EnterLock() failed:  Network stream has already been closed due to previous exceptions.");
            }
            try
            {
                // acquire lock
                for (int iacq = 0; ; )
                {
                    try
                    {
                        if (null == locknamebytes)
                        {
                            ntsm.WriteByte((byte)'Q');
                        }
                        else
                        {
                            ntsm.WriteByte(128 + (byte)'Q'); // 128+Q
                            XContent.SendXContent(ntsm, locknamebytes);
                        }
                    }
                    catch
                    {
                        if (++iacq > 60 / 5)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(1000 * 5);
                        _reopen();
                        continue;
                    }
                    break;
                }
                if (hasLock)
                {
                    throw new Exception("CriticalSection.EnterLock() failed:  cannot enter lock twice");
                }
                if (ntsm.ReadByte() == (int)'+')
                {
                    hasLock = true;
                    return true;
                }
                else
                {
                    throw new Exception("CriticalSection.EnterLock() handshake failed.");
                }
            }
            catch
            {
                if (object.ReferenceEquals(this, currentSection))
                {
                    currentSection = null;
                }
                ntsm.Close();
                ntsm = null;
                sock.Close();
                sock = null;
                throw;
            }
        }

        ~CriticalSection()
        {
            if (ntsm != null)
            {               
                ntsm.Close();
                ntsm = null;
            }
            if (sock != null)
            {
                sock.Close();
                sock = null;
            }
        }
    }

    public class GlobalCriticalSection: IDisposable
    {
        private CriticalSection cs = null;

        public static IDisposable GetLock()
        {
            CriticalSection _cs = CriticalSection.Create();
            _cs.EnterLock();

            GlobalCriticalSection gs = new GlobalCriticalSection();
            gs.cs = _cs;
            return gs;
        }

        public static IDisposable GetLock_internal(string x)
        {
            CriticalSection _cs = CriticalSection.Create(x);
            _cs.EnterLock();

            GlobalCriticalSection gs = new GlobalCriticalSection();
            gs.cs = _cs;
            return gs;
        }

        public void Dispose()
        {
            if (cs != null)
            {
                cs.ReleaseLock();
                cs = null;
            }            
        }
    }
}