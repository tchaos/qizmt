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

//#define FAILOVER_DEBUG

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        internal class VitalsMonitor
        {   
            static string[] hosts = null;
            static int heartbeattimeout = 0;
            static int heartbeatretries = 0;
            static int heartbeatexpired = 0;
            static int tattletimeout = 0;

            static Dictionary<string, string> rogueHosts = null;
            static Dictionary<string, long> heartBeats = null;
            static Dictionary<string, System.Net.Sockets.NetworkStream> hostToNetsms = null;
            static System.Threading.Thread[] receivethds = null;
            static System.Threading.Thread monitorthd = null;            
            static bool stop = false;

            internal static void Start(string[] _hosts, int _heartbeattimeout, int _heartbeatretries, int _heartbeatexpired, int _tattletimeout)
            {                
                hosts = new string[_hosts.Length];
                for (int i = 0; i < _hosts.Length; i++)
                {
                    hosts[i] = _hosts[i].ToLower();
                }
                heartbeattimeout = _heartbeattimeout;
                heartbeatretries = _heartbeatretries;
                heartbeatexpired = _heartbeatexpired;
                tattletimeout = _tattletimeout;                

                rogueHosts = new Dictionary<string, string>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());
                heartBeats = new Dictionary<string, long>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());
                hostToNetsms = new Dictionary<string, System.Net.Sockets.NetworkStream>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());

                receivethds = new System.Threading.Thread[hosts.Length];
                for (int i = 0; i < hosts.Length; i++)
                {
                    string host = hosts[i];
                    heartBeats[host] = 0;
                    System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ReceiveThreadProc));
                    receivethds[i] = thd;
                    thd.Priority = System.Threading.ThreadPriority.Highest;
                    thd.IsBackground = true;
                    thd.Start(host);
                }

                System.Threading.Thread.Sleep(heartbeattimeout * 2); //give some time for receivethds to start.

                monitorthd = new System.Threading.Thread(new System.Threading.ThreadStart(MonitorThreadProc));
                monitorthd.Priority = System.Threading.ThreadPriority.Highest;
                monitorthd.IsBackground = true;
                monitorthd.Start();

#if FAILOVER_DEBUG
                Log("VitalMonitor started for jid:" + jid.ToString());
#endif
            }

            static void ReceiveThreadProc(object _host)
            {
                string host = (string)_host;
                byte[] buf = new byte[8 + 4 + 4 + 4]; //!
                Entry.LongToBytes(jid, buf, 0);
                Entry.ToBytes(heartbeattimeout, buf, 8);
                Entry.ToBytes(heartbeatretries, buf, 8 + 4);
                Entry.ToBytes(tattletimeout, buf, 8 + 4 + 4);

                try
                {
                    System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                    sock.Connect(host, 55900);
                    System.Net.Sockets.NetworkStream netsm = new XNetworkStream(sock);
                    lock (hostToNetsms)
                    {
                        hostToNetsms[host] = netsm;
                    }

                    netsm.WriteByte((byte)'h'); // start vitals reporter
                    XContent.SendXContent(netsm, buf);

                    while (!stop)
                    {
                        int ib = netsm.ReadByte();
                        if (ib == -1)
                        {
                            break;
                        }
                        else if (ib == 'h') //heartbeat
                        {
#if FAILOVER_DEBUG
                            Log("Heartbeat thread received a heartbeat from:" + host);
#endif
                            lock (heartBeats)
                            {
                                heartBeats[host] = DateTime.Now.Ticks;
                            }
                        }
                        else if (ib == (byte)'e') //tattled bad hosts
                        {
                            string strbhs = XContent.ReceiveXString(netsm, buf);
#if FAILOVER_DEBUG
                            Log("Heartbeat thread received tattled host from:" + host + ";tattled host=" + strbhs);
#endif
                            string[] bhs = strbhs.ToLower().Split(';');
                            lock (rogueHosts)
                            {
                                foreach (string bh in bhs)
                                {
                                    if (!rogueHosts.ContainsKey(bh))
                                    {
                                        rogueHosts.Add(bh, "Tattled by " + host);
                                    }
                                }
                            }
#if FAILOVER_DEBUG
                            Log("VitalMonitor.rogueHosts:" + string.Join(";", (new List<string>(rogueHosts.Keys).ToArray())));
#endif
                        }
                        else
                        {
#if FAILOVER_DEBUG
                            Log("VitalMonitor receiving thread obtained unknown heartbeat: " + ib.ToString() + " from host:" + host);
#endif
                            throw new Exception("VitalMonitor receiving thread obtained unknown heartbeat: " + ib.ToString() + " from host:" + host);
                        }
                    }
                }
                catch (Exception e)
                {
                    LogOutputToFile("VitalMonitor receiver thread exception:" + e.ToString());
                }
            }

            static void MonitorThreadProc()
            {                
                //1 ms = 10000 ticks
                long maxdiff = heartbeatexpired * 10000;
                while (!stop)
                {
#if FAILOVER_DEBUG
                    Log("monitor checking");
#endif
                    lock (heartBeats)
                    {
                        long tsnow = DateTime.Now.Ticks;

                        for (int i = 0; i < hosts.Length; i++)
                        {
                            string host = hosts[i];
                            if (tsnow - heartBeats[host] > maxdiff) //too many ticks have passed since the last heartbeat
                            {
#if FAILOVER_DEBUG
                                Log("monitor: roguehost found because of delayed heartbeat:" + host + "; " + (tsnow - heartBeats[host]).ToString());
#endif

                                lock (rogueHosts)
                                {
                                    rogueHosts[host] = "Delayed heartbeat";
                                }
                            }
                        }
                    }
                    System.Threading.Thread.Sleep(heartbeattimeout); //gives time for heartbeat to catch up.
                }                
            }

            internal static bool IsRogueHost(string host, out string reason)
            {
                reason = null;
                lock (rogueHosts)
                {
                    if (rogueHosts.ContainsKey(host))
                    {
                        reason = rogueHosts[host];
                        return true;
                    }
                    return false;
                }                
            }

            internal static void Stop()
            {
                stop = true;

                if (hostToNetsms != null)
                {
                    lock (hostToNetsms)
                    {
                        foreach (KeyValuePair<string, System.Net.Sockets.NetworkStream> pair in hostToNetsms)
                        {
                            System.Net.Sockets.NetworkStream netsm = pair.Value;
                            try
                            {
                                netsm.WriteByte((byte)'s'); //Stop vitals reporter
                            }
                            catch
                            {
                            }
                            finally
                            {
                                netsm.Close(1000);
                                netsm.Dispose();
                            }
                        }
                    }
                }

                if (receivethds != null)
                {
                    foreach (System.Threading.Thread thd in receivethds)
                    {
                        try
                        {
                            thd.Abort();
                        }
                        catch
                        {
                        }
                    }
                }

                if (monitorthd != null)
                {
                    try
                    {
                        monitorthd.Abort();
                    }
                    catch
                    {
                    }
                }
            }

#if FAILOVER_DEBUG
            internal static void Log(string msg)
            {
                string tempfile = @"c:\temp\vitalmonitorlog_68D91606-1916-49f4-B940-70DBAFDE6A55.txt";
                lock (typeof(VitalsMonitor))
                {
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(tempfile, true))
                    {
                        w.WriteLine("  [" + DateTime.Now.ToString() + "]");
                        w.WriteLine(msg);
                    }
                }
            }   
#endif
        }
    }
}
