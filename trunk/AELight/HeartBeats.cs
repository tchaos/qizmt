//#define HEARTBEAT_DEBUG

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        internal class HeartBeats
        {
            internal static Dictionary<string, int> rogueHosts = null;
            internal static Dictionary<string, long> heartBeats = null;            
            static System.Threading.Thread[] receivethds = null;
            static System.Threading.Thread monitorthd = null;
            static Dictionary<string, System.Net.Sockets.NetworkStream> hostToNetsms = null;
            static bool stop = false;
            static byte[] buf = null;

            //timeout=frequency of heartbeat
            //retries=how many retries slave should do before sending a heartbeat
            //expired=when does a heartbeat expired
            internal static void Start(string[] _hosts, int timeout, int retries, int expired)
            {
                rogueHosts = new Dictionary<string, int>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());
                heartBeats = new Dictionary<string, long>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());
                hostToNetsms = new Dictionary<string, System.Net.Sockets.NetworkStream>(_hosts.Length, new Surrogate.CaseInsensitiveEqualityComparer());

                string[] hosts = new string[_hosts.Length];
                for (int i = 0; i < _hosts.Length; i++)
                {
                    hosts[i] = _hosts[i].ToLower();
                }

                receivethds = new System.Threading.Thread[hosts.Length];

#if HEARTBEAT_DEBUG
                Log("Heartbeat started");
#endif

                buf = new byte[4 + 4];
                Entry.ToBytes(timeout, buf, 0);
                Entry.ToBytes(retries, buf, 4);

                for (int i = 0; i < hosts.Length; i++)
                {
                    hosts[i] = hosts[i].ToLower();
                    string host = hosts[i];
                    heartBeats[host] = 0;

                    System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                    {
                        try
                        {
                            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                            sock.Connect(host, 55900);
                            System.Net.Sockets.NetworkStream netsm = new XNetworkStream(sock);
                            lock (hostToNetsms)
                            {
                                hostToNetsms[host] = netsm;
                            }

                            netsm.WriteByte((byte)'h'); // start heartbeat
                            XContent.SendXContent(netsm, buf);

                            while(!stop)
                            {
                                int ib = netsm.ReadByte();
                                if (ib == -1)
                                {
                                    break;
                                }
                                lock (heartBeats)
                                {
                                    heartBeats[host] = DateTime.Now.Ticks;
                                }
                            }
                        }
                        catch(Exception e)
                        {
                            LogOutputToFile("HeartBeat receive thread exception:" + e.ToString());
                        }          
                    }));
                    receivethds[i] = thd;
                    thd.Priority = System.Threading.ThreadPriority.Highest;
                    thd.IsBackground = true;
                    thd.Start();
                }

                System.Threading.Thread.Sleep(timeout * 2); //give some time for receivethds to start.

                //monitor
                monitorthd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                {
                    //1 ms = 10000 ticks
                    long maxdiff = expired * 10000;
                    while (!stop)
                    {
#if HEARTBEAT_DEBUG
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
#if HEARTBEAT_DEBUG
                                    Log("roguehost found:" + host + "; " + (tsnow - heartBeats[host]).ToString());
#endif

                                    lock (rogueHosts)
                                    {
                                        rogueHosts[host] = 0;
                                    }
                                }
                            }
                        }

                        System.Threading.Thread.Sleep(timeout); //gives time for heartbeat to catch up.
                    }
                }));
                monitorthd.Priority = System.Threading.ThreadPriority.Highest;
                monitorthd.IsBackground = true;
                monitorthd.Start();
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
                                netsm.WriteByte((byte)'s'); //Stop heartbeat
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

#if HEARTBEAT_DEBUG
            internal static void Log(string msg)
            {
                string tempfile = @"c:\temp\heartbeat_68D91606-1916-49f4-B940-70DBAFDE6A55.txt";
                lock (typeof(HeartBeats))
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
