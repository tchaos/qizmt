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

namespace MySpace.DataMining.DistributedObjects5
{
    public class VitalsReporter
    {
        long jobid = 0;
        System.Net.Sockets.NetworkStream clientstm = null;        
        System.Threading.Thread heartbeatthd = null;
        System.Threading.Thread tattlethd = null;
        string heartbeattempfile = null;
        int heartbeattimeout = 0;
        int heartbeatretries = 0;
        int tattletimeout = 0;
        long tattlelastupdated = 0;
        bool badlocalhost = false;
        bool stop = false;

        public VitalsReporter(long jobID, System.Net.Sockets.NetworkStream netstm)
        {
            jobid = jobID;
            clientstm = netstm;
        }

        internal void Start(int _heartbeattimeout, int _heartbeatretries, int _tattletimeout)
        {
            heartbeattimeout = _heartbeattimeout;
            heartbeatretries = _heartbeatretries;
            tattletimeout = _tattletimeout;            
            heartbeattempfile = Environment.CurrentDirectory + @"\" + Guid.NewGuid().ToString() + ".hbt";     
          
            if (heartbeatthd != null)
            {
                try
                {
                    heartbeatthd.Abort();
                    heartbeatthd = null;
                }
                catch
                {
                }
            }
            heartbeatthd = new System.Threading.Thread(new System.Threading.ThreadStart(HeartbeatThreadProc));
            heartbeatthd.IsBackground = true;
            heartbeatthd.Priority = System.Threading.ThreadPriority.Highest;
            heartbeatthd.Start();   
            
            if (tattlethd != null)
            {
                try
                {
                    tattlethd.Abort();
                    tattlethd = null;
                }
                catch
                {
                }
            }
            tattlethd = new System.Threading.Thread(new System.Threading.ThreadStart(TattleThreadProc));
            tattlethd.IsBackground = true;
            tattlethd.Priority = System.Threading.ThreadPriority.Highest;
            tattlethd.Start();            
        }        

        void HeartbeatThreadProc()
        {
            int triesremain = heartbeatretries;
            while (!badlocalhost && !stop)
            {
                try
                {
                    using (System.IO.FileStream fs = new System.IO.FileStream(heartbeattempfile, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None))
                    {
                        fs.WriteByte((byte)'T');
                        fs.Seek(0, System.IO.SeekOrigin.Begin);
                        if ((int)'T' != fs.ReadByte())
                        {
                            throw new System.IO.IOException("Heartbeat thread data written was not read back correctly.");
                        }
                        fs.Close();
                    }
                    System.IO.File.Delete(heartbeattempfile);
                    triesremain = heartbeatretries; //reset                    

                    lock (clientstm)
                    {
                        clientstm.WriteByte((byte)'h');
                    }
#if FAILOVER_DEBUG
                    System.IO.File.AppendAllText(@"c:\temp\vitalsreporter_A5B1E053-9A32-417b-8068-91A7CD5CDEAB.txt", DateTime.Now.ToString() + " heartbeat sent" + Environment.NewLine);
#endif

                    System.Threading.Thread.Sleep(heartbeattimeout);
                }
                catch(Exception e)
                {
#if FAILOVER_DEBUG
                    System.IO.File.AppendAllText(@"c:\temp\vitalsreporter_A5B1E053-9A32-417b-8068-91A7CD5CDEAB.txt", DateTime.Now.ToString() + 
                        " heartbeat thread error: " + e.ToString() + Environment.NewLine);
#endif
                    if (--triesremain <= 0)
                    {
#if FAILOVER_DEBUG
                        System.IO.File.AppendAllText(@"c:\temp\vitalsreporter_A5B1E053-9A32-417b-8068-91A7CD5CDEAB.txt", DateTime.Now.ToString() +
                            " heartbeat thread error; tries=" + triesremain.ToString() 
                            + " ;caused to break out: " + e.ToString() + Environment.NewLine);
#endif
                        break;
                    }
                }
            }
        }

        void TattleThreadProc()
        {
            string localhost = System.Net.Dns.GetHostName();
            List<string> badhosts = new List<string>(10);
            while(!stop)
            {
                long newlastupdated = RogueHosts.Get(jobid, tattlelastupdated, badhosts);
                if (newlastupdated != tattlelastupdated) // has new tattled host
                {
                    tattlelastupdated = newlastupdated;
                    if (badhosts.Count > 0)
                    {
                        string strbhs = "";
                        for (int i = 0; i < badhosts.Count; i++)
                        {
                            if (i > 0)
                            {
                                strbhs += ";";
                            }
                            strbhs += badhosts[i];

                            if (string.Compare(badhosts[i], localhost, true) == 0)
                            {
                                badlocalhost = true;
                            }
                        }
                        lock (clientstm)
                        {
                            clientstm.WriteByte((byte)'e');
                            XContent.SendXContent(clientstm, strbhs);
                        }
#if FAILOVER_DEBUG
                        System.IO.File.AppendAllText(@"c:\temp\vitalsreporter_A5B1E053-9A32-417b-8068-91A7CD5CDEAB.txt", DateTime.Now.ToString() + " rogues sent:" + strbhs + Environment.NewLine);
#endif
                    }
                }
                System.Threading.Thread.Sleep(tattletimeout);
            }
        }

        internal void Stop()
        {
            stop = true;

            if (heartbeatthd != null)
            {
                try
                {
                    heartbeatthd.Abort();
                    heartbeatthd = null;
                    System.IO.File.Delete(heartbeattempfile);
                }
                catch
                {
                }
            }

            if (tattlethd != null)
            {
                try
                {
                    tattlethd.Abort();
                    tattlethd = null;
                }
                catch
                {
                }
            }
            RogueHosts.Clear(jobid);
        }
    }
}
