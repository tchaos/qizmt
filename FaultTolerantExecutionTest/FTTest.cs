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

#define CAP_NETWORK_BUFFERS
//#define TESTFAULTTOLERANT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public class FTTest
    {
#if TESTFAULTTOLERANT
        public static bool enabled = true;
#else
        public static bool enabled = false;
#endif

        public static string controlfilename = "fttest_23014BB4-9383-406e-BD0E-49CA2E10F244.txt";
        long jobid = 0;
        Dictionary<string, int> hosttophase = new Dictionary<string, int>(50);
        Dictionary<string, int> phasetomax = new Dictionary<string, int>(50);
        System.Net.Sockets.Socket sock = null;
        System.Net.Sockets.NetworkStream netsm = null;

        public FTTest(long jid)
        {
            jobid = jid;

            //load control file
            try
            {
                string controlfile = @"\\" + Surrogate.MasterHost + @"\c$\temp\" + controlfilename;
                if (System.IO.File.Exists(controlfile))
                {
                    string[] lines = System.IO.File.ReadAllLines(controlfile);
                    foreach (string line in lines)
                    {
                        string[] parts = line.Split(':');
                        if (parts.Length >= 2)
                        {
                            string host = parts[0].ToLower();
                            string phase = parts[1].ToLower();

                            if (host[0] == '{') //wildcard breakpoint {max}
                            {
                                int max = Int32.Parse(host.Substring(1, host.Length - 2));
                                if (!phasetomax.ContainsKey(phase))
                                {
                                    phasetomax.Add(phase, max);
                                }
                            }
                            else //regular breakpoint
                            {
                                string key = host + ":" + phase;
                                if (!hosttophase.ContainsKey(key))
                                {
                                    hosttophase.Add(key, 0);
                                }
                            }
                        }
                    }
                }
            }
            catch
            {
            }

            //try
            //{
                sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                   System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                sock.Connect(Surrogate.MasterHost, 55900);
                netsm = new System.Net.Sockets.NetworkStream(sock);
            //}
            //catch
            //{
            //}                
        }

        public bool CanBreak(string host, string phase)
        {
            host = host.ToLower();
            phase = phase.ToLower();

            if (hosttophase.ContainsKey(host + ":" + phase))
            {
                return true;
            }
            else if (phasetomax.ContainsKey(phase))
            {
                int max = phasetomax[phase];

                int ib = (byte)'-';  //can't throw by default
                lock (hosttophase)
                {
                    //Call to surrogate to check if it can throw.
                    netsm.WriteByte((byte)'1'); // Fault tolerant test
                    XContent.SendXContent(netsm, jobid.ToString() + ":" + host + ":" + phase + ":" + max.ToString());
                    ib = netsm.ReadByte();                   
                }                
                if (ib == (byte)'+')
                {
                    return true;
                }
            }
            return false;
        }       

        public void BreakPoint(string host, string phase, Exception ex)
        {
            if (CanBreak(host, phase))
            {
                throw ex;
            }
        }

        public void Close()
        {
            try
            {
                if (netsm != null)
                {
                    netsm.Close();
                    netsm = null;
                }

                if (sock != null)
                {
                    sock.Close();
                    sock = null;
                }
            }
            catch
            {
            }            
        }
    }   
}
