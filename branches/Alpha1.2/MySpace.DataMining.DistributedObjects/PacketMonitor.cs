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
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace MySpace.DataMining.DistributedObjects5
{
    /// <summary>
    /// A class that intercepts IP packets on a specific interface.
    /// </summary>
    /// <remarks>
    /// This class only works on Windows 2000 and higher.
    /// </remarks>
    public class PacketMonitor
    {
        private Socket m_monitor;
        private IPAddress m_ip;
        private byte[] m_buffer;
        private string m_filepath = null;
        private static object obj = new object();
       
        public PacketMonitor(string host, string dir)
        {            
            if (Environment.OSVersion.Platform != PlatformID.Win32NT || Environment.OSVersion.Version.Major < 5)
            {
                throw new NotSupportedException("This program requires Windows 2000, Windows XP or Windows .NET Server!");
            }

            string ip = null;
            System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(host);
            for (int i = 0; i < addresslist.Length; i++)
            {
                if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                {
                    string x = addresslist[i].ToString();
                    if (x.StartsWith("10."))
                    {
                        ip = x;
                        break;
                    }                   
                }
            }

            if (ip == null)
            {
                throw new Exception("No IPv4 10.* address found for " + host);
            }

            m_ip = IPAddress.Parse(ip);
            m_buffer = new byte[65535];
            m_filepath = dir + "\\packetsniff.txt";
        }
       
        ~PacketMonitor()
        {
            Stop();
        }
      
        public void Start()
        {
            if (m_monitor == null)
            {
                try
                {
                    if (System.IO.File.Exists(m_filepath))
                    {
                        System.IO.File.Delete(m_filepath);
                    }    

                    m_monitor = new Socket(AddressFamily.InterNetwork, SocketType.Raw, ProtocolType.IP);
                    m_monitor.Blocking = false;
                    m_monitor.Bind(new IPEndPoint(IP, 0));
                    m_monitor.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.HeaderIncluded, 1);
                    m_monitor.IOControl(IOControlCode.ReceiveAll, new byte[4] { 1, 0, 0, 0 }, null);
                    m_monitor.BeginReceive(m_buffer, 0, m_buffer.Length, SocketFlags.None, new AsyncCallback(this.OnReceive), null);
                }
                catch
                {
                    Stop();
                    throw;
                }
            }
        }
       
        public void Stop()
        {
            if (m_monitor != null)
            {
                m_monitor.Close();
                m_monitor = null;               
            }     
        }        
      
        private void OnReceive(IAsyncResult ar)
        {
            try
            {
                int received = m_monitor.EndReceive(ar);
                try
                {
                    if (m_monitor != null)
                    {
                        byte[] packet = new byte[received];
                        Array.Copy(Buffer, 0, packet, 0, received);
                        Packet p = new Packet(packet);
                        string line = p.Time.ToString("MM/dd/yyyy hh:mm:ss.fff tt") + "|" + p.Protocol.ToString() + "|" + p.Source + "|" + p.Destination + "|" + p.TotalLength + Environment.NewLine;

                        lock (obj)
                        {
                            System.IO.File.AppendAllText(m_filepath, line);
                        }
                        Console.WriteLine(line);
                    }
                }
                catch { } // invalid packet; ignore
                m_monitor.BeginReceive(Buffer, 0, Buffer.Length, SocketFlags.None, new AsyncCallback(this.OnReceive), null);
            }
            catch
            {
                Stop();
            }
        }
       
        public IPAddress IP
        {
            get
            {
                return m_ip;
            }
        }
      
        protected byte[] Buffer
        {
            get
            {
                return m_buffer;
            }
        }

        public enum Precedence
        {
            Routine = 0,
            Priority = 1,
            Immediate = 2,
            Flash = 3,
            FlashOverride = 4,
            CRITICECP = 5,
            InternetworkControl = 6,
            NetworkControl = 7
        }
     
        public enum Delay
        {
            NormalDelay = 0,
            LowDelay = 1
        }
       
        public enum Throughput
        {
            NormalThroughput = 0,
            HighThroughput = 1
        }
      
        public enum Reliability
        {
            NormalReliability = 0,
            HighReliability = 1
        }
     
        public enum Protocol
        {
            Ggp = 3,
            Icmp = 1,
            Idp = 22,
            Igmp = 2,
            IP = 4,
            ND = 77,
            Pup = 12,
            Tcp = 6,
            Udp = 17,
            Other = -1
        }
     
        private class Packet
        {           
            private byte[] m_Raw;
            private DateTime m_Time;
            private int m_Version;
            private int m_HeaderLength;
            private Precedence m_Precedence;
            private Delay m_Delay;
            private Throughput m_Throughput;
            private Reliability m_Reliability;
            private int m_TotalLength;
            private int m_Identification;
            private int m_TimeToLive;
            private Protocol m_Protocol;
            private byte[] m_Checksum;
            private IPAddress m_SourceAddress;
            private IPAddress m_DestinationAddress;
            private int m_SourcePort;
            private int m_DestinationPort;
            public Packet(byte[] raw) : this(raw, DateTime.Now) { }
           
            public Packet(byte[] raw, DateTime time)
            {
                if (raw == null)
                    throw new ArgumentNullException();
                if (raw.Length < 20)
                    throw new ArgumentException(); // invalid IP packet
                m_Raw = raw;
                m_Time = time;
                m_Version = (raw[0] & 0xF0) >> 4;
                m_HeaderLength = (raw[0] & 0x0F) * 4 /* sizeof(int) */;
                if ((raw[0] & 0x0F) < 5)
                    throw new ArgumentException(); // invalid header of packet
                m_Precedence = (Precedence)((raw[1] & 0xE0) >> 5);
                m_Delay = (Delay)((raw[1] & 0x10) >> 4);
                m_Throughput = (Throughput)((raw[1] & 0x8) >> 3);
                m_Reliability = (Reliability)((raw[1] & 0x4) >> 2);
                m_TotalLength = raw[2] * 256 + raw[3];
                if (m_TotalLength != raw.Length)
                    throw new ArgumentException(); // invalid size of packet
                m_Identification = raw[4] * 256 + raw[5];
                m_TimeToLive = raw[8];
                if (Enum.IsDefined(typeof(Protocol), (int)raw[9]))
                    m_Protocol = (Protocol)raw[9];
                else
                    m_Protocol = Protocol.Other;
                m_Checksum = new byte[2];
                m_Checksum[0] = raw[11];
                m_Checksum[1] = raw[10];
                m_SourceAddress = new IPAddress(BitConverter.ToUInt32(raw, 12));
                m_DestinationAddress = new IPAddress(BitConverter.ToUInt32(raw, 16));
                if (m_Protocol == Protocol.Tcp || m_Protocol == Protocol.Udp)
                {
                    m_SourcePort = raw[m_HeaderLength] * 256 + raw[m_HeaderLength + 1];
                    m_DestinationPort = raw[m_HeaderLength + 2] * 256 + raw[m_HeaderLength + 3];
                }
                else
                {
                    m_SourcePort = -1;
                    m_DestinationPort = -1;
                }
            }
           
            protected byte[] Raw
            {
                get
                {
                    return m_Raw;
                }
            }
          
            public DateTime Time
            {
                get
                {
                    return m_Time;
                }
            }
           
            public int Version
            {
                get
                {
                    return m_Version;
                }
            }
          
            public int HeaderLength
            {
                get
                {
                    return m_HeaderLength;
                }
            }
          
            public Precedence Precedence
            {
                get
                {
                    return m_Precedence;
                }
            }
           
            public Delay Delay
            {
                get
                {
                    return m_Delay;
                }
            }
           
            public Throughput Throughput
            {
                get
                {
                    return m_Throughput;
                }
            }
           
            public Reliability Reliability
            {
                get
                {
                    return m_Reliability;
                }
            }
          
            public int TotalLength
            {
                get
                {
                    return m_TotalLength;
                }
            }
           
            public int Identification
            {
                get
                {
                    return m_Identification;
                }
            }
           
            public int TimeToLive
            {
                get
                {
                    return m_TimeToLive;
                }
            }
            
            public Protocol Protocol
            {
                get
                {
                    return m_Protocol;
                }
            }
           
            public byte[] Checksum
            {
                get
                {
                    return m_Checksum;
                }
            }
            
            public IPAddress SourceAddress
            {
                get
                {
                    return m_SourceAddress;
                }
            }
            
            public IPAddress DestinationAddress
            {
                get
                {
                    return m_DestinationAddress;
                }
            }
         
            public int SourcePort
            {
                get
                {
                    return m_SourcePort;
                }
            }
          
            public int DestinationPort
            {
                get
                {
                    return m_DestinationPort;
                }
            }
           
            public string Source
            {
                get
                {
                    if (m_SourcePort != -1)
                        return SourceAddress.ToString() + ":" + m_SourcePort.ToString();
                    else
                        return SourceAddress.ToString();
                }
            }
          
            public string Destination
            {
                get
                {
                    if (m_DestinationPort != -1)
                        return DestinationAddress.ToString() + ":" + m_DestinationPort.ToString();
                    else
                        return DestinationAddress.ToString();
                }
            }
          
            public override string ToString()
            {
                return this.ToString(false);
            }
          
            public string ToString(bool raw)
            {
                StringBuilder sb = new StringBuilder(Raw.Length);
                if (raw)
                {
                    for (int i = 0; i < Raw.Length; i++)
                    {
                        if (Raw[i] > 31)
                            sb.Append((char)Raw[i]);
                        else
                            sb.Append(".");
                    }
                }
                else
                {
                    string rawString = this.ToString(true);
                    for (int i = 0; i < Raw.Length; i += 16)
                    {
                        for (int j = i; j < Raw.Length && j < i + 16; j++)
                        {
                            sb.Append(Raw[j].ToString("X2") + " ");
                        }
                        if (rawString.Length < i + 16)
                        {
                            sb.Append(' ', ((16 - (rawString.Length % 16)) % 16) * 3);
                            sb.Append(" " + rawString.Substring(i) + "\r\n");
                        }
                        else
                        {
                            sb.Append(" " + rawString.Substring(i, 16) + "\r\n");
                        }
                    }
                }
                return sb.ToString();
            }           
        }
    }
}