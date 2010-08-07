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
using System.Text;


namespace MySpace.DataMining.DistributedObjects
{

    public class IOUtils
    {
        public static string SafeTextPath(string s)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char ch in s)
            {
                if (sb.Length >= 150)
                {
                    sb.Append('`');
                    sb.Append(s.GetHashCode());
                    break;
                }
                if ('.' == ch)
                {
                    if (0 == ch)
                    {
                        sb.Append("%2E");
                        continue;
                    }
                }
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch)
                {
                    sb.Append('%');
                    if (ch > 0xFF)
                    {
                        sb.Append('u');
                        sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                    }
                    else
                    {
                        sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (0 == sb.Length)
            {
                return "_";
            }
            return sb.ToString();
        }

        private static string tempdir = null;
        public static string GetTempDirectory()
        {
            if (tempdir == null)
            {
                tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\usertemp";
                if (!System.IO.Directory.Exists(tempdir))
                {
                    System.IO.Directory.CreateDirectory(tempdir);
                }
            }
            return tempdir;
        }


        private static Random _rrt = new Random(unchecked(
            System.Threading.Thread.CurrentThread.ManagedThreadId
            + DateTime.Now.Millisecond));
        public static int RealRetryTimeout(int timeout)
        {
            if (timeout <= 3)
            {
                return timeout;
            }
            lock (_rrt)
            {
                return _rrt.Next(timeout / 4, timeout + 1);
            }
        }

    }


    public class NetUtils
    {

        public class ActiveConnection
        {
            public string Protocol;
            public string LocalAddress;
            public int LocalPort;
            public string ForeignAddress;
            public int ForeignPort;
            public string State;

            public override string ToString()
            {
                string slp = "";
                if (LocalPort >= 0)
                {
                    slp = ":" + LocalPort;
                }
                string sfp = "";
                if (ForeignPort >= 0)
                {
                    sfp = ":" + ForeignPort;
                }
                return Protocol
                    + "\t" + LocalAddress + slp
                    + "\t" + ForeignAddress + sfp
                    + "\t" + State;
            }

            public override int GetHashCode()
            {
                return unchecked(Protocol.GetHashCode()
                    + LocalAddress.GetHashCode()
                    + LocalPort.GetHashCode()
                    + ForeignAddress.GetHashCode()
                    + ForeignPort.GetHashCode()
                    + State.GetHashCode());

            }

            public override bool Equals(object obj)
            {
                ActiveConnection ac = obj as ActiveConnection;
                if (null == ac)
                {
                    return false;
                }
                return Equals(ac);
            }

            public bool Equals(ActiveConnection that)
            {
                return this.Protocol == that.Protocol
                    && this.LocalAddress == that.LocalAddress
                    && this.LocalPort == that.LocalPort
                    && this.ForeignAddress == that.ForeignAddress
                    && this.ForeignPort == that.ForeignPort
                    && this.State == that.State;
            }

        }

        public static ActiveConnection[] GetActiveConnections()
        {
            List<ActiveConnection> results = new List<ActiveConnection>();
            string[] lines = Exec.Shell("netstat -n").Split(new char[] { '\r', '\n' },
                StringSplitOptions.RemoveEmptyEntries);
            char[] sp = new char[] { ' ', '\t' };
            bool StartedActiveConnections = false;
            bool StartedProtoHeader = false;
            foreach (string line in lines)
            {
                if (!StartedActiveConnections)
                {
                    if (line.StartsWith("Active Connections"))
                    {
                        StartedActiveConnections = true;
                    }
                }
                else if (!StartedProtoHeader)
                {
                    string tline = line.Trim();
                    if (tline.StartsWith("Proto"))
                    {
                        StartedProtoHeader = true;
                    }
                }
                else
                {
                    if (0 == line.Length
                        || (' ' != line[0] && '\t' != line[0]))
                    {
                        break;
                    }
                    string[] parts = line.Split(sp, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 4)
                    {
                        break;
                    }
                    ActiveConnection ac = new ActiveConnection();
                    ac.Protocol = parts[0];
                    GetAddressParts(parts[1], out ac.LocalAddress, out ac.LocalPort);
                    GetAddressParts(parts[2], out ac.ForeignAddress, out ac.ForeignPort);
                    ac.State = parts[3];
                    results.Add(ac);
                }

            }
            return results.ToArray();
        }


        public static void GetAddressParts(string input, out string host, out int port)
        {
            int ilc = input.LastIndexOf(':');
            string sport = "";
            if (-1 != ilc)
            {
                host = input.Substring(0, ilc);
                sport = input.Substring(ilc + 1);
            }
            else
            {
                host = input;
            }
            if (!int.TryParse(sport, out port))
            {
                port = -1;
            }
        }

    }


    public class MemoryUtils
    {
        static int _deffbsz = 0;

        public static int DefaultFileBufferSize
        {
            get
            {
                return 0x1000;
            }
        }


        static int _ncpus = 0;

        public static int NumberOfProcessors
        {
            get
            {
                if (_ncpus < 1)
                {
                    try
                    {
                        _ncpus = int.Parse(Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"));
                    }
                    catch
                    {
                        _ncpus = 1;
                    }
                }
                return _ncpus;
            }
        }
    }


    public class CommandUtils
    {
        public static long ParseLongByteSize(string capacity)
        {
            try
            {
                if (null == capacity || 0 == capacity.Length)
                {
                    throw new FormatException("Invalid capacity: capacity not specified");
                }
                if ('-' == capacity[0])
                {
                    throw new FormatException("Invalid capacity: negative");
                }
                checked
                {
                    switch (capacity[capacity.Length - 1])
                    {
                        case 'B':
                            if (1 == capacity.Length)
                            {
                                throw new FormatException("Invalid capacity: " + capacity);
                            }
                            switch (capacity[capacity.Length - 2])
                            {
                                case 'K': // KB
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024);

                                case 'M': // MB
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024);

                                case 'G': // GB
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024);

                                case 'T': // TB
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024);

                                case 'P': // PB
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024 * 1024);

                                default: // Just bytes with B suffix.
                                    return (long)Math.Ceiling(Decimal.Parse(capacity.Substring(0, capacity.Length - 1)));
                            }
                        //break;

                        default: // Assume just bytes without a suffix.
                            return (long)Math.Ceiling(Decimal.Parse(capacity));
                    }
                }
            }
            catch (FormatException e)
            {
                throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.Message, e);
            }
            catch (OverflowException e)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.Message, e);
            }
        }

        public static int ParseIntByteSize(string capacity)
        {
            long result = ParseLongByteSize(capacity);
            if (result > int.MaxValue)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: out of range, must be less than 2GB");
            }
            return (int)result;
        }

    }


    public class MapReduceUtils
    {
        internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "do5mrlog");

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
                System.IO.StreamWriter fstm = System.IO.File.AppendText("do5mapreduce.txt");
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

}
