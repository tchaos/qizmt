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


using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;

namespace MySpace.DataMining.DistributedObjects5
{
    public class XContent
    {
#if CAP_NETWORK_BUFFERS
        const int NETWORK_CAP_SIZE = 0x400 * 64;
#endif


        public static string IntHexString(Int64 x)
        {
            return x.ToString("x16");
        }

        public static byte[] IntHexBytes(Int64 x)
        {
            string sx = IntHexString(x);
            return Encoding.ASCII.GetBytes(sx);
        }


        public static void SendXContent(NetworkStream nstm, byte[] content, int length)
        {
            byte[] balen = IntHexBytes(length);
            nstm.Write(balen, 0, balen.Length);

#if CAP_NETWORK_BUFFERS
            int offset = 0;
            while (length - offset > 0)
            {
                int caplength = length - offset;
                if (caplength > NETWORK_CAP_SIZE)
                {
                    caplength = NETWORK_CAP_SIZE;
                }
                nstm.Write(content, offset, caplength);
                offset += caplength;
            }
#else
            nstm.Write(content, 0, length);
#endif
        }

        public static void SendXContent(NetworkStream nstm, byte[] content)
        {
            //byte[] balen = IntHexBytes(content.LongLength);
            byte[] balen = IntHexBytes(content.Length); // Don't use LongLength here because nstm.Write only uses int anyway.
            nstm.Write(balen, 0, balen.Length);

            nstm.Write(content, 0, content.Length);
        }

        public static void SendXContent(NetworkStream nstm, string content)
        {
            SendXContent(nstm, Encoding.UTF8.GetBytes(content));
        }


        public static long ReceiveXLength(NetworkStream nstm, byte[] buffer)
        {
            if (null == buffer || buffer.Length < 16)
                buffer = new byte[16];
            int offset = 0;
            while (offset < 16)
            {
                int i = nstm.Read(buffer, offset, 16 - offset);
                if (i <= 0)
                    throw new Exception("Unable to receive XLength");
                offset += i;
            }
            string slen = Encoding.ASCII.GetString(buffer, 0, 16); // ToDo: prevent string alloc.
            try
            {
                return long.Parse(slen, System.Globalization.NumberStyles.HexNumber);
            }
            catch (FormatException e)
            {
                throw new FormatException("Expected 16 hex digits, got \"" + slen + "\"", e);
            }
        }


        // XBytesCount is updated with the number of valid bytes in the return.
        // Note that buffer isn't always used, such as when the length is larger.
        public static byte[] ReceiveXBytes(NetworkStream nstm, out int XBytesCount, byte[] buffer)
        {
            int initbuflen = (null == buffer) ? -1 : buffer.Length;
            int len = (int)ReceiveXLength(nstm, buffer);
            XBytesCount = len;
            if (null == buffer || buffer.Length < len)
            {
                buffer = new byte[len];
            }
            int offset = 0;
            while (offset < len)
            {
                int caplength = len - offset;
#if CAP_NETWORK_BUFFERS
                if (caplength > NETWORK_CAP_SIZE)
                {
                    caplength = NETWORK_CAP_SIZE;
                }
#endif
                int i = -1;
                try
                {
                    i = nstm.Read(buffer, offset, caplength);
                }
                catch (Exception e)
                {
                    string better_error = e.ToString();
                    better_error += System.Environment.NewLine;
                    better_error += "----------------------------------------------------" + System.Environment.NewLine;
                    better_error += "initbuflen: " + initbuflen.ToString() + System.Environment.NewLine;
                    better_error += "buffer.length: " + buffer.Length.ToString() + System.Environment.NewLine;
                    better_error += "offset: " + offset.ToString() + System.Environment.NewLine;
                    better_error += "caplength: " + caplength.ToString() + System.Environment.NewLine;
                    better_error += "----------------------------------------------------" + System.Environment.NewLine;
                    throw new Exception(better_error);
                }
                if (i <= 0)
                    throw new Exception("Unable to receive XContent");
                offset += i;
            }
            return buffer;
        }


        public static string ReceiveXString(NetworkStream nstm, byte[] buffer)
        {
            int len;
            buffer = ReceiveXBytes(nstm, out len, buffer);
            string result = Encoding.UTF8.GetString(buffer, 0, len);
            return result;
        }
    }


    public class XNetworkStream : NetworkStream
    {

        public XNetworkStream(Socket socket)
            : base(socket)
        {
        }

        public XNetworkStream(Socket socket, bool ownsSocket)
            : base(socket, ownsSocket)
        {
        }

        public XNetworkStream(Socket socket, System.IO.FileAccess access)
            : base(socket, access)
        {
        }

        public XNetworkStream(Socket socket, System.IO.FileAccess access, bool ownsSocket)
            : base(socket, access, ownsSocket)
        {
        }


#if DEBUG
        System.IO.FileStream readlog;

        public void EnableReadLogging(string name)
        {
            if (null != readlog)
            {
                throw new Exception("EnableReadLogging already called");
            }
            readlog = new System.IO.FileStream(name + ".readlog"
                + "." + ((System.DateTime.Now - new DateTime(2000, 1, 1, 12, 0, 0, 0, DateTimeKind.Utc)).Ticks / 10000).ToString().PadLeft(15, '0')
                + "." + System.Diagnostics.Process.GetCurrentProcess().Id.ToString()
                + "." + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() + ".log",
                System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read);
        }


        System.IO.FileStream writelog;

        public void EnableWriteLogging(string name)
        {
            if (null != writelog)
            {
                throw new Exception("EnableWriteLogging already called");
            }
            writelog = new System.IO.FileStream(name + ".writelog"
                + "." + ((System.DateTime.Now - new DateTime(2000, 1, 1, 12, 0, 0, 0, DateTimeKind.Utc)).Ticks / 10000).ToString().PadLeft(15, '0')
                + "." + System.Diagnostics.Process.GetCurrentProcess().Id.ToString()
                + "." + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() + ".log",
                System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read);
        }
#endif


        byte[] _wbyte = null, _rbyte = null;

        public override void WriteByte(byte value)
        {
            if (null == _wbyte)
            {
                _wbyte = new byte[1];
            }
            _wbyte[0] = value;
            Write(_wbyte, 0, 1);
        }

        public override int ReadByte()
        {
            if (null == _rbyte)
            {
                _rbyte = new byte[1];
            }
            if (1 == Read(_rbyte, 0, 1))
            {
                return _rbyte[0];
            }
            return -1;
        }


#if DEBUG
        public override void Write(byte[] buffer, int offset, int size)
        {
            base.Write(buffer, offset, size);
#if DEBUG
            if (null != writelog)
            {
                writelog.Write(buffer, offset, size);
                writelog.Flush();
            }
#endif
        }
#endif

#if DEBUG
        public override int Read(byte[] buffer, int offset, int size)
        {
            int result = base.Read(buffer, offset, size);
#if DEBUG
            if (null != readlog && result > 0)
            {
                readlog.Write(buffer, offset, result);
                readlog.Flush();
            }
#endif
            return result;
        }
#endif


    }

}
