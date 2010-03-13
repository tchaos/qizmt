using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DfsProtocolDLL
{
    public class DfsClient
    {
        System.Net.Sockets.NetworkStream netstm = null;
        byte[] buf;


        public void Connect(string surrogatehost)
        {
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            try
            {
                sock.Connect(surrogatehost, 55905);
                netstm = new XNetworkStream(sock);
                netstm.WriteByte(199);
                if (netstm.ReadByte() != 199 / 3)
                {
                    throw new Exception("DFS protocol did not respond correctly to handshake");
                }
            }
            catch
            {
                if (netstm != null)
                {
                    netstm.Close();
                    netstm = null;
                }
                sock.Close();
                sock = null;
                throw;
            }
            buf = new byte[1024 * 4];
        }


        public void Close()
        {
            if (null != netstm)
            {
                try
                {
                    netstm.WriteByte((byte)'c');
                    ensurenextplus();
                }
                catch
                {
                }
                netstm.Close();
                netstm = null;
            }
        }


        void ensureconnected()
        {
            if (null == netstm)
            {
                throw new Exception("Not connected to DFS protocol");
            }
        }

        void ensurenextplus()
        {
#if DEBUG
            ensureconnected();
#endif
            int ib = netstm.ReadByte();
            if ('+' != ib)
            {
                if ('-' == ib)
                {
                    string errmsg = XContent.ReceiveXString(netstm, null);
                    throw new Exception("Error from DFS protocol: " + errmsg);
                }
                else
                {
                    throw new Exception("DFS protocol did not return a success");
                }
            }
        }


        public string GetFileSizeString(string dfsfile)
        {
            ensureconnected();

            netstm.WriteByte((byte)'s');
            XContent.SendXContent(netstm, dfsfile);
            ensurenextplus();
            string ssize = XContent.ReceiveXString(netstm, buf);
            return ssize;
        }

        public long GetFileSize(string dfsfile)
        {
            string ssize = GetFileSizeString(dfsfile);
            long size;
            if (!long.TryParse(ssize, out size) || size < 0)
            {
                throw new FormatException("<Size> for DFS file '" + dfsfile + "' has invalid value: " + ssize);
            }
            return size;
        }


        public string GetFilePartCountString(string dfsfile)
        {
            ensureconnected();

            netstm.WriteByte((byte)'n');
            XContent.SendXContent(netstm, dfsfile);
            ensurenextplus();
            string ssize = XContent.ReceiveXString(netstm, buf);
            return ssize;
        }

        public int GetFilePartCount(string dfsfile)
        {
            string spartcount = GetFilePartCountString(dfsfile);
            int partcount;
            if (!int.TryParse(spartcount, out partcount) || partcount < 0)
            {
                throw new FormatException("Part count for DFS file '" + dfsfile + "' has invalid value: " + spartcount);
            }
            return partcount;
        }


        public byte[] GetFileContent(string dfsfile)
        {
            ensureconnected();

            netstm.WriteByte((byte)'g');
            XContent.SendXContent(netstm, dfsfile);
            ensurenextplus();
            int len;
            buf = XContent.ReceiveXBytes(netstm, out len, buf);
            byte[] content = new byte[len];
            Buffer.BlockCopy(buf, 0, content, 0, len);
            return content;
        }


        public void SetFileContent(string dfsfile, string dfsfiletype, byte[] content, int contentlength)
        {
            ensureconnected();

            netstm.WriteByte((byte)'p');
            XContent.SendXContent(netstm, dfsfile);
            XContent.SendXContent(netstm, dfsfiletype);
            XContent.SendXContent(netstm, content, contentlength);
            ensurenextplus();
        }

        public void SetFileContent(string dfsfile, string dfsfiletype, byte[] content)
        {
            SetFileContent(dfsfile, dfsfiletype, content, content.Length);
        }


        public void DeleteFile(string dfsfile)
        {
            ensureconnected();

            netstm.WriteByte((byte)'d');
            XContent.SendXContent(netstm, dfsfile);
            ensurenextplus();
        }


    }

}
