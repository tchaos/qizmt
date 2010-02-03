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

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{
    public class RemotePart : ArrayComboListPart
    {
        protected RemotePart()
            : base(1, 1, 1, false)
        {
        }

        public static RemotePart Create()
        {
            return new RemotePart();
        }


        IRemote rem = null;

        protected override void ProcessCommand(System.Net.Sockets.NetworkStream nstm, char tag)
        {
            int len;

            switch (tag)
            {
                case 'R': // Remote!
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("remote");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("remote");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            System.IO.FileStream stm = System.IO.File.Create(dllfn);
                            stm.Write(buf, 0, len);
                            stm.Close();
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IRemote plugin named " + xclassname + " for remote: " + dllfn);
                        }

                        rem = LoadRemotePlugin(dllfn, classname);
#if DEBUG
                        try
                        {
                            rem.OnRemote();
                        }
                        catch (Exception e)
                        {
                            throw new UserException(e);
                        }
#else
                        rem.OnRemote();
#endif
                    }
                    break;

                case 'O': //Query DGlobals
                    {
                        int byteCount = DGlobalsM.ToBytes(ref buf);
                        XContent.SendXContent(nstm, buf, byteCount);
                    }
                    break;

                case 'r':
                    {                      
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        int n = Entry.BytesToInt(buf);
                        int count = 0;
                        if (null != rem)
                        {
                            List<long> appendsizes = new List<long>();
                            try
                            {
                                count = rem.GetOutputFileCount(n, appendsizes);
                            }
                            catch(Exception e)
                            {
                                throw new DistributedObjectsSlave.DistObjectAbortException(e);
                            }
                            
                            if (buf.Length < 4 + 8 * appendsizes.Count)
                            {
                                buf = new byte[Entry.Round2Power(4 + 8 * appendsizes.Count)];
                            }
                            Entry.ToBytes(count, buf, 0);
                            int offset = 4;
                            for (int i = 0; i < appendsizes.Count; i++, offset += 8)
                            {
                                Entry.LongToBytes(appendsizes[i], buf, offset);
                            }
                            XContent.SendXContent(nstm, buf, 4 + 8 * appendsizes.Count);
                            break; // !
                        }
                        Entry.ToBytes(count, buf, 0);
                        XContent.SendXContent(nstm, buf, 4);
                    }
                    break;

                default:
                    base.ProcessCommand(nstm, tag);
                    break;
            }
        }

        protected internal IRemote LoadRemotePlugin(string dllfilepath, string classname)
        {
            try
            {
                MySpace.DataMining.DistributedObjects.IRemote plugin = null;
                System.Reflection.Assembly assembly = System.Reflection.Assembly.LoadFrom(dllfilepath);
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface("MySpace.DataMining.DistributedObjects.IRemote") != null)
                            {
                                plugin = (MySpace.DataMining.DistributedObjects.IRemote)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new Exception("Class " + classname + " was found, but does not implement interface IRemote");
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new Exception("Plugin from '" + dllfilepath + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.ToString();
                }
                throw new Exception("ReflectionTypeLoadException error(s) with plugin '" + dllfilepath + "': " + x + "  [Note: ensure DLL was linked against distributed IRemote (IMapReduce.dll), not single-machine]");
            }
        }


    }

}

