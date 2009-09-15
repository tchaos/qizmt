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
using System.IO;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects5
{
    public class SpeedTest
    {
        public static void MeasureReadWrite(ulong filesize, ref double write, ref double read)
        {
            string filename = Environment.CurrentDirectory + "\\" + GenFilename();
            write = MeasureWrite(filename, filesize);
            read = MeasureRead(filename);
            File.Delete(filename);
        }

        private static string GenFilename()
        {
            return "spTest_" + Guid.NewGuid() + ".txt";
        }

        private static byte[] GenBytes()
        {
            byte[] buf = new byte[1024];
            Random rnd = new Random();

            for (int i = 0; i < buf.Length; i++)
            {
                buf[i] = (byte)rnd.Next(1, 256);
            }

            return buf;
        }

        private static void GenFile(string filename, ulong filesize)
        {
            MeasureWrite(filename, filesize);
        }

        private static double MeasureWrite(string filename, ulong filesize)
        {
            ulong bytesWritten = 0;
            byte[] buf = GenBytes();

            using (BinaryWriter writer = new BinaryWriter(File.Create(filename)))
            {
                DateTime start = DateTime.Now;

                while (bytesWritten < filesize)
                {
                    writer.Write(buf);
                    bytesWritten += (ulong)buf.Length;
                }

                double sec = (DateTime.Now - start).TotalSeconds;

                writer.Close();

                return ((double)bytesWritten / (1024d * 1024d)) / sec;
            }
        }

        private static double MeasureRead(string filename)
        {
            ulong bytesRead = 0;

            using (BinaryReader reader = new BinaryReader(File.Open(filename, FileMode.Open)))
            {
                DateTime start = DateTime.Now;

                for (; ; )
                {
                    byte[] buf = reader.ReadBytes(1024);

                    if (buf.Length == 0)
                    {
                        break;
                    }

                    bytesRead += (ulong)buf.Length;
                }

                double sec = (DateTime.Now - start).TotalSeconds;

                reader.Close();

                return ((double)bytesRead / (1024d * 1024d)) / sec;
            }
        }

        public static void MeasureNetworkSpeed(string targetNetPath, ulong filesize, ref double download, ref double upload)
        {
            string filename = GenFilename();
            string sFile = Environment.CurrentDirectory + "\\" + filename;           
            string tFile = targetNetPath + "\\" + filename;
            ulong bytesUpload = filesize;

            //Generate file locally.
            GenFile(sFile, bytesUpload);

            //Upload
            DateTime start = DateTime.Now;           
            System.IO.File.Copy(sFile, tFile);
            double sec = (DateTime.Now - start).TotalSeconds;
            upload = ((double)bytesUpload / (1024d * 1024d)) / sec;

            //Download
            string sFile2 = Environment.CurrentDirectory + "\\" + GenFilename();
            start = DateTime.Now;
            System.IO.File.Copy(tFile, sFile2);
            sec = (DateTime.Now - start).TotalSeconds;
            download = ((double)bytesUpload / (1024d * 1024d)) / sec;

            System.IO.File.Delete(sFile);
            System.IO.File.Delete(sFile2);
            System.IO.File.Delete(tFile);
        }
    }
}
