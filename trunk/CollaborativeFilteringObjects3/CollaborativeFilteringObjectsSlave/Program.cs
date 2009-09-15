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

using MySpace.DataMining.CollaborativeFilteringObjects3;


namespace CollaborativeFilteringObjectsSlave
{
    class Program
    {
        static void Main(string[] args)
        {

            {
                string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
					 if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                {
                    if (!System.IO.File.Exists("nosleep.txt"))
                    {
                        System.Threading.Thread.Sleep(1000 * 8);
                        int i32 = 1 + 32;
                    }
                }
            }

            try
            {
                SlaveProcess sp = SlaveProcess.IsSlaveProcess(args);
                if (null == sp)
                {
                    Console.Error.WriteLine("CollaborativeFilteringObjects Sub Process needs to be started by CollaborativeFilteringObjects");
                    return;
                }

                sp.Start();
                sp.WaitAll();
                sp.Verify();
            }
            catch (Exception e)
            {
                System.IO.File.AppendAllText("CollaborativeFilteringObjectsSlave.errors.txt", "[" + System.DateTime.Now.ToString() + "] " + e.ToString() + "\r\n--------------------------------\r\n");
            }

        }

    }

}

