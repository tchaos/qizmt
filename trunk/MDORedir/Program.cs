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

namespace MDORedir
{
    class Program
    {
        static void Main(string[] args)
        {
            string exedir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            string sargs = "";
            {
                StringBuilder sbargs = new StringBuilder(1000);
                for (int i = 0; i < args.Length; i++)
                {
                    if (0 != sbargs.Length)
                    {
                        sbargs.Append(' ');
                    }
                    if (-1 != args[i].IndexOf(' '))
                    {
                        sbargs.Append('"');
                        sbargs.Append(args[i].Replace("\"", "\\\""));
                        sbargs.Append('"');
                    }
                    else
                    {
                        sbargs.Append(args[i].Replace("\"", "\\\""));
                    }
                }
                sargs = sbargs.ToString();
            }

            System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(exedir + @"\dspace.exe", sargs);
            psi.UseShellExecute = false;
            psi.EnvironmentVariables.Add("MDORedir", System.Diagnostics.Process.GetCurrentProcess().Id.ToString());
            System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
            proc.WaitForExit();
            proc.Close();

        }

    }

}
