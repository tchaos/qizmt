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

namespace QizmtEC2ServiceInit
{
    public class Exec
    {

        public static string Shell(string line, bool suppresserrors)
        {
            System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("cmd.exe", @"/C " + line);
            psi.CreateNoWindow = true;
            //psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
            psi.UseShellExecute = false;
            psi.RedirectStandardOutput = true;
            if (!suppresserrors)
            {
                psi.RedirectStandardError = true;
            }
            string result;
            using (System.Diagnostics.Process process = System.Diagnostics.Process.Start(psi))
            {
                ShellErrInfo sei = null;
                System.Threading.Thread errthd = null;
                if (!suppresserrors)
                {
                    sei = new ShellErrInfo();
                    sei.reader = process.StandardError;
                    errthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(shellerrthreadproc));
                    errthd.Start(sei);
                }
                result = process.StandardOutput.ReadToEnd();
                process.WaitForExit();
                if (!suppresserrors)
                {
                    errthd.Join();
                    if (!string.IsNullOrEmpty(sei.err))
                    {
                        sei.err = sei.err.Trim();
                        if (sei.err.Length != 0)
                        {
                            throw new ShellException(line, sei.err);
                        }
                    }
                }
            }
            return result;
        }

        class ShellErrInfo
        {
            public string err;
            public System.IO.StreamReader reader;
        }

        static void shellerrthreadproc(object obj)
        {
            ShellErrInfo sei = (ShellErrInfo)obj;
            sei.err = sei.reader.ReadToEnd();
        }


        public static string Shell(string line)
        {
            return Shell(line, false);
        }


        public class ShellException : Exception
        {
            public ShellException(string cmd, string msg)
                : base("Shell(\"" + cmd + "\") error: " + msg)
            {
            }
        }
    }
}
