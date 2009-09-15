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
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;


namespace MySpace.DataMining.DistributedObjects5
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();

            this.AfterUninstall += new InstallEventHandler(ProjectInstaller_AfterUninstall);
        }



        void ProjectInstaller_AfterUninstall(object sender, InstallEventArgs e)
        {

            // Remove from Machine PATH...
            {
                string path = Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Machine);
                if (null == path || 0 == path.Trim().Length)
                {
                }
                else
                {
                    string[] pparts = path.Split(';');
                    string addpath = (new System.IO.FileInfo(((AssemblyInstaller)Parent).Path)).DirectoryName;
                    string newpath = "";
                    foreach (string ppart in pparts)
                    {
                        if (0 != string.Compare(ppart, addpath, true))
                        {
                            if (addpath.Length > 0)
                            {
                                newpath += ";";
                            }
                            newpath += ppart;
                        }
                    }
                    Environment.SetEnvironmentVariable("PATH", newpath, EnvironmentVariableTarget.Machine);
                }
            }
        }


        private void ProjectInstaller_Committed(object sender, InstallEventArgs e)
        {

            

            // Attach to InstallUtil...
            //System.Threading.Thread.Sleep(20 * 1000);
            //int i23zz = 23 + 23;

            // Add to Machine PATH...
            {
                string path = Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Machine);
                string addpath = (new System.IO.FileInfo(((AssemblyInstaller)Parent).Path)).DirectoryName;
                string newpath = null;
                if (null == path || 0 == path.Trim().Length)
                {
                    newpath = addpath;
                }
                else
                {
                    if (-1 == (";" + path + ";").IndexOf(";" + addpath + ";", StringComparison.OrdinalIgnoreCase))
                    {
                        newpath = path + ";" + addpath;
                    }
                }
                if (null != newpath)
                {
                    Environment.SetEnvironmentVariable("PATH", newpath, EnvironmentVariableTarget.Machine);
                }
            }

            // Start the service...
            {
                System.ServiceProcess.ServiceController[] services = System.ServiceProcess.ServiceController.GetServices();
                for (int i = 0; i < services.Length; i++)
                {
                    if (services[i].DisplayName == "DistributedObjects")
                    {
                        services[i].Start(new string[] { "-installed" });
                        break;
                    }
                }
            }

        }
    }
}
