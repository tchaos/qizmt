using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace mrdebug_regressiontests
{
    public class ShellCtrl
    {
        private Process m_Process;
        private StreamWriter m_input;
        //private StreamReader m_output;
        //private StreamReader m_error;
        private ProcessStartInfo m_starInfo;
        public ShellCtrl(string sPath)
        {
            string[] sParts = sPath.Split(' ');
            string sExe = sParts[0];
            string sParam = sParts[1];
            m_starInfo = new ProcessStartInfo(sExe, sParam);
            //m_starInfo.RedirectStandardError = true;
            m_starInfo.RedirectStandardInput = true;
            //m_starInfo.RedirectStandardOutput = true;
            m_starInfo.UseShellExecute = false;

            m_Process = Process.Start(m_starInfo);
            
            m_input = m_Process.StandardInput;
            //m_output = m_Process.StandardOutput;
            //m_error = m_Process.StandardError;
        }
        public void WriteLine(string sLine)
        {
            m_input.WriteLine(sLine);
        }
    }
}