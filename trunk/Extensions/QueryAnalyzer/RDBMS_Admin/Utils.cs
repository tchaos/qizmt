using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    public class Utils
    {
        public static string[] GetQizmtHosts()
        {
            string[] lines = Exec.Shell("qizmt clusterconfigview Machine_MachineList", false).Split('\n');
            List<string> _hosts = new List<string>(lines.Length);
            for(int i = 0; i < lines.Length; i++)
            {
                string line = lines[i].Trim();
                if (line.Length > 0)
                {
                    _hosts.AddRange(line.Split(';'));
                }
            }
            return _hosts.ToArray();
        }
    }
}
