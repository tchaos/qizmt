
#if DEBUG
//#define DEBUG_TEST
#endif

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Windows.Forms;

namespace QueryAnalyzer_ADOTrafficMonitor
{
    public partial class Form1 : Form
    {
        private bool stopMon = false;
        private bool stopquery = false;
        private List<System.Threading.Thread> testthds = new List<System.Threading.Thread>();
        private Dictionary<string, List<System.Diagnostics.PerformanceCounter>> dtSent = new Dictionary<string, List<System.Diagnostics.PerformanceCounter>>();
        private Dictionary<string, List<System.Diagnostics.PerformanceCounter>> dtReceived = new Dictionary<string, List<System.Diagnostics.PerformanceCounter>>();
        private int startedThreadCount = 0;

        public Form1()
        {
            InitializeComponent();            

            
#if DEBUG_TEST
            txtHosts.Text = System.Net.Dns.GetHostName().ToLower() + ",Localhost";
#endif
        }

        private void btnStartMon_Click(object sender, EventArgs e)
        {
            btnStartMon.Enabled = false;
            btnStopMon.Enabled = false;    
            string[] hosts = SplitList(txtHosts.Text);
            stopMon = false;
            txtStatus.Text = "";
            txtStatus.Refresh();

            SetStatusStrip("Getting perfmon counters...");
            if(GetCounters(hosts) != 0)
            {
                return;
            }
            SetStatusStrip("");

            System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                {                    
                    while (!stopMon)
                    {
                        try
                        {
                            double minbytes, maxbytes, avgbytes;
                            double bytes = GetReadings(hosts, out minbytes, out maxbytes, out avgbytes);
                            double Gb = bytes * 8 / 1024 / 1024 / 1024;
                            double minGb = minbytes * 8 / 1024 / 1024 / 1024;
                            double maxGb = maxbytes * 8 / 1024 / 1024 / 1024;
                            double avgGb = avgbytes * 8 / 1024 / 1024 / 1024;

                            int provalue = Convert.ToInt32(bytes / 64);
                            if (provalue > progressBar1.Maximum)
                            {
                                provalue = progressBar1.Maximum;
                            }
                            SetControlPropertyValue(progressBar1, "Value", provalue);

                            SetControlPropertyValue(label4, "Text", "Current: " + Math.Round(Gb, 2).ToString() + " Gb/s  Max: " + Math.Round(maxGb, 2).ToString() + " Gb/s  Min: " + Math.Round(minGb, 2).ToString() + " Gb/s  Avg: " + Math.Round(avgGb, 2).ToString() + " Gb/s");

                            System.Threading.Thread.Sleep(1000);
                        }
                        catch(Exception emon)
                        {
                            AppendStatusText(txtStatus, "Text", "Monitor thread exception: " + emon.ToString());
                        }
                    }                          
                }));

            thd.IsBackground = true;
            thd.Start();
            
            btnStopMon.Enabled = true;            
        }

        delegate void SetControlValueCallback(Control oControl, string propName, object propValue);
        private void SetControlPropertyValue(Control oControl, string propName, object propValue)
        {
            if (oControl.InvokeRequired)
            {
                SetControlValueCallback d = new SetControlValueCallback(SetControlPropertyValue);
                oControl.Invoke(d, new object[] { oControl, propName, propValue });
            }
            else
            {
                Type t = oControl.GetType();
                PropertyInfo p = t.GetProperty(propName);                
                p.SetValue(oControl, propValue, null);                
            }
        }
                
        private int GetCounters(string[] hosts)
        {
            dtSent.Clear();
            dtReceived.Clear();
            int error = 0;

            QueryAnalyzer_ADOTrafficMonitor.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string slave)
            {
                if (string.Compare(slave, "localhost", true) == 0)
                {
                    slave = System.Net.Dns.GetHostName();
                }

                List<System.Diagnostics.PerformanceCounter> received = new List<System.Diagnostics.PerformanceCounter>();
                List<System.Diagnostics.PerformanceCounter> sent = new List<System.Diagnostics.PerformanceCounter>();

                try
                {
                    System.Diagnostics.PerformanceCounterCategory cat = new System.Diagnostics.PerformanceCounterCategory("Network Interface", slave);
                    string[] instances = cat.GetInstanceNames();     
           
                    foreach (string s in instances)
                    {
                        if (s.ToLower().IndexOf("loopback") == -1)
                        {
                            received.Add(new System.Diagnostics.PerformanceCounter("Network Interface", "Bytes Received/sec", s, slave));
                            sent.Add(new System.Diagnostics.PerformanceCounter("Network Interface", "Bytes Sent/sec", s, slave));
                        }
                    }

                    //Initial reading.
                    foreach (System.Diagnostics.PerformanceCounter pc in received)
                    {
                        pc.NextValue();
                    }

                    foreach (System.Diagnostics.PerformanceCounter pc in sent)
                    {
                        pc.NextValue();
                    }

                    lock (dtSent)
                    {                        
                        dtSent.Add(slave, sent);
                        dtReceived.Add(slave, received);
                    }        
                }
                catch (Exception e)
                {
                    AppendStatusText(txtStatus, "Text", "Error while getting counter from " + slave + ".  Error: " + e.ToString());
                    System.Threading.Interlocked.Increment(ref error);
                }
            }
            ), hosts, hosts.Length);

            return error;
        }

        private double GetReadings(string[] hosts, out double minbytes, out double maxbytes, out double avgbytes)
        {
            bool first = true;
            double total = 0, min = double.NaN, max = double.NaN;

            QueryAnalyzer_ADOTrafficMonitor.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string slave)
            {
                if (string.Compare(slave, "localhost", true) == 0)
                {
                    slave = System.Net.Dns.GetHostName();
                }   

                lock (dtSent)
                {
                    if (!dtSent.ContainsKey(slave))
                    {
                        return;
                    }
                    if (!dtReceived.ContainsKey(slave))
                    {
                        return;
                    }

                    List<System.Diagnostics.PerformanceCounter> received = dtReceived[slave];
                    List<System.Diagnostics.PerformanceCounter> sent = dtSent[slave];

                    try
                    {
                        double current = 0;
                        foreach (System.Diagnostics.PerformanceCounter counter in received)
                        {
                            current += counter.NextValue();
                        }
                        foreach (System.Diagnostics.PerformanceCounter counter in sent)
                        {
                            current += counter.NextValue();
                        }
#if DEBUG_TEST
                        current += (new Random()).Next(95837341, 480808080);
#endif
                        total += current;
                        if (!first)
                        {
                            if (current < min)
                            {
                                min = current;
                            }
                            if (current > max)
                            {
                                max = current;
                            }
                        }
                        else
                        {
                            first = false;
                            min = current;
                            max = current;
                        }
                    }
                    catch(Exception e)
                    {
                        AppendStatusText(txtStatus, "Text", "Error while reading perfmon data from: " + slave + ".  Error:  " + e.ToString());                        
                    }                    
                }
            }
            ), hosts, hosts.Length);

            minbytes = min;
            maxbytes = max;
            avgbytes = total / hosts.Length;
            return total;
        }

        private void ToggleBtnStartMon()
        {
            btnStartMon.Enabled = txtHosts.Text.Trim().Length > 0;
        }

        private void ToggleBtnStartQuery()
        {
            btnStartQuery.Enabled = txtClients.Text.Trim().Length > 0 && txtSQL.Text.Trim().Length > 0 && txtHosts.Text.Trim().Length > 0;
        }

        private void ToggleBtnKillall()
        {
            btnKillall.Enabled = txtHosts.Text.Trim().Length > 0 || txtClients.Text.Trim().Length > 0;
        }

        private void txtHosts_TextChanged(object sender, EventArgs e)
        {
            ToggleBtnStartMon();
            ToggleBtnStartQuery();
            ToggleBtnKillall();
        }

        private void btnStopMon_Click(object sender, EventArgs e)
        {
            btnStopMon.Enabled = false;
            stopMon = true;
            ToggleBtnStartMon();
        }

        private void txtClients_TextChanged(object sender, EventArgs e)
        {
            ToggleBtnStartQuery();
            ToggleBtnKillall();
        }

        private void txtSQL_TextChanged(object sender, EventArgs e)
        {
            ToggleBtnStartQuery();
        }       

        private void btnStartQuery_Click(object sender, EventArgs e)
        {
            btnStartQuery.Enabled = false;
            txtStatus.Text = "";
            lblThreadCount.Text = "0";
            startedThreadCount = 0;
            SetStatusStrip("Running tests...");

            string[] clients = SplitList(txtClients.Text);
            string[] hosts = SplitList(txtHosts.Text);
            testthds.Clear();           
            stopquery = false;
            bool isRoundRobin = radRobin.Checked;
            Random rnd = new Random();
            int nextdatasource = rnd.Next() % hosts.Length;

            for(int i = 0; i < clients.Length; i++)
            {
                string client = clients[i];
                string datasource = "";

                if (!isRoundRobin)
                {
                    datasource = txtHosts.Text.Trim();
                }
                else
                {
                    if (++nextdatasource > hosts.Length - 1)
                    {
                        nextdatasource = 0;
                    }
                    datasource = hosts[nextdatasource];
                }

                System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                    {
                        if (string.Compare(client, "localhost", true) == 0)
                        {
                            client = System.Net.Dns.GetHostName();
                        }

                        System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                            System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                        System.Net.Sockets.NetworkStream netstm = null;
                        try
                        {
                            sock.Connect(client, 55903);
                            netstm = new System.Net.Sockets.NetworkStream(sock);
                            netstm.WriteByte((byte)'s'); //Start test.
                            if (radNonquery.Checked)
                            {
                                netstm.WriteByte((byte)'n');
                            }
                            else
                            {
                                netstm.WriteByte((byte)'q');
                            }                            
                            XContent.SendXContent(netstm, txtSQL.Text.Trim());
                            string myconnstr = "Data Source = " + datasource;
                            {
                                string xcsoset = ConnStrOtherBox.Text.Trim();
                                if (0 != xcsoset.Length)
                                {
                                    myconnstr += "; " + xcsoset;
                                }
                            }
                            XContent.SendXContent(netstm, myconnstr);

                            if (netstm.ReadByte() != (byte)'+')
                            {
                                throw new Exception("Didn't receive success signal from protocol to start test for client: " + client);
                            }

                            lock (hosts)
                            {
                                startedThreadCount++;
                                SetControlPropertyValue(lblThreadCount, "Text", "Client Count: " + startedThreadCount.ToString());
                            }

                            while (!stopquery)
                            {
                                //Check to see if tests are still running.
                                netstm.WriteByte((byte)'p');
                                if (netstm.ReadByte() != '+')
                                {
                                    AppendStatusText(txtStatus, "Text", "Protocol didn't return a success signal.  Stopping test for client: " + client);
                                    break;
                                }
                                System.Threading.Thread.Sleep(3000);
                            }

                            netstm.WriteByte((byte)'t'); //stop test.

                            if (netstm.ReadByte() != (byte)'+')
                            {
                                throw new Exception("Didn't receive success signal from protocol to end test for client: " + client);
                            }
                        }
                        catch (Exception ex)
                        {
                            AppendStatusText(txtStatus, "Text", "Error while running test for client: " + client + ".  Error: " + ex.ToString());
                        }
                        finally
                        {
                            if (netstm != null)
                            {
                                netstm.Close();
                                netstm = null;
                            }
                            sock.Close();
                            sock = null;
                        } 
                    }));

                testthds.Add(thd);
                thd.Start();                
            }

            btnStopQuery.Enabled = true;
        }

        private void btnStopQuery_Click(object sender, EventArgs e)
        {
            btnStopQuery.Enabled = false;
            stopquery = true;
            toolStripStatusLabel1.Text = "Stopping all tests...";
            object obj = new object();
            
            System.Threading.Thread stopthd = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
            {              
                foreach (System.Threading.Thread thd in testthds)
                {
                    thd.Join();
                    lock (obj)
                    {
                        startedThreadCount--;
                        SetControlPropertyValue(lblThreadCount, "Text", "Client Count: " + startedThreadCount.ToString());
                    }
                }
                toolStripStatusLabel1.Text = "";
                SetControlPropertyValue(btnStartQuery, "Enabled", txtClients.Text.Trim().Length > 0 && txtSQL.Text.Trim().Length > 0 && txtHosts.Text.Trim().Length > 0);
            }));
            stopthd.IsBackground = true;
            stopthd.Start();        
        }

        private void Form1_Resize(object sender, EventArgs e)
        {
            int y = progressBar1.Location.Y + progressBar1.Height + 5;
            int x = progressBar1.Location.X;

            lblIndex0.Location = new Point(x, y);
            lblIndex1.Location = new Point(progressBar1.Width / 2 + x - lblIndex1.Width / 2, y);
            lblIndex2.Location = new Point(progressBar1.Width + x - lblIndex2.Width, y);
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            this.WindowState = FormWindowState.Maximized;
        }

        private string[] SplitList(string str)
        {
            return str.Trim().Split(new string[] { ",", ";", Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
        }

        private void SetStatusStrip(string status)
        {
            toolStripStatusLabel1.Text = status;
            statusStrip1.Refresh();        
        }

        private void AppendStatusText(Control oControl, string propName, object propValue)
        {
            if (oControl.InvokeRequired)
            {
                SetControlValueCallback d = new SetControlValueCallback(AppendStatusText);
                oControl.Invoke(d, new object[] { oControl, propName, propValue });
            }
            else
            {
                Type t = oControl.GetType();
                PropertyInfo p = t.GetProperty(propName);
                string oldvalue = p.GetValue(oControl, null).ToString();
                if (oldvalue.Length > 0)
                {
                    oldvalue += Environment.NewLine;
                }
                p.SetValue(oControl, oldvalue + propValue.ToString(), null);
            }
        }

        private void btnKillall_Click(object sender, EventArgs e)
        {
            btnKillall.Enabled = false;
            SetStatusStrip("Killing all...");
            txtStatus.Text = "";
            txtStatus.Refresh();
            string[] hosts = SplitList(txtHosts.Text);
            string[] clients = SplitList(txtClients.Text);
            Dictionary<string, int> unique = new Dictionary<string, int>();

            for (int i = 0; i < hosts.Length; i++)
            {
                string host = hosts[i].ToUpper();
                if (!unique.ContainsKey(host))
                {
                    unique.Add(host, 0);
                }
            }
            for (int i = 0; i < clients.Length; i++)
            {
                string client = clients[i].ToUpper();
                if (!unique.ContainsKey(client))
                {
                    unique.Add(client, 0);
                }
            }

            string allmachines = "";
            foreach (string key in unique.Keys)
            {
                allmachines += "," + key;
            }
            allmachines.Trim(',');

            try
            {
                txtStatus.Text = Exec.Shell("QueryAnalyzer_Admin killall -f " + allmachines, false);
            }
            catch (Exception ex)
            {
                txtStatus.Text = ex.ToString();
            }

            btnKillall.Enabled = true;
            SetStatusStrip("");
        }
    }
}
