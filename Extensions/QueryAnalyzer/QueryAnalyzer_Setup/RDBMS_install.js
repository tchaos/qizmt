debugger
var ws = WScript.CreateObject("WScript.Shell");
var install_query_analyzer_xml = "RDBMS_Install_Query_Analyzer.DBCORE";

forceCScript();
SystemEnvironmentInstall();
Main();

function forceCScript()
{
    if (WScript.FullName.toLowerCase().indexOf("wscript.exe") > -1)
    {
        var args = "";

        if (WScript.arguments.Length > 0)
        {
            for (var i = 0; i < WScript.arguments.Length; i++)
            {
                args += " \"" + WScript.arguments(i) + "\"";
            }
        }

        ws.Run(ws.ExpandEnvironmentStrings("%comspec%") + " /k cscript.exe //nologo \"" + WScript.ScriptFullName + "\" " + args);
        WScript.Quit();
    }
}

function SystemEnvironmentInstall(addpath)
{
    var sysenv = ws.Environment("System");
    var path = sysenv("PATH");
    //WScript.StdOut.WriteLine("Current path = " + path);
    var newpath = null;
    if (!path || path.length == 0)
    {
        newpath = addpath;
    }
    else if ((";" + path + ";").toLowerCase().indexOf(";" + addpath + ";") != -1)
    {
        newpath = path + ";" + addpath;
    }
    else
    {
        newpath = path; // Force it to be set to broadcast change from service.
    }
    if (newpath != null)
    {
        //WScript.StdOut.WriteLine("New path = " + newpath);
        sysenv("PATH") = newpath;
    }
}

function Main()
{
    if (WScript.arguments.Length < 2)
    {
        WScript.StdOut.WriteLine("Missing arguments: RDBMS_Install.js <account> <password>");
        return;
    }

    var account = WScript.arguments(0);
    var password = WScript.arguments(1);
    var notables = false;
    var deployonly = false;
    var deploytoclusters;

    for (var ai = 2; ai < WScript.arguments.Length; ai++)
    {
        var thisarg = WScript.arguments(ai);
        if (thisarg == "-notables")
        {
            notables = true;
        }
        else
        {
            deploytoclusters = GetClusters(ParseHostList(thisarg));
            deployonly = true;
        }
    }

    WScript.StdOut.WriteLine("Begin installation...");

    var thispath = WScript.ScriptFullName;
    var del = thispath.lastIndexOf("\\");
    var installdir = thispath.substr(0, del + 1);
    var WshNetwork = WScript.CreateObject("WScript.Network");
    var parts = installdir.split(":");
    var installdirNetpath = "\\\\" + WshNetwork.ComputerName + "\\" + parts[0] + "$" + parts[1];
    var tmpdir = installdir + "tmp";
    var tmpdirNetpath = installdirNetpath + "tmp";

    var fs = fso = new ActiveXObject("Scripting.FileSystemObject");

    try
    {
        WScript.StdOut.WriteLine("Begin preparing files for import into DFS...");
        if (!fs.FileExists(installdir + install_query_analyzer_xml))
        {
            throw install_query_analyzer_xml + " is not found in " + installdir;
        }

        if (fs.FolderExists(tmpdir))
        {
            fs.DeleteFolder(tmpdir, true);
        }
        fs.CreateFolder(tmpdir);
        tmpdir += "\\";

        var installfolder = fs.GetFolder(installdir);
        var allfiles = new Enumerator(installfolder.files);
        for (; !allfiles.atEnd(); allfiles.moveNext())
        {
            var name = allfiles.item().Name;
            var ext = name.substr(name.length - 7).toUpperCase();
            if (ext == ".DBCORE")
            {
                fs.CopyFile(installdir + name, tmpdir + name);
            }
        }
        WScript.StdOut.WriteLine("Completed preparing files for import into DFS...");

        if (deployonly)
        {
            DeployToClusters(deploytoclusters, installdirNetpath, tmpdirNetpath, account, password, notables);
        }
        else
        {
            InstallToMachine(null, installdirNetpath, tmpdir, account, password, notables); //install to local cluster.
        }

        SystemEnvironmentInstall(installdir);
        WScript.StdOut.WriteLine("Begin cleaning up...");
        fs.DeleteFolder(tmpdir.substr(0, tmpdir.length - 1), true);
        WScript.StdOut.WriteLine("Completed cleaning up...");
    }
    catch (e)
    {
        WScript.StdOut.WriteLine("Installation failed.  Error: " + e);
        return;
    }
    WScript.StdOut.WriteLine("Installation completed");
}

function DeployToClusters(clusters, installdirNetpath, tempjobsdirNetpath, account, password, notables)
{
    for (var machine in clusters)
    {
        InstallToMachine(machine, installdirNetpath, tempjobsdirNetpath, account, password, notables);
    }
}

function InstallToMachine(machine, installdirNetpath, tempjobsdir, account, password, notables)
{
    WScript.StdOut.WriteLine("Begin installation on " + (machine == null ? "local cluster" : machine));
    WScript.StdOut.WriteLine("Begin importing jobs into DFS...");    
   
    ExecDSpace("del RDBMS_*.DBCORE", true, machine);
    if (machine == null)
    {
        ExecDSpace("importdirmt " + tempjobsdir, true);
    }
    else
    {
        ExecDSpace("exec \"//Job[@Name='RDBMS_ImportJobs']/IOSettings/LocalHost=" + machine + "\" RDBMS_ImportJobs.DBCORE \"" + tempjobsdir + "\"", true);
    }
    
    ExecDSpace("del RDBMS_QA_Usage.xml", true, machine);
    ExecDSpace("put \"" + installdirNetpath + "\\RDBMS_QA_Usage.xml\"", true, machine);
    ExecDSpace("put \"" + installdirNetpath + "\\RDBMS_DBCORE.dll\"", true, machine); // DLLs always overwrite.
    ExecDSpace("put \"" + installdirNetpath + "\\RDBMS_DfsProtocol.dll\"", true, machine); // DLLs always overwrite.
    WScript.StdOut.WriteLine("Completed importing jobs into DFS...");

    var nextargt = installdirNetpath.substr(0, installdirNetpath.length - 1);
    WScript.StdOut.WriteLine("Begin executing " + install_query_analyzer_xml + "...");
    ExecDSpace("exec " + install_query_analyzer_xml + " \"" + nextargt + "\" \"" + account + "\" \"" + password + "\" {73045AA6-2F6B-4166-BDE2-806F1E43854B}", true, machine);
    WScript.StdOut.WriteLine("Completed executing " + install_query_analyzer_xml + "...");

    if (!notables)
    {
        try
        {
            var output = ExecDSpace("info RDBMS_SysTables", false, machine); // Doesn't return "No such file", but throws it.
            {
                ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE paintings\"", true, machine);
                ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE artists\"", true, machine);
                ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE paintingsArchived\"", true, machine);
            }
        }
        catch (e8905)
        {
        }

        WScript.StdOut.WriteLine("Begin generating sample paintings table...");
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE paintings (paintingID INT, year INT, title CHAR(300), size DOUBLE, pixel LONG, artistID INT, bday DATETIME)\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (11, 1498, 'The Last Supper', 100.45, 374000000, 200, '1/1/1498 10:00:00 AM')\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (12, 1503, 'Mona Lisa', 4.75, 600000000, 200, '1/1/1503 10:00:00 AM')\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (13, 1889, 'The Starry Night', 1.5, 100000000, 201, '1/1/1889 10:00:00 AM')\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (14, 1889, 'Irises', 4.93, 100000000, 201, '1/1/1889 10:00:00 AM')\"", true, machine);
        WScript.StdOut.WriteLine("Completed generating sample paintings table");

        WScript.StdOut.WriteLine("Begin generating sample artists table...");
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE artists (artistID INT, artistName CHAR(300), year INT, birthday DATETIME)\"",true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (200,  'Leonardo da Vinci', 1452, '4/15/1452 12:00:00 AM')\"",true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (201,  'Vincent van Gogh', 1853, '3/30/1853 12:00:00 AM')\"",true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (202,  'Claude Monet', 1840, '11/14/1840 12:00:00 AM')\"", true,machine);
        WScript.StdOut.WriteLine("Completed generating sample artists table");

        WScript.StdOut.WriteLine("Begin generating sample paintingsArchived table...");
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE paintingsArchived (paintingID INT, year INT, title CHAR(300), size DOUBLE, pixel LONG, artistID INT, bday DATETIME)\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintingsArchived VALUES (4, 1866, 'The Woman in the Green Dress', 9.87, 809880000, 202, '1/1/1498 10:00:00 AM')\"", true, machine);
        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintingsArchived VALUES (5, 1872, 'Impression, Sunrise', 1.4, 9000000, 202, '1/1/1498 10:00:00 AM')\"", true, machine);
        WScript.StdOut.WriteLine("Completed generating sample paintingsArchived table");

        WScript.StdOut.WriteLine("Completed generating sample database tables");
    }
    WScript.StdOut.WriteLine("Completed installation on " + (machine == null ? "local cluster" : machine));
}

function ExecDSpace(cmd, verbose, runat)
{
    var runatarg = "";
    if (runat != null)
    {
        runatarg = "@=" + runat + " ";
    }
    var oExec = ws.Exec("dspace.exe " + runatarg + cmd);

    var output = "";
    if (!oExec.StdOut.AtEndOfStream)
    {
        output = oExec.StdOut.ReadAll();
        if (verbose == true)
        {
            WScript.StdOut.Write(output);
        }
    }


    var err = "";
    if (!oExec.StdErr.AtEndOfStream)
    {
        err = oExec.StdErr.ReadAll();
    }

    if (err != "")
    {
        throw err;
    }

    if (err.length > 0)
    {
        throw err;
    }

    return output;
}

function ParseHostList(hostlist)
{
    var results;
    if (hostlist.charAt(0) == "@")
    {
        results = new Array();
        var filepath = hostlist.substr(1);
        var fso, ts;
        fso = new ActiveXObject("Scripting.FileSystemObject");
        ts = fso.OpenTextFile(filepath, 1);
        var i = 0;
        while (!ts.AtEndOfStream)
        {
            var r = ts.ReadLine();
            if (r.charAt(0) != "#")
            {
                results[i++] = TrimWhiteSpace(r);
            }
        }
        ts.Close();
    }
    else
    {
        var delimiter = ";";
        if (hostlist.indexOf(",") > -1)
        {
            delimiter = ",";
        }
        results = hostlist.split(delimiter);
    }
    return results;
}

function GetClusters(hosts)
{
    var clusters = new Object();
    for (var i = 0; i < hosts.length; i++)
    {
        var host = TrimWhiteSpace(hosts[i]).toUpperCase();
        if (!IsInClusters(host, clusters))
        {
            clusters[host] = new Array();
            var output = ExecDSpace("slaveinstalls", false, host);
            var lines = output.split("\r");
            var ci = 0;
            for (var li = 0; li < lines.length; li++)
            {
                var line = lines[li];
                if (line.length > 1)
                {
                    var del = line.indexOf(" ");
                    if (del > -1)
                    {
                        var machine = line.substr(0, del).toUpperCase();
                        clusters[host][ci++] = TrimWhiteSpace(machine);
                    }
                }
            }
        }
    }
    return clusters;
}

function IsInClusters(machine, clusters)
{
    for (var i in clusters)
    {
        var cluster = clusters[i];
        for (var j = 0; j < cluster.length; j++)
        {
            if (machine == cluster[j])
            {
                return true;
            }
        }
    }
    return false;
}

function TrimWhiteSpace(str)
{
    var rExp = /[\r\s\n]/gi;
    return str.replace(rExp, "");
}