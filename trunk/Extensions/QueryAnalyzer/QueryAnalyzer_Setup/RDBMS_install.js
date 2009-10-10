debugger
var ws = WScript.CreateObject("WScript.Shell");

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

    var notables = (WScript.arguments.Length > 2 && "-notables" == WScript.arguments(2));

    WScript.StdOut.WriteLine("Begin installation...");

    var thispath = WScript.ScriptFullName;
    var del = thispath.lastIndexOf("\\");
    var installdir = thispath.substr(0, del + 1);

    var fs = fso = new ActiveXObject("Scripting.FileSystemObject");
    var install_query_analyzer_xml = "RDBMS_Install_Query_Analyzer.DBCORE";

    try
    {
        WScript.StdOut.WriteLine("Begin preparing files for import into DFS...");
        if (!fs.FileExists(installdir + install_query_analyzer_xml))
        {
            throw install_query_analyzer_xml + " is not found in " + installdir;
        }

        var tmpdir = installdir + "tmp";

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

        WScript.StdOut.WriteLine("Begin importing jobs into DFS...");
        ExecDSpace("del RDBMS_*.DBCORE", true);
        ExecDSpace("importdirmt " + tmpdir, true);

        var WshNetwork = WScript.CreateObject("WScript.Network");
        var parts = installdir.split(":");
        var installdirNetpath = "\\\\" + WshNetwork.ComputerName + "\\" + parts[0] + "$" + parts[1];

        ExecDSpace("del RDBMS_QA_Usage.xml", true);
        ExecDSpace("put \"" + installdirNetpath + "\\RDBMS_QA_Usage.xml\"", true);

        ExecDSpace("put \"" + installdirNetpath + "\\RDBMS_DBCORE.dll\"", true); // DLLs always overwrite.

        WScript.StdOut.WriteLine("Completed importing jobs into DFS...");

        var nextargt = installdirNetpath.substr(0, installdirNetpath.length - 1);
        //WScript.StdOut.WriteLine("Begin executing " + install_query_analyzer_xml + " " + nextargt + "...");
        WScript.StdOut.WriteLine("Begin executing " + install_query_analyzer_xml + "...");
        ExecDSpace("exec " + install_query_analyzer_xml + " \"" + nextargt + "\" \"" + account + "\" \"" + password + "\" {73045AA6-2F6B-4166-BDE2-806F1E43854B}", true);
        WScript.StdOut.WriteLine("Completed executing " + install_query_analyzer_xml + "...");

        if (!notables)
        {
            try
            {
                //WScript.StdOut.WriteLine("Checking existence of RDBMS_SysTables...");
                var output = ExecDSpace("info RDBMS_SysTables"); // Doesn't return "No such file", but throws it.
                {
                    //WScript.StdOut.WriteLine("RDBMS_SysTables is found.");
                    //WScript.StdOut.WriteLine("Checking existence of paintings table...");
                    //output = ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"select TOP 1 * FROM sys.tables WHERE table = 'paintings' ORDER BY table\"", true);
                    //if (output.indexOf("paintings") > -1)
                    {
                        //WScript.StdOut.WriteLine("paintings table is found.  Dropping paintings table...");
                        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE paintings\"", true);
                        //WScript.StdOut.WriteLine("paintings table is dropped.");
                    }

                    //WScript.StdOut.WriteLine("Checking existence of artists table...");
                    //output = ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"select TOP 1 * FROM sys.tables WHERE table = 'artists' ORDER BY table\"", true);
                    //if (output.indexOf("artists") > -1)
                    {
                        //WScript.StdOut.WriteLine("artists table is found.  Dropping artists table...");
                        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE artists\"", true);
                        //WScript.StdOut.WriteLine("artists table is dropped.");
                    }

                    //WScript.StdOut.WriteLine("Checking existence of paintingsArchived table...");
                    //output = ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"select TOP 1 * FROM sys.tables WHERE table = 'paintingsArchived' ORDER BY table\"", true);
                    //if (output.indexOf("paintingsArchived") > -1)
                    {
                        //WScript.StdOut.WriteLine("paintingsArchived table is found.  Dropping paintingsArchived table...");
                        ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"DROP TABLE paintingsArchived\"", true);
                        //WScript.StdOut.WriteLine("paintingsArchived table is dropped.");
                    }
                }
            }
            catch (e8905)
            {
            }

            WScript.StdOut.WriteLine("Begin generating sample paintings table...");
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE paintings (paintingID INT, year INT, title CHAR(300), size DOUBLE, pixel LONG, artistID INT, bday DATETIME)\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (11, 1498, 'The Last Supper', 100.45, 374000000, 200, '1/1/1498 10:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (12, 1503, 'Mona Lisa', 4.75, 600000000, 200, '1/1/1503 10:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (13, 1889, 'The Starry Night', 1.5, 100000000, 201, '1/1/1889 10:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintings VALUES (14, 1889, 'Irises', 4.93, 100000000, 201, '1/1/1889 10:00:00 AM')\"", true);
            WScript.StdOut.WriteLine("Completed generating sample paintings table");

            WScript.StdOut.WriteLine("Begin generating sample artists table...");
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE artists (artistID INT, artistName CHAR(300), year INT, birthday DATETIME)\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (200,  'Leonardo da Vinci', 1452, '4/15/1452 12:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (201,  'Vincent van Gogh', 1853, '3/30/1853 12:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO artists VALUES (202,  'Claude Monet', 1840, '11/14/1840 12:00:00 AM')\"", true);
            WScript.StdOut.WriteLine("Completed generating sample artists table");

            WScript.StdOut.WriteLine("Begin generating sample paintingsArchived table...");
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"CREATE TABLE paintingsArchived (paintingID INT, year INT, title CHAR(300), size DOUBLE, pixel LONG, artistID INT, bday DATETIME)\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintingsArchived VALUES (4, 1866, 'The Woman in the Green Dress', 9.87, 809880000, 202, '1/1/1498 10:00:00 AM')\"", true);
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE \"INSERT INTO paintingsArchived VALUES (5, 1872, 'Impression, Sunrise', 1.4, 9000000, 202, '1/1/1498 10:00:00 AM')\"", true);
            WScript.StdOut.WriteLine("Completed generating sample paintingsArchived table");

            WScript.StdOut.WriteLine("Completed generating sample database tables");
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

function ExecDSpace(cmd, verbose)
{
    var oExec = ws.Exec("dspace.exe " + cmd);

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
