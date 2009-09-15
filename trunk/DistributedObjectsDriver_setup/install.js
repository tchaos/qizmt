// Usage:
//  cscript install.js <install|uninstall|uninstallclean> <machine1>[,<machineN>] <TargetDir> <SourceDir> <user> <password>
// Or:
//  cscript install.js @<args.txt>
// (where <args.txt> contains line-delimited arguments)

var fso = WScript.CreateObject("Scripting.FileSystemObject");

var iarg = 0;
var args;
if (WScript.Arguments.length > 0 && WScript.Arguments(0).charAt(0) == '@') {
    var ForReading = 1;
    var rspf = fso.OpenTextFile(WScript.Arguments(0).substring(1), ForReading);
    args = rspf.ReadAll().replace(/\r\n/g, "\n").split("\n");
}
else {
    args = new Array();
    for (var i = 0; i < WScript.Arguments.length; i++) {
        args[i] = WScript.Arguments(i);
    }
}

var action;
if (args.length > iarg) {
    action = args[iarg++];
}

if ("start" == action || "stop" == action || "restart" == action) {
    var machinecsv;
    if (args.length > iarg) {
        machinecsv = args[iarg++];
    }
    if (null == machinecsv || machinecsv.length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');

        var shell = WScript.CreateObject("WScript.Shell");
        for (var im = 0; im < machines.length; im++) {

            if ("stop" == action || "restart" == action) {
                shell.Exec("sc \\\\" + machines[im] + " stop DistributedObjects");
                WScript.Sleep(7000);
                WScript.Echo(machines[im] + " stopped");
            }

            if ("start" == action || "restart" == action) {
                shell.Exec("sc \\\\" + machines[im] + " start DistributedObjects");
                WScript.Echo(machines[im] + " started");
            }

        }
    }

}
else if ("cleanfiles" == action) {
    var machinecsv;
    if (args.length > iarg) {
        machinecsv = args[iarg++];
    }
    if (null == machinecsv || machinecsv.length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');

        if (args.length > iarg) {
            var dir = args[iarg++];
            for (var im = 0; im < machines.length; im++) {
                var netdir = getNetworkPath(dir, undefined, machines[im]);
                WScript.Echo("Deleting files from: " + netdir);
                var folder = fso.GetFolder(netdir);
                var efiles = new Enumerator(folder.Files);
                for (; !efiles.atEnd();  efiles.moveNext()) {
                    //WScript.Echo("  Deleting file: " + efiles.item());
                    try {
                        fso.DeleteFile(efiles.item(), false);
                    }
                    catch (e) {
                    }
                }
            }
        }
        else {
            WScript.Echo("Directory name expected");
        }
        
    }
}
else if ("delmetadata" == action){
    var machinecsv;
    if (args.length > iarg) {
        machinecsv = args[iarg++];
    }
    if (null == machinecsv || machinecsv.length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');
        
        if (args.length > iarg) {
            var dir = args[iarg++];
            for(var im = 0; im < machines.length; im++) {
                var netdir = getNetworkPath(dir, undefined, machines[im]);
                try {
                    fso.DeleteFile(netdir + "\\dfs.xml");
                }
                catch (e) {
                }
            }
            WScript.Echo("Done");
        }
        else {
            WScript.Echo("Directory name expected");
        }
    }
}
else if ("status" == action) {
    var machinecsv;
    if (args.length > iarg) {
        machinecsv = args[iarg++];
    }
    if (null == machinecsv || machinecsv.length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');

        var shell = WScript.CreateObject("WScript.Shell");
        for (var im = 0; im < machines.length; im++) {

            var ex = shell.Exec("sc \\\\" + machines[im] + " query DistributedObjects");
            var exlines = ex.StdOut.ReadAll().split("\n");
            var found = false;
            for (var iex = 0; iex < exlines.length; iex++) {
                var x = exlines[iex].replace(/^\s+/, "").replace(/\s+$/, "");
                if (x.substring(0, 6) == "STATE ") {
                    var state = x.substring(6);
                    var ils = state.lastIndexOf(' ');
                    if (-1 != ils) {
                        state = state.substring(ils + 1);
                    }
                    state = state.replace(/\s/g, "");
                    if ("RUNNING" == state) {
                        WScript.Echo(machines[im] + ": " + state);
                    }
                    else {
                        WScript.Echo(machines[im] + ": " + state + " *** WARNING ***");
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                WScript.Echo(machines[im] + ": FAILED *** ERROR ***");
            }

        }
    }
}
else if ("install" == action || "uninstall" == action || "uninstallclean" == action) {
    var machinecsv;
    if (args.length > iarg) {
        machinecsv = args[iarg++];
    }
    if (null == machinecsv || machinecsv.length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');

        var installdir;
        if (args.length > iarg) {
            installdir = args[iarg++];
        }
        if ((null == installdir || installdir.length < 1) && "install" == action) {
            WScript.Echo("Invalid directory");
        }
        else {
            var srcpath;
            if (args.length > iarg) {
                srcpath = args[iarg++];
            }
            if (null == srcpath || srcpath.length < 1) {
                WScript.Echo("Invalid installer path");
            }
            else {

                var appdir = ".";
                {
                    var scriptpath = WScript.ScriptFullName;
                    var ils = scriptpath.lastIndexOf('\\');
                    if (ils != -1) {
                        appdir = scriptpath.substring(0, ils);
                    }
                }

                /*
                if (srcpath == ".") {
                srcpath = "";
                }
                srcpath = getNetworkPath(srcpath, appdir);
                */

                var authuser;
                var authpass;
                if (args.length > iarg) {
                    authuser = args[iarg++];
                }
                if (args.length > iarg) {
                    authpass = args[iarg++];
                }

                var problems = "";
                var success = "";
                var shell = WScript.CreateObject("WScript.Shell");
                for (var im = 0; im < machines.length; im++) {
                    try {
                        var destpath = getNetworkPath(installdir, undefined, machines[im]);
                        if (action == "install") {
                            WScript.Echo("Installing to " + machines[im] + "");

                            try {
                                shell.Exec(srcpath + "\\urights.exe \"" + authuser + "\" \"" + machines[im] + "\" SeServiceLogonRight");
                            }
                            catch (exajj) {
                            }

                            /*try{
                            fso.CreateFolder(destpath);
                            }
                            catch(e335){
                            }*/
                            //WScript.Echo("srcpath=" + srcpath + "; destpath=" + destpath);
                            fso.CopyFolder(srcpath, destpath, true);
                            //{
                            var runcmd = "sc \\\\" + machines[im] + " create DistributedObjects binPath= \"" + installdir + "\\MySpace.DataMining.DistributedObjects.exe\" start= auto obj= \"" + authuser + "\" DisplayName= DistributedObjects password= \"" + authpass + "\"";
                            //WScript.Echo("Run: " + runcmd);
                            var e1 = shell.Exec(runcmd);
                            var err1 = e1.StdErr.ReadAll();
                            if (err1.length != 0) {
                                throw "Problem creating service: " + err1;
                            }
                            WScript.Echo(e1.StdOut.ReadAll());
                            //}
                            //{
                            var e2 = shell.Exec("sc \\\\" + machines[im] + " start DistributedObjects");
                            var err2 = e2.StdErr.ReadAll();
                            if (err2.length != 0) {
                                throw "Problem starting service: " + err2;
                            }
                            WScript.Echo(e2.StdOut.ReadAll());
                            //}
                            if (0 == success.length) {
                                success = "\r\n\r\nInstalled to machines:";
                            }
                            success += "\r\n" + machines[im];
                        }
                        else if (action == "uninstall") {
                            try {
                                var e33 = shell.Exec("sc \\\\" + machines[im] + " stop DistributedObjects");
                                var e34 = shell.Exec("sc \\\\" + machines[im] + " delete DistributedObjects");
                            }
                            catch (xasdf) {
                            }
                        }
                        else if (action == "uninstallclean") {
                            try {
                                var e43 = shell.Exec("sc \\\\" + machines[im] + " stop DistributedObjects");
                                var e44 = shell.Exec("sc \\\\" + machines[im] + " delete DistributedObjects");
                                WScript.Sleep(7000);
                                fso.DeleteFolder(destpath);
                            }
                            catch (jaja) {
                            }
                        }
                    }
                    catch (ex) {
                        if (problems.length == 0) {
                            problems = "\r\n\r\nFailed machines:";
                        }
                        var m = ex.message;
                        if (!m) {
                            m = "<unknown error>";
                        }
                        problems += "\r\n" + machines[im] + ": " + m;
                    }
                }

                WScript.Echo("Done" + success + problems);


            }
        }
    }
}
else {
    WScript.Echo("Unknown action: " + action);
}


// reldir and defaulthost are optional
function getNetworkPath(path, reldir, defaulthost) {
    if (path.substring(0, 2) != "\\\\") {
        if (defaulthost == undefined) {
            var net = WScript.CreateObject("WScript.Network");
            defaulthost = net.ComputerName;
        }
        if (path.charAt(1) != ':') {
            if (reldir == undefined) {
                var shell = WScript.CreateObject("WScript.Shell");
                reldir = shell.CurrentDirectory;
            }
            path = reldir + "\\" + path;
        }
        path = "\\\\" + defaulthost + "\\" + path.charAt(0) + "$" + path.substring(2);
    }
    return path;
}

