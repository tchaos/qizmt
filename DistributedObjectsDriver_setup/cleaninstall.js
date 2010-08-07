// Usage:
//  cscript cleaninstall.js <machine1>[,<machineN>] <TargetDir> <SourceDir> <user> <password>
// Or:
//  cscript cleaninstall.js @<args.txt>
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
        WScript.Echo("Stopping services: " + machines[im]);
        try {
            shell.Exec("sc \\\\" + machines[im] + " stop DistributedObjects");
        }
        catch (e48u) {
        }
        try {
            shell.Exec("sc \\\\" + machines[im] + " stop QueryAnalyzer_Protocol");
        }
        catch (e48u) {
        }
        try {
            shell.Exec("sc \\\\" + machines[im] + " stop MemCachePin");
        }
        catch (e48u) {
        }
    }
    WScript.Sleep(7000);

    if (args.length > iarg) {
        var installdir = args[iarg++];

        if (args.length > iarg) {
            var sourcedir = args[iarg++];
            if (args.length > iarg + 1) {
                var username = args[iarg++];
                var password = args[iarg++];

                for (var im = 0; im < machines.length; im++) {
                    WScript.Echo("Cleaning disk: " + machines[im]);
                    var netinstalldir = getNetworkPath(installdir, undefined, machines[im]);
                    // It's fine if the dir doesn't exist, but if it does, it has to be cleaned.
                    try {
                        fso.DeleteFolder(netinstalldir, true); // force=true
                    }
                    catch (e4883f) {
                    }
                }
                WScript.Sleep(1000);

                for (var im = 0; im < machines.length; im++) {
                    WScript.Echo("Deleting services: " + machines[im]);
                    try {
                        shell.Exec("sc \\\\" + machines[im] + " delete DistributedObjects");
                    }
                    catch (e48u) {
                    }
                    try {
                        shell.Exec("sc \\\\" + machines[im] + " delete QueryAnalyzer_Protocol");
                    }
                    catch (e48u) {
                    }
                    try {
                        shell.Exec("sc \\\\" + machines[im] + " delete MemCachePin");
                    }
                    catch (e48u) {
                    }
                }
                WScript.Sleep(2000);

                WScript.Echo("Installing...");
                var installproc = shell.Exec("cmd /c cscript //D //NoLogo \"" + sourcedir + "\\install.js\" install " + machinecsv + " \"" + installdir + "\" \"" + sourcedir + "\" \"" + username + "\" \"" + password + "\" 2>&1");
                while (!installproc.StdOut.AtEndOfStream) {
                    WScript.Echo("  " + installproc.StdOut.ReadLine());
                }

                for (var im = 0; im < machines.length; im++) {
                    WScript.Echo("Removing excess files: " + machines[im]);
                    try {
                        var netinstalldir = getNetworkPath(installdir, undefined, machines[im]);
                        try { fso.DeleteFile(netinstalldir + "\\DFS.xml", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\slave.dat", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\execlog.txt", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\execq.dat", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\jid.dat", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\notify.xml", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\schedule.xml", true); } catch(e4231j) { } // force=true
                        try { fso.DeleteFile(netinstalldir + "\\harddrive_history.txt", true); } catch(e4231j) { } // force=true
                    }
                    catch (e3312a) {
                    }
                }

                WScript.Echo("Done");

            }
            else {
                WScript.Echo("User name and password expected");
            }
        }
        else {
            WScript.Echo("Source directory expected");
        }

    }
    else {
        WScript.Echo("Target directory expected");
    }

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

