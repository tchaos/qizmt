// cscript installnlp.js <srcmachine> <destmachine1>[,<destmachineN>]

var iarg = 0;

var srchost;
if (WScript.Arguments.length > iarg) {
    srchost = WScript.Arguments(iarg++);
}
if (null == srchost || srchost.length < 1) {
    WScript.Echo("Invalid source host");
}
else {
    if (WScript.Arguments.length <= iarg || WScript.Arguments(iarg).length < 1) {
        WScript.Echo("Invalid machines");
    }
    else {
        var machinecsv = WScript.Arguments(iarg++);
        var machines = (-1 != machinecsv.indexOf(';')) ? machinecsv.split(';') : machinecsv.split(',');

        var fso = WScript.CreateObject("Scripting.FileSystemObject");

        for (var im = 0; im < machines.length; im++) {
            try {
                try {
                    fso.CreateFolder("\\\\" + machines[im] + "\\C$\\Projects");
                }
                catch (exx) {
                }
                try {
                    fso.CreateFolder("\\\\" + machines[im] + "\\C$\\Projects\\DotNet");
                }
                catch (exx) {
                }
                fso.CopyFolder("C:\\Projects\\DotNet\\OpenNLP", "\\\\" + machines[im] + "\\C$\\Projects\\DotNet\\OpenNLP", true);
                WScript.Echo("NLP installed to " + machines[im]);
            }
            catch (e) {
                WScript.Echo("Failure installing NLP to " + machines[im] + ": " + e.message);
            }
        }
        WScript.Echo("Done");

    }
}
