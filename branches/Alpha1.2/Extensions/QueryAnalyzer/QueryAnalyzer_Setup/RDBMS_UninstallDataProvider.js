debugger
var ws = WScript.CreateObject("WScript.Shell");

forceCScript();
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

function Main()
{
    WScript.StdOut.WriteLine("Begin uninstalling Data Provider...");
    var thispath = WScript.ScriptFullName;
    var del = thispath.lastIndexOf("\\");
    var thisdir = thispath.substr(0, del + 1);
    try
    {
        WScript.StdOut.WriteLine("Begin uninstalling service...");
        var WshNetwork = WScript.CreateObject("WScript.Network");
        ws.Exec("sc \\\\" + WshNetwork.ComputerName + " stop QueryAnalyzer_Protocol");
        WScript.Sleep(5000);
        ws.Exec("sc \\\\" + WshNetwork.ComputerName + " delete QueryAnalyzer_Protocol");
        WScript.StdOut.WriteLine("Service uninstalled.");

        WScript.StdOut.WriteLine("Begin removing from gac and updating machine.config...");
        oExec = ws.Exec(thisdir + "QueryAnalyzer_EditConfig.exe r");
        var msg = oExec.StdErr.ReadAll();
        if (msg.length != 0)
        {
            throw "gac or machine.config update error: " + msg;
        }
        msg = oExec.StdOut.ReadAll();
        WScript.StdOut.WriteLine(msg);

        WScript.StdOut.WriteLine("Data Provider has been uninstalled successfully.");
    }
    catch (e)
    {
        WScript.StdOut.WriteLine("Error:" + e);
    }
}