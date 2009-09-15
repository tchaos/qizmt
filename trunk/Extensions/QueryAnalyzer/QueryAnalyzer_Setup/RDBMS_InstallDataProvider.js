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
    if (WScript.arguments.Length < 2)
    {
        WScript.StdOut.WriteLine("Missing arguments: RDBMS_InstallDataProvider.js <account> <password>");
        return;
    }

    var account = WScript.arguments(0);
    var password = WScript.arguments(1);
    var thispath = WScript.ScriptFullName;
    var del = thispath.lastIndexOf("\\");
    var thisdir = thispath.substr(0, del + 1);
    
    WScript.StdOut.WriteLine("Begin installing Data Provider...");
    try
    {
        var WshNetwork = WScript.CreateObject("WScript.Network");
        var thismachine = WshNetwork.ComputerName;

        WScript.StdOut.WriteLine("Begin adding to gac and updating machine.config...");
        oExec = ws.Exec(thisdir + "QueryAnalyzer_EditConfig.exe a");
        msg = oExec.StdErr.ReadAll();
        if (msg.length != 0)
        {
            throw "gac or machine.config update error: " + msg;
        }
        msg = oExec.StdOut.ReadAll();        
        WScript.StdOut.WriteLine(msg);

        WScript.StdOut.WriteLine("Begin installing service...");
        var obj = ws.Exec("sc \\\\" + thismachine + " query QueryAnalyzer_Protocol");
        var output = obj.StdOut.ReadAll();

        if (output.indexOf("SERVICE_NAME: QueryAnalyzer_Protocol") > -1)
        {
            WScript.StdOut.WriteLine("Service exists already.  Removing previous service...");
            ws.Exec("sc \\\\" + thismachine + " stop QueryAnalyzer_Protocol");
            ws.Exec("sc \\\\" + thismachine + " delete QueryAnalyzer_Protocol");
            WScript.Sleep(7000);            
        }
        
        ws.Exec("sc \\\\" + thismachine + " create QueryAnalyzer_Protocol binPath= \"" + thisdir + "QueryAnalyzer_Protocol.exe\" start= auto obj= \"" + account + "\" DisplayName= QueryAnalyzer_Protocol password= \"" + password + "\"");
        WScript.Sleep(5000); 
        WScript.StdOut.WriteLine("Service installed.");

        ws.Exec("sc \\\\" + thismachine + " start QueryAnalyzer_Protocol");
        WScript.StdOut.WriteLine("Service started.");
        
        WScript.StdOut.WriteLine("Data Provider has been installed successfully.");
    }
    catch (e)
    {
        WScript.StdOut.WriteLine("Error:" + e);
    }   
}