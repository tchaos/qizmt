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
    WScript.StdOut.WriteLine("Begin to uninstall...");   
    var thispath = WScript.ScriptFullName;
    var del = thispath.lastIndexOf("\\");
    var thisdir = thispath.substr(0, del + 1);

    var fs = fso = new ActiveXObject("Scripting.FileSystemObject");
    var uninstall_query_analyzer_xml = "RDBMS_Uninstall_Query_Analyzer.DBCORE";

    try
    {
        WScript.StdOut.WriteLine("Begin executing " + uninstall_query_analyzer_xml + "...");
        ExecDSpace("exec " + uninstall_query_analyzer_xml, true);
        WScript.StdOut.WriteLine("Completed executing " + uninstall_query_analyzer_xml + "...");
        
        WScript.StdOut.WriteLine("Begin removing files from DFS...");
        ExecDSpace("del RDBMS_*.DBCORE", true);
        WScript.StdOut.WriteLine("Completed removing files from DFS...");  
    }
    catch(e)
    {
        WScript.StdOut.WriteLine("Unable to unstall.  Error:  " + e);
        return;
    }

    WScript.StdOut.WriteLine("Uninstall completed");
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
