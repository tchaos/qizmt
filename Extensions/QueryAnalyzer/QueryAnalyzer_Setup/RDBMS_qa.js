debugger
var ws = WScript.CreateObject("WScript.Shell");

forceCScript();
Main(WScript.arguments);

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

function Main(args)
{
    var action = "";

    if (args.Length == 0)
    {
        action = "start";
    }
    else
    {
        action = args(0).toLowerCase();
    }
    switch (action)
    {
        case "start":
            BeginShellSession();
            break;
        case "query":
            if (args.Length < 2)
            {
                WScript.StdOut.WriteLine("<query> action requires query.");
            }
            ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE " + args(1), true);
            break;
        default:
            WScript.StdOut.WriteLine("Unknown action " + action);
            break;
    }
}

function BeginShellSession()
{
    var prompt = "qa> ";
    var promptmore = " -> ";

    WScript.StdOut.WriteLine("***** Begin shell session*****");
    WScript.StdOut.WriteLine("          quit; to exit");
    WScript.StdOut.WriteLine("          help; for usage");

    for (; ; )
    {
        var acmds = new Array();
        var userinput = "";
        WScript.StdOut.Write(prompt);
        for (; ; )
        {
            var done = false;
            userinput += WScript.StdIn.ReadLine();
            if (userinput.charAt(userinput.length - 2) == 'c' && userinput.charAt(userinput.length - 3) == '\\')
            {
                acmds = new Array();
                break;
            }
            var instr = false; // In single-quoted string.
            for (var nextcmd = true; nextcmd; )
            {
                nextcmd = false;
                instr = false;
                for (var i = 0; i < userinput.length; i++)
                {
                    if (instr)
                    {
                        if (userinput.charAt(i) == '\'')
                        {
                            if (userinput.charAt(i + 1) == '\'')
                            {
                                i++;
                            }
                            else
                            {
                                instr = false;
                            }
                        }
                    }
                    else
                    {
                        if (userinput.charAt(i) == '\'')
                        {
                            instr = true;
                        }
                        else if (userinput.charAt(i) == ';')
                        {
                            var newcmd = userinput.substring(0, i);
                            if ("\\c" == StringTrim(newcmd))
                            {
                                acmds = new Array();
                            }
                            else
                            {
                                acmds[acmds.length] = newcmd; // substring(start, end)
                            }
                            userinput = userinput.substring(i + 1, userinput.length); // substring(start, end)
                            if (StringTrim(userinput).length == 0)
                            {
                                done = true;
                            }
                            else
                            {
                                nextcmd = true;
                            }
                            break;
                        }
                    }
                }
            }
            if (done)
            {
                break;
            }
            if (instr)
            {
                userinput += "\r\n";
            }
            else
            {
                userinput += " ";
            }
            WScript.StdOut.Write(promptmore);
        }

        var doexit = false;
        for (var i = 0; i < acmds.length; i++)
        {
            try
            {
                var cmd = StringTrim(acmds[i]);
                var cmdparts = cmd.split(/ /g);
                var cmdname = cmdparts[0].toUpperCase();
                if ("HELP" == cmdname)
                {
                    ShowUsage(cmdparts);
                }
                else if ("EXIT" == cmdname || "QUIT" == cmdname)
                {
                    WScript.StdOut.WriteLine("***** End of shell session *****");
                    doexit = true;
                    break;
                }
                else
                {
                    var qacmd = acmds[i];
                    qacmd = qacmd.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/\r/g, "&#13;").replace(/\n/g, "&#10;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    qacmd = '"' + qacmd + '"';
                    ExecDSpace("exec RDBMS_QueryAnalyzer.DBCORE " + qacmd, true);
                }
            }
            catch (e)
            {
                WScript.StdOut.WriteLine("Error: " + e);
            }
        }
        if (doexit)
        {
            break;
        }
        
    }
    
}

function ExecDSpace(cmd, verbose) 
{
    var err = "";

    if (verbose == true)
    {
        WScript.StdOut.WriteLine("Executing... dspace.exe " + cmd);
    }

    var oExec = ws.Exec("dspace.exe " + cmd);

    if (verbose == true)
    {
        while (!oExec.StdOut.AtEndOfStream)
        {
            WScript.StdOut.Write(oExec.StdOut.Read(1));
        }
    }
    else
    {
        while (!oExec.StdOut.AtEndOfStream)
        {
            oExec.StdOut.Read(1);
        }
    }

    while (!oExec.StdErr.AtEndOfStream)
    {     
        err += oExec.StdErr.ReadAll();
    }

    if (err.length > 0)
    {
        throw err;
    }
}

var commandKeys;
var commandNodes;
function ShowUsage(args)
{
    if (commandKeys == null)
    {
        var usagexml = new ActiveXObject("Microsoft.XMLDOM");
        usagexml.async = false;
        usagexml.load("RDBMS_QA_Usage.xml");
        commandNodes = usagexml.selectNodes("//command");
        var nodes = usagexml.selectNodes("//keyword/text()");
        commandKeys = new Array(nodes.length);

        for (var i = 0; i < nodes.length; i++)
        {
            commandKeys[i] = nodes[i].text.toUpperCase();
        }
    }

    var _searchstr = "";
    if(args == null || args.length < 2)
    {
        _searchstr = "";
    }
    else
    {
        for(var i = 1; i< args.length; i++)
        {
            _searchstr += " " + args[i];
        }
    }
    _searchstr = _searchstr.replace(/\d/g, "").replace(/\s{2,}/g, " ").replace(/^\s+/g, "").replace(/\s+$/g, "").toUpperCase();

    if (_searchstr.length > 0)
    {
        for (var i = 0; i < commandKeys.length; i++)
        {
            if (commandKeys[i].indexOf(_searchstr) > -1)
            {
                var node = commandNodes[i];
                ShowManual(i);
                var asfdasfds = 10;
            }
        }
        WScript.StdOut.WriteLine("");
    }
    else
    {
        for (var i = 0; i < commandKeys.length; i++)
        {
            WScript.StdOut.WriteLine("help " + commandKeys[i] + ";");
        }
    }
}

function ShowManual(index)
{
    var node = commandNodes[index];
    WriteLine("================================================================================");
    WriteLine("Command Name:");
    WriteLine(node.childNodes[0].text, "     ");
    WriteLine("Description:");
    WriteLine(node.childNodes[1].text, "     ");
    WriteLine("Usage:");
    WriteLine(node.childNodes[2].text, "     ");
    WriteLine("Example 1:");
    WriteLine(node.childNodes[3].text, "     ");
    WriteLine("Example 2:");
    WriteLine(node.childNodes[4].text, "     ");
}

function WriteLine(line, head)
{
    var MAXLEN = 80;
    var _head = head == null ? "" : head;
    var headlen = _head.length;
    var words = line.split(" ");
    var charremain = MAXLEN;
    for (var i = 0; i < words.length; i++)
    {
        if (i == 0 || charremain <= 0 || charremain < words[i].length)
        {
            charremain = MAXLEN;
            WScript.StdOut.Write("\r\n" + _head);
            charremain -= headlen;
        }
        WScript.StdOut.Write(words[i] + " ");
        charremain = charremain - words[i].length - 1;
    }
    WScript.StdOut.WriteLine("");
}

function StringTrim(s)
{
    return s.replace(/^\s+|\s+$/g, "");
}
