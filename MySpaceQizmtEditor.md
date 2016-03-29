<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Qizmt Editor #

## Smart C# Syntax Coloring ##

Qizmt editor automatically colors C# syntax as it is typed or when code is pasted into the editor.

<table>
<tr><td> <b>Concept</b> </td><td> <b>Color</b> </td><td> <b>Examples</b> </td></tr>

<tr>
<td> Keywords </td><td> Blue </td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorKeywords.png' alt='SyntaxColorKeywords' /> </td>
</tr>

<tr>
<td> Comments </td><td> Green </td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorComments.png' alt='SyntaxColorComments' /> </td>
</tr>

<tr>
<td> Strings </td><td> Brown </td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorStrings.png' alt='SyntaxColorStrings' />
<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorChars.png' alt='SyntaxColorChars' /> </td>
</tr>

<tr>
<td> XML </td><td> GreyPurple </td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorXML.png' alt='SyntaxColorXML' /> </td>
</tr>

<tr>
<td> Other text </td><td> Black </td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorOtherText1.png' alt='SyntaxColorOtherText1' />   <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorOtherText2.png' alt='SyntaxColorOtherText2' />   <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorOtherText3.png' alt='SyntaxColorOtherText3' />  </td>
</tr>

<tr>
<td> Heap Allocations </td> <td> Red </td> <td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SyntaxColorHeapAllocs.png' alt='SyntaxColorHeapAllocs' />
<blockquote></td>
</tr></blockquote>

</table>


## C# Auto Complete ##

Activated when ‘.’ Key is pressed. When available, displays list of member functions on an object:

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorAutoComplete.png' alt='EditorAutoComplete' />


## Go to Line Number ##
**Ctrl+G** or **Edit -> Go to...**

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorGoto.png' alt='EditorGoto' />


## Code Search ##
**Ctrl+F** or **Edit->Find…/Find Next/Replace**

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorFindReplace.png' alt='EditorFindReplace' />


## Built-In Debugger ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorDebugger.png' alt='EditorDebugger' />

<table>
<tr><td> <b>Debug Command</b> </td><td> <b>Hot Key</b> </td><td> <b>Action</b> </td></tr>
<tr><td> Resume Debugging </td><td> F5 </td><td> Execute to next breakpoint. </td></tr>
<tr><td> Stop Debugging </td><td> F12 </td><td> Stop debug mode to continue editing mapreducer. </td></tr>
<tr><td> Step Into </td><td> F11 </td><td> Step into a function on call stack. </td></tr>
<tr><td> Step Over </td><td> F10 </td><td> Step over a function on call stack. </td></tr>
<tr><td> Step Out </td><td> Shift-F11 </td><td> Step out of a function on call stack. </td></tr>
<tr><td> Toggle Breakpoint </td><td> F9 </td><td> Add or remove a breakpoint. </td></tr>
<tr><td> Skip to Reduce </td><td> Shift-F12 </td><td> Start reduce phase at end of current map cycle. </td></tr>
</table>

## Start Debugger Half Way through Map Data ##
  1. During debug mode, open the Map Input Scroll Control.<br /><a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorOpenMapInputScrollControl.png' alt='EditorOpenMapInputScrollControl' />
  1. Press the Nudge button to skip through map data.<br /><a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorMapInputScrollControl.png' alt='EditorMapInputScrollControl' />

## Locating Build Error without IDE ##

  1. Write a mapreducer that contains an error: <br /> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditorSyntaxError.png' alt='EditorSyntaxError' />
  1. Run mapreducer: <br /> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_ExecSyntaxError.png' alt='ExecSyntaxError' />
  1. View executed code of last error from machine on which it occurred; use Ctrl-G to go to the line number of the error. In this example The error occurred on line **1307** and glyph 33:  <br />  **`Qizmt edit *errors*`** <br /><a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_EditErrors.png' alt='EditErrors' />