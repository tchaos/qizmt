<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



## Qizmt Coding Standards for Contributors ##

  * There is and has never been deprecation in the functionality of Qizmt, including methods exposed to mapreducer, local and remote jobs. We put a fairly heavy amount of thought and time into getting the overall design right to prevent this. All versions of Qizmt aim to be 100% reverse compatible.

  * Always use spaces for indentation, not tabs. 4 spaces per indent. You can set this in your Visual Studio settings.

  * Try to keep lines shorter than 120 characters.

  * When you edit a file, try to stick with the conventions used in the surrounding code.

  * Write your contributions using Visual Studio.net 2008 with the VisualSVN plug-in.

  * Avoid mixing purely cosmetic changes (such as removing trailing white-space) with functional changes, as this makes review of the actual change (whether it’s a check-in or a patch) more difficult.

  * Never place a ‘{‘ or ‘}’ on the same line as other code, e.g.

**Acceptable**

```
public void SomeFunction()
{
    //
}
```

**Not Acceptable**
```
public void SomeFunction(){
    //
}
```

  * Always submit patches or commits with sufficient comments to well document the change. Start with what the change accomplishes followed by as much specification about the change as possible.

  * Avoid unnecessary inline comments in your code contributions and opt for good variable and function names. Unnecessary inline comments typically create a maintenance issue.


## Qizmt Overview ##

This guide is intended for use by:

  * Those who wish to download the Qizmt source code.
  * Those who wish to contribute to the Qizmt open source project.
> <img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Contributor.png' alt='Qizmt Contributor' />


## Walkthrough ##

  1. Read the Qizmt Coding Standards
  1. Read the GPL v3 License, currently at http://www.gnu.org/copyleft/gpl.html
  1. If not already installed, install Visual Studio 2008 SP1, VisualSVN and TortoiseSVN onto Windows Vista or Windows 7
  1. Uninstall any existing installation of Qizmt with Window’s _add/remove programs_ tool.
  1. In Visual Studio, select **VisualSVN -`>` Repo-Browser**
  1. Enter http://qizmt.googlecode.com/svn/trunk into the **URL** box
    * (if you have a contributor account set up via request to myspace.com/Qizmt you will need to enter “https” instead of “http”)
> > <img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_googleCodeUrl.png' alt='Qizmt Google Url' />
  1. Select **OK** and the **Repository Browser** will show

&lt;Br/&gt;

<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RepositoryBrowser.png' alt='Qizmt Repository Browser' />
  1. Expand the tree view and Right-click the **trunk** folder and select **checkout**.

&lt;Br/&gt;

<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_CheckOut.png' alt='Qizmt Checkout' /><br /><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_CheckOutLocation.png' alt='Qizmt Checkout Location' />
  1. Select **OK** on the **Checkout** window
  1. Wait for the checkout progress to complete<br /><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_CheckOutProgress.png' alt='Qizmt Checkout' />
  1. At this point you have downloaded the Qizmt source code
  1. Navigate to Qizmt source code directory<br /><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Directory.png' alt='Qizmt Directory' />
  1. Run the **DistributedObjects.sln** solution to load the Qizmt source code into Visual Studio.
  1. Select **OK** on the Source Control prompt<br /><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_OkPrompt.png' alt='Qizmt OkPrompt' />
  1. Select **“Temporarily work uncontrolled”** on the next Source Control prompt. The solution has both TFS and SVN bindings, however you will just be using SVN. If you commit (check-in) a patch with modifications to the TFS bindings it will be rejected

&lt;Br/&gt;

<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_UnableAccessDB.png' alt='Qizmt Unable Access DB' />
  1. Once the project is open you will see a little green light next to all items in the **Solutions Explorer**. This tells you that the item has not changed since you last **updated (same idea as “get latest” in TFS)**

&lt;Br/&gt;

<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_VS.png' alt='Qizmt VS' />
  1. Hit **Ctrl-Shift-B** to build
  1. On your Windows desktop, right click on **my computer icon** and select **properties**. Then select **advanced system settings** then select **Environment Variables…** in the **Advanced** tab. Under **System variables** select the **path** variable and select **“Edit…”** Add `;<qizmtdir>\MySpace.DataMining.DistributedObjects\bin\Debug\` to the end of the path. Then select **OK, OK, OK**
  1. In Windows, click on the start button and enter into **start search** “visual studio 2008 command prompt” and hit enter
  1. Using the Visual Studio 2008 Command Prompt, navigate to:

&lt;Br/&gt;

`<qizmtdir>\MySpace.DataMining.DistributedObjects\bin\Debug\`
  1. Run the command `installutil MySpace.DataMining.DistributedObjects.exe`
  1. In Windows, click on the start button and enter into **start search** “Services” and hit enter.
  1. Locate the **Distributed Objects** service<br /><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Services.png' alt='Qizmt Services' />
  1. Right click on it and select **start**
  1. At the Windows command line issue the command **qizmt @format machines=localhost**
  1. Qizmt is now built and manually installed on your development computer as a single machine cluster.
  1. You can request **SVN commit** access by sending the following information to http://www.myspace.com/qizmt<br />`Google account email address`<br />`<first name> <last name>`<br />`[<company>/<title>]`<br />`<telephone number>`<br />`<address>`