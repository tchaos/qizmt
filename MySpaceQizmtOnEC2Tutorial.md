

# Qizmt on EC2 Tutorial #

> ## _Overview_ ##

In this tutorial we will attempt to use the Qizmt-on-EC2 wizard (GPLv3) to automate the rental of EC2 instances linked to an EC2 account, generate a set of random words and execute a Qizmt Mapreduce job which will count the number of occurrences of each word. This tutorial also covers cluster diagnostics and using Qizmt Mapreduce IDE/Debugger on EC2.

![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_001_QizmtCloudImage.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_001_QizmtCloudImage.png)

The actual time that it takes to complete this tutorial varies and there are no guarantees that any software described in this tutorial will work properly or function as expected.

> ## _Results from Qizmt on EC2 word-count test 12/21/2010_ ##

| Execution Time | 8hrs 11min |        | Per virtual core | 2.5 EC2 Compute Units |
|:---------------|:-----------|:-------|:-----------------|:----------------------|
| Instance setup | ~1hrs      |             | MR.DFS size      | ~7.14TB (TiB)         |
| Gen inputs time | ~1hrs 14min |        | Disk IO per instance | ~160Mb/s (observed)   |
| Input doc size | ~1TB (TiB) |        | Total disk IO    | ~3.1Gb/s (observed)   |
| Output doc size | ~2MB       |             |                  |                       |
|                |            |                                  |                  |                       |
| Replication level used | 1          |         | MR Algorithm     | Grouped               |
| Instances rented | 20         |              | Reduce           | Partial Reduce (reduce applied both before and after exchange phase) |
| Instance type rented | High-CPU XL |        | Logic            | Map all words as keys with value as 1, in partial reduce produce intermediate word counts, in reduce add up total count for each word. |
| Total virtual cores | 160        |          |                  |                       |

> ## _Preparing your EC2 account on Amazon Web Services_ ##

  1. Read and make sure that you understand this entire tutorial prior to performing any of its described actions. Also, because this tutorial describes how to automate the rental of EC2 instances, you should review the code for Qizmt-on-EC2 (GPLv3) wizard via SVN at http://code.google.com/p/qizmt/
  1. Before resuming this tutorial, make sure that you fully understand all current EC2 rental costs, concepts and terminology and that you have fully reviewed that Qizmt-on-EC2 wizard code that you will be using to automate the rental of EC2 instances. Also be sure that you have read and agree to the GPLv3 license under which all code on <http://code.google.com/p/qizmt/> is released.
  1. Sign up for a free Amazon Web Services Account <http://aws.amazon.com/>.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_002_AWSSignup.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_002_AWSSignup.png)
> > You can use your existing Amazon account login information, or create a new account.
  1. Sign up for EC2 through the Amazon Elastic Compute Cloud (EC2) section under the Products tab
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_003_EC2Section.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_003_EC2Section.png)  ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_004_EC2SignUp.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_004_EC2SignUp.png)
  1. Go to the Security Credentials under the Account tab to start getting your X.509 certificate.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_005_AWSCredsLink.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_005_AWSCredsLink.png)
  1. Go to the Access Credentials and then X.509 Certificates tab
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_006_AWSCredsView.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_006_AWSCredsView.png)
  1. Create a new certificate.
  1. Download the certificate to your computer. The file has pattern **`cert-*.pem`**
  1. Download the private key to your computer. The file has pattern **`pk-*.pem`**  If you are attempting to use an existing certificate, you must already have this private key file and it cannot be downloaded again (in this case, create and use a new certificate).
  1. Enter the AWS Management Console to start to create and download an EC2 KeyPair. A link can be found at the top of the page.  Note that this is not the same key as from the previous steps.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_007_AWSMgmtConsole.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_007_AWSMgmtConsole.png)
  1. Click the Key Pairs link under NETWORKING & SECURITY in the left Navigation menu under the EC2 tab.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_008_EC2KeyPairs.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_008_EC2KeyPairs.png)
  1. If you do not have a key pair, create one with a name such as _mykeypair1_.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_009_EC2KeyPairCreate.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_009_EC2KeyPairCreate.png)
  1. Download the private key file to your computer. The file will be named **_mykeypair1.pem_** (for a key pair named _mykeypair1_).
> > ## _Get  EC2 Command-line Tools_ ##
  1. Ensure that Java is installed on your computer.
  1. Download ec2-api-tools.zip file from http://aws.amazon.com/developertools/351?_encoding=UTF8&jiveRedirect=1 to your computer if you do not have it already.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_010_EC2APIToolsDL.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_010_EC2APIToolsDL.png)
  1. Un-zip to your computer.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_011_EC2APIToolsUnzip.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_011_EC2APIToolsUnzip.png)
> > ## _Qizmt on EC2 Wizard_ ##
  1. Download Qizmt EC2 setup wizard from http://code.google.com/p/qizmt/downloads/list
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_012_QizmtOnEC2DL.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_012_QizmtOnEC2DL.png)
> > The code for this wizard is available on http://code.google.com/p/qizmt/
> > Notes on the AMI setup is in the appendix of this document.
  1. Run the “Qizmt on EC2” wizard and fill out the Setup tab:
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_013_QizmtOnEC2Setup.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_013_QizmtOnEC2Setup.png)
  1. Java home folder is where Java is installed.
  1. EC2 command-line tools home folder is the directory extracted from the tools zip file.
  1. EC2 X.509 certificate file was created during EC2 sign-up and matches pattern **`cert-*.pem`**
  1. EC2 private key file was created during EC2 sign-up and matches pattern **`pk-*.pem`**
  1. Click Next to go to the EC2 Machines tab:
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_014_QizmtOnEC2Machines.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_014_QizmtOnEC2Machines.png)
  1. Select the ami-76bb4a1f - MySpace Qizmt x86\_64 instance-store - Server2003r2-x86\_64-Win-v1.07 AMI ID. AMI ID is the Amazon image ID with the operating system. A custom built one for MySpace Qizmt must be used for this tutorial.
  1. Select the c1.xlarge - x86\_64 - High-CPU/Extra-Large Instance type. For this wizard you can select any instance of the 64 bit instance types. For info on the listing types and rental costs see http://aws.amazon.com/ec2/instance-types/ and http://aws.amazon.com/ec2/pricing/
  1. Select the KeyPair created in EC2 in prior steps of this tutorial. KeyPair name is the name of the key pair. The example used in this tutorial is test3. The drop down button can be used to fetch your available KeyPair names from your EC2 account. ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_015_ComboLoading.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_015_ComboLoading.png)
  1. Set KeyPair private key file to the private key file downloaded in prior steps of this tutorial. KeyPair private key file is the file downloaded after creating the key pair and matches file name **_test3.pem_** (for a key pair named _test3_).
  1. Leave the Availability Zone option blank. Availability Zone is where the machines are located. This can be left at the default or can be changed to another availability zone. Note that availability zones may have different pricing; see http://aws.amazon.com/ec2/ for information.
  1. Leave Security Groups specifying “default” Security Groups can list the security groups you wish to use, such as to restrict access. Note that this should be done carefully and can prevent Qizmt from operating.
  1. Click Next to go to the Qizmt Cluster tab.
  1. Enter the number of machines to rent for this Qizmt cluster. This is the number of EC2 instances that will be powered on to create this cluster. Note that each machine counts as an EC2 instance and has additional pricing; see the http://aws.amazon.com/ec2/  for information.
  1. Set an administrator password and retype it.
> > <font color='red'><b>Reminder: By clicking on “Start Qizmt Cluster” you will be attempting to automate the rental of multiple EC2 instances. The code for this wizard is available on <a href='http://code.google.com/p/qizmt/'>http://code.google.com/p/qizmt/</a> and it is your responsibility to review and re-build the code for the Qizmt-on-EC2 wizard as there is never a guarantee that it or any other code on <a href='http://code.google.com/p/qizmt/'>http://code.google.com/p/qizmt/</a> or the Qizmt EC2 AIMs will function properly.</b></font>
  1. Click the Start Qizmt Cluster button and wait for your cluster to be rented. Click OK to confirm. This can take about over 40 minutes for EC2 to allocate the Qizmt instances. While waiting you should log into EC2 and confirm that your instances are launching.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_016_QizmtOnEC2Cluster.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_016_QizmtOnEC2Cluster.png)
  1. When the Qizmt cluster is ready, the Qizmt-on-EC2 wizard will automatically launch a remote desktop session. Log in using user name Administrator and the password you entered. Leave the Qizmt-on-EC2 wizard open while you are still using the cluster.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_017_QizmtOnEC2StartCluster.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_017_QizmtOnEC2StartCluster.png)
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_018_RDConnecting.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_018_RDConnecting.png)
  1. Open a Command Prompt in the Remote Desktop connected to one of the EC2 machines.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_019_WinConsole.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_019_WinConsole.png)
> > Issuing Qizmt commands from one of the machines in the EC2 Qizmt cluster will operate on the cluster as a whole.
  1. Type **`Qizmt dir`** to view the files contained in MR.DFS (Qizmt’s Map Reduce Distributed File System).
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_020_QizmtDir.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_020_QizmtDir.png)
> > There are no files yet. Free disk space may be different.
  1. Type **`Qizmt ps`** to view the Qizmt jobs and other Qizmt processes currently running on the cluster.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_021_QizmtPs.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_021_QizmtPs.png)
> > The only thing currently running is the Qizmt ps just invoked. Total and free memory may be different. Number of processes and machines may be different.
  1. Type **`Qizmt examples`** to generate some example jobs into MR.DFS. Qizmt dir can be used again to list these files
  1. Type **`Qizmt edit Qizmt-WordCountByPartialReduce.xml`** to view the built-in word count example.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_022_QizmtEdit.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_022_QizmtEdit.png)
  1. Press Cancel and then type **`Qizmt edit `_MyJob.xml_** (or instead of MyJob.xml, type another job file name you wish). This opens a new jobs editor with a template file that can be edited.
  1. Add breakpoints by clicking on the margin.
  1. Press F5 to begin debugging.
> > This allows the job to be debugged directly on the cluster on your current machine to test the logic in the environment in which it will run.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_023_QizmtDebug.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_023_QizmtDebug.png)
  1. Close the jobs editor, save if desired.
  1. In the Command Prompt type **`Qizmt exec Qizmt-WordCountByPartialReduce.xml`**
> > This will execute the jobs in the file specified. In this case it is a built-in example. The job will run across the machines in the cluster and the output will be listed in MR.DFS
  1. Type **`Qizmt exec Qizmt-LargeWordCount.xml 1TB`** to start a terabyte word count test, designed for 20 high-CPU 8-core EC2 instances. This test may take around 8 hours to complete.
> > If you have less machines or less powerful machines, you can change the 1TB to a smaller byte size.
> > The input data will be generated first.
> > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_024_QizmtJobs.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_024_QizmtJobs.png)
> > Followed by a map reduce job to perform word counts.
  1. **`Qizmt perfmon diskio`** can be called in another Command Prompt window to display the Disk I/O performance of the machines in the cluster while the job is running.
> > Note that this can take several minutes to complete due to the job using resources.
  1. **`Qizmt perfmon cputime`** can be used to get the CPU usage of the machines in the cluster.
  1. Type **`Qizmt perfmon availablememory`** to get the available memory of the machines in the cluster.
  1. Type **`Qizmt head Qizmt-LargeWordCount-WordCounts.txt 30`** to view the first 30 lines of the word count output file when the job is successfully done.
  1. Type **`qizmt get Qizmt-LargeWordCount-WordCounts.txt \\<host>\d$\<dir>\wordcounts.txt`** to get a copy of the resulting word counts out of MR.DFS
  1. When you are done using the cluster,
    * click on <font color='brown'>Terminate Cluster</font>,
    * or if you no longer have the “Qizmt on EC2” wizard open:
      1. log into the AWS Management Console,
      1. click the Instances link under INSTANCES in the left Navigation menu for EC2,
      1. place a check mark next to all the instances you wish to terminate,
      1. using the Instance Actions drop down choose <font color='brown'>Terminate</font>.
> > > > ![http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_027_EC2Terminate.png](http://qizmt.googlecode.com/svn/wiki/images/QizmtOnEC2_027_EC2Terminate.png)
  1. Log into EC2 and confirm that all of your instances have been terminated.



<table width='100%'><tr><td width='100%' align='center'>
<h2>Appendix</h2>
<h3>AMI setup notes for Qizmt on EC2</h3>
</td></tr></table>

This is for preparing to create an AMI (Amazon EC2 OS image) with Qizmt and QizmtEC2Service preinstalled.

#### EC2 base images: ####
_**Instance Store**_

`ami-dd20c3b4 x86_64 windows "ec2-public-windows-images/Server2003r2-x86_64-Win-v1.07.manifest.xml"`

`ami-df20c3b6 i386 windows "ec2-public-windows-images/Server2003r2-i386-Win-v1.07.manifest.xml"`


#### Steps to prepare Windows instance on EC2 for Qizmt: ####

-	Log into EC2 instance via Remote Desktop using auto-generated password from EC2 (through elasticfox, etc).
(In Remote Desktop’s options, enable ‘Local devices and resources’ for Drives to allow copying files into EC2)

-	Set Network Location: Home (Not on 2003)

-	Change network and sharing settings in Network and Sharing Center: (Not on 2003)
  1. Turn On "File sharing"
  1. Turn Off "Password protected sharing"

-	Disable Windows Firewall. (Only if already enabled)

-	Apply Qizmt registry changes.

-	Set console defaults.
Put "Command Prompt" shortcut on desktop.

-	Folder settings: (explorer: alt, tools, folder options, view)
  1. don't hide file extensions
  1. show hidden files

-	Install Qizmt at C:\Qizmt with `.\Administrator` account.
If it is already installed, ensure it is pointing to `C:\Qizmt`:
`sc config "DistributedObjects" binPath= "C:\Qizmt\MySpace.DataMining.DistributedObjects.exe"`
Note: Qizmt must have been built with `#define LOGON_MACHINES` in Surrogate.cs

-	Update system environment variables to include `D:\Qizmt` before `C:\Qizmt`.
Note: `D:\Qizmt` won’t necessarily exist yet.

-	Copy 2 exe's to `C:\QizmtEC2Service`
  1. `QizmtEC2Service.exe`
  1. `QizmtEC2ServiceInit.exe`

-	Run:  `sc create "QizmtEC2Service" binPath= "C:\QizmtEC2Service\QizmtEC2Service.exe" start= auto`

-	then start it:  `net start "QizmtEC2Service"`

-	Delete `C:\QizmtEC2Service\QizmtEC2Service-status.txt` (after looking at it).
Check for `C:\QizmtEC2Service\QizmtEC2Service-errors.txt`
(Ignore and delete if error 404)

-	Restore hosts file at `%SystemRoot%\system32\drivers\etc\hosts`
with `C:\QizmtEC2Service\hosts.old` (restore default, if found)
(can potentially skip restoring hosts file; shouldn't hurt anything)
(note: if running service or init twice, hosts.old won't be the default)

-	`C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml` -
or `C:\Program Files (x86)\Amazon\Ec2ConfigSetup\config.xml` -
Plugin Ec2SetPassword: `<State>Enabled</State>`

-	`C:\Program Files\Amazon\Ec2ConfigService\Logs\Ec2ConfigLog.txt` -
or `C:\Program Files (x86)\Amazon\Ec2ConfigSetup\Ec2ConfigLog.txt`
replace contents with "`Preparing for Qizmt
`" (newline)

-	Delete `\Qizmt\logon.dat` (if exists)
`del c:\logon.dat`
`del d:\logon.dat`

-	Stop `DistributedObjects` service and set to Manual start.

-	Delete in `C:\Qizmt`:
  1. `del harddrive_history.txt *.xlib *.ylib zfoil* *.tmp service-stoplog.txt`
  1. `del slave.dat`
  1. `del dfs.xml execlog.txt errors.txt jid.dat`

-	Delete any temp files (e.g. qizmt msi)
Empty recycle bin.


#### Create AMI: ####

**_S3:_**

http://docs.amazonwebservices.com/AWSEC2/latest/UserGuide/index.html?creating-an-ami-s3.html

`ec2-bundle-instance <instance_id> -b <bucket_name> -p <bundle_name> -o <access_key_id> -w <secret_access_key>`

such as: `ec2-bundle-instance i-2afaa947 -b qizmt2 -p "MySpaceQizmt2_x86_64" -o AKIADQKE4SARGYLE -w eW91dHViZS5jb20vd2F0Y2g/dj1SU3NKMTlzeTNKSQ==`
(-o and -p are in account credentials)

`ec2-describe-bundle-tasks`

`ec2-register <your-s3-bucket>/image.manifest.xml -n image_name`

such as: `ec2-register "qizmt2/MySpaceQizmt2_x86_64.manifest.xml" -n "MySpaceQizmt2_x86_64"`

succeeded with:  `IMAGE   ami-baea1cd3`

**_EBS:_**

http://docs.amazonwebservices.com/AWSEC2/latest/UserGuide/index.html?creating-an-ami-ebs.html

`ec2-create-image -n <your_image_name>  <instance_id>`

such as:  `ec2-create-image -n "MySpaceQizmt1" i-90f3effb`

succeeded with:  `IMAGE   ami-b8c42cd1`

View at:  https://console.aws.amazon.com/ec2/home#c=EC2&s=Images

