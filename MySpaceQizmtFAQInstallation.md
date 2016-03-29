<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Installation #

## After I install Qizmt and run the format command from one of the machines in the cluster, I am still unable to run any mapreducers jobs. ##

---


Are there any errors when issue the command `health –a`?

Are there any firewalls between the machines of the cluster? Qizmt clusters should not have any firewall between the machines of the cluster.

Qizmt needs to be installed with an account that has sufficient read/write access to `\\<host>\<drive>$\<installdir>\` from every machine in the cluster to every other machine in the cluster.

To expose `\\<host>\<drive>$\<installdir>\` to every machine in the cluster both the share properties on the disk drive and the folder properties of the install dir need to grant read and write access to the account that qizmt was installed with, e.g. when installing the installer prompts for a login/password which qizmt will run on.