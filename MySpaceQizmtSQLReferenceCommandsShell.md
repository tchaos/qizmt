<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md) / [MySpace Qizmt SQL Command](MySpaceQizmtSQLReferenceCommand.md)


# `SHELL` #

```
SHELL '<program> [<arguments...>]';
```

Executes a command on the cluster. Typically, this is used to execute Qizmt commands remotely or as part of a sequence of SQL statements. Newlines from the standard out are persisted and the loss-less standard out may be obtained by concatenating all of the resulting tuples. See the Qizmt documentation for available commands.

### Examples ###

```
SHELL 'qizmt ps’;
```


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_ShellPs.png' alt='QSQL Shell ps' />


