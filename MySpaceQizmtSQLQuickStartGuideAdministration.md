<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)


# Qizmt SQL Administration #

The Qizmt SQL administration command is available to every machine in the cluster. If you need to run a killall on qizmt cluster, the Qizmt SQL Extension should be stopped first then restarted afterward. This will prevent ADO.NET request from being served while the underlying Qizmt cluster is being restarted.

```
Usage:
    RDBMS_admin <action> [<arguments>]
Actions:
    killall                 kill all protocol services
    stopall                 stop all protocol services
    startall                start all protocol services
    version                 get version of protocol service
    viewlog                 view log entries
    clearlog                clear logs entries
    examples                generate built-in examples
    rindexfilteringstresstest
                            [maxPrimary] [maxAssociations]
                            [maxSharedAssociations] [batchSize]
                            [-v verbose]
                            generate and run rindex filtering stress test
    rindexbasicstresstest   generate a basic rindex stress test
    health   check health of protocol services


```