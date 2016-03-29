<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Local Jobs #

## Can I explicitly specify what machine a local job runs on? ##

---

Yes, can also specify an explicit host for a local job:
```
<SourceCode>
  <Jobs>
    <Job Name="ExplicitLocalHost" Custodian="" Email="">
      <IOSettings>
        <JobType>local</JobType>
        <LocalHost>localhost</LocalHost>
      </IOSettings>
      <Local>
      <![CDATA[
      public virtual void Local()
      {
        Qizmt_Log("This local job is running from " + Qizmt_MachineHost);
      }
      ]]>
      </Local>
   </Job>
  </Jobs>
</SourceCode>
```