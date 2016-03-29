<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)

## Multiple Input and Output Files in a Remote Job ##

### Example ###
```
<Job Name="testJob" Custodian="" Email="" Description="">
  <IOSettings>
    <JobType>remote</JobType>
    <DFS_IO>
      <DFSReader>dfs://job_Input1.txt;dfs://job_Input2.txt</DFSReader>
      <DFSWriter>dfs://job_Output1.txt;dfs://job_Output2.txt</DFSWriter>
    </DFS_IO>
  </IOSettings>
  <Remote>
    <![CDATA[
        public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
        {
            StringBuilder sb = new StringBuilder();
            while(dfsinput.ReadLineAppend(sb))
            {                    
                if(StaticGlobals.Qizmt_InputFileName == "job_Input1.txt")
                {
                    dfsoutput.GetOutputByIndex(0).WriteLine(sb.ToString());
                }
                else
                {
                    dfsoutput.GetOutputByIndex(1).WriteLine(sb.ToString());
                }  
                sb.Length = 0;
            }
        }
    ]]>
  </Remote>
</Job>
```