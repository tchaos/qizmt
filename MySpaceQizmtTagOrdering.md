<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Tag Ordering #

The order of tags in jobs does matter for some tags. The required tags of each job type have a specific order as shown in the default template when creating a job. `<Job>` tags are executed in the order. In a mapreduce job, the child nodes of `<MapReduce>` must be in order, `<Map>` first, `<Reduce>` second.


## Local Tag Ordering ##
```
    <Job Name="" Custodian="" email="">
      <IOSettings> 
                <!-- child nodes in any order -->
      </IOSettings>
      <Local>
        <![CDATA[ ... ]]>
      </Local>
    </Job>
```


## Remote Tag Ordering ##
```
    <Job Name="" Custodian="" email="">
      <IOSettings> 
                <!-- child nodes in any order -->
      </IOSettings>
      <Remote>
        <![CDATA[ ... ]]>
      </Remote>
    </Job>
```


## Mapreduce Tag Ordering ##
```
    <Job Name="" Custodian="" email="">
      <IOSettings>
                <!-- child nodes in any order -->
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[ ... ]]>
        </Map>
        <Reduce>
          <![CDATA[ ... ]]>
        </Reduce>
      </MapReduce>
    </Job>
```

## `KeyLength` ##
The `KeyLength` tag of mapreducer jobs may be set to a specific number of bytes or set to comma separated list of data types, e.g. **`<KeyLength>int,long,double,55</KeyLength>`**
