<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Keys #


## I specify key length 16, but my key actually smaller. Should String.Trim() remove spaces? ##

---

If key length is 16, then the string can be up to 8 glyphs. If the key is output as string or mstring, it will automatically be right padded
or glyphs removed until it is at or under keylengh in bytes. Bytes are removed at glyph boundaries so that a glyph is not cut in half. If
you do not want any string or mstring keys to loose glyps, then `<KeyLength>` needs to be set larger than the largest string that will be
supported in the key, however the larger the KeyLength the more processing overhead for the exchange phase.

Also note that the text input and output to a mapreducer is UTF8 but the output of Map() is UTF16 and the input to Reduce() is UTF16.

In mapper:
```
mstring sMyString = mstring.Prepare(“car”);
output.Add(sMyString);
</source>
In reducer: 
<source lang="csharp">
mstring sMyString = mstring.Prepare(UnpadKey(key));
```

UTF8 -`>` Map() -`>` [data: UTF16](intermediate.md) -`>` Reduce() -`>` UTF8

e.g.
```
<Job Name="WordCount" Custodian="" email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    .
    .
    .
  </IOSettings>
  <MapReduce>
    <Map>
    <![CDATA[
    public virtual void Map(ByteSlice line, MapOutput output)
    {
      mstring sLine= mstring.Prepare(line);
      output.Add(sLine, mstring.Prepare(1));  // Output automatically right pads strings with \0 or removes from right glyphs until it fits 

the KeyLength
    }
    ]]>
    </Map>

    <Reduce>
    <![CDATA[
    public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
    {
      mstring sLine = mstring.Prepare(UnpadKey(key));
      sLine = sLine.AppendM(',').AppendM(values.Length);              
      output.Add(sLine);
    }
    ]]>
    </Reduce>
</MapReduce>
</Job>
```