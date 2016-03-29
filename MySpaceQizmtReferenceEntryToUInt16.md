<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `ToUInt16` #
`public static UInt16 ToUInt16(Int16 x)`

Converts Int16 x to UInt16.

### Example ###
This example sorts a mixture of positive and negative integers of type Int16.

```
//Map code
byte[] buffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Entry.ToUInt16(Convert.ToInt16(sLine));
    Entry.UInt16ToBytes(i, buffer, 0);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
}

//Reducer code
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);
    Int16 num = Entry.ToInt16(Entry.BytesToUInt16(buffer));
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```