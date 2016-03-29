<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `ToInt32` #
`public static Int32 ToInt32(UInt32 x)`

Converts UInt32 to int.

### Example ###
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```