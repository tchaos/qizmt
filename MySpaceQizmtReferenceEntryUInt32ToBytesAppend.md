<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `UInt32ToBytesAppend` #
`public static void UInt32ToBytesAppend(UInt32 x, List<byte> buffer)`

Converts UInt32 x to bytes in big endian byte ordering and appends to buffer.

### Example ###
This example sorts a mixture of negative and positive integers.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));

    buffer.Clear();
    Entry.UInt32ToBytesAppend(i, buffer);

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