<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `ToBytes` #
`public static byte[] ToBytes(Int32 x)`

Converts int x to bytes in big endian byte ordering.
### Remarks ###
A new `byte[]` is allocated each time this method is called.  Consider a more efficient overload of this method in which a `byte[]` is passed in

as a parameter, or consider the `ToBytesAppend` method.

### Example ###
```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();	
    byte[] buffer = Entry.ToBytes(Convert.ToInt32(sLine));
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}


//Reducer code
byte[] buffer = new byte[4];
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);
    int num = Entry.BytesToInt(buffer);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static void ToBytes(Int32 x, byte[] resultbuf, int bufoffset)`

Converts int x to bytes in big endian byte ordering with offset.
### Remarks ###
Please see example 2 when sorting on a mixture of negative and positive integers.

### Example 1 ###
This example sorts positive integers.

```
byte[] myBuffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.ToBytes(Convert.ToInt32(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

### Example 2 ###
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