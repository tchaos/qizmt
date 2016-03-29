<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Non-static methods of class `ByteSlice` #


## `ToString` ##

`public override string ToString()`

Converts this buffer to string using UTF-8 encoding.

#### Example ####

```
/*
This code uses this sample input:

K38000,Gary Doe
I56463,Joes Smith
A14546,John Smith
C94854,Mary Doe
Y139409,Sam Smith
E09809,John Doe
*/
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));
} 
```

---



## `ToBytes` ##
`public byte[] ToBytes()`

Returns a copy of this `ByteSlice`’s buffer.
#### Remarks ####
This method allocates a new `byte[]` each time when it is called.  Consider the more efficient methods `CopyTo` or `AppendTo` when necessary.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    byte[] myBuffer = key.ToBytes();

    //Examine the first byte of key.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---



## `CopyTo` ##
`public void CopyTo(byte[] myBuffer)`

Copies this buffer into myBuffer.
#### Remarks ####
This is a more efficient version of the `ToBytes()` method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new

byte array every time.

#### Example ####
In this example, a byte array myBuffer is allocated outside of the Reduce method.   This byte array will last for the lifetime of the slave.

Slaves are single threaded.

The Qizmt\_KeyLength constant is used here during the allocation of the byte array.  This ensures that the byte array is large enough when the key

buffer is copied into it.

```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[Qizmt_KeyLength];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(myBuffer);

    //Examine the first byte of key.
    //key[0] works also.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---




`public void CopyTo(byte[] myBuffer, int myBufferOffset)`

Copies this buffer into myBuffer starting at myBufferOffset.
#### Remarks ####
This is a more efficient version of the `ToBytes()` method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new

byte array every time.

#### Example ####
```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[16];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(myBuffer, 0);

    //Examine the first byte of key.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---





## `AppendTo` ##
`public void AppendTo(List<byte> list)`

Appends this `ByteSlice`’s buffer to list.
#### Remarks ####
`List<byte>` is usually the easiest way to work with binary.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    myBuffer.Clear();
    key.AppendTo(myBuffer);

    if (myBuffer[0] == 0x61)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else.
    }
} 
```