using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;

namespace Axon.MessagePack
{
    public interface IMessagePackProtocol : IProtocol
    {
    }

    public class MessagePackProtocol : AProtocol, IMessagePackProtocol
    {
        public MessagePackProtocol() : base()
        {
        }

        public override async Task WriteData(ITransport transport, IDictionary<string, byte[]> metadata, Action<IProtocolWriter> handler)
        {
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(transport, this, buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                await transport.Send(data, metadata);
            }
        }
        public override async Task WriteData(ITransport transport, string messageId, IDictionary<string, byte[]> metadata, Action<IProtocolWriter> handler)
        {
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(transport, this, buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                await transport.Send(messageId, data, metadata);
            }
        }

        public override async Task ReadData(ITransport transport, Action<IProtocolReader, IDictionary<string, byte[]>> handler)
        {
            var receivedData = await transport.Receive();

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            handler(reader, receivedData.Metadata);
        }
        public override async Task<TResult> ReadData<TResult>(ITransport transport, Func<IProtocolReader, IDictionary<string, byte[]>, TResult> handler)
        {
            var receivedData = await transport.Receive();

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            var result = handler(reader, receivedData.Metadata);

            return result;
        }
        public override async Task ReadData(ITransport transport, string messageId, Action<IProtocolReader, IDictionary<string, byte[]>> handler)
        {
            var receivedData = await transport.Receive(messageId);

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            handler(reader, receivedData.Metadata);
        }
        public override async Task<TResult> ReadData<TResult>(ITransport transport, string messageId, Func<IProtocolReader, IDictionary<string, byte[]>, TResult> handler)
        {
            var receivedData = await transport.Receive(messageId);

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            var result = handler(reader, receivedData.Metadata);

            return result;
        }

        public override async Task ReadTaggedData(ITransport transport, Action<IProtocolReader, string, IDictionary<string, byte[]>> handler)
        {
            var receivedData = await transport.ReceiveTagged();

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            handler(reader, receivedData.MessageId, receivedData.Metadata);
        }
        public override async Task<TResult> ReadTaggedData<TResult>(ITransport transport, Func<IProtocolReader, string, IDictionary<string, byte[]>, TResult> handler)
        {
            var receivedData = await transport.ReceiveTagged();

            var buffer = new MemoryStream(receivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, buffer);

            var result = handler(reader, receivedData.MessageId, receivedData.Metadata);

            return result;
        }

        public override async Task<Func<Action<IProtocolReader, IDictionary<string, byte[]>>, Task>> WriteAndReadData(ITransport transport, IDictionary<string, byte[]> metadata, Action<IProtocolWriter> handler)
        {
            Func<Task<ReceivedData>> receiveHandler;
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(transport, this, buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                receiveHandler = await transport.SendAndReceive(data, metadata);
            }

            return new Func<Action<IProtocolReader, IDictionary<string, byte[]>>, Task>(async (readHandler) => {
                var receivedData = await receiveHandler();

                var buffer = new MemoryStream(receivedData.Data);
                var reader = new MessagePackProtocolReader(transport, this, buffer);

                readHandler(reader, receivedData.Metadata);
            });
        }
        public override async Task<Func<Func<IProtocolReader, IDictionary<string, byte[]>, TResult>, Task<TResult>>> WriteAndReadData<TResult>(ITransport transport, IDictionary<string, byte[]> metadata, Action<IProtocolWriter> handler)
        {
            Func<Task<ReceivedData>> receiveHandler;
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(transport, this, buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                receiveHandler = await transport.SendAndReceive(data, metadata);
            }

            return new Func<Func<IProtocolReader, IDictionary<string, byte[]>, TResult>, Task<TResult>>(async (readHandler) => {
                var receivedData = await receiveHandler();

                var buffer = new MemoryStream(receivedData.Data);
                var reader = new MessagePackProtocolReader(transport, this, buffer);

                return readHandler(reader, receivedData.Metadata);
            });
        }

        public override async Task<Func<Action<IProtocolWriter>, Task>> ReadAndWriteData(ITransport transport, Action<IProtocolReader, IDictionary<string, byte[]>> handler)
        {
            var receiveResults = await transport.ReceiveAndSend();

            var receiveBuffer = new MemoryStream(receiveResults.ReceivedData.Data);
            var reader = new MessagePackProtocolReader(transport, this, receiveBuffer);
            handler(reader, receiveResults.ReceivedData.Metadata);

            return new Func<Action<IProtocolWriter>, Task>(async (writeHandler) =>
            {
                using (var buffer = new MemoryStream())
                {
                    var writer = new MessagePackProtocolWriter(transport, this, buffer);
                    writeHandler(writer);

                    buffer.Position = 0;
                    var data = buffer.ToArray();
                    await receiveResults.SendHandler(data, receiveResults.ReceivedData.Metadata);
                }
            });
        }
    }

    public interface IMessagePackProtocolReader : IProtocolReader
    {
        Stream DecoderStream { get; }
    }

    public class MessagePackProtocolReader : AProtocolReader, IMessagePackProtocolReader
    {
        public Stream DecoderStream { get; private set; }

        public MessagePackProtocolReader(ITransport transport, IProtocol protocol, Stream decoderStream)
            : base(transport, protocol)
        {
            this.DecoderStream = decoderStream;
        }

        public override string ReadStringValue()
        {
            return MessagePackBinary.ReadString(this.DecoderStream);
        }
        public override bool ReadBooleanValue()
        {
            return MessagePackBinary.ReadBoolean(this.DecoderStream);
        }
        public override byte ReadByteValue()
        {
            return MessagePackBinary.ReadByte(this.DecoderStream);
        }
        public override short ReadShortValue()
        {
            return MessagePackBinary.ReadInt16(this.DecoderStream);
        }
        public override int ReadIntegerValue()
        {
            return MessagePackBinary.ReadInt32(this.DecoderStream);
        }
        public override long ReadLongValue()
        {
            return MessagePackBinary.ReadInt64(this.DecoderStream);
        }
        public override float ReadFloatValue()
        {
            return MessagePackBinary.ReadSingle(this.DecoderStream);
        }
        public override double ReadDoubleValue()
        {
            return MessagePackBinary.ReadDouble(this.DecoderStream);
        }
        public override T ReadEnumValue<T>()
        {
            var encodedValue = this.ReadIntegerValue();

            return (T)Enum.ToObject(typeof(T), encodedValue);
        }
        public override object ReadIndeterminateValue()
        {
            throw new NotImplementedException("Indeterminate values not supported at this time");
        }
    }

    public interface IMessagePackProtocolWriter : IProtocolWriter
    {
        Stream EncoderStream { get; }
    }

    public class MessagePackProtocolWriter : AProtocolWriter, IMessagePackProtocolWriter
    {
        public Stream EncoderStream { get; private set; }

        public MessagePackProtocolWriter(ITransport transport, IProtocol protocol, Stream encoderStream)
            : base(transport, protocol)
        {
            this.EncoderStream = encoderStream;
        }

        public override void WriteStringValue(string value)
        {
            MessagePackBinary.WriteString(this.EncoderStream, value);
        }
        public override void WriteBooleanValue(bool value)
        {
            MessagePackBinary.WriteBoolean(this.EncoderStream, value);
        }
        public override void WriteByteValue(byte value)
        {
            MessagePackBinary.WriteByte(this.EncoderStream, value);
        }
        public override void WriteShortValue(short value)
        {
            MessagePackBinary.WriteInt16(this.EncoderStream, value);
        }
        public override void WriteIntegerValue(int value)
        {
            MessagePackBinary.WriteInt32(this.EncoderStream, value);
        }
        public override void WriteLongValue(long value)
        {
            MessagePackBinary.WriteInt64(this.EncoderStream, value);
        }
        public override void WriteFloatValue(float value)
        {
            MessagePackBinary.WriteSingle(this.EncoderStream, value);
        }
        public override void WriteDoubleValue(double value)
        {
            MessagePackBinary.WriteDouble(this.EncoderStream, value);
        }
        public override void WriteEnumValue<T>(T value)
        {
            var decodeValue = value.ToInt32(System.Globalization.CultureInfo.InvariantCulture);

            this.WriteIntegerValue(decodeValue);
        }
        public override void WriteIndeterminateValue(object value)
        {
            throw new NotImplementedException("Indeterminate values not supported at this time");
        }
    }

    public static class MessagePackProtocolFactory
    {
        public static IProtocol CreateProtocol(string[] args)
        {
            return new MessagePackProtocol();
        }
    }
}
