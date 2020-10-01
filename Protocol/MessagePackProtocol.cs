using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using System.Threading;

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

        public override async Task WriteData(ITransport transport, ITransportMetadata metadata, Action<IProtocolWriter> handler)
        {
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                await transport.Send(new TransportMessage(data, VolatileTransportMetadata.FromMetadata(metadata)));
            }
        }
        public override async Task WriteData(ITransport transport, string messageId, ITransportMetadata metadata, Action<IProtocolWriter> handler)
        {
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                await transport.Send(messageId, new TransportMessage(data, VolatileTransportMetadata.FromMetadata(metadata)));
            }
        }

        public override async Task ReadData(ITransport transport, Action<IProtocolReader, ITransportMetadata> handler)
        {
            var receivedData = await transport.Receive();

            var buffer = new MemoryStream(receivedData.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            handler(reader, receivedData.Metadata);
        }
        public override async Task<TResult> ReadData<TResult>(ITransport transport, Func<IProtocolReader, ITransportMetadata, TResult> handler)
        {
            var receivedData = await transport.Receive();

            var buffer = new MemoryStream(receivedData.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            var result = handler(reader, receivedData.Metadata);

            return result;
        }
        public override async Task ReadData(ITransport transport, string messageId, Action<IProtocolReader, ITransportMetadata> handler)
        {
            var receivedData = await transport.Receive(messageId);

            var buffer = new MemoryStream(receivedData.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            handler(reader, receivedData.Metadata);
        }
        public override async Task<TResult> ReadData<TResult>(ITransport transport, string messageId, Func<IProtocolReader, ITransportMetadata, TResult> handler)
        {
            var receivedData = await transport.Receive(messageId);

            var buffer = new MemoryStream(receivedData.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            var result = handler(reader, receivedData.Metadata);

            return result;
        }

        public override async Task ReadTaggedData(ITransport transport, Action<IProtocolReader, string, ITransportMetadata> handler)
        {
            var receivedData = await transport.ReceiveTagged();

            var buffer = new MemoryStream(receivedData.Message.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            handler(reader, receivedData.Id, receivedData.Message.Metadata);
        }
        public override async Task<TResult> ReadTaggedData<TResult>(ITransport transport, Func<IProtocolReader, string, ITransportMetadata, TResult> handler)
        {
            var receivedData = await transport.ReceiveTagged();

            var buffer = new MemoryStream(receivedData.Message.Payload);
            var reader = new MessagePackProtocolReader(buffer);

            var result = handler(reader, receivedData.Id, receivedData.Message.Metadata);

            return result;
        }

        public override async Task<Func<Action<IProtocolReader, ITransportMetadata>, Task>> WriteAndReadData(ITransport transport, ITransportMetadata metadata, Action<IProtocolWriter> handler)
        {
            Func<Task<TransportMessage>> receiveHandler;
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                receiveHandler = await transport.SendAndReceive(new TransportMessage(data, VolatileTransportMetadata.FromMetadata(metadata)));
            }

            return new Func<Action<IProtocolReader, ITransportMetadata>, Task>(async (readHandler) => {
                var receivedData = await receiveHandler();

                var buffer = new MemoryStream(receivedData.Payload);
                var reader = new MessagePackProtocolReader(buffer);

                readHandler(reader, receivedData.Metadata);
            });
        }
        public override async Task<Func<Func<IProtocolReader, ITransportMetadata, TResult>, Task<TResult>>> WriteAndReadData<TResult>(ITransport transport, ITransportMetadata metadata, Action<IProtocolWriter> handler)
        {
            Func<Task<TransportMessage>> receiveHandler;
            using (var buffer = new MemoryStream())
            {
                var writer = new MessagePackProtocolWriter(buffer);
                handler(writer);

                buffer.Position = 0;
                var data = buffer.ToArray();
                receiveHandler = await transport.SendAndReceive(new TransportMessage(data, VolatileTransportMetadata.FromMetadata(metadata)));
            }

            return new Func<Func<IProtocolReader, ITransportMetadata, TResult>, Task<TResult>>(async (readHandler) => {
                var receivedData = await receiveHandler();

                var buffer = new MemoryStream(receivedData.Payload);
                var reader = new MessagePackProtocolReader(buffer);

                return readHandler(reader, receivedData.Metadata);
            });
        }

        public override async Task<Func<Action<IProtocolWriter>, Task>> ReadAndWriteData(ITransport transport, Action<IProtocolReader, ITransportMetadata> handler)
        {
            throw new NotImplementedException();
            //var receiveResults = await transport.ReceiveAndSend();

            //var receiveBuffer = new MemoryStream(receiveResults.ReceivedData.Data);
            //var reader = new MessagePackProtocolReader(transport, this, receiveBuffer);
            //handler(reader, receiveResults.ReceivedData.Metadata);

            //return new Func<Action<IProtocolWriter>, Task>(async (writeHandler) =>
            //{
            //    using (var buffer = new MemoryStream())
            //    {
            //        var writer = new MessagePackProtocolWriter(transport, this, buffer);
            //        writeHandler(writer);

            //        buffer.Position = 0;
            //        var data = buffer.ToArray();
            //        await receiveResults.SendHandler(data, receiveResults.ReceivedData.Metadata);
            //    }
            //});
        }

        public override Task WriteData(ITransport transport, ITransportMetadata metadata, CancellationToken cancellationToken, Action<IProtocolWriter> handler)
        {
            throw new NotImplementedException();
        }

        public override Task WriteData(ITransport transport, string messageId, ITransportMetadata metadata, CancellationToken cancellationToken, Action<IProtocolWriter> handler)
        {
            throw new NotImplementedException();
        }

        public override Task ReadData(ITransport transport, CancellationToken cancellationToken, Action<IProtocolReader, ITransportMetadata> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<TResult> ReadData<TResult>(ITransport transport, CancellationToken cancellationToken, Func<IProtocolReader, ITransportMetadata, TResult> handler)
        {
            throw new NotImplementedException();
        }

        public override Task ReadData(ITransport transport, string messageId, CancellationToken cancellationToken, Action<IProtocolReader, ITransportMetadata> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<TResult> ReadData<TResult>(ITransport transport, string messageId, CancellationToken cancellationToken, Func<IProtocolReader, ITransportMetadata, TResult> handler)
        {
            throw new NotImplementedException();
        }

        public override Task ReadTaggedData(ITransport transport, CancellationToken cancellationToken, Action<IProtocolReader, string, ITransportMetadata> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<TResult> ReadTaggedData<TResult>(ITransport transport, CancellationToken cancellationToken, Func<IProtocolReader, string, ITransportMetadata, TResult> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<Func<Action<IProtocolReader, ITransportMetadata>, Task>> WriteAndReadData(ITransport transport, ITransportMetadata metadata, CancellationToken cancellationToken, Action<IProtocolWriter> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<Func<Func<IProtocolReader, ITransportMetadata, TResult>, Task<TResult>>> WriteAndReadData<TResult>(ITransport transport, ITransportMetadata metadata, CancellationToken cancellationToken, Action<IProtocolWriter> handler)
        {
            throw new NotImplementedException();
        }

        public override Task<Func<Action<IProtocolWriter>, Task>> ReadAndWriteData(ITransport transport, CancellationToken cancellationToken, Action<IProtocolReader, ITransportMetadata> handler)
        {
            throw new NotImplementedException();
        }
    }

    public interface IMessagePackProtocolReader : IProtocolReader
    {
        Stream DecoderStream { get; }
    }

    public class MessagePackProtocolReader : AProtocolReader, IMessagePackProtocolReader
    {
        public Stream DecoderStream { get; private set; }

        public MessagePackProtocolReader(Stream decoderStream)
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

        public override RequestHeader ReadRequestStart()
        {
            var actionName = this.ReadStringValue();
            var argumentCount = this.ReadIntegerValue();

            return new RequestHeader(actionName, argumentCount);
        }
        public override void ReadRequestEnd()
        {
        }

        public override RequestArgumentHeader ReadRequestArgumentStart()
        {
            var argumentName = this.ReadStringValue();
            var type = this.ReadStringValue();

            return new RequestArgumentHeader(argumentName, type);
        }
        public override void ReadRequestArgumentEnd()
        {
        }

        public override ResponseHeader ReadResponseStart()
        {
            var success = this.ReadBooleanValue();
            var type = this.ReadStringValue();
            var argumentCount = this.ReadIntegerValue();

            return new ResponseHeader(success, type, argumentCount);
        }
        public override void ReadResponseEnd()
        {
        }

        public override ResponseArgumentHeader ReadResponseArgumentStart()
        {
            var argumentName = this.ReadStringValue();
            var type = this.ReadStringValue();

            return new ResponseArgumentHeader(argumentName, type);
        }
        public override void ReadResponseArgumentEnd()
        {
        }

        public override ModelHeader ReadModelStart()
        {
            var modelName = this.ReadStringValue();
            var propertyCount = this.ReadIntegerValue();

            return new ModelHeader(modelName, propertyCount);
        }
        public override void ReadModelEnd()
        {
        }

        public override ModelPropertyHeader ReadModelPropertyStart()
        {
            var propertyName = this.ReadStringValue();
            var type = this.ReadStringValue();

            return new ModelPropertyHeader(propertyName, type);
        }
        public override void ReadModelPropertyEnd()
        {
        }

        public override ArrayHeader ReadArrayStart()
        {
            var itemCount = this.ReadIntegerValue();

            return new ArrayHeader(itemCount);
        }
        public override void ReadArrayEnd()
        {
        }

        public override ArrayItemHeader ReadArrayItemStart()
        {
            var type = this.ReadStringValue();

            return new ArrayItemHeader(type);
        }
        public override void ReadArrayItemEnd()
        {
        }

        public override DictionaryHeader ReadDictionaryStart()
        {
            var recordCount = this.ReadIntegerValue();

            return new DictionaryHeader(recordCount);
        }
        public override void ReadDictionaryEnd()
        {
        }

        public override DictionaryItemHeader ReadDictionaryItemStart()
        {
            var keyType = this.ReadStringValue();
            var valueType = this.ReadStringValue();

            return new DictionaryItemHeader(keyType, valueType);
        }
        public override void ReadDictionaryItemEnd()
        {
        }

        public override IndefiniteValueHeader ReadIndefiniteValueStart()
        {
            var valueType = this.ReadStringValue();

            return new IndefiniteValueHeader(valueType);
        }
        public override void ReadIndefiniteValueEnd()
        {
        }
    }

    public interface IMessagePackProtocolWriter : IProtocolWriter
    {
        Stream EncoderStream { get; }
    }

    public class MessagePackProtocolWriter : AProtocolWriter, IMessagePackProtocolWriter
    {
        public Stream EncoderStream { get; private set; }

        public MessagePackProtocolWriter(Stream encoderStream)
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

        public override void WriteRequestStart(RequestHeader header)
        {
            this.WriteStringValue(header.ActionName);
            this.WriteIntegerValue(header.ArgumentCount);
        }
        public override void WriteRequestEnd()
        {
        }

        public override void WriteRequestArgumentStart(RequestArgumentHeader header)
        {
            this.WriteStringValue(header.ArgumentName);
            this.WriteStringValue(header.Type);
        }
        public override void WriteRequestArgumentEnd()
        {
        }

        public override void WriteResponseStart(ResponseHeader header)
        {
            this.WriteBooleanValue(header.Success);
            this.WriteStringValue(header.Type);
            this.WriteIntegerValue(header.ArgumentCount);
        }
        public override void WriteResponseEnd()
        {
        }

        public override void WriteResponseArgumentStart(ResponseArgumentHeader header)
        {
            this.WriteStringValue(header.ArgumentName);
            this.WriteStringValue(header.Type);
        }
        public override void WriteResponseArgumentEnd()
        {
        }

        public override void WriteModelStart(ModelHeader header)
        {
            this.WriteStringValue(header.ModelName);
            this.WriteIntegerValue(header.PropertyCount);
        }
        public override void WriteModelEnd()
        {
        }

        public override void WriteModelPropertyStart(ModelPropertyHeader header)
        {
            this.WriteStringValue(header.PropertyName);
            this.WriteStringValue(header.Type);
        }
        public override void WriteModelPropertyEnd()
        {
        }

        public override void WriteArrayStart(ArrayHeader header)
        {
            this.WriteIntegerValue(header.ItemCount);
        }
        public override void WriteArrayEnd()
        {
        }

        public override void WriteArrayItemStart(ArrayItemHeader header)
        {
            this.WriteStringValue(header.Type);
        }
        public override void WriteArrayItemEnd()
        {
        }

        public override void WriteDictionaryStart(DictionaryHeader header)
        {
            this.WriteIntegerValue(header.RecordCount);
        }
        public override void WriteDictionaryEnd()
        {
        }

        public override void WriteDictionaryItemStart(DictionaryItemHeader header)
        {
            this.WriteStringValue(header.KeyType);
            this.WriteStringValue(header.ValueType);
        }
        public override void WriteDictionaryItemEnd()
        {
        }

        public override void WriteIndefiniteValueStart(IndefiniteValueHeader header)
        {
            this.WriteStringValue(header.ValueType);
        }
        public override void WriteIndefiniteValueEnd()
        {
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
