﻿using System.IO;
using System.Text;
using Rhea.Compression.Debugging;
using Rhea.Compression.Dictionary;
using Rhea.Compression.Huffman;
using System;
#if NETSTANDARD2_1 || NETCOREAPP2_1
using System.Buffers;
#endif


namespace Rhea.Compression
{
    public class CompressionHandler
    {
        private readonly byte[] _dictionary;
        private readonly HuffmanPacker _packer;
        private readonly SubstringPacker _substringPacker;

        public void Save(Stream stream)
        {
            using (var bw = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
            {
                // 1337 - signature that this is us
                bw.Write7BitEncodedInt(1337);
                bw.Write7BitEncodedInt(1); // version
                bw.Write7BitEncodedInt(_dictionary.Length);
                bw.Write(_dictionary);
                _packer.Save(bw);
            }
        }

        public static CompressionHandler Load(Stream stream)
        {
            using (var br = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true))
            {
                if (br.Read7BitEncodedInt() != 1337)
                    throw new InvalidDataException("Not a saved compression handler");
                if (br.Read7BitEncodedInt() != 1)
                    throw new InvalidDataException("Not a known version");

                var dicLen = br.Read7BitEncodedInt();
                var readBytes = br.ReadBytes(dicLen);
                var packer = HuffmanPacker.Load(br);

                return new CompressionHandler(readBytes, packer);
            }
        }

        public CompressionHandler(byte[] dictionary, HuffmanPacker packer)
        {
            _dictionary = dictionary;
            _packer = packer;
            _substringPacker = new SubstringPacker(_dictionary);
        }

        public int Compress(string input, Stream output)
        {
#if NETSTANDARD2_1 || NETCOREAPP2_1
            var encoding = Encoding.UTF8;
            var maxBytes = encoding.GetMaxByteCount(input.Length);
            using (var memoryHandle = MemoryPool<byte>.Shared.Rent(maxBytes))
            {
                var bytes = memoryHandle.Memory;
                var encodedBytes = encoding.GetBytes(input, bytes.Span);
                return Compress(bytes.Slice(0, encodedBytes), output);
            }
#else
            return Compress(Encoding.UTF8.GetBytes(input), output);
#endif
        }

#if NETSTANDARD2_1 || NETCOREAPP2_1
        public int Compress(ReadOnlySpan<char> input, Stream output)
        {
            var encoding = Encoding.UTF8;
            var maxBytes = encoding.GetMaxByteCount(input.Length);
            using (var memoryHandle = MemoryPool<byte>.Shared.Rent(maxBytes))
            {
                var bytes = memoryHandle.Memory;
                var encodedBytes = encoding.GetBytes(input, bytes.Span);
                return Compress(bytes.Slice(0, encodedBytes), output);
            }
        }
#endif

#if NETSTANDARD2_1 || NETCOREAPP2_1
        public string CompressDebug(ReadOnlySpan<char> input)
        {
            var encoding = Encoding.UTF8;
            var maxBytes = encoding.GetMaxByteCount(input.Length);
            using (var memoryHandle = MemoryPool<byte>.Shared.Rent(maxBytes))
            {
                var bytes = memoryHandle.Memory;
                var encodedBytes = encoding.GetBytes(input, bytes.Span);
                var consumerContext = new StringWriter();
                _substringPacker.Pack(bytes.Slice(0, encodedBytes), new DebugPackerOutput(), consumerContext);
                return consumerContext.GetStringBuilder().ToString();
            }
        }
#else
        public string CompressDebug(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var consumerContext = new StringWriter();
            _substringPacker.Pack(bytes, new DebugPackerOutput(), consumerContext);
            return consumerContext.GetStringBuilder().ToString();
        }
#endif

#if NETSTANDARD2_1 || NETCOREAPP2_1
        public int Compress(ReadOnlyMemory<byte> input, Stream output)
#else
        public int Compress(byte[] input, Stream output)
#endif
        {
            using (var outputBitStream = new OutputBitStream(output, leaveOpen: true))
            {
                _substringPacker.Pack(input, _packer, outputBitStream);
                return outputBitStream.Length / 8;
            }
        }

        public byte[] Decompress(Stream compressed)
        {
            using (var bitStream = new InputBitStream(compressed, leaveOpen: true))
            {
                var unpacker = new SubstringUnpacker(_dictionary);
                _packer.Unpack(bitStream, unpacker);
                return unpacker.UncompressedData();
            }
        }

        public string DecompressDebug(string input)
        {
            var substringUnpacker = new SubstringUnpacker(_dictionary);
            var debugUnpackerOutput = new DebugUnpackerOutput(new StringReader(input), substringUnpacker);
            debugUnpackerOutput.Unpack();
            var uncompressedData = substringUnpacker.UncompressedData();
            return Encoding.UTF8.GetString(uncompressedData);
        }
    }
}