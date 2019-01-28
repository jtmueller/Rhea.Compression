// -----------------------------------------------------------------------
//  <copyright file="PrefixHash.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Buffers;

namespace Rhea.Compression.Dictionary
{
    public class PrefixHash : IDisposable
    {
        public static int PrefixLength = 4;

        private readonly IMemoryOwner<int> _hashHandle;
        private readonly IMemoryOwner<int> _heapHandle;
        private readonly IMemoryOwner<byte>? _bufferHandle;
        private readonly ReadOnlyMemory<byte> _buffer;
        private readonly Memory<int> _hash;
        private readonly Memory<int> _heap;

        public PrefixHash(Span<byte> buf, bool addToHash)
        {
            _bufferHandle = MemoryPool<byte>.Shared.Rent(buf.Length);
            var buffer = _bufferHandle.Memory.Slice(0, buf.Length);
            buf.CopyTo(buffer.Span);

            _buffer = buffer;
            int hashLen = (int)(1.75 * buf.Length);
            _hashHandle = MemoryPool<int>.Shared.Rent(hashLen);
            _hash = _hashHandle.Memory.Slice(0, hashLen);

            _heapHandle = MemoryPool<int>.Shared.Rent(buf.Length);
            _heap = _heapHandle.Memory.Slice(0, buf.Length);

            Init(addToHash);
        }

        public PrefixHash(ReadOnlyMemory<byte> buf, bool addToHash)
        {
            _buffer = buf;
            int hashLen = (int)(1.75 * buf.Length);
            _hashHandle = MemoryPool<int>.Shared.Rent(hashLen);
            _hash = _hashHandle.Memory.Slice(0, hashLen);

            _heapHandle = MemoryPool<int>.Shared.Rent(buf.Length);
            _heap = _heapHandle.Memory.Slice(0, buf.Length);

            Init(addToHash);
        }

        private void Init(bool addToHash)
        {
            var hash = _hash.Span;
            for (int i = 0; i < hash.Length; i++)
            {
                hash[i] = -1;
            }

            var heap = _heap.Span;
            for (int i = 0; i < heap.Length; i++)
            {
                heap[i] = -1;
            }
            if (addToHash)
            {
                for (int i = 0, count = _buffer.Length - PrefixLength; i < count; i++)
                {
                    Put(i);
                }
            }
        }

        public void DumpState(TextWriter output)
        {
            output.WriteLine("Hash:");
            var hash = _hash.Span;
            for (int i = 0; i < _hash.Length; i++)
            {
                if (hash[i] == -1)
                    continue;
                output.WriteLine("hash[{0,3}] = {1,3};", i, hash[i]);
            }

            output.WriteLine("Heap:");
            var heap = _heap.Span;
            for (int i = 0; i < _heap.Length; i++)
            {
                if (heap[i] == -1)
                    continue;
                output.WriteLine("heap[{0,3}] = {1,3};", i, heap[i]);
            }
        }

        private int HashIndex(ReadOnlySpan<byte> buf, int i)
        {
            int code = (buf[i] & 0xff) | ((buf[i + 1] & 0xff) << 8) | ((buf[i + 2] & 0xff) << 16) |
                       ((buf[i + 3] & 0xff) << 24);
            return (code & 0x7fffff) % _hash.Length;
        }

        public void Put(int index)
        {
            int hi = HashIndex(_buffer.Span, index);
            var heap = _heap.Span;
            var hash = _hash.Span;
            heap[index] = hash[hi];
            hash[hi] = index;
        }

        public Match GetBestMatch(int index, ReadOnlySpan<byte> targetBuf)
        {
            int bestMatchIndex = 0;
            int bestMatchLength = 0;

            int bufLen = _buffer.Length;

            if (bufLen == 0)
            {
                return new Match(0, 0);
            }

            var bufferBytes = _buffer.Span;
            int targetBufLen = targetBuf.Length;

            int maxLimit = Math.Min(255, targetBufLen - index);

            int targetHashIndex = HashIndex(targetBuf, index);
            int candidateIndex = _hash.Span[targetHashIndex];
            var heap = _heap.Span;
            while (candidateIndex >= 0)
            {
                int distance;
                if (targetBuf.Length != bufferBytes.Length || !targetBuf.SequenceEqual(bufferBytes))
                {
                    distance = index + bufLen - candidateIndex;
                }
                else
                {
                    distance = index - candidateIndex;
                }
                if (distance > (2 << 15) - 1)
                {
                    // Since we are iterating over nearest offsets first, once we pass 64k
                    // we know the rest are over 64k too.
                    break;
                }

                int maxMatchJ = index + Math.Min(maxLimit, bufLen - candidateIndex);
                int j, k;
                for (j = index, k = candidateIndex; j < maxMatchJ; j++, k++)
                {
                    if (bufferBytes[k] != targetBuf[j])
                    {
                        break;
                    }
                }

                int matchLength = j - index;
                if (matchLength > bestMatchLength)
                {
                    bestMatchIndex = candidateIndex;
                    bestMatchLength = matchLength;
                }
                candidateIndex = heap[candidateIndex];
            }

            return new Match(bestMatchIndex, bestMatchLength);
        }

        public void Dispose()
        {
            _hashHandle.Dispose();
            _heapHandle.Dispose();
            _bufferHandle?.Dispose();
        }

        public readonly struct Match
        {
            public readonly int BestMatchIndex;
            public readonly int BestMatchLength;

            public Match(int bestMatchIndex, int bestMatchLength)
            {
                BestMatchIndex = bestMatchIndex;
                BestMatchLength = bestMatchLength;
            }
        }
    }
}