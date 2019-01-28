// -----------------------------------------------------------------------
//  <copyright file="DictionaryOptimizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Collections.Pooled;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Rhea.Compression.Dictionary
{
    public class DictionaryOptimizer : IDisposable
    {
        private readonly PooledList<byte> _stream = new PooledList<byte>();
        private readonly PooledList<int> _starts = new PooledList<int>();
        private int[] _suffixArray = new int[0];
        private int[] _lcp = new int[0];
        private SubstringArray? _substrings;

        public void Add(ReadOnlySpan<byte> doc)
        {
            _starts.Add(_stream.Count);
            _stream.AddRange(doc);
        }

        public DocumentEnumerable Documents
            => new DocumentEnumerable(_stream.Span, _starts.Span);

        public void Add(string doc)
        {
#if NETCOREAPP2_1
            Add(doc.AsSpan());
#else
            Add(Encoding.UTF8.GetBytes(doc));
#endif
        }

#if NETCOREAPP2_1
        public void Add(ReadOnlySpan<char> doc)
        {
            var maxBytes = Encoding.UTF8.GetMaxByteCount(doc.Length);
            using (var bytes = new PooledList<byte>(maxBytes))
            {
                var span = bytes.AddSpan(maxBytes);
                var bytesWritten = Encoding.UTF8.GetBytes(doc, span);
                Add(span.Slice(0, bytesWritten));
            }
        }
#endif

        public void DumpSuffixes(TextWriter output)
        {
            if (_suffixArray != null)
            {
                for (int i = 0; i < _suffixArray.Length; i++)
                {
                    output.Write(_suffixArray[i] + "\t");
                    output.Write(_lcp[i] + "\t");
#if NETCOREAPP2_1
                    var bytes = _stream.Span.Slice(_suffixArray[i], Math.Min(40, _stream.Count - _suffixArray[i]));
                    output.Write(Encoding.UTF8.GetString(bytes));
#else
                    var bytes = _stream.ToArray();
                    output.Write(Encoding.UTF8.GetString(bytes, _suffixArray[i], Math.Min(40, bytes.Length - _suffixArray[i])));
#endif
                    output.WriteLine();
                }
            }
        }

        public void DumpSubstrings(TextWriter output)
        {
            if (_substrings != null)
            {
                for (int j = _substrings.Size - 1; j >= 0; j--)
                {
                    if (_substrings.Length(j) == 0)
                        continue;
                    output.Write(_substrings.Score(j) + "\t");
#if NETCOREAPP2_1
                    var bytes = _stream.Span.Slice(_suffixArray[_substrings.Index(j)], Math.Min(40, _substrings.Length(j)));
                    output.Write(Encoding.UTF8.GetString(bytes));
#else
                    output.Write(Encoding.UTF8.GetString(_stream.ToArray(), _suffixArray[_substrings.Index(j)], Math.Min(40, _substrings.Length(j))));
#endif
                    output.WriteLine();
                }
            }
        }

        public ReadOnlyMemory<byte> Optimize(int desiredLength)
        {
            var bytes = _stream.Span;
            _suffixArray = SuffixArray.ComputeSuffixArray(bytes);
            _lcp = SuffixArray.ComputeLCP(bytes, _suffixArray);
            ComputeSubstrings();
            return Pack(bytes, desiredLength);
        }

        private ReadOnlyMemory<byte> Pack(ReadOnlySpan<byte> bytes, int desiredLength)
        {
            if (_substrings is null)
                throw new InvalidOperationException("Substrings must be initialized before calling Pack.");

            using (var pruned = new SubstringArray(1024))
            {
                int i, size = 0;

                for (i = _substrings.Size - 1; i >= 0; i--)
                {
                    bool alreadyCovered = false;
                    for (int j = 0, c = pruned.Size; j < c; j++)
                    {
                        if (pruned.IndexOf(j, _substrings, i, bytes, _suffixArray) != -1)
                        {
                            alreadyCovered = true;
                            break;
                        }
                    }

                    if (alreadyCovered)
                    {
                        continue;
                    }

                    for (int j = pruned.Size - 1; j >= 0; j--)
                    {
                        if (_substrings.IndexOf(i, pruned, j, bytes, _suffixArray) != -1)
                        {
                            size -= pruned.Length(j);
                            pruned.Remove(j);
                        }
                    }
                    pruned.SetScore(pruned.Size, _substrings.Index(i), _substrings.Length(i), _substrings.Score(i));
                    size += _substrings.Length(i);
                    // We calculate 2x because when we lay the strings out end to end we will merge common prefix/suffixes
                    if (size >= 2 * desiredLength)
                    {
                        break;
                    }
                }

                byte[] packed = new byte[desiredLength];
                int pi = desiredLength;

                int count;
                for (i = 0, count = pruned.Size; i < count && pi > 0; i++)
                {
                    int length = pruned.Length(i);
                    if (pi - length < 0)
                    {
                        length = pi;
                    }
                    pi -= Prepend(bytes, _suffixArray[pruned.Index(i)], packed, pi, length);
                }

                if (pi > 0)
                {
                    return packed.AsMemory(pi);
                }

                return packed;
            }
        }

        private int Prepend(ReadOnlySpan<byte> from, int fromIndex, byte[] to, int toIndex, int length)
        {
            int l;
            // See if we have a common suffix/prefix between the string being merged in, and the current strings in the front
            // of the destination.  For example if we pack " the " and then pack " and ", we should end up with " and the ", not " and  the ".
            for (l = Math.Min(length - 1, to.Length - toIndex); l > 0; l--)
            {
                if (from.Slice(fromIndex + length - l, l).SequenceEqual(to.AsSpan(toIndex, l)))
                {
                    break;
                }
            }

            from.Slice(fromIndex, length - l)
                .CopyTo(to.AsSpan(toIndex - length + l, length - l));
            return length - l;
        }

        public string Suffix(int i)
        {
            var x = _suffixArray[i];
#if NETCOREAPP2_1
            var bytes = _stream.Span.Slice(x, Math.Min(15, _stream.Count - x));
            return Encoding.UTF8.GetString(bytes);
#else
            var bytes = _stream.ToArray();
            return Encoding.UTF8.GetString(bytes, x, Math.Min(15, bytes.Length - x));
#endif
        }

        // TODO Bring this up to parity with C++ version, which has optimized
        private void ComputeSubstrings()
        {
            using (var activeSubstrings = new SubstringArray(128))
            {
                var uniqueDocIds = new HashSet<int>();

                _substrings?.Dispose();
                _substrings = new SubstringArray(1024);
                int n = _lcp.Length;

                int lastLCP = _lcp[0];
                for (int i = 1; i <= n; i++)
                {
                    // Note we need to process currently existing runs, so we do that by acting like we hit an LCP of 0 at the end.
                    // That is why the we loop i <= n vs i < n.  Otherwise runs that exist at the end of the suffixarray/lcp will
                    // never be "cashed in" and counted in the substrings.  DictionaryOptimizerTest has a unit test for this.
                    int currentLCP = i == n ? 0 : _lcp[i];

                    if (currentLCP > lastLCP)
                    {
                        // The order here is important so we can optimize adding redundant strings below.
                        for (int j = lastLCP + 1; j <= currentLCP; j++)
                        {
                            activeSubstrings.Add(i, j, 0);
                        }
                    }
                    else if (currentLCP < lastLCP)
                    {
                        int lastActiveIndex = -1, lastActiveLength = -1, lastActiveCount = -1;
                        for (int j = activeSubstrings.Size - 1; j >= 0; j--)
                        {
                            if (activeSubstrings.Length(j) > currentLCP)
                            {
                                int activeCount = i - activeSubstrings.Index(j) + 1;
                                int activeLength = activeSubstrings.Length(j);
                                int activeIndex = activeSubstrings.Index(j);

                                // Ok we have a string which occurs activeCount times.  The true measure of its
                                // value is how many unique documents it occurs in, because occurring 1000 times in the same
                                // document isn't valuable because once it occurs once, subsequent occurrences will reference
                                // a previous occurring instance in the document.  So for 2 documents: "garrick garrick garrick toubassi",
                                // "toubassi", the string toubassi is far more valuable in a shared dictionary.  So find out
                                // how many unique documents this string occurs in.  We do this by taking the start position of
                                // each occurrence, and then map that back to the document using the "starts" array, and uniquing.
                                // 
                                // TODO Bring this up to parity with C++ version, which has optimized
                                //

                                for (int k = activeSubstrings.Index(j) - 1; k < i; k++)
                                {
                                    int byteIndex = _suffixArray[k];

                                    // Could make this a lookup table if we are willing to burn an int[bytes.length] but thats a lot
                                    int docIndex = _starts.BinarySearch(byteIndex);

                                    if (docIndex < 0)
                                    {
                                        docIndex = -docIndex - 2;
                                    }

                                    // While we are at it lets make sure this is a string that actually exists in a single
                                    // document, vs spanning two concatenanted documents.  The idea is that for documents
                                    // "http://espn.com", "http://google.com", "http://yahoo.com", we don't want to consider
                                    // ".comhttp://" to be a legal string.  So make sure the length of this string doesn't
                                    // cross a document boundary for this particular occurrence.
                                    int nextDocStart = docIndex < _starts.Count - 1 ? _starts[docIndex + 1] : _stream.Count;
                                    if (activeLength <= nextDocStart - byteIndex)
                                    {
                                        uniqueDocIds.Add(docIndex);
                                    }
                                }

                                int scoreCount = uniqueDocIds.Count;

                                uniqueDocIds.Clear();

                                activeSubstrings.Remove(j);

                                if (scoreCount == 0)
                                {
                                    continue;
                                }

                                // Don't add redundant strings.  If we just  added ABC, don't add AB if it has the same count.  This cuts down the size of substrings
                                // from growing very large.
                                if (!(lastActiveIndex != -1 && lastActiveIndex == activeIndex && lastActiveCount == activeCount && lastActiveLength > activeLength))
                                {

                                    if (activeLength > 3)
                                    {
                                        _substrings.Add(activeIndex, activeLength, scoreCount);
                                    }
                                }
                                lastActiveIndex = activeIndex;
                                lastActiveLength = activeLength;
                                lastActiveCount = activeCount;
                            }
                        }
                    }
                    lastLCP = currentLCP;
                }
                _substrings.Sort();
            }
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _starts?.Dispose();
            _substrings?.Dispose();
        }

        public ref struct DocumentEnumerable
        {
            private readonly Span<byte> _bytes;
            private readonly Span<int> _starts;

            internal DocumentEnumerable(Span<byte> bytes, Span<int> starts)
            {
                _bytes = bytes;
                _starts = starts;
            }

            public DocumentEnumerator GetEnumerator()
                => new DocumentEnumerator(_bytes, _starts);
        }

        public ref struct DocumentEnumerator
        {
            private readonly Span<byte> _bytes;
            private readonly Span<int> _starts;
            private Span<byte> _current;
            private int _index;
            private int _lastStart;

            internal DocumentEnumerator(Span<byte> bytes, Span<int> starts)
            {
                _bytes = bytes;
                _starts = starts;
                _lastStart = starts[0];
                _index = 1;
                _current = Span<byte>.Empty;
            }

            public Span<byte> Current => _current;

            public bool MoveNext()
            {
                if (_starts.Length == 0)
                {
                    _current = _bytes;
                    _index = -1;
                    return true;
                }
                else if ((uint)_index < (uint)_starts.Length)
                {
                    _current = _bytes.Slice(_lastStart, _starts[_index] - _lastStart);
                    _lastStart = _starts[_index];
                    _index++;
                    return true;
                }

                return false;
            }
        }
    }
}