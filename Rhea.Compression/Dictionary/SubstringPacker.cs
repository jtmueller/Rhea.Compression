// -----------------------------------------------------------------------
//  <copyright file="StringPacker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Rhea.Compression.Dictionary
{
    public class SubstringPacker
    {
        private static readonly int MinimumMatchLength = PrefixHash.PrefixLength;

        private readonly byte[] _dictionary;
        private readonly PrefixHash _dictHash;

        public SubstringPacker(byte[] dictionary)
        {
            _dictionary = dictionary = dictionary ?? new byte[0];
            _dictHash = new PrefixHash(dictionary, true);
        }

        public void Pack(byte[] rawBytes, IPackerOutput packerOutput, object? consumerContext)
            => Pack(rawBytes.AsMemory(), packerOutput, consumerContext);

        public void Pack(ReadOnlyMemory<byte> bytes, IPackerOutput packerOutput, object? consumerContext)
        {
            var rawBytes = bytes.Span;
            var hash = new PrefixHash(bytes, false);
            int dictLen = _dictionary.Length;

            int previousMatchIndex = 0;
            int previousMatchLength = 0;
            
            int curr, count;
            for (curr = 0, count = rawBytes.Length; curr < count; curr++)
            {
                int bestMatchIndex = 0;
                int bestMatchLength = 0;

                if (curr + PrefixHash.PrefixLength - 1 < count)
                {
                    PrefixHash.Match match = _dictHash.GetBestMatch(curr, rawBytes);
                    bestMatchIndex = match.BestMatchIndex;
                    bestMatchLength = match.BestMatchLength;

                    match = hash.GetBestMatch(curr, rawBytes);

                    // Note the >= because we prefer a match that is nearer (and a match
                    // in the string being compressed is always closer than one from the dict).
                    if (match.BestMatchLength >= bestMatchLength)
                    {
                        bestMatchIndex = match.BestMatchIndex + dictLen;
                        bestMatchLength = match.BestMatchLength;
                    }

                    hash.Put(curr);
                }

                if (bestMatchLength < MinimumMatchLength)
                {
                    bestMatchIndex = bestMatchLength = 0;
                }

                if (previousMatchLength > 0 && bestMatchLength <= previousMatchLength)
                {
                    // We didn't get a match or we got one and the previous match is better
                    packerOutput.EncodeSubstring(-(curr + dictLen - 1 - previousMatchIndex), previousMatchLength, consumerContext);

                    // Make sure locations are added for the match.  This allows repetitions to always
                    // encode the same relative locations which is better for compressing the locations.
                    int endMatch = curr - 1 + previousMatchLength;
                    curr++;
                    while (curr < endMatch && curr + PrefixHash.PrefixLength < count)
                    {
                        hash.Put(curr);
                        curr++;
                    }
                    curr = endMatch - 1; // Make sure 'curr' is pointing to the last processed byte so it is at the right place in the next iteration
                    previousMatchIndex = previousMatchLength = 0;
                }
                else if (previousMatchLength > 0 && bestMatchLength > previousMatchLength)
                {
                    // We have a match, and we had a previous match, and this one is better.
                    previousMatchIndex = bestMatchIndex;
                    previousMatchLength = bestMatchLength;
                    packerOutput.EncodeLiteral(rawBytes[curr - 1], consumerContext);
                }
                else if (bestMatchLength > 0)
                {
                    // We have a match, but no previous match
                    previousMatchIndex = bestMatchIndex;
                    previousMatchLength = bestMatchLength;
                }
                else if (bestMatchLength == 0 && previousMatchLength == 0)
                {
                    // No match, and no previous match.
                    packerOutput.EncodeLiteral(rawBytes[curr], consumerContext);
                }
            }
            packerOutput.EndEncoding(consumerContext);
        }

    }
}