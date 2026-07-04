// Polyfill for System.Buffers.Text.Base64Url (net9.0+). URL-safe base64 without padding.
// Internal and #if-gated so it compiles to nothing on net9.0+. Linked into every library project.

#if !NET9_0_OR_GREATER
namespace System.Buffers.Text
{
    internal static class Base64Url
    {
        public static string EncodeToString(ReadOnlySpan<byte> source)
        {
            if (source.IsEmpty)
            {
                return string.Empty;
            }

            string base64 = Convert.ToBase64String(
#if NETSTANDARD2_0
                source.ToArray()
#else
                source
#endif
            );

            int length = base64.Length;
            while (length > 0 && base64[length - 1] == '=')
            {
                length--;
            }

            var chars = new char[length];
            for (int i = 0; i < length; i++)
            {
                chars[i] = base64[i] switch
                {
                    '+' => '-',
                    '/' => '_',
                    var c => c,
                };
            }

            return new string(chars);
        }
    }
}
#endif
