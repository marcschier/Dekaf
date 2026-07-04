// Polyfill for X509CertificateLoader (net9.0+) covering the members Dekaf uses.
// On earlier targets it delegates to the (non-obsolete there) X509Certificate2 constructors.
// Internal and #if-gated so it compiles to nothing on net9.0+. Linked into every library project.

#if !NET9_0_OR_GREATER
namespace System.Security.Cryptography.X509Certificates
{
    internal static class X509CertificateLoader
    {
        public static X509Certificate2 LoadPkcs12FromFile(string path, string? password)
            => new X509Certificate2(path, password);

        public static X509Certificate2 LoadPkcs12(byte[] data, string? password)
            => new X509Certificate2(data, password);

        public static X509Certificate2 LoadCertificateFromFile(string path)
            => new X509Certificate2(path);
    }
}
#endif
