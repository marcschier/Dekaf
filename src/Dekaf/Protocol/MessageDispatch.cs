#if !NET7_0_OR_GREATER
using System.Collections.Concurrent;
using System.Reflection;
#endif

namespace Dekaf.Protocol;

/// <summary>
/// Bridges the connection dispatch to the per-message protocol metadata.
/// On net7.0+ this forwards directly to the static abstract members on
/// <see cref="IKafkaRequest{TResponse}"/> / <see cref="IKafkaResponse"/> (zero
/// overhead, identical IL to calling them inline). On target frameworks without
/// static abstract interface members (netstandard2.0/2.1) it resolves the same
/// information from the concrete message types via cached reflection and a
/// generated <see cref="ApiKey"/>-keyed response reader.
/// </summary>
internal static class MessageDispatch
{
    public static ApiKey GetApiKey<TRequest, TResponse>()
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
#if NET7_0_OR_GREATER
        => TRequest.ApiKey;
#else
        => MessageMeta.GetApiKey(typeof(TRequest));
#endif

    public static short GetRequestHeaderVersion<TRequest, TResponse>(short version)
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
#if NET7_0_OR_GREATER
        => TRequest.GetRequestHeaderVersion(version);
#else
        => MessageMeta.GetRequestHeaderVersion(typeof(TRequest), version);
#endif

    public static short GetResponseHeaderVersion<TRequest, TResponse>(short version)
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
#if NET7_0_OR_GREATER
        => TRequest.GetResponseHeaderVersion(version);
#else
        => MessageMeta.GetResponseHeaderVersion(typeof(TRequest), version);
#endif

    public static TResponse ReadResponse<TRequest, TResponse>(ref KafkaProtocolReader reader, short version)
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
#if NET7_0_OR_GREATER
        => (TResponse)TResponse.Read(ref reader, version);
#else
        => (TResponse)GeneratedResponseReader.Read(MessageMeta.GetApiKey(typeof(TRequest)), ref reader, version);
#endif
}

#if !NET7_0_OR_GREATER
/// <summary>
/// Reflection-backed cache of the static protocol metadata that lives on the
/// concrete message types (the <c>ApiKey</c> property and the optional header
/// version overrides). Used only on frameworks that cannot express these as
/// static abstract interface members.
/// </summary>
internal static class MessageMeta
{
    private static readonly ConcurrentDictionary<Type, MetaEntry> Cache = new();

    public static ApiKey GetApiKey(Type type) => Get(type).ApiKey;

    public static short GetRequestHeaderVersion(Type type, short version)
        => Get(type).RequestHeader?.Invoke(version) ?? (short)2;

    public static short GetResponseHeaderVersion(Type type, short version)
        => Get(type).ResponseHeader?.Invoke(version) ?? (short)1;

    private static MetaEntry Get(Type type) => Cache.GetOrAdd(type, static messageType =>
    {
        var property = messageType.GetProperty("ApiKey", BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                $"Message type {messageType} does not expose a public static ApiKey property.");
        var apiKey = (ApiKey)property.GetValue(null)!;
        return new MetaEntry(
            apiKey,
            CreateShortFunc(messageType, "GetRequestHeaderVersion"),
            CreateShortFunc(messageType, "GetResponseHeaderVersion"));
    });

    private static Func<short, short>? CreateShortFunc(Type type, string name)
    {
        var method = type.GetMethod(
            name,
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(short) },
            modifiers: null);
        return method is null
            ? null
            : (Func<short, short>)method.CreateDelegate(typeof(Func<short, short>));
    }

    private sealed class MetaEntry
    {
        public MetaEntry(ApiKey apiKey, Func<short, short>? requestHeader, Func<short, short>? responseHeader)
        {
            ApiKey = apiKey;
            RequestHeader = requestHeader;
            ResponseHeader = responseHeader;
        }

        public ApiKey ApiKey { get; }

        public Func<short, short>? RequestHeader { get; }

        public Func<short, short>? ResponseHeader { get; }
    }
}
#endif
