using Dekaf.Admin;
using Dekaf.Compression.Lz4;
using Dekaf.Producer;
using Dekaf.Protocol.Records;
using Dekaf.SchemaRegistry;
using Dekaf.SchemaRegistry.Protobuf;
using Dekaf.Tests.Unit.SchemaRegistry;
using NSubstitute;

namespace Dekaf.Tests.Unit.Builder;

public sealed class ObsoleteBuilderMethodsTests
{
    [Test]
    public async Task ProducerUseTls_DelegatesToWithTls()
    {
#pragma warning disable CS0618 // Verifies the compatibility forwarder.
        await using var obsoleteProducer = Kafka.CreateProducer<string, string>()
            .UseTls()
#pragma warning restore CS0618
            .WithBootstrapServers("localhost:9092")
            .Build();

        await using var modernProducer = Kafka.CreateProducer<string, string>()
            .WithTls()
            .WithBootstrapServers("localhost:9092")
            .Build();

        var obsoleteOptions = GetPrivateField<ProducerOptions>(obsoleteProducer, "_options");
        var modernOptions = GetPrivateField<ProducerOptions>(modernProducer, "_options");

        await Assert.That(obsoleteOptions.UseTls).IsTrue();
        await Assert.That(obsoleteOptions.UseTls).IsEqualTo(modernOptions.UseTls);
    }

    [Test]
    public async Task ProducerUseCompression_DelegatesToWithCompression()
    {
#pragma warning disable CS0618 // Verifies the compatibility forwarder.
        await using var obsoleteProducer = Kafka.CreateProducer<string, string>()
            .UseCompression(CompressionType.Zstd)
#pragma warning restore CS0618
            .WithBootstrapServers("localhost:9092")
            .Build();

        await using var modernProducer = Kafka.CreateProducer<string, string>()
            .WithCompression(CompressionType.Zstd)
            .WithBootstrapServers("localhost:9092")
            .Build();

        var obsoleteOptions = GetPrivateField<ProducerOptions>(obsoleteProducer, "_options");
        var modernOptions = GetPrivateField<ProducerOptions>(modernProducer, "_options");

        await Assert.That(obsoleteOptions.CompressionType).IsEqualTo(CompressionType.Zstd);
        await Assert.That(obsoleteOptions.CompressionType).IsEqualTo(modernOptions.CompressionType);
    }

    [Test]
    public async Task ProducerUseLz4Compression_DelegatesToWithLz4Compression()
    {
#pragma warning disable CS0618 // Verifies the compatibility forwarder.
        await using var obsoleteProducer = Kafka.CreateProducer<string, string>()
            .UseLz4Compression()
#pragma warning restore CS0618
            .WithBootstrapServers("localhost:9092")
            .Build();

        await using var modernProducer = Kafka.CreateProducer<string, string>()
            .WithLz4Compression()
            .WithBootstrapServers("localhost:9092")
            .Build();

        var obsoleteOptions = GetPrivateField<ProducerOptions>(obsoleteProducer, "_options");
        var modernOptions = GetPrivateField<ProducerOptions>(modernProducer, "_options");

        await Assert.That(obsoleteOptions.CompressionType).IsEqualTo(CompressionType.Lz4);
        await Assert.That(obsoleteOptions.CompressionType).IsEqualTo(modernOptions.CompressionType);
    }

    [Test]
    public async Task AdminUseMutualTls_DelegatesToWithMutualTls()
    {
#pragma warning disable CS0618 // Verifies the compatibility forwarder.
        await using var obsoleteAdmin = new AdminClientBuilder()
            .UseMutualTls("ca.pem", "client.pem", "client.key", "secret")
#pragma warning restore CS0618
            .WithBootstrapServers("localhost:9092")
            .Build();

        await using var modernAdmin = new AdminClientBuilder()
            .WithMutualTls("ca.pem", "client.pem", "client.key", "secret")
            .WithBootstrapServers("localhost:9092")
            .Build();

        var obsoleteOptions = GetPrivateField<AdminClientOptions>(obsoleteAdmin, "_options");
        var modernOptions = GetPrivateField<AdminClientOptions>(modernAdmin, "_options");

        await Assert.That(obsoleteOptions.UseTls).IsEqualTo(modernOptions.UseTls);
        await Assert.That(obsoleteOptions.TlsConfig!.CaCertificatePath)
            .IsEqualTo(modernOptions.TlsConfig!.CaCertificatePath);
        await Assert.That(obsoleteOptions.TlsConfig.ClientCertificatePath)
            .IsEqualTo(modernOptions.TlsConfig.ClientCertificatePath);
        await Assert.That(obsoleteOptions.TlsConfig.ClientKeyPath)
            .IsEqualTo(modernOptions.TlsConfig.ClientKeyPath);
        await Assert.That(obsoleteOptions.TlsConfig.ClientKeyPassword)
            .IsEqualTo(modernOptions.TlsConfig.ClientKeyPassword);
    }

    [Test]
    public async Task ProducerUseProtobufSchemaRegistry_DelegatesToWithProtobufSchemaRegistry()
    {
        var schemaRegistry = Substitute.For<ISchemaRegistryClient>();

#pragma warning disable CS0618 // Verifies the compatibility forwarder.
        var obsoleteBuilder = Kafka.CreateProducer<string, TestMessage>()
            .UseProtobufSchemaRegistry(schemaRegistry);
#pragma warning restore CS0618
        var modernBuilder = Kafka.CreateProducer<string, TestMessage>()
            .WithProtobufSchemaRegistry(schemaRegistry);

        await Assert.That(GetPrivateField<object>(obsoleteBuilder, "_valueSerializer"))
            .IsTypeOf<ProtobufSchemaRegistrySerializer<TestMessage>>();
        await Assert.That(GetPrivateField<object>(modernBuilder, "_valueSerializer"))
            .IsTypeOf<ProtobufSchemaRegistrySerializer<TestMessage>>();
    }

    private static TField GetPrivateField<TField>(object target, string fieldName)
    {
        var field = target.GetType().GetField(
            fieldName,
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?? throw new InvalidOperationException($"Could not find {fieldName} field.");

        return (TField)field.GetValue(target)!;
    }
}
