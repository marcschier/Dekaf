using System.Runtime.Intrinsics.X86;
using Dekaf.Protocol.Records;
using ArmCrc32 = System.Runtime.Intrinsics.Arm.Crc32;

namespace Dekaf.Tests.Unit.Protocol;

public class Crc32CTests
{
    [Test]
    public async Task Compute_EmptyInput_ReturnsZero()
    {
        await Assert.That(Crc32C.Compute([])).IsEqualTo(0u);
    }

    [Test]
    public async Task Compute_KnownVector_ReturnsCastagnoliChecksum()
    {
        await Assert.That(Crc32C.Compute("123456789"u8)).IsEqualTo(0xE3069283u);
    }

    [Test]
    public async Task Compute_MixedLengths_MatchesBitwiseReference()
    {
        for (var length = 0; length <= 512; length++)
        {
            var data = CreateDeterministicBytes(length);
            var expected = ComputeBitwise(data);

            await Assert.That(Crc32C.Compute(data)).IsEqualTo(expected);
        }
    }

    [Test]
    public async Task ComputeSoftware_MixedLengths_MatchesBitwiseReference()
    {
        for (var length = 0; length <= 512; length++)
        {
            var data = CreateDeterministicBytes(length);
            var expected = ComputeBitwise(data);

            await Assert.That(Crc32C.ComputeSoftware(data)).IsEqualTo(expected);
        }
    }

    [Test]
    public async Task ComputeHardwareX86_WhenSupported_MatchesBitwiseReference()
    {
        if (!Sse42.IsSupported)
            return;

        for (var length = 0; length <= 512; length++)
        {
            var data = CreateDeterministicBytes(length);
            var expected = ComputeBitwise(data);

            await Assert.That(Crc32C.ComputeHardwareX86(data)).IsEqualTo(expected);
        }
    }

    [Test]
    public async Task ComputeHardwareArm_WhenSupported_MatchesBitwiseReference()
    {
        if (!ArmCrc32.IsSupported)
            return;

        for (var length = 0; length <= 512; length++)
        {
            var data = CreateDeterministicBytes(length);
            var expected = ComputeBitwise(data);

            await Assert.That(Crc32C.ComputeHardwareArm(data)).IsEqualTo(expected);
        }
    }

    private static byte[] CreateDeterministicBytes(int length)
    {
        var data = new byte[length];

        for (var i = 0; i < data.Length; i++)
        {
            data[i] = (byte)((i * 251) + (length * 17));
        }

        return data;
    }

    private static uint ComputeBitwise(ReadOnlySpan<byte> data)
    {
        const uint polynomial = 0x82F63B78;
        var crc = 0xFFFFFFFFu;

        foreach (var b in data)
        {
            crc ^= b;

            for (var i = 0; i < 8; i++)
            {
                crc = (crc & 1) != 0 ? (crc >> 1) ^ polynomial : crc >> 1;
            }
        }

        return crc ^ 0xFFFFFFFFu;
    }
}
