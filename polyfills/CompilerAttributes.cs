// Polyfills for compiler-required attributes on target frameworks whose BCL lacks them.
// Each type is internal and #if-gated so it compiles to nothing on frameworks that already
// provide it (net8.0+). Linked into every library project via src/Directory.Build.props.

#if !NET5_0_OR_GREATER
namespace System.Runtime.CompilerServices
{
    using System.ComponentModel;

    /// <summary>Enables C# <c>init</c>-only setters on frameworks earlier than net5.0.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit
    {
    }

    /// <summary>Polyfill for <c>[SkipLocalsInit]</c> (net5.0+).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    [AttributeUsage(
        AttributeTargets.Module | AttributeTargets.Class | AttributeTargets.Struct |
        AttributeTargets.Interface | AttributeTargets.Constructor | AttributeTargets.Method |
        AttributeTargets.Property | AttributeTargets.Event,
        Inherited = false)]
    internal sealed class SkipLocalsInitAttribute : Attribute
    {
    }
}
#endif

#if !NET7_0_OR_GREATER
namespace System.Runtime.CompilerServices
{
    /// <summary>Polyfill enabling the C# <c>required</c> modifier (net7.0+).</summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false)]
    internal sealed class RequiredMemberAttribute : Attribute
    {
    }

    /// <summary>Polyfill required by the compiler for <c>required</c> members (net7.0+).</summary>
    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName)
        {
            FeatureName = featureName;
        }

        public string FeatureName { get; }

        public bool IsOptional { get; set; }

        public const string RefStructs = nameof(RefStructs);
        public const string RequiredMembers = nameof(RequiredMembers);
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>Polyfill for <c>[SetsRequiredMembers]</c> (net7.0+).</summary>
    [AttributeUsage(AttributeTargets.Constructor, Inherited = false)]
    internal sealed class SetsRequiredMembersAttribute : Attribute
    {
    }
}
#endif
