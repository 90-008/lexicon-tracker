export const createRegexFilter = (pattern: string): RegExp | null => {
  if (!pattern) return null;

  try {
    // Check if pattern contains regex metacharacters
    const hasRegexChars = /[.*+?^${}()|[\]\\]/.test(pattern);

    if (hasRegexChars) {
      // Use as regex with case-insensitive flag
      return new RegExp(pattern, "i");
    } else {
      // Smart case: case-insensitive unless pattern has uppercase
      const hasUppercase = /[A-Z]/.test(pattern);
      const flags = hasUppercase ? "" : "i";
      // Escape the pattern for literal matching
      const escapedPattern = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return new RegExp(escapedPattern, flags);
    }
  } catch (e) {
    // Invalid regex, return null
    return null;
  }
};
