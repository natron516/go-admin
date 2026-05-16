/**
 * Scripture Reference Detector
 * Parses transcript text for Bible scripture references.
 * Supports full names, abbreviations, and spoken forms.
 */

// All 66 books with common abbreviations and spoken variants
const BOOKS = [
  // Old Testament
  { canonical: 'Genesis', patterns: ['genesis', 'gen'] },
  { canonical: 'Exodus', patterns: ['exodus', 'exod', 'ex'] },
  { canonical: 'Leviticus', patterns: ['leviticus', 'lev'] },
  { canonical: 'Numbers', patterns: ['numbers', 'num'] },
  { canonical: 'Deuteronomy', patterns: ['deuteronomy', 'deut'] },
  { canonical: 'Joshua', patterns: ['joshua', 'josh'] },
  { canonical: 'Judges', patterns: ['judges', 'judg'] },
  { canonical: 'Ruth', patterns: ['ruth'] },
  { canonical: '1 Samuel', patterns: ['1 samuel', 'first samuel', '1st samuel', '1 sam', 'i samuel'] },
  { canonical: '2 Samuel', patterns: ['2 samuel', 'second samuel', '2nd samuel', '2 sam', 'ii samuel'] },
  { canonical: '1 Kings', patterns: ['1 kings', 'first kings', '1st kings', '1 kgs', 'i kings'] },
  { canonical: '2 Kings', patterns: ['2 kings', 'second kings', '2nd kings', '2 kgs', 'ii kings'] },
  { canonical: '1 Chronicles', patterns: ['1 chronicles', 'first chronicles', '1st chronicles', '1 chr', '1 chron', 'i chronicles'] },
  { canonical: '2 Chronicles', patterns: ['2 chronicles', 'second chronicles', '2nd chronicles', '2 chr', '2 chron', 'ii chronicles'] },
  { canonical: 'Ezra', patterns: ['ezra'] },
  { canonical: 'Nehemiah', patterns: ['nehemiah', 'neh'] },
  { canonical: 'Esther', patterns: ['esther', 'esth'] },
  { canonical: 'Job', patterns: ['job'] },
  { canonical: 'Psalms', patterns: ['psalms', 'psalm', 'ps', 'psa'] },
  { canonical: 'Proverbs', patterns: ['proverbs', 'prov', 'pro'] },
  { canonical: 'Ecclesiastes', patterns: ['ecclesiastes', 'eccl', 'ecc'] },
  { canonical: 'Song of Solomon', patterns: ['song of solomon', 'song of songs', 'sos', 'song'] },
  { canonical: 'Isaiah', patterns: ['isaiah', 'isa'] },
  { canonical: 'Jeremiah', patterns: ['jeremiah', 'jer'] },
  { canonical: 'Lamentations', patterns: ['lamentations', 'lam'] },
  { canonical: 'Ezekiel', patterns: ['ezekiel', 'ezek', 'eze'] },
  { canonical: 'Daniel', patterns: ['daniel', 'dan'] },
  { canonical: 'Hosea', patterns: ['hosea', 'hos'] },
  { canonical: 'Joel', patterns: ['joel'] },
  { canonical: 'Amos', patterns: ['amos'] },
  { canonical: 'Obadiah', patterns: ['obadiah', 'obad'] },
  { canonical: 'Jonah', patterns: ['jonah'] },
  { canonical: 'Micah', patterns: ['micah', 'mic'] },
  { canonical: 'Nahum', patterns: ['nahum', 'nah'] },
  { canonical: 'Habakkuk', patterns: ['habakkuk', 'hab'] },
  { canonical: 'Zephaniah', patterns: ['zephaniah', 'zeph'] },
  { canonical: 'Haggai', patterns: ['haggai', 'hag'] },
  { canonical: 'Zechariah', patterns: ['zechariah', 'zech'] },
  { canonical: 'Malachi', patterns: ['malachi', 'mal'] },
  // New Testament
  { canonical: 'Matthew', patterns: ['matthew', 'matt', 'mat'] },
  { canonical: 'Mark', patterns: ['mark', 'mk'] },
  { canonical: 'Luke', patterns: ['luke', 'lk'] },
  { canonical: 'John', patterns: ['john', 'jn'] },
  { canonical: 'Acts', patterns: ['acts'] },
  { canonical: 'Romans', patterns: ['romans', 'rom'] },
  { canonical: '1 Corinthians', patterns: ['1 corinthians', 'first corinthians', '1st corinthians', '1 cor', 'i corinthians'] },
  { canonical: '2 Corinthians', patterns: ['2 corinthians', 'second corinthians', '2nd corinthians', '2 cor', 'ii corinthians'] },
  { canonical: 'Galatians', patterns: ['galatians', 'gal'] },
  { canonical: 'Ephesians', patterns: ['ephesians', 'eph'] },
  { canonical: 'Philippians', patterns: ['philippians', 'phil', 'php'] },
  { canonical: 'Colossians', patterns: ['colossians', 'col'] },
  { canonical: '1 Thessalonians', patterns: ['1 thessalonians', 'first thessalonians', '1st thessalonians', '1 thess', '1 thes', 'i thessalonians'] },
  { canonical: '2 Thessalonians', patterns: ['2 thessalonians', 'second thessalonians', '2nd thessalonians', '2 thess', '2 thes', 'ii thessalonians'] },
  { canonical: '1 Timothy', patterns: ['1 timothy', 'first timothy', '1st timothy', '1 tim', 'i timothy'] },
  { canonical: '2 Timothy', patterns: ['2 timothy', 'second timothy', '2nd timothy', '2 tim', 'ii timothy'] },
  { canonical: 'Titus', patterns: ['titus'] },
  { canonical: 'Philemon', patterns: ['philemon', 'phlm'] },
  { canonical: 'Hebrews', patterns: ['hebrews', 'heb'] },
  { canonical: 'James', patterns: ['james', 'jas'] },
  { canonical: '1 Peter', patterns: ['1 peter', 'first peter', '1st peter', '1 pet', 'i peter'] },
  { canonical: '2 Peter', patterns: ['2 peter', 'second peter', '2nd peter', '2 pet', 'ii peter'] },
  { canonical: '1 John', patterns: ['1 john', 'first john', '1st john', 'i john'] },
  { canonical: '2 John', patterns: ['2 john', 'second john', '2nd john', 'ii john'] },
  { canonical: '3 John', patterns: ['3 john', 'third john', '3rd john', 'iii john'] },
  { canonical: 'Jude', patterns: ['jude'] },
  { canonical: 'Revelation', patterns: ['revelation', 'revelations', 'rev'] },
];

// Build a flat lookup: pattern -> canonical name
// Sort by longest pattern first so "first corinthians" matches before "first"
const PATTERN_MAP = [];
for (const book of BOOKS) {
  for (const p of book.patterns) {
    PATTERN_MAP.push({ pattern: p, canonical: book.canonical });
  }
}
PATTERN_MAP.sort((a, b) => b.pattern.length - a.pattern.length);

// Build one big regex alternation for all book patterns
const bookAlternation = PATTERN_MAP.map(p => p.pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');

// Matches patterns like:
//   "John 3:16"
//   "John 3 16"
//   "John chapter 3 verse 16"
//   "John 3:16-18"
//   "John 3 verses 16 through 18"
//   "Psalm 23" (chapter only)
const SCRIPTURE_REGEX = new RegExp(
  `(?:turn to |go to |look at |read |in |from )?` +
  `(${bookAlternation})` +
  `(?:\\s+chapter)?\\s+` +
  `(\\d{1,3})` +
  `(?:` +
    `(?:\\s*[:\\s]\\s*|\\s+verse(?:s)?\\s+)` +
    `(\\d{1,3})` +
    `(?:\\s*[-–—]\\s*|\\s+through\\s+|\\s+to\\s+)?(\\d{1,3})?` +
  `)?`,
  'gi'
);

/**
 * Detect scripture references in a transcript string.
 * Returns array of { book, chapter, verse, endVerse, reference }
 */
function detectScriptures(text) {
  const results = [];
  let match;
  
  // Reset lastIndex for global regex
  SCRIPTURE_REGEX.lastIndex = 0;
  
  while ((match = SCRIPTURE_REGEX.exec(text)) !== null) {
    const rawBook = match[1].toLowerCase().trim();
    const chapter = parseInt(match[2], 10);
    const verse = match[3] ? parseInt(match[3], 10) : null;
    const endVerse = match[4] ? parseInt(match[4], 10) : null;
    
    // Look up canonical name
    const entry = PATTERN_MAP.find(p => p.pattern === rawBook);
    if (!entry) continue;
    
    const book = entry.canonical;
    
    // Build display reference
    let reference = `${book} ${chapter}`;
    if (verse) {
      reference += `:${verse}`;
      if (endVerse) reference += `-${endVerse}`;
    }
    
    results.push({ book, chapter, verse, endVerse, reference });
  }
  
  // Deduplicate (same reference within one transcript chunk)
  const seen = new Set();
  return results.filter(r => {
    if (seen.has(r.reference)) return false;
    seen.add(r.reference);
    return true;
  });
}

module.exports = { detectScriptures, BOOKS };
