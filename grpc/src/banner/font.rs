//! A minimal FIGlet font reader, matching `go-figure`'s behaviour rather than the FIGlet spec.
//!
//! The difference matters. A conformant FIGlet renderer *smushes* adjacent glyphs together
//! according to the font's layout rules; `go-figure` does not — it concatenates each glyph's
//! row verbatim. Rendering the same name through a spec-conformant crate produces noticeably
//! tighter art, so matching the Go runtime's banner means matching its renderer, quirks and
//! all.

/// The characters a glyph's final line can be terminated by.
const DELIMITERS: [char; 3] = ['@', '#', '$'];

/// A hardblank of `a` or `2` is a misparsed header rather than a real one, so it reads as a
/// space — the same guard `go-figure` applies.
const HARDBLANK_BLACKLIST: [char; 2] = ['a', '2'];

/// The first character a font defines. Everything is indexed from here.
const FIRST_ASCII: char = ' ';
/// The last character a font defines.
const LAST_ASCII: char = '~';

/// A parsed FIGlet font: the glyphs, and the measurements needed to lay them out.
pub struct Font {
    height: usize,
    baseline: usize,
    hardblank: char,
    /// One entry per character from [`FIRST_ASCII`], each holding [`Font::height`] rows.
    glyphs: Vec<Vec<String>>,
}

impl Font {
    /// Parses a `.flf` font.
    ///
    /// Returns `None` when the content carries no `flf2` header, which is the only failure
    /// worth distinguishing: everything after it is either present or renders blank.
    pub fn parse(content: &str) -> Option<Self> {
        let mut lines = content.lines();
        let header = lines.by_ref().find(|line| line.starts_with("flf2"))?;

        let fields: Vec<&str> = header.split_whitespace().collect();
        let hardblank = match fields.first()?.chars().last()? {
            c if HARDBLANK_BLACKLIST.contains(&c) => ' ',
            c => c,
        };
        let height: usize = fields.get(1)?.parse().ok()?;
        let baseline: usize = fields.get(2)?.parse().ok()?;

        // The space glyph is not read from the font. go-figure hardcodes it two columns wide,
        // and speed.flf's own space is a seven-column diagonal of hardblanks — so honouring
        // the font here would space every banner out four times as far as the Go one.
        let mut glyphs: Vec<Vec<String>> = vec![vec!["  ".to_string(); height]];

        // Comment lines are skipped by the same rule that ends a glyph: nothing before the
        // first terminator is collected, because `index` is still zero.
        let mut index = 0usize;
        for line in lines {
            let terminates = ends_glyph(line, height);
            let cut = if terminates && height > 1 { 2 } else { 1 };

            if terminates {
                glyphs.push(Vec::new());
            }
            if index > 0 {
                let row: String = if line.chars().count() > 1 {
                    let keep = line.chars().count().saturating_sub(cut);
                    line.chars().take(keep).collect()
                } else {
                    String::new()
                };
                glyphs[index].push(row);
            }
            if terminates {
                index += 1;
            }
        }

        Some(Self {
            height,
            baseline,
            hardblank,
            glyphs,
        })
    }

    /// Renders `text` as rows of ASCII art.
    ///
    /// Rows below the baseline that came out entirely blank are dropped, which is how a name
    /// with no descender prints five rows where one with a `y` prints six.
    pub fn render(&self, text: &str) -> Vec<String> {
        let mut rows = Vec::with_capacity(self.height);

        for r in 0..self.height {
            let mut row = String::new();
            for ch in text.chars() {
                // A character the font has no glyph for is drawn as a question mark, which is
                // visible in the banner rather than silently missing from it.
                let ch = if (FIRST_ASCII..=LAST_ASCII).contains(&ch) {
                    ch
                } else {
                    '?'
                };
                let index = ch as usize - FIRST_ASCII as usize;
                if let Some(glyph) = self.glyphs.get(index)
                    && let Some(glyph_row) = glyph.get(r)
                {
                    row.push_str(&glyph_row.replace(self.hardblank, " "));
                }
            }

            if r < self.baseline || !row.trim().is_empty() {
                rows.push(row.trim_end_matches(' ').to_string());
            }
        }

        rows
    }
}

/// Whether `line` is a glyph's final row.
///
/// A glyph ends with its terminator doubled — `@@` in most fonts — except in a
/// single-row font, where one is all there is room for.
fn ends_glyph(line: &str, height: usize) -> bool {
    let want = if height == 1 && !line.is_empty() {
        1
    } else {
        2
    };
    let tail: Vec<char> = line.chars().rev().take(want).collect();
    tail.len() == want && DELIMITERS.contains(&tail[0]) && tail.iter().all(|c| *c == tail[0])
}
