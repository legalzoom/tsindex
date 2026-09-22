use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use ignore::WalkBuilder;

use crate::lang::{
    detect_language_from_file, known_language_ids, languages_for_manifest, shebang_matches,
};
use crate::model::LanguageBreakdown;

const MANIFEST_WEIGHT: f64 = 100.0;
const EXTENSION_WEIGHT: f64 = 1.0;
const SHEBANG_WEIGHT: f64 = 20.0;

pub fn detect_languages(root: &Path) -> Result<Vec<LanguageBreakdown>> {
    let mut votes: HashMap<String, f64> = HashMap::new();
    let mut file_counts: HashMap<String, usize> = HashMap::new();

    for language in known_language_ids() {
        votes.entry(language.to_string()).or_insert(0.0);
    }

    let mut builder = WalkBuilder::new(root);
    builder.hidden(false);
    builder.git_ignore(true);
    builder.git_exclude(true);
    builder.git_global(true);
    builder.add_custom_ignore_filename(".tsindexignore");
    for result in builder.build() {
        let entry = match result {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(language) = detect_language_from_file(root, path)? {
            *votes.entry(language.to_string()).or_default() += EXTENSION_WEIGHT;
            *file_counts.entry(language.to_string()).or_default() += 1;
            if shebang_matches(path, language) {
                *votes.entry(language.to_string()).or_default() += SHEBANG_WEIGHT;
            }
        }
    }

    for manifest in discover_manifests(root)? {
        let languages = languages_for_manifest(&manifest);
        let matching_sources = languages
            .iter()
            .copied()
            .filter(|language| file_counts.get(*language).copied().unwrap_or_default() > 0)
            .collect::<Vec<_>>();
        let languages = if languages.len() > 1 && !matching_sources.is_empty() {
            matching_sources
        } else {
            languages
        };
        for language in languages {
            *votes.entry(language.to_string()).or_default() += MANIFEST_WEIGHT;
        }
    }

    let total_votes: f64 = votes.values().sum();
    let mut breakdown = votes
        .into_iter()
        .filter_map(|(language, score)| {
            let files = file_counts.get(&language).copied().unwrap_or_default();
            if score <= 0.0 && files == 0 {
                return None;
            }
            let confidence = if total_votes == 0.0 {
                0.0
            } else {
                score / total_votes
            };
            Some(LanguageBreakdown {
                language,
                files,
                confidence,
            })
        })
        .collect::<Vec<_>>();
    breakdown.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.files.cmp(&a.files))
            .then_with(|| a.language.cmp(&b.language))
    });
    Ok(breakdown)
}

fn discover_manifests(root: &Path) -> Result<Vec<String>> {
    let mut manifests = Vec::new();
    let mut builder = WalkBuilder::new(root);
    builder.hidden(false);
    builder.max_depth(Some(3));
    builder.git_ignore(true);
    builder.git_exclude(true);
    builder.git_global(true);
    builder.add_custom_ignore_filename(".tsindexignore");
    for result in builder.build() {
        let entry = match result {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|value| value.to_str())
            && !languages_for_manifest(name).is_empty()
        {
            manifests.push(name.to_string());
        }
    }
    Ok(manifests)
}
