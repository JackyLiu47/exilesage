# ExileSage Changelog

_Player advisor for Path of Exile 2 — progress log, written for humans._

All notable changes to ExileSage are documented here. This changelog reflects milestone-level shipped work, not every individual commit. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

---

## [Stage 2 — Phase 2D: Freshness Tracking] — 2026-04-21

*Commit 85efb41*

The advisor now knows when its own data may be out of date — and tells the user.

Previously, the advisor's answers came from a community-maintained export of the game's files, and that export inevitably lags behind live patches. The advisor had no way to know it was running on stale data, so it would confidently answer with outdated information. That's now fixed on three fronts.

First, every downloaded file is fingerprinted, so the app can detect any change to remote data without having to re-download gigabytes. Second, the advisor automatically polls Grinding Gear Games' own official patch-notes feed — if a new Path of Exile 2 patch has shipped since the last data refresh, the advisor flags its own data as potentially stale. Third, the patch version the advisor's data reflects is now included in every answer's context, and a "data may be stale" warning is automatically added when the advisor is running on out-of-date info.

A new CLI command, `exilesage update --check`, lets players verify freshness before asking anything. It returns standard exit codes so it can be scripted into a pre-query routine.

**Why it matters:** a confident wrong answer is worse than "I don't know." Players need to calibrate their trust — "this advisor is on patch 0.2.0a, current patch is 0.2.0e, so treat skill numbers as approximate." That calibration is now automatic and always visible.

---

## [Stage 2 — Phase 0: Edge-Case Hardening] — 2026-04-21

*Commit 50bb33c*

A second reliability pass — an independent audit catching what the first pass missed.

After the first reliability pass shipped, a fresh reviewer was invited to try to break it. They found real bugs across eight classes of edge case. All were fixed before this commit: accented letters from wiki copy-paste now match stored data correctly, malicious or oversized search input can't lock the app, pressing Ctrl-C during a data refresh now cleanly rolls back instead of leaving a half-written state, and the app now refuses to silently open databases from future versions of itself that it can't safely handle.

**Why it matters:** the first pass was solid; the second pass proved it. Bugs that would only bite real users in the wild are now covered by automated checks, so the same class of problem can't silently come back.

---

## [Stage 2 — Phase 0: Reliability Hardening] — 2026-04-21

*Commit d752f46*

The foundation: the app no longer breaks on ordinary user input or mid-refresh interruptions.

Before this work, several classes of real-world input could crash or confuse the advisor. Searches with apostrophes (`"Winter's Blast"`) or accented characters could crash the search engine. A mid-refresh error could leave the local database in a half-written state the app couldn't recover from. Opening a database from an older version might silently produce wrong answers.

All of these are now fixed. Data refreshes are atomic — they either fully succeed or fully roll back, with existing data untouched if anything goes wrong. Future data relationships (e.g. gems granting skills) will update in the correct order automatically, without producing broken references. Version upgrades are detected and handled without corrupting older databases.

**Why it matters:** reliability is table-stakes. A crash on an apostrophe isn't a bug — it's the end of the session. This phase turned the prototype into something a player can actually depend on.

---

## [Stage 1 — Foundation] — 2026-04-09

*Pre-changelog; retrospectively recorded*

The initial version of ExileSage shipped with a working advisor, a command-line interface (`exilesage ask`), and a structured knowledge base covering 18,645 rows across mods, base items, currencies, and augments — all searchable. The advisor can already answer factual questions about crafting modifiers, item bases, and currencies with citations from real in-game names. What was missing at the end of Stage 1: skills and gems (Fireball, support gems), ascendancy classes, and unique items. Addressing those gaps is the work of Stage 2.

---

_This log is maintained alongside the repo. Each entry corresponds to a shipped commit on `main`. Upcoming work is tracked separately in the project's CLAUDE.md stage tracker._
