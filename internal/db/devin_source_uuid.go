package db

import (
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

// legacyDevinScopedSourceUUID returns the session-scoped form of a bare
// Devin source uuid stored by a pre-110 archive. Bare Devin node/step ids
// are only unique per session; the scoped form is what the parser now emits
// (data version 110): "devin:<raw>" session ids scope uuids as
// "<raw>:<id>". Remote sessions carry a host prefix ("host~devin:<raw>")
// that is stripped before matching, and the scoped form keeps the raw id —
// host prefixes are an archive namespace, not part of the source identity.
// The second result is false when the session is not a Devin session, the
// uuid is empty, or the uuid is already scoped.
func legacyDevinScopedSourceUUID(
	sessionID, uuid string,
) (string, bool) {
	_, rawID := parser.StripHostPrefix(sessionID)
	if !strings.HasPrefix(rawID, "devin:") ||
		uuid == "" ||
		strings.Contains(uuid, ":") {
		return "", false
	}
	return strings.TrimPrefix(rawID, "devin:") + ":" + uuid, true
}

// devinLegacyUUIDPredicateSQL selects rows still carrying a bare legacy
// Devin uuid: the session is Devin — local "devin:<raw>" or remote
// "host~devin:<raw>" — and the uuid column is non-empty and not yet
// session-scoped (scoped values contain ':').
func devinLegacyUUIDPredicateSQL(uuidCol, sessionCol string) string {
	return `(` + sessionCol + ` LIKE 'devin:%' OR ` + sessionCol +
		` LIKE '%~devin:%') AND ` + uuidCol + ` != '' AND instr(` +
		uuidCol + `, ':') = 0`
}

// devinScopedSourceUUIDSQL translates a stored uuid column to the
// session-scoped form the parser emits for Devin sessions, leaving every
// other value untouched. Host names cannot contain '~' or ':', so
// instr(<sessionCol>, 'devin:') unambiguously locates the prefix after an
// optional 'host~' host prefix.
func devinScopedSourceUUIDSQL(uuidCol, sessionCol string) string {
	return `CASE WHEN ` +
		devinLegacyUUIDPredicateSQL(uuidCol, sessionCol) +
		` THEN substr(` + sessionCol + `, instr(` + sessionCol +
		`, 'devin:')+6) || ':' || ` + uuidCol +
		` ELSE ` + uuidCol + ` END`
}
